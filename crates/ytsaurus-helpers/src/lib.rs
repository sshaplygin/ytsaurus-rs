//! Derive macros for [`ytsaurus-client`](https://docs.rs/ytsaurus-client).
//!
//! A job's rows are already described by a Rust struct. A table schema
//! describes the same thing to the cluster, so writing it out again by hand is
//! a chance to disagree with yourself. [`macro@TableRow`] reads the struct.
//!
//! ```ignore
//! use ytsaurus_client::TableRow;
//!
//! #[derive(TableRow)]
//! struct Visit<'a> {
//!     #[yt(key)]
//!     host: &'a str,               // utf8, required, sorted
//!     size: i64,                   // int64, required
//!     referrer: Option<&'a str>,   // utf8, optional — the Rust type says so
//! }
//!
//! client.create_table("//tmp/visits", &Visit::table_schema())?;
//! ```
//!
//! Nothing here runs on a cluster: a procedural-macro crate is loaded by the
//! compiler and contributes nothing to the binary a worker ships.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields, GenericArgument, PathArguments, Type, parse_macro_input};

/// Derives a YTsaurus table schema from a struct's fields.
///
/// One column per field, in declaration order, named after the field. The
/// Rust type decides the column type, and `Option<T>` is the *only* thing that
/// makes a column optional — a bare `T` is required.
///
/// | Rust | column type |
/// | --- | --- |
/// | `i8` `i16` `i32` `i64` | `int8` … `int64` |
/// | `u8` `u16` `u32` `u64` | `uint8` … `uint64` |
/// | `f32` / `f64` | `float` / `double` |
/// | `bool` | `boolean` |
/// | `String`, `&str`, `Cow<str>` | `utf8` |
/// | `Vec<u8>`, `&[u8]` | `string` — YTsaurus strings are arbitrary bytes |
/// | `YsonValue` | `any` |
/// | `Option<T>` | `T`, not required |
///
/// Anything else is a compile error naming the field: guessing a column type
/// from an unknown Rust type is how a schema comes to disagree with the data.
/// Say what you mean with `#[yt(column_type = "…")]`.
///
/// # Attributes
///
/// On the struct:
///
/// - `#[yt(non_strict)]` — let rows carry columns the schema does not mention.
/// - `#[yt(unique_keys)]` — promise no two rows share a key. Requires a key.
/// - `#[yt(crate_path = "::path::to::ytsaurus_client")]` — for a renamed
///   dependency.
///
/// On a field:
///
/// - `#[yt(key)]` — a key column, sorted ascending. Key fields must come first:
///   the cluster refuses a schema whose keys are not a prefix, so the macro
///   refuses it earlier and names the field.
/// - `#[yt(name = "…")]` — the column name, when it differs from the field's.
/// - `#[yt(column_type = "…")]` — the column type, by its wire name.
/// - `#[yt(skip)]` — not a column at all.
///
/// # Example
///
/// ```ignore
/// #[derive(TableRow)]
/// #[yt(unique_keys)]
/// struct Session {
///     #[yt(key)]
///     user_id: i64,
///     #[yt(key)]
///     started_at: i64,
///     #[yt(name = "duration_s")]
///     duration: i64,
///     #[yt(column_type = "any")]
///     details: ytsaurus_client::yson_build::YsonValue,
///     #[yt(skip)]
///     cached: usize,
/// }
/// ```
#[proc_macro_derive(TableRow, attributes(yt))]
pub fn derive_table_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// What a `#[yt(...)]` on the struct can say.
struct StructOptions {
    strict: bool,
    unique_keys: bool,
    crate_path: syn::Path,
}

/// What a `#[yt(...)]` on a field can say.
#[derive(Default)]
struct FieldOptions {
    key: bool,
    skip: bool,
    name: Option<String>,
    column_type: Option<(String, proc_macro2::Span)>,
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let options = struct_options(input)?;
    let fields = named_fields(input)?;

    let mut columns = Vec::new();
    let mut names: Vec<(String, proc_macro2::Span)> = Vec::new();
    let mut keys_ended = false;
    let mut any_key = false;

    for field in fields {
        let ident = field.ident.as_ref().expect("named fields only");
        let span = ident.span();
        let field_options = field_options(field)?;

        if field_options.skip {
            continue;
        }

        let name = field_options.name.unwrap_or_else(|| ident.to_string());
        if let Some((first, _)) = names.iter().find(|(seen, _)| *seen == name) {
            return Err(syn::Error::new(
                span,
                format!(
                    "two columns would be named {first:?}; \
                     a table cannot have duplicate column names"
                ),
            ));
        }
        names.push((name.clone(), span));

        // The cluster: "Key columns must form a prefix of schema". Catching it
        // here names the field instead of returning error 314 from a create.
        if field_options.key {
            if keys_ended {
                return Err(syn::Error::new(
                    span,
                    "key columns must be the first fields of the struct; \
                     move this field up, or drop #[yt(key)]",
                ));
            }
            any_key = true;
        } else {
            keys_ended = true;
        }

        let (column_type, optional) = match &field_options.column_type {
            // An explicit column type still lets `Option<T>` say the column is
            // optional: the attribute names the type, not the optionality.
            Some((name, span)) => (
                named_column_type(name, *span)?,
                option_inner(&field.ty).is_some(),
            ),
            None => {
                column_type_of(&field.ty).ok_or_else(|| unsupported_type(&field.ty, span, &name))?
            }
        };

        let krate = &options.crate_path;
        let variant = format_ident!("{}", column_type);
        // Three types can never be required — the cluster answers `Column of
        // type "any" cannot be "required"` and `Null type cannot be required`.
        // Each already means "there may be nothing here".
        let required = if optional || NEVER_REQUIRED.contains(&column_type) {
            quote!()
        } else {
            quote!(.required())
        };
        let key = if field_options.key {
            quote!(.key())
        } else {
            quote!()
        };

        columns.push(quote! {
            #krate::Column::new(#name, #krate::ColumnType::#variant) #required #key
        });
    }

    if options.unique_keys && !any_key {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "unique_keys promises no two rows share a key, but no field is #[yt(key)]",
        ));
    }

    let krate = &options.crate_path;
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let non_strict = if options.strict {
        quote!()
    } else {
        quote!(.non_strict())
    };
    let unique_keys = if options.unique_keys {
        quote!(.with_unique_keys(true))
    } else {
        quote!()
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::TableRow for #name #ty_generics #where_clause {
            fn table_schema() -> #krate::TableSchema {
                #krate::TableSchema::new([ #(#columns),* ]) #non_strict #unique_keys
            }
        }
    })
}

/// The named fields of a struct, or a useful error.
fn named_fields(input: &DeriveInput) -> syn::Result<impl Iterator<Item = &syn::Field>> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "TableRow describes a table's columns, so it can only be derived for a struct",
        ));
    };

    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "a table's columns have names, so TableRow needs a struct with named fields",
        ));
    };

    Ok(named.named.iter())
}

fn struct_options(input: &DeriveInput) -> syn::Result<StructOptions> {
    let mut options = StructOptions {
        strict: true,
        unique_keys: false,
        crate_path: syn::parse_quote!(::ytsaurus_client),
    };

    for attr in input.attrs.iter().filter(|a| a.path().is_ident("yt")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("non_strict") {
                options.strict = false;
            } else if meta.path.is_ident("unique_keys") {
                options.unique_keys = true;
            } else if meta.path.is_ident("crate_path") {
                let value: syn::LitStr = meta.value()?.parse()?;
                options.crate_path = value.parse()?;
            } else {
                return Err(meta.error(
                    "unknown option; the struct takes non_strict, unique_keys and crate_path",
                ));
            }
            Ok(())
        })?;
    }

    Ok(options)
}

fn field_options(field: &syn::Field) -> syn::Result<FieldOptions> {
    let mut options = FieldOptions::default();

    for attr in field.attrs.iter().filter(|a| a.path().is_ident("yt")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("key") {
                options.key = true;
            } else if meta.path.is_ident("skip") {
                options.skip = true;
            } else if meta.path.is_ident("name") {
                let value: syn::LitStr = meta.value()?.parse()?;
                options.name = Some(value.value());
            } else if meta.path.is_ident("column_type") {
                let value: syn::LitStr = meta.value()?.parse()?;
                options.column_type = Some((value.value(), value.span()));
            } else {
                return Err(
                    meta.error("unknown option; a field takes key, skip, name and column_type")
                );
            }
            Ok(())
        })?;
    }

    Ok(options)
}

/// Every column type the derive knows, by wire name and `ColumnType` variant.
///
/// The wire names are the `type` spelling. `bool` and `yson` — the `type_v3`
/// spellings of `boolean` and `any` — are accepted here so that a user who
/// reaches for the other vocabulary is not stopped by it.
const NEVER_REQUIRED: &[&str] = &["Any", "Null", "Void"];

const TYPE_NAMES: &[(&str, &str)] = &[
    ("int8", "Int8"),
    ("int16", "Int16"),
    ("int32", "Int32"),
    ("int64", "Int64"),
    ("uint8", "Uint8"),
    ("uint16", "Uint16"),
    ("uint32", "Uint32"),
    ("uint64", "Uint64"),
    ("float", "Float"),
    ("double", "Double"),
    ("boolean", "Boolean"),
    ("bool", "Boolean"),
    ("string", "String"),
    ("utf8", "Utf8"),
    ("any", "Any"),
    ("yson", "Any"),
    ("date", "Date"),
    ("datetime", "Datetime"),
    ("timestamp", "Timestamp"),
    ("interval", "Interval"),
    ("date32", "Date32"),
    ("datetime64", "Datetime64"),
    ("timestamp64", "Timestamp64"),
    ("interval64", "Interval64"),
    ("json", "Json"),
    ("uuid", "Uuid"),
    ("void", "Void"),
    ("null", "Null"),
];

fn named_column_type(name: &str, span: proc_macro2::Span) -> syn::Result<&'static str> {
    TYPE_NAMES
        .iter()
        .find(|(wire, _)| *wire == name)
        .map(|(_, variant)| *variant)
        .ok_or_else(|| {
            let known: Vec<&str> = TYPE_NAMES.iter().map(|(wire, _)| *wire).collect();
            syn::Error::new(
                span,
                format!(
                    "{name:?} is not a column type; try one of {}",
                    known.join(", ")
                ),
            )
        })
}

/// Maps a Rust type to a `ColumnType` variant, and says whether it is optional.
///
/// Syntactic by necessity — a derive macro sees tokens, not types — so
/// `Vec<u8>` is recognised by its spelling. A type alias for it is not, which
/// is what `#[yt(column_type = "…")]` is for.
fn column_type_of(ty: &Type) -> Option<(&'static str, bool)> {
    if let Some(inner) = option_inner(ty) {
        // Option<Option<T>> has no meaning in a schema: a column is either
        // there or it is not.
        if option_inner(inner).is_some() {
            return None;
        }
        return Some((simple_column_type(inner)?, true));
    }
    Some((simple_column_type(ty)?, false))
}

fn simple_column_type(ty: &Type) -> Option<&'static str> {
    match ty {
        // `&str`, `&[u8]`, `&&str` — the reference is not part of the type as
        // far as a column is concerned.
        Type::Reference(reference) => simple_column_type(&reference.elem),
        // `[u8]` and `[u8; N]`.
        Type::Slice(slice) => is_u8(&slice.elem).then_some("String"),
        Type::Array(array) => is_u8(&array.elem).then_some("String"),
        Type::Path(path) => {
            let segment = path.path.segments.last()?;
            let name = segment.ident.to_string();

            match name.as_str() {
                "i8" => Some("Int8"),
                "i16" => Some("Int16"),
                "i32" => Some("Int32"),
                "i64" => Some("Int64"),
                "u8" => Some("Uint8"),
                "u16" => Some("Uint16"),
                "u32" => Some("Uint32"),
                "u64" => Some("Uint64"),
                "f32" => Some("Float"),
                "f64" => Some("Double"),
                "bool" => Some("Boolean"),
                // A Rust `String` is UTF-8 by construction, so it maps to the
                // column type that says so. Bytes that are not text are
                // `Vec<u8>`, which maps to `string`.
                "String" | "str" => Some("Utf8"),
                "YsonValue" => Some("Any"),
                "Vec" => generic_argument(segment)
                    .filter(|arg| is_u8(arg))
                    .map(|_| "String"),
                "Cow" => {
                    // Cow<'a, str> and Cow<'a, [u8]>: the lifetime comes first.
                    let PathArguments::AngleBracketed(args) = &segment.arguments else {
                        return None;
                    };
                    args.args
                        .iter()
                        .find_map(|arg| match arg {
                            GenericArgument::Type(ty) => Some(ty),
                            _ => None,
                        })
                        .and_then(simple_column_type)
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    (segment.ident == "Option").then(|| generic_argument(segment))?
}

fn generic_argument(segment: &syn::PathSegment) -> Option<&Type> {
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn is_u8(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.path.is_ident("u8"))
}

fn unsupported_type(ty: &Type, span: proc_macro2::Span, column: &str) -> syn::Error {
    let rendered = quote!(#ty).to_string();
    let hint = if option_inner(ty).and_then(option_inner).is_some() {
        "Option<Option<T>> has no meaning in a schema: a column is either there or it is not"
    } else {
        "supported types are the integers, f32/f64, bool, String/&str, Vec<u8>/&[u8] and \
         YsonValue, each optionally wrapped in Option; \
         for anything else say what you mean with #[yt(column_type = \"…\")], \
         or drop the field with #[yt(skip)]"
    };

    syn::Error::new(
        span,
        format!("cannot infer a column type for {column:?} from `{rendered}`: {hint}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_str;

    fn infer(rust: &str) -> Option<(&'static str, bool)> {
        column_type_of(&parse_str::<Type>(rust).expect("a type"))
    }

    #[test]
    fn integers_map_to_their_own_width() {
        assert_eq!(infer("i8"), Some(("Int8", false)));
        assert_eq!(infer("i16"), Some(("Int16", false)));
        assert_eq!(infer("i32"), Some(("Int32", false)));
        assert_eq!(infer("i64"), Some(("Int64", false)));
        assert_eq!(infer("u8"), Some(("Uint8", false)));
        assert_eq!(infer("u64"), Some(("Uint64", false)));
    }

    #[test]
    fn floats_keep_their_precision() {
        assert_eq!(infer("f32"), Some(("Float", false)));
        assert_eq!(infer("f64"), Some(("Double", false)));
    }

    /// The distinction the whole codec is careful about: a Rust `String` is
    /// text, a `Vec<u8>` is bytes, and YTsaurus has a different column type
    /// for each.
    #[test]
    fn text_and_bytes_do_not_collapse_into_one_type() {
        assert_eq!(infer("String"), Some(("Utf8", false)));
        assert_eq!(infer("&str"), Some(("Utf8", false)));
        assert_eq!(infer("&'a str"), Some(("Utf8", false)));
        assert_eq!(infer("Cow<'a, str>"), Some(("Utf8", false)));

        assert_eq!(infer("Vec<u8>"), Some(("String", false)));
        assert_eq!(infer("&[u8]"), Some(("String", false)));
        assert_eq!(infer("&'a [u8]"), Some(("String", false)));
        assert_eq!(infer("[u8; 16]"), Some(("String", false)));
        assert_eq!(infer("Cow<'a, [u8]>"), Some(("String", false)));
    }

    #[test]
    fn option_is_the_one_source_of_optionality() {
        assert_eq!(infer("Option<i64>"), Some(("Int64", true)));
        assert_eq!(infer("Option<&'a str>"), Some(("Utf8", true)));
        assert_eq!(infer("Option<Vec<u8>>"), Some(("String", true)));
        assert_eq!(infer("bool"), Some(("Boolean", false)));
    }

    #[test]
    fn a_doubly_optional_column_is_refused() {
        // A column is present or it is not; there is no second layer for the
        // schema to describe.
        assert_eq!(infer("Option<Option<i64>>"), None);
    }

    #[test]
    fn a_qualified_path_is_still_recognised() {
        assert_eq!(infer("std::string::String"), Some(("Utf8", false)));
        assert_eq!(infer("ytsaurus_yson::YsonValue"), Some(("Any", false)));
        assert_eq!(infer("core::option::Option<i64>"), Some(("Int64", true)));
    }

    #[test]
    fn a_type_with_no_column_shape_is_refused() {
        // Each of these would have to be guessed at, and a guess in a schema
        // is a lie the cluster enforces on every write.
        assert_eq!(infer("Vec<i64>"), None);
        assert_eq!(infer("HashMap<String, i64>"), None);
        assert_eq!(infer("MyStruct"), None);
        assert_eq!(infer("(i64, i64)"), None);
        assert_eq!(infer("Vec<Vec<u8>>"), None);
    }

    #[test]
    fn every_wire_name_names_a_variant() {
        for (wire, variant) in TYPE_NAMES {
            assert_eq!(
                named_column_type(wire, proc_macro2::Span::call_site()).unwrap(),
                *variant
            );
        }
    }

    #[test]
    fn an_unknown_wire_name_lists_the_known_ones() {
        let err =
            named_column_type("int128", proc_macro2::Span::call_site()).expect_err("must refuse");
        let message = err.to_string();
        assert!(message.contains("int128"), "{message}");
        assert!(message.contains("int64"), "{message}");
    }
}
