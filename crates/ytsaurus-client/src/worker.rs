//! Can this binary run on a cluster node?
//!
//! [`Client::upload_current_exe`](crate::Client::upload_current_exe) uploads
//! the running executable, and the running executable is often not something a
//! node can exec: a launcher built on macOS is Mach-O, and one built on a
//! developer's Linux is dynamically linked against a libc the node does not
//! have. Both fail on the node, minutes later, with an error that names neither
//! cause.
//!
//! So the header is read before the upload. This is a few bytes of ELF, not a
//! parser: enough to tell "Linux x86-64, no interpreter" from everything else.
//! Reference: the ELF-64 header, `e_ident` / `e_machine` / the program header
//! table.

/// Little-endian 64-bit ELF for x86-64, with no interpreter — or why not.
pub(crate) fn check_worker_binary(bytes: &[u8]) -> Result<(), String> {
    /// Offsets and constants from the ELF-64 header.
    const HEADER_LEN: usize = 64;
    const MAGIC: &[u8] = b"\x7fELF";
    const CLASS_64: u8 = 2;
    const DATA_LITTLE_ENDIAN: u8 = 1;
    const MACHINE_X86_64: u16 = 0x3E;
    const PT_INTERP: u32 = 3;

    if bytes.len() < HEADER_LEN || &bytes[..4] != MAGIC {
        return Err(
            "it is not an ELF binary, so a Linux node cannot exec it. Build the worker \
             with scripts/build-worker.sh (cargo build --target x86_64-unknown-linux-musl) \
             and upload that with upload_worker"
                .to_owned(),
        );
    }

    if bytes[4] != CLASS_64 || bytes[5] != DATA_LITTLE_ENDIAN {
        return Err("it is not a little-endian 64-bit ELF binary".to_owned());
    }

    let machine = u16::from_le_bytes([bytes[18], bytes[19]]);
    if machine != MACHINE_X86_64 {
        return Err(format!(
            "it is built for ELF machine {machine:#x}, not x86-64 ({MACHINE_X86_64:#x}); \
             YTsaurus nodes are x86-64"
        ));
    }

    if has_segment(bytes, PT_INTERP) {
        return Err(
            "it is dynamically linked: it names an interpreter, and a node has neither \
             your loader nor your libc. Build it static — the musl target does this by \
             default"
                .to_owned(),
        );
    }

    Ok(())
}

/// Whether the program header table contains a segment of this type.
///
/// Returns `false` rather than an error on a table that does not fit the file:
/// this is a sanity check on the way to an upload, not a linter, and refusing a
/// binary over a header it never reaches would be worse than letting the node
/// judge it.
fn has_segment(bytes: &[u8], segment_type: u32) -> bool {
    let offset = usize::try_from(u64::from_le_bytes(
        bytes[0x20..0x28].try_into().expect("64 bytes were checked"),
    ))
    .unwrap_or(usize::MAX);
    let entry_size = usize::from(u16::from_le_bytes([bytes[0x36], bytes[0x37]]));
    let count = usize::from(u16::from_le_bytes([bytes[0x38], bytes[0x39]]));

    if entry_size < 4 {
        return false;
    }

    (0..count).any(|i| {
        // Every step is checked: `e_phoff` and `e_phnum` come off disk and may
        // say anything at all, and overflow here would be a panic in the
        // launcher over a file it was only inspecting.
        let Some(start) = i
            .checked_mul(entry_size)
            .and_then(|at| offset.checked_add(at))
        else {
            return false;
        };
        let Some(end) = start.checked_add(4) else {
            return false;
        };

        bytes.get(start..end).is_some_and(|field| {
            u32::from_le_bytes(field.try_into().expect("four bytes")) == segment_type
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal ELF-64 header: x86-64, little-endian, one program header of
    /// `segment_type` placed straight after the header.
    fn elf(machine: u16, segments: &[u32]) -> Vec<u8> {
        const PH_SIZE: usize = 56;
        let mut out = vec![0_u8; 64];

        out[..4].copy_from_slice(b"\x7fELF");
        out[4] = 2; // ELF64
        out[5] = 1; // little endian
        out[18..20].copy_from_slice(&machine.to_le_bytes());
        out[0x20..0x28].copy_from_slice(&64_u64.to_le_bytes()); // e_phoff
        out[0x36..0x38].copy_from_slice(&(PH_SIZE as u16).to_le_bytes()); // e_phentsize
        out[0x38..0x3A].copy_from_slice(&(segments.len() as u16).to_le_bytes()); // e_phnum

        for segment in segments {
            let mut header = vec![0_u8; PH_SIZE];
            header[..4].copy_from_slice(&segment.to_le_bytes());
            out.extend_from_slice(&header);
        }
        out
    }

    #[test]
    fn a_static_linux_x86_64_binary_is_accepted() {
        // PT_LOAD (1) and PT_GNU_STACK (0x6474e551), no PT_INTERP.
        assert_eq!(
            check_worker_binary(&elf(0x3E, &[1, 1, 0x6474_e551])),
            Ok(())
        );
    }

    #[test]
    fn a_dynamically_linked_binary_is_refused() {
        let err = check_worker_binary(&elf(0x3E, &[1, 3, 1])).expect_err("PT_INTERP must fail");
        assert!(err.contains("dynamically linked"), "{err}");
    }

    #[test]
    fn a_mach_o_binary_is_refused() {
        // What a launcher built on macOS looks like: the Mach-O magic.
        let mut mach_o = vec![0_u8; 128];
        mach_o[..4].copy_from_slice(&0xFEED_FACF_u32.to_le_bytes());

        let err = check_worker_binary(&mach_o).expect_err("Mach-O must fail");
        assert!(err.contains("not an ELF binary"), "{err}");
        assert!(
            err.contains("build-worker.sh"),
            "the error must say what to do"
        );
    }

    #[test]
    fn an_aarch64_binary_is_refused() {
        // A Linux ELF built on an arm64 machine: right format, wrong machine.
        let err = check_worker_binary(&elf(0xB7, &[1])).expect_err("aarch64 must fail");
        assert!(err.contains("x86-64"), "{err}");
    }

    #[test]
    fn a_shell_script_is_refused() {
        let err = check_worker_binary(b"#!/bin/sh\nexec ./worker\n").expect_err("must fail");
        assert!(err.contains("not an ELF binary"), "{err}");
    }

    #[test]
    fn a_truncated_program_header_table_does_not_panic() {
        // Claims twenty program headers and provides none. The node can have
        // the last word; this must not crash the launcher.
        let mut truncated = elf(0x3E, &[]);
        truncated[0x38..0x3A].copy_from_slice(&20_u16.to_le_bytes());
        assert_eq!(check_worker_binary(&truncated), Ok(()));
    }

    #[test]
    fn an_absurd_program_header_offset_does_not_panic() {
        let mut absurd = elf(0x3E, &[1]);
        absurd[0x20..0x28].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(check_worker_binary(&absurd), Ok(()));
    }
}
