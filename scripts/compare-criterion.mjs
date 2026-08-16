import { readFileSync } from "node:fs";

const [basePath, candidatePath, baseRevision, candidateRevision] = process.argv.slice(2);
const REGRESSION_THRESHOLD = 20;

if (!basePath || !candidatePath || !baseRevision || !candidateRevision) {
    throw new Error(
        "usage: compare-criterion.mjs <main-output> <pr-output> <main-revision> <pr-revision>",
    );
}

const UNIT_TO_NANOSECONDS = new Map([
    ["ns", 1],
    ["µs", 1_000],
    ["us", 1_000],
    ["ms", 1_000_000],
    ["s", 1_000_000_000],
]);

function parseBenchmarks(path) {
    const lines = readFileSync(path, "utf8").split(/\r?\n/);
    const benchmarks = new Map();

    for (let index = 0; index + 1 < lines.length; index += 1) {
        const name = lines[index].trim();
        const time = lines[index + 1].match(
            /^\s*time:\s+\[\s*([\d.]+)\s*(ns|µs|us|ms|s)\s+([\d.]+)\s*(ns|µs|us|ms|s)\s+([\d.]+)\s*(ns|µs|us|ms|s)\s*\]$/,
        );

        if (!time || !name || name.startsWith("Benchmarking ")) {
            continue;
        }

        const unit = time[4];
        if (time[2] !== unit || time[6] !== unit) {
            throw new Error(`${path}: Criterion printed mixed units for ${name}`);
        }

        if (benchmarks.has(name)) {
            throw new Error(`${path}: benchmark ${name} was reported twice`);
        }

        benchmarks.set(name, {
            display: `${time[3]} ${unit}`,
            nanoseconds: Number(time[3]) * UNIT_TO_NANOSECONDS.get(unit),
        });
    }

    if (benchmarks.size === 0) {
        throw new Error(`${path}: no Criterion time measurements found`);
    }

    return benchmarks;
}

function markdown(value) {
    return value.replaceAll("\\", "\\\\").replaceAll("|", "\\|");
}

function revision(value) {
    return value.slice(0, 12);
}

function change(value) {
    return `${value >= 0 ? "+" : ""}${value.toFixed(1)}%`;
}

const main = parseBenchmarks(basePath);
const pullRequest = parseBenchmarks(candidatePath);
const names = [...new Set([...main.keys(), ...pullRequest.keys()])].sort((left, right) =>
    left.localeCompare(right),
);

let regressions = 0;
let improvements = 0;
let unavailable = 0;
const rows = names.map((name) => {
    const base = main.get(name);
    const candidate = pullRequest.get(name);

    if (!base || !candidate) {
        unavailable += 1;
        return `| ${markdown(name)} | ${base?.display ?? "—"} | ${candidate?.display ?? "—"} | — | not comparable |`;
    }

    // Classify the same one-decimal value the report prints. Without this,
    // e.g. a displayed +20.0% may be a binary float infinitesimally below the
    // advisory threshold and get a contradictory "within runner noise" label.
    const relativeChange = Number(
        ((candidate.nanoseconds / base.nanoseconds - 1) * 100).toFixed(1),
    );
    let signal = "ℹ️ within runner noise";
    if (relativeChange >= REGRESSION_THRESHOLD) {
        signal = "⚠️ regression";
        regressions += 1;
    } else if (relativeChange <= -REGRESSION_THRESHOLD) {
        signal = "✅ improved";
        improvements += 1;
    }

    return `| ${markdown(name)} | ${base.display} | ${candidate.display} | ${change(relativeChange)} | ${signal} |`;
});

const summary = [
    `📊 ${names.length} benchmark(s) compared`,
    `⚠️ ${regressions} regression(s) at ≥${REGRESSION_THRESHOLD}%`,
    `✅ ${improvements} improvement(s) at ≥${REGRESSION_THRESHOLD}%`,
];
if (unavailable > 0) {
    summary.push(`➖ ${unavailable} not comparable`);
}

console.log("<!-- ytsaurus-rs:criterion-benchmark-comparison -->");
console.log("## 📊 Criterion benchmarks: main vs PR");
console.log("");
console.log(`🔍 Main \`${revision(baseRevision)}\` → PR \`${revision(candidateRevision)}\``);
console.log("");
console.log(`**${summary.join(" · ")}**`);
console.log("");
console.log("ℹ️ Time is lower-is-better. Both revisions ran on the same GitHub-hosted VM; ±20% is an advisory marker, not a merge gate.");
console.log("");
console.log("| Benchmark | main | PR | change | signal |");
console.log("| --- | ---: | ---: | ---: | --- |");
for (const row of rows) {
    console.log(row);
}
