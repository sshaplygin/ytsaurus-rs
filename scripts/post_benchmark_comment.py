#!/usr/bin/env python3
"""Posts the Criterion comparison on a pull request, editing its own last one.

`.github/workflows/benchmarks.yml` renders a report on every push to a PR. If
each run left a new comment, a branch with ten pushes would bury the discussion
under ten benchmark tables, so this finds the one it wrote before and rewrites
it.

**It matches on the HTML marker, not on the author.** That distinction is the
whole reason this is a script and not `gh pr comment --edit-last`, which edits
the most recent comment by the *actor* — so any second bot commenting on the
same PR silently steals the edit and this report starts overwriting something
else. The marker is emitted by `compare_criterion.py` as the first line of the
report and is invisible in the rendered comment.

This replaces 27 lines of JavaScript that ran inline in the workflow through
`actions/github-script`. That action supplied an authenticated API client for
free; `gh` is authenticated the same way from `GH_TOKEN`, which the workflow
already has through `permissions: pull-requests: write`.

How it fails, so that its silence means something: a missing report file, an
unset `GH_TOKEN`, a `gh` that is not on PATH, and any non-zero `gh api` all exit
non-zero. It never decides that "no comment was posted" is a success.

Run by the workflow as:

    GH_TOKEN=... python3 scripts/post_benchmark_comment.py \\
        --repo owner/name --pr 123 --body-file benchmark-report.md
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

# Emitted by compare_criterion.py as the report's first line. Changing it on one
# side only would orphan every comment already posted, which is why it is a
# constant in both files rather than a value passed between them.
MARKER = "<!-- ytsaurus-rs:criterion-benchmark-comparison -->"


def gh(*args: str, stdin: str | None = None) -> str:
    """Run `gh` and return stdout, or exit with its error.

    A failure here is fatal rather than warned about: a report nobody can see is
    not a run that succeeded, and this is the only step that publishes anything.
    """
    binary = shutil.which("gh")
    if binary is None:
        sys.exit("gh is required to post the benchmark comment and was not found on PATH")

    proc = subprocess.run([binary, *args], input=stdin, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        sys.exit(f"gh {' '.join(args)} failed:\n{proc.stderr.strip()}")
    return proc.stdout


def existing_comment_id(repo: str, pr: int) -> int | None:
    """The id of the comment this script wrote before, if there is one."""
    # --paginate, because a busy PR pushes older comments off the first page and
    # a missed marker means a duplicate table rather than an edit.
    raw = gh("api", "--paginate", f"repos/{repo}/issues/{pr}/comments")

    # `--paginate` concatenates one JSON array per page rather than merging them,
    # so the pages are decoded in sequence instead of with a single json.loads.
    decoder, index, comments = json.JSONDecoder(), 0, []
    while index < len(raw):
        if raw[index].isspace():
            index += 1
            continue
        page, index = decoder.raw_decode(raw, index)
        comments.extend(page)

    for comment in comments:
        if MARKER in (comment.get("body") or ""):
            comment_id: int = comment["id"]
            return comment_id
    return None


def post(repo: str, pr: int, body: str) -> None:
    """Rewrite this script's previous comment, or leave a new one."""
    # Through --input rather than -f body=…: the report is markdown full of
    # backticks, pipes and newlines, and JSON is the only encoding of it that
    # neither the shell nor gh's own field parsing can mangle.
    payload: dict[str, Any] = {"body": body}

    comment_id = existing_comment_id(repo, pr)
    if comment_id is None:
        gh(
            "api",
            "--method",
            "POST",
            f"repos/{repo}/issues/{pr}/comments",
            "--input",
            "-",
            stdin=json.dumps(payload),
        )
        print(f"posted a new comment on {repo}#{pr}")
    else:
        gh(
            "api",
            "--method",
            "PATCH",
            f"repos/{repo}/issues/comments/{comment_id}",
            "--input",
            "-",
            stdin=json.dumps(payload),
        )
        print(f"updated comment {comment_id} on {repo}#{pr}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--pr", required=True, type=int, help="pull request number")
    parser.add_argument("--body-file", required=True, type=Path, help="the rendered report to post")
    args = parser.parse_args()

    if not os.environ.get("GH_TOKEN"):
        sys.exit("GH_TOKEN is not set; gh cannot authenticate")
    if not args.body_file.is_file():
        sys.exit(f"no report at {args.body_file}")

    body = args.body_file.read_text()
    if MARKER not in body:
        # Without the marker the next run cannot find this comment and would post
        # a second one, so a report missing it is a bug worth stopping for.
        sys.exit(f"{args.body_file} does not carry {MARKER}; it would never be edited again")

    post(args.repo, args.pr, body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
