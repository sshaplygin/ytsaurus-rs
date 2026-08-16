#!/usr/bin/env bash
# Starts a local YTsaurus cluster in Docker, following
# https://ytsaurus.tech/docs/en/overview/try-yt
#
#   tests/cluster-e2e/run_local_cluster.sh          # start
#   tests/cluster-e2e/run_local_cluster.sh --stop   # tear down
#
# Leaves the HTTP proxy on localhost:8000, the RPC proxy on localhost:8011,
# and the web UI on localhost:8001.
set -euo pipefail

WORKDIR="${YT_LOCAL_DIR:-$HOME/yt-local}"
SCRIPT_URL=https://raw.githubusercontent.com/ytsaurus/ytsaurus/main/yt/docker/local/run_local_cluster.sh

if [ "${1:-}" = "--stop" ]; then
  echo "Stopping local YTsaurus containers..."
  docker rm -f yt.backend yt.frontend 2>/dev/null || true
  echo "Done."
  exit 0
fi

if ! docker info >/dev/null 2>&1; then
  cat >&2 <<'EOF'
error: the Docker daemon is not reachable.

Start Docker (Docker Desktop on macOS, `systemctl start docker` on Linux) and
try again.
EOF
  exit 1
fi

# YTsaurus publishes x86_64 images only. On Apple Silicon this runs under
# emulation, which the YTsaurus docs explicitly say is not guaranteed to work —
# worth knowing before debugging a mysterious failure.
if [ "$(uname -s)" = "Darwin" ] && [ "$(uname -m)" = "arm64" ]; then
  cat >&2 <<'EOF'
note: Apple Silicon detected.

YTsaurus ships x86_64 images only, so the cluster will run under Rosetta 2
emulation. The YTsaurus documentation states this is not guaranteed to work.
Enable "Use Rosetta for x86_64/amd64 emulation" in Docker Desktop's settings if
the cluster fails to start.

EOF
fi

mkdir -p "$WORKDIR"
cd "$WORKDIR"

if [ ! -x run_local_cluster.sh ]; then
  echo "Downloading run_local_cluster.sh into $WORKDIR..."
  curl -fsSL "$SCRIPT_URL" -o run_local_cluster.sh
  chmod +x run_local_cluster.sh
fi

./run_local_cluster.sh --rpc-proxy-count 1 --rpc-proxy-port 8011

echo
echo "HTTP proxy: localhost:8000    RPC proxy: localhost:8011    UI: localhost:8001"
echo "Now run: tests/cluster-e2e/run_e2e.sh"
echo "Or:      cargo run -p ytsaurus-rpc --example rpc_e2e"
