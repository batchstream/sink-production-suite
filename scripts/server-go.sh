#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
server_dir="${SINK_SERVER_DIR:-${suite_dir}/../sink}"
artifacts="${SINK_CANDIDATE_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-candidate.XXXXXXXX")}"
mkdir -p "${artifacts}"
artifacts="$(cd "${artifacts}" && pwd)"
overlay="${artifacts}/server-tests-overlay.json"
python3 "${script_dir}/prepare-server-tests.py" --server "${server_dir}" --output "${overlay}"
if [[ "$#" == 0 ]]; then
  echo "usage: $0 <test|build|list|vet> [go arguments...]" >&2
  exit 1
fi
operation="$1"
shift
case "${operation}" in
  test|build|list|vet) ;;
  *) echo "unsupported Go operation: ${operation}" >&2; exit 1 ;;
esac
go -C "${server_dir}" "${operation}" -mod=readonly -overlay="${overlay}" "$@"
