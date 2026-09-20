#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
suite_dir="$(cd "${script_dir}/.." && pwd)"
server_dir="$(cd "${SINK_SERVER_DIR:-${suite_dir}/../sink}" && pwd)"
tool_dir="${suite_dir}/tools/sink-perf"
artifacts="${SINK_CANDIDATE_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-candidate.XXXXXXXX")}"
mkdir -p "${artifacts}"
artifacts="$(cd "${artifacts}" && pwd)"
modfile="${artifacts}/perf.go.mod"
cp "${tool_dir}/go.mod" "${modfile}"
cp "${tool_dir}/go.sum" "${artifacts}/perf.go.sum"
go -C "${tool_dir}" mod edit -modfile="${modfile}" -replace="github.com/batchstream/sink=${server_dir}"
operation="${1:?usage: perf-go.sh <build|test|vet> [go arguments...]}"
shift
case "${operation}" in
  build|test|vet) ;;
  *) echo "unsupported Go operation: ${operation}" >&2; exit 1 ;;
esac
GOWORK=off go -C "${tool_dir}" "${operation}" -mod=readonly -modfile="${modfile}" "$@"
