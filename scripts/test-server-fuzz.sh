#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
artifacts="${SINK_CANDIDATE_ARTIFACTS:-$(mktemp -d "${TMPDIR:-/tmp}/sink-candidate.XXXXXXXX")}"
mkdir -p "${artifacts}/fuzz"
export SINK_CANDIDATE_ARTIFACTS="$(cd "${artifacts}" && pwd)"
artifacts="${SINK_CANDIDATE_ARTIFACTS}"
echo "Server fuzz evidence: ${artifacts}/fuzz"
for target in queue:FuzzMutationEnvelope storage:FuzzValidateBSONDocument; do
  package="${target%%:*}"
  name="${target#*:}"
  directory="${artifacts}/fuzz/${package}"
  mkdir -p "${directory}"
  bash "${script_dir}/server-go.sh" test "./internal/${package}" -c \
    -fuzz="^${name}$" -o "${directory}/fuzz.test"
  # Go writes minimized failures relative to the test process. Run from the
  # evidence directory so even a failure cannot write into the Sink checkout.
  (
    cd "${directory}"
    ./fuzz.test -test.run '^$' -test.fuzz="^${name}$" \
      -test.fuzztime="${SERVER_FUZZ_TIME:-30s}" -test.parallel=2 \
      -test.fuzzcachedir="${directory}/corpus"
  ) 2>&1 | tee "${artifacts}/fuzz/${name}.log"
  grep -q 'new interesting:' "${artifacts}/fuzz/${name}.log"
done
