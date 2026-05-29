#!/usr/bin/env bash
# Launch a Phase 3.1 cptools2 production validation driver in tmux on Eddie.

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  scripts/eddie_phase31_launch.sh --config CONFIG --run-root RUN_ROOT [options]

Options:
  --session NAME                  tmux session name
  --diagnostics MODE              Nextflow diagnostics mode: full|minimal|off
  --cleanup-policy POLICY         cleanup policy: keep|success|verified
  --resume                        pass --resume to cptools2 pipeline
  --continue-on-batch-failure     let independent later batches continue
  --help                          show this help
USAGE
}

CONFIG_PATH=""
RUN_ROOT=""
SESSION_NAME=""
DIAGNOSTICS_MODE="minimal"
CLEANUP_POLICY="verified"
RESUME_FLAG=""
CONTINUE_FLAG=""

while [ "$#" -gt 0 ]; do
    case "$1" in
        --config)
            CONFIG_PATH="${2:?--config requires a value}"
            shift 2
            ;;
        --run-root)
            RUN_ROOT="${2:?--run-root requires a value}"
            shift 2
            ;;
        --session)
            SESSION_NAME="${2:?--session requires a value}"
            shift 2
            ;;
        --diagnostics)
            DIAGNOSTICS_MODE="${2:?--diagnostics requires a value}"
            shift 2
            ;;
        --cleanup-policy)
            CLEANUP_POLICY="${2:?--cleanup-policy requires a value}"
            shift 2
            ;;
        --resume)
            RESUME_FLAG="--resume"
            shift
            ;;
        --continue-on-batch-failure)
            CONTINUE_FLAG="--continue-on-batch-failure"
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [ -z "${CONFIG_PATH}" ] || [ -z "${RUN_ROOT}" ]; then
    usage >&2
    exit 2
fi

case "${DIAGNOSTICS_MODE}" in
    full|minimal|off) ;;
    *)
        echo "--diagnostics must be one of full, minimal, or off" >&2
        exit 2
        ;;
esac

case "${CLEANUP_POLICY}" in
    keep|success|verified) ;;
    *)
        echo "--cleanup-policy must be one of keep, success, or verified" >&2
        exit 2
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_ROOT="$(mkdir -p "${RUN_ROOT}" && cd "${RUN_ROOT}" && pwd)"
CONFIG_PATH="$(cd "$(dirname "${CONFIG_PATH}")" && pwd)/$(basename "${CONFIG_PATH}")"

if [ -z "${SESSION_NAME}" ]; then
    SESSION_NAME="cptools2-phase31-$(basename "${RUN_ROOT}")"
fi

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
    echo "tmux session already exists: ${SESSION_NAME}" >&2
    exit 1
fi

export CPTOOLS2_PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT:-${PROJECT_ROOT}}"
export CPTOOLS2_SCRATCH_ROOT="${RUN_ROOT}"

# Source after CPTOOLS2_SCRATCH_ROOT is set so NXF_HOME and all runtime caches
# are isolated under this run root.
. "${PROJECT_ROOT}/config/eddie_env.sh"

DRIVER_ENV_DIR="${RUN_ROOT}/driver_env"
mkdir -p "${DRIVER_ENV_DIR}" "${RUN_ROOT}/logs" "${RUN_ROOT}/traces"

env | sort > "${DRIVER_ENV_DIR}/env.before.txt"
module list > "${DRIVER_ENV_DIR}/modules.before.txt" 2>&1 || true
qstat -u "${USER}" > "${DRIVER_ENV_DIR}/qstat.before.txt" 2>&1 || true
ps -fu "${USER}" > "${DRIVER_ENV_DIR}/processes.before.txt" 2>&1 || true

DRIVER_SCRIPT="${RUN_ROOT}/driver.sh"
PIPELINE_LOG="${RUN_ROOT}/launcher.log"
STATUS_JSON="${RUN_ROOT}/driver_status.json"

cat > "${DRIVER_SCRIPT}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "${PROJECT_ROOT}"
export CPTOOLS2_PROJECT_ROOT="${CPTOOLS2_PROJECT_ROOT}"
export CPTOOLS2_SCRATCH_ROOT="${RUN_ROOT}"
. "${PROJECT_ROOT}/config/eddie_env.sh"
mkdir -p "${DRIVER_ENV_DIR}"
started_at="\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '{"status":"running","started_at":"%s","session":"%s"}\n' "\${started_at}" "${SESSION_NAME}" > "${STATUS_JSON}"
qstat -u "\${USER}" > "${DRIVER_ENV_DIR}/qstat.driver-start.txt" 2>&1 || true
ps -fu "\${USER}" > "${DRIVER_ENV_DIR}/processes.driver-start.txt" 2>&1 || true
set +e
python -m cptools2.__main__ pipeline "${CONFIG_PATH}" ${RESUME_FLAG} --nextflow-diagnostics "${DIAGNOSTICS_MODE}" --cleanup-policy "${CLEANUP_POLICY}" ${CONTINUE_FLAG}
rc=\$?
set -e
qstat -u "\${USER}" > "${DRIVER_ENV_DIR}/qstat.driver-finish.txt" 2>&1 || true
ps -fu "\${USER}" > "${DRIVER_ENV_DIR}/processes.driver-finish.txt" 2>&1 || true
finished_at="\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '{"status":"finished","exit_code":%s,"finished_at":"%s","session":"%s"}\n' "\${rc}" "\${finished_at}" "${SESSION_NAME}" > "${STATUS_JSON}"
exit "\${rc}"
EOF

chmod 0755 "${DRIVER_SCRIPT}"

tmux new-session -d -s "${SESSION_NAME}" "bash '${DRIVER_SCRIPT}' > '${PIPELINE_LOG}' 2>&1"

cat > "${RUN_ROOT}/status.txt" <<EOF
status=launched
session=${SESSION_NAME}
run_root=${RUN_ROOT}
config=${CONFIG_PATH}
log=${PIPELINE_LOG}
driver_status=${STATUS_JSON}
diagnostics=${DIAGNOSTICS_MODE}
cleanup_policy=${CLEANUP_POLICY}
continue_on_batch_failure=$([ -n "${CONTINUE_FLAG}" ] && echo true || echo false)
EOF

echo "Launched ${SESSION_NAME}"
echo "Run root: ${RUN_ROOT}"
echo "Log: ${PIPELINE_LOG}"
echo "Status: ${STATUS_JSON}"
