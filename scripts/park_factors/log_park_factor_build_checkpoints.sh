#!/usr/bin/env bash
set -u -o pipefail

interval_sec="${1:-60}"
pid_file="${2:-data/processed/park_factors/park_factor_build.pid}"
log_file="${3:-data/processed/park_factors/park_factor_build_current.log}"
out_dir="${4:-data/processed/park_factors}"

csv_path="${out_dir}/park_factor_build_checkpoints.csv"
latest_path="${out_dir}/park_factor_build_latest.txt"
md_path="${out_dir}/BUILD_CHECKPOINT.md"
logger_pid_path="/tmp/park_factor_build_checkpoint_logger.pid"

mkdir -p "${out_dir}"

if [[ -f "${logger_pid_path}" ]]; then
  old_pid="$(cat "${logger_pid_path}" 2>/dev/null || true)"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    echo "Checkpoint logger already running (pid=${old_pid})."
    exit 0
  fi
  rm -f "${logger_pid_path}"
fi

echo "$$" > "${logger_pid_path}"
cleanup() {
  rm -f "${logger_pid_path}"
}
trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

if [[ ! -f "${csv_path}" ]]; then
  echo "local_time,pid,run_state,stage,last_validation,last_line" > "${csv_path}"
fi

extract_stage() {
  local lf="$1"
  if [[ ! -f "${lf}" ]]; then
    echo "log_missing"
    return
  fi

  if rg -q "Park factor build complete\\." "${lf}"; then
    echo "completed"
    return
  fi
  if rg -q "Building Savant-style display table\\.\\.\\." "${lf}"; then
    echo "building_display"
    return
  fi
  if rg -q "component model fit complete" "${lf}"; then
    echo "component_fit_running"
    return
  fi
  if rg -q "Fitting .* component model" "${lf}"; then
    echo "fitting_component_model"
    return
  fi
  if rg -q "Final park-factor model fit complete\\." "${lf}"; then
    echo "extracting_tables"
    return
  fi
  if rg -q "Fitting final park-factor model on full dataset\\.\\.\\." "${lf}"; then
    echo "fitting_final_model"
    return
  fi
  if rg -q "Main rolling validation complete\\." "${lf}"; then
    echo "validation_done"
    return
  fi
  if rg -q "Validation fold: target=" "${lf}"; then
    echo "validation_running"
    return
  fi
  if rg -q "Loaded defense composite for" "${lf}"; then
    echo "pre_validation"
    return
  fi
  if rg -q "Reading BBE input:" "${lf}"; then
    echo "startup"
    return
  fi
  echo "unknown"
}

while true; do
  local_now="$(date '+%Y-%m-%d %H:%M %Z')"

  build_pid=""
  run_state="no_pid"
  if [[ -f "${pid_file}" ]]; then
    build_pid="$(cat "${pid_file}" 2>/dev/null || true)"
  fi

  if [[ -n "${build_pid}" ]]; then
    if kill -0 "${build_pid}" 2>/dev/null; then
      run_state="running"
    else
      run_state="stopped"
    fi
  fi

  stage="$(extract_stage "${log_file}")"
  last_validation="$(rg -n "Validation fold(:| complete): target=" "${log_file}" 2>/dev/null | tail -n 1 | sed 's/^[0-9]*://')"
  last_line="$(tail -n 1 "${log_file}" 2>/dev/null || true)"

  esc_last_validation="$(printf "%s" "${last_validation}" | sed 's/"/""/g')"
  esc_last_line="$(printf "%s" "${last_line}" | sed 's/"/""/g')"
  echo "\"${local_now}\",\"${build_pid}\",\"${run_state}\",\"${stage}\",\"${esc_last_validation}\",\"${esc_last_line}\"" >> "${csv_path}"

  {
    echo "local_time=${local_now}"
    echo "pid=${build_pid}"
    echo "run_state=${run_state}"
    echo "stage=${stage}"
    echo "last_validation=${last_validation}"
    echo "last_line=${last_line}"
  } > "${latest_path}"

  {
    echo "## Park Factor Build Checkpoint"
    echo
    echo "- Timestamp (local): ${local_now}"
    echo "- PID: ${build_pid}"
    echo "- Run state: ${run_state}"
    echo "- Stage: ${stage}"
    echo "- Log file: \`${log_file}\`"
    if [[ -n "${last_validation}" ]]; then
      echo "- Last validation marker: \`${last_validation}\`"
    fi
    echo
    echo "### Recent log tail"
    echo
    echo '```'
    tail -n 20 "${log_file}" 2>/dev/null || true
    echo '```'
  } > "${md_path}"

  if [[ "${run_state}" == "stopped" ]]; then
    break
  fi

  sleep "${interval_sec}"
done
