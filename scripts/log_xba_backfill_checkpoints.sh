#!/usr/bin/env bash
set -u -o pipefail

interval_sec="${1:-600}"
out_dir="data/processed/park_factors"
csv_path="${out_dir}/xba_backfill_checkpoints.csv"
latest_path="${out_dir}/xba_backfill_latest.txt"
pid_path="/tmp/xba_backfill_checkpoint_logger.pid"

mkdir -p "${out_dir}"

if [[ -f "${pid_path}" ]]; then
  old_pid="$(cat "${pid_path}" 2>/dev/null || true)"
  if [[ -n "${old_pid}" ]] && kill -0 "${old_pid}" 2>/dev/null; then
    echo "Checkpoint logger already running (pid=${old_pid})."
    exit 0
  fi
  rm -f "${pid_path}"
fi

echo "$$" > "${pid_path}"
cleanup() {
  rm -f "${pid_path}"
}
trap cleanup EXIT
trap 'cleanup; exit 0' INT TERM

if [[ ! -f "${csv_path}" ]]; then
  echo "local_time,done,total,pct,next_missing,last_manifest_local,last_chunk,last_status,lag_min,error" > "${csv_path}"
fi

while true; do
  local_now="$(date '+%Y-%m-%d %H:%M %Z')"

  metrics=""
  metric_error=""
  if ! metrics="$(
    Rscript -e '
      read_manifest_safe <- function(path, tries = 5L) {
        for (k in seq_len(tries)) {
          x <- tryCatch(read.csv(path, stringsAsFactors = FALSE), error = function(e) NULL)
          if (!is.null(x)) return(x)
          Sys.sleep(0.2)
        }
        stop("manifest read failed after retries")
      }

      fs <- list.files("data/raw/statcast_bbe_store_chunks", pattern = "_bbe.csv$", full.names = TRUE)
      ids <- as.integer(substr(basename(fs), 1, 3))
      ord <- order(ids)
      fs <- fs[ord]
      ids <- ids[ord]

      has_col <- logical(length(fs))
      for (i in seq_along(fs)) {
        hdr <- tryCatch(
          names(read.csv(fs[i], nrows = 0, stringsAsFactors = FALSE, check.names = FALSE)),
          error = function(e) character(0)
        )
        has_col[i] <- "estimated_ba_using_speedangle" %in% hdr
      }

      done <- sum(has_col)
      total <- length(has_col)
      pct <- if (total > 0) 100 * done / total else NA_real_
      missing <- ids[!has_col]
      next_missing <- if (length(missing) > 0) sprintf("%03d", min(missing)) else "none"

      m <- read_manifest_safe("data/raw/statcast_bbe_store_chunks/chunk_manifest.csv")
      m$ts <- as.POSIXct(m$timestamp_utc, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      i <- which.max(m$ts)
      last_local <- format(m$ts[i], "%Y-%m-%d %H:%M", tz = "America/Los_Angeles")
      last_chunk <- as.character(m$chunk_id[i])
      last_status <- as.character(m$status[i])
      lag_min <- as.numeric(difftime(Sys.time(), m$ts[i], units = "mins"))

      cat(sprintf("%d|%d|%.2f|%s|%s|%s|%s|%.1f",
                  done, total, pct, next_missing, last_local, last_chunk, last_status, lag_min))
    '
  )"; then
    metric_error="$(printf "%s" "${metrics}" | tr '\n' ' ' | sed 's/"/""/g')"
    done="NA"
    total="NA"
    pct="NA"
    next_missing="NA"
    last_manifest_local="NA"
    last_chunk="NA"
    last_status="NA"
    lag_min="NA"
  else
    IFS='|' read -r done total pct next_missing last_manifest_local last_chunk last_status lag_min <<< "${metrics}"
  fi

  echo "\"${local_now}\",${done},${total},${pct},\"${next_missing}\",\"${last_manifest_local}\",\"${last_chunk}\",\"${last_status}\",${lag_min},\"${metric_error}\"" >> "${csv_path}"

  {
    echo "local_time=${local_now}"
    echo "done=${done}"
    echo "total=${total}"
    echo "pct=${pct}"
    echo "next_missing=${next_missing}"
    echo "last_manifest_local=${last_manifest_local}"
    echo "last_chunk=${last_chunk}"
    echo "last_status=${last_status}"
    echo "lag_min=${lag_min}"
    echo "error=${metric_error}"
  } > "${latest_path}"

  sleep "${interval_sec}"
done
