#!/usr/bin/env bash
set -euo pipefail

# Legacy wrapper: delegate to the R implementation so ADP logic is defined once.
exec Rscript scripts/fetch_nfbc_adp.R "$@"
