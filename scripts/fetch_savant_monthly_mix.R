#!/usr/bin/env Rscript
# ===========================================================================
# Fetch Savant monthly pitch-mix panel (pitcher x month x pitch family)
# ===========================================================================
# Source of truth for pitch mix = Baseball Savant (user decision 2026-07-15).
# statcast_search grouped CSV, group_by=name-month, one query per pitch
# FAMILY per season (multi-type hfPT filters aggregate into one count):
#   ff FF | si SI | fc FC | slfam SL|ST|SV | cufam CU|KC|CS | ch CH | fs FS | kn KN
# plus one no-filter "total" query per season as the pitcher-month spine.
# Grain: regular season; columns kept: pitches, total_pitches, pitch_percent,
# velocity, spin_rate. ~2,800-45,00 rows per file, well under the 10k cap.
# Cache: data/raw/savant_monthly_mix/mix_<season>_<family>.csv (idempotent —
# existing files are skipped; delete a file to refetch).
# Usage: Rscript scripts/fetch_savant_monthly_mix.R [seasons...] (default 2021-2026)
# ===========================================================================

args <- commandArgs(trailingOnly = TRUE)
SEASONS <- if (length(args)) as.integer(args) else 2021:2026
OUT_DIR <- file.path("data", "raw", "savant_monthly_mix")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

FAMILIES <- list(
  total = NULL,
  ff = "FF", si = "SI", fc = "FC",
  slfam = c("SL", "ST", "SV"),
  cufam = c("CU", "KC", "CS"),
  ch = "CH", fs = "FS", kn = "KN",
  # individual slider-family members (sweeper / slurve counted as their own
  # pitches under the v2.1+ taxonomy); slfam stays for back-compat.
  sl = "SL", st = "ST", sv = "SV"
)

url_for <- function(season, types) {
  pt <- if (is.null(types)) "" else paste0("&hfPT=", paste0(paste(types, collapse = "%7C"), "%7C"))
  paste0("https://baseballsavant.mlb.com/statcast_search/csv?",
         "hfGT=R%7C&hfSea=", season, "%7C&player_type=pitcher&group_by=name-month",
         pt, "&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&sort_order=desc")
}

for (s in SEASONS) {
  for (fam in names(FAMILIES)) {
    f <- file.path(OUT_DIR, sprintf("mix_%d_%s.csv", s, fam))
    if (file.exists(f) && file.size(f) > 200) {
      cat(sprintf("  skip %d %s (cached)\n", s, fam)); next
    }
    ok <- tryCatch({
      download.file(url_for(s, FAMILIES[[fam]]), f, quiet = TRUE)
      TRUE
    }, error = function(e) { cat(sprintf("  ERROR %d %s: %s\n", s, fam, conditionMessage(e))); FALSE })
    n <- if (ok && file.exists(f)) length(readLines(f, warn = FALSE)) - 1 else NA
    cat(sprintf("  %d %-6s rows=%s\n", s, fam, n))
    if (!is.na(n) && n >= 9999) cat("    WARNING: at 10k response cap — data truncated!\n")
    Sys.sleep(1.5)
  }
}
cat("Done.\n")
