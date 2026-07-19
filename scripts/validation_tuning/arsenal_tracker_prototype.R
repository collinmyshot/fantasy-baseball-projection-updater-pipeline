#!/usr/bin/env Rscript
# ===========================================================================
# Arsenal Tracker prototype — validate the windowed change detector (Phase 3a)
# ===========================================================================
# Detect pitch-mix changes with DISJOINT windows:
#   recent   = L30 (last 30 days)
#   baseline = opening day .. start of L30  (excludes L30 — undiluted contrast)
# Per pitcher: usage share per family in each window (Savant statcast_search,
# group_by=name + hfPT family filter + game_date range; pitch_percent is
# window-specific). Then:
#   Mix Shift %  = Total Variation Distance = 0.5 * sum|recent - baseline|
#   Top mover    = family with the largest |delta|
#   New pitch    = family with baseline < 3% and recent >= 10%
# Goal: confirm the fetch works and the feed is face-valid before building the
# fbb-tools module. NOT the shippable module — a validation harness.
# ===========================================================================

suppressPackageStartupMessages({ library(curl) })

SEASON   <- 2026
TODAY    <- Sys.Date()                      # 2026-07-15 in this environment
L30_GT   <- as.character(TODAY - 30)
L30_LT   <- as.character(TODAY)
BASE_GT  <- paste0(SEASON, "-03-01")        # safely before opening day
BASE_LT  <- as.character(TODAY - 31)
MIN_WINDOW_PITCHES <- 150                    # sample gate per window

# family -> Savant pitch-type codes (grouped taxonomy)
FAMILIES <- list(
  ff = "FF", si = "SI", fc = "FC",
  slfam = c("SL","ST","SV"), cufam = c("CU","KC","CS"),
  ch = "CH", fs = "FS", kn = "KN"
)

win_url <- function(types, dg, dl) {
  pt <- paste0("&hfPT=", paste0(paste(types, collapse = "%7C"), "%7C"))
  paste0("https://baseballsavant.mlb.com/statcast_search/csv?",
         "hfGT=R%7C&hfSea=", SEASON, "%7C&player_type=pitcher&group_by=name",
         pt, "&game_date_gt=", dg, "&game_date_lt=", dl,
         "&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&sort_order=desc")
}
tot_url <- function(dg, dl) {
  paste0("https://baseballsavant.mlb.com/statcast_search/csv?",
         "hfGT=R%7C&hfSea=", SEASON, "%7C&player_type=pitcher&group_by=name",
         "&game_date_gt=", dg, "&game_date_lt=", dl,
         "&min_pitches=0&min_results=0&min_pas=0&sort_col=pitches&sort_order=desc")
}

fetch_csv <- function(url) {
  r <- tryCatch(curl_fetch_memory(url), error = function(e) NULL)
  if (is.null(r) || r$status_code != 200) return(NULL)
  raw <- r$content
  if (length(raw) >= 3 && raw[1] == 0xEF && raw[2] == 0xBB && raw[3] == 0xBF) raw <- raw[-(1:3)]
  read.csv(text = rawToChar(raw), check.names = FALSE, stringsAsFactors = FALSE)
}

# Build a window's usage matrix: rows = pitcher, cols = family share (0-1)
fetch_window <- function(dg, dl, label) {
  cat(sprintf("  fetching %s (%s .. %s)\n", label, dg, dl))
  tot <- fetch_csv(tot_url(dg, dl))
  if (is.null(tot)) stop("total fetch failed for ", label)
  tot$player_id <- as.integer(tot$player_id)
  tot$total <- as.numeric(tot$pitches)
  base <- data.frame(player_id = tot$player_id, player_name = tot$player_name,
                     total = tot$total, stringsAsFactors = FALSE)
  for (fam in names(FAMILIES)) {
    d <- fetch_csv(win_url(FAMILIES[[fam]], dg, dl))
    Sys.sleep(0.3)
    cnt <- rep(0, nrow(base))
    if (!is.null(d) && nrow(d)) {
      d$player_id <- as.integer(d$player_id)
      m <- match(base$player_id, d$player_id)
      cnt <- ifelse(is.na(m), 0, as.numeric(d$pitches[m]))
    }
    base[[fam]] <- cnt
  }
  fam_cols <- names(FAMILIES)
  base[, fam_cols] <- base[, fam_cols] / pmax(base$total, 1)
  base
}

cat("Arsenal Tracker prototype — disjoint L30 vs season-before-L30\n")
rec  <- fetch_window(L30_GT, L30_LT, "L30")
base <- fetch_window(BASE_GT, BASE_LT, "baseline")

fam_cols <- names(FAMILIES)
# join on pitcher; require both windows to clear the sample gate
j <- merge(rec, base, by = "player_id", suffixes = c("_r", "_b"))
j <- j[j$total_r >= MIN_WINDOW_PITCHES & j$total_b >= MIN_WINDOW_PITCHES, ]
cat(sprintf("\nPitchers with both windows >= %d pitches: %d\n", MIN_WINDOW_PITCHES, nrow(j)))

R <- as.matrix(j[, paste0(fam_cols, "_r")]); colnames(R) <- fam_cols
B <- as.matrix(j[, paste0(fam_cols, "_b")]); colnames(B) <- fam_cols
D <- R - B

j$mix_shift <- 0.5 * rowSums(abs(D))                 # Total Variation Distance
top_i <- apply(abs(D), 1, which.max)
j$top_fam   <- fam_cols[top_i]
j$top_delta <- D[cbind(seq_len(nrow(D)), top_i)]
j$top_from  <- B[cbind(seq_len(nrow(B)), top_i)]
j$top_to    <- R[cbind(seq_len(nrow(R)), top_i)]
# new pitch: any family from <3% to >=10%
newp <- (B < 0.03) & (R >= 0.10)
j$new_pitch <- apply(newp, 1, function(z) if (any(z)) fam_cols[which(z)[1]] else NA_character_)

ord <- order(-j$mix_shift)
cat("\nTop 15 mix changers (Mix Shift % = TVD):\n")
cat(sprintf("  %-22s %6s  %-7s %5s->%3s  %s\n", "Pitcher", "Shift", "Pitch", "from", "to", "new?"))
for (i in head(ord, 15)) {
  cat(sprintf("  %-22s %5.1f%%  %-7s %4.0f%%->%3.0f%%  %s\n",
              substr(j$player_name_r[i], 1, 22), 100 * j$mix_shift[i],
              j$top_fam[i], 100 * j$top_from[i], 100 * j$top_to[i],
              ifelse(is.na(j$new_pitch[i]), "", paste0("NEW ", j$new_pitch[i]))))
}

cat(sprintf("\nMix Shift distribution: median %.1f%%, p90 %.1f%%, max %.1f%%\n",
            100*median(j$mix_shift), 100*quantile(j$mix_shift, .9), 100*max(j$mix_shift)))
cat(sprintf("New-pitch pitchers: %d\n", sum(!is.na(j$new_pitch))))
