#!/usr/bin/env Rscript
# find_similar_hitters.R
# Player similarity via Mahalanobis distance in bat-tracking feature space.
#
# USAGE (interactive):
#   source("scripts/article_research/find_similar_hitters.R")
#   find_similar("Aaron Judge")
#   find_similar("Aaron Judge", year = 2025, method = "euclidean")
#   find_similar("Aaron Judge", features = c("avg_bat_speed", "hard_swing_rate"))

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
})

# ── Configuration ────────────────────────────────────────────────────────────
# Change these freely — any column available in the joined dataset is fair game.
# Run show_available_features() to see the full column list.
DEFAULT_FEATURES <- c(
  "miss_distance",   # avg miss distance (swing-timing)
  "avg_bat_speed",   # average bat speed mph (bat-tracking)
  "hard_swing_rate", # rate of "fast" swings (bat-tracking)
  "whiff_rate"       # whiffs per swing (swing-timing)
)

DEFAULT_YEAR    <- 2026   # set NA to use each player's most recent available year
MIN_SWINGS      <- 150    # eligibility floor (n_swings from swing-timing)
DATA_DIR        <- "/Users/ckaufman/Downloads"


# ── Data loading ─────────────────────────────────────────────────────────────
load_bat_data <- function(data_dir = DATA_DIR) {
  bt <- read_csv(
    file.path(data_dir, "bat-tracking.csv"),
    show_col_types = FALSE,
    name_repair = "minimal"
  )
  st <- read_csv(
    file.path(data_dir, "bat-tracking-swing-timing.csv"),
    show_col_types = FALSE,
    name_repair = "minimal"
  )

  # Normalize the BOM-mangled id column name if present
  names(bt)[1] <- "id"
  names(st)[1] <- "id"

  # Join on player id + year (more reliable than name matching across sources)
  joined <- inner_join(
    bt |> select(id, name, year, avg_bat_speed, hard_swing_rate,
                 blast_per_swing, squared_up_per_swing, swing_length,
                 whiff_per_swing, batter_run_value),
    st |> select(id, year, team_name, miss_distance, whiff_rate,
                 on_time_percent, early_percent, late_percent,
                 perfect_percent, flawed_percent, n_swings),
    by = c("id", "year")
  )

  joined
}


# ── Core similarity function ──────────────────────────────────────────────────
#
# method = "mahalanobis"  — accounts for correlations between features (default)
# method = "euclidean"    — z-score normalized Euclidean; assumes independence
#
# Mahalanobis is preferred when features are correlated (bat speed & hard_swing_rate
# definitely are). Euclidean is more interpretable for explaining *why* two players
# are similar.

find_similar <- function(
  player_name,
  year     = DEFAULT_YEAR,
  features = DEFAULT_FEATURES,
  method   = "mahalanobis",   # "mahalanobis" | "euclidean"
  n        = 10,
  min_swings = MIN_SWINGS,
  data     = NULL              # pass pre-loaded data to skip re-reading
) {
  if (is.null(data)) data <- load_bat_data()

  # Year filter
  if (!is.na(year)) {
    pool <- data |> filter(year == !!year)
  } else {
    # Most recent year per player
    pool <- data |>
      group_by(id) |>
      filter(year == max(year)) |>
      ungroup()
  }

  # Eligibility
  pool <- pool |> filter(n_swings >= min_swings)

  # Validate features
  missing_cols <- setdiff(features, names(pool))
  if (length(missing_cols) > 0) {
    stop("Feature(s) not found in dataset: ", paste(missing_cols, collapse = ", "),
         "\nCall show_available_features() to see valid options.")
  }

  # Drop rows with any NA in the feature set
  feature_data <- pool |>
    select(all_of(features)) |>
    as.data.frame()
  complete_rows <- complete.cases(feature_data)
  pool         <- pool[complete_rows, ]
  feature_data <- feature_data[complete_rows, ]

  # Locate the target player — handles "Last, First" or "First Last" input
  player_name_lc <- tolower(trimws(player_name))
  target_idx <- which(tolower(pool$name) == player_name_lc)
  if (length(target_idx) == 0) {
    # Try reversing "First Last" → "Last, First"
    parts <- strsplit(player_name_lc, " ")[[1]]
    if (length(parts) >= 2) {
      reversed <- paste0(paste(parts[-1], collapse = " "), ", ", parts[1])
      target_idx <- which(tolower(pool$name) == reversed)
    }
  }
  if (length(target_idx) == 0) {
    # Fallback: partial substring match
    target_idx <- which(grepl(player_name, pool$name, ignore.case = TRUE))
  }
  if (length(target_idx) == 0) {
    cat("Player not found. Available players (sample):\n")
    print(sort(pool$name)[1:20])
    stop("Player '", player_name, "' not found in ", year, " data with >= ", min_swings, " swings.")
  }
  if (length(target_idx) > 1) {
    cat("Multiple matches found — using first:\n")
    print(pool$name[target_idx])
    target_idx <- target_idx[1]
  }

  target_vec <- as.numeric(feature_data[target_idx, ])
  X          <- as.matrix(feature_data)

  # ── Distance calculation ──────────────────────────────────────────────────
  if (method == "mahalanobis") {
    cov_mat <- cov(X)
    # Guard against near-singular covariance (can happen with highly correlated features)
    cov_inv <- tryCatch(
      solve(cov_mat),
      error = function(e) {
        message("Covariance matrix near-singular; falling back to pseudoinverse via MASS.")
        MASS::ginv(cov_mat)
      }
    )
    distances <- apply(X, 1, function(row) {
      diff <- row - target_vec
      sqrt(as.numeric(t(diff) %*% cov_inv %*% diff))
    })

  } else if (method == "euclidean") {
    # Z-score normalize each feature independently
    X_scaled  <- scale(X)
    target_z  <- (target_vec - attr(X_scaled, "scaled:center")) /
                  attr(X_scaled, "scaled:scale")
    distances <- apply(X_scaled, 1, function(row) sqrt(sum((row - target_z)^2)))

  } else {
    stop("method must be 'mahalanobis' or 'euclidean'")
  }

  pool$distance <- distances

  # ── Output ───────────────────────────────────────────────────────────────
  target_row <- pool[target_idx, ]
  others     <- pool[-target_idx, ] |>
    arrange(distance) |>
    head(n)

  cat("\n══════════════════════════════════════════════════\n")
  cat(sprintf(" %d Most Similar to %s (%d) — %s\n",
              n, target_row$name, year, toupper(method)))
  cat("══════════════════════════════════════════════════\n")
  cat(sprintf(" Features: %s\n\n", paste(features, collapse = ", ")))

  # Target player summary
  target_display <- target_row |>
    select(name, team_name, all_of(features)) |>
    mutate(across(where(is.numeric), \(x) round(x, 3)))
  cat("TARGET:\n")
  print(as.data.frame(target_display), row.names = FALSE)
  cat("\nMOST SIMILAR:\n")

  results_display <- others |>
    select(name, team_name, all_of(features), distance) |>
    mutate(
      rank = row_number(),
      across(where(is.numeric), \(x) round(x, 3))
    ) |>
    select(rank, everything())
  print(as.data.frame(results_display), row.names = FALSE)

  invisible(list(target = target_row, similar = others, features = features, method = method))
}


# ── Helper: show what columns are available ───────────────────────────────────
show_available_features <- function() {
  data <- load_bat_data()
  numeric_cols <- names(data)[sapply(data, is.numeric)]
  exclude      <- c("id", "year")
  cat("Available numeric features for similarity:\n")
  cat(paste(" -", setdiff(numeric_cols, exclude)), sep = "\n")
}


# ── If run directly from command line ────────────────────────────────────────
# Detect direct execution vs being sourced (Rscript -e "source(...)")
is_direct_run <- any(grepl("--file=", commandArgs(FALSE)))
if (is_direct_run) {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) == 0) {
    cat("Usage: Rscript find_similar_hitters.R \"Player Name\" [year] [method]\n")
    cat("Example: Rscript find_similar_hitters.R \"Aaron Judge\" 2026 mahalanobis\n")
    quit(status = 0)
  }
  player <- args[1]
  yr     <- if (length(args) >= 2) as.integer(args[2]) else DEFAULT_YEAR
  meth   <- if (length(args) >= 3) args[3] else "mahalanobis"
  find_similar(player, year = yr, method = meth)
}
