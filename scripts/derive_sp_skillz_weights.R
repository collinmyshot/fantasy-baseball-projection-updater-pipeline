#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
config_path <- file.path("config", "pipeline.yml")
if (length(args) >= 2 && args[[1]] == "--config") {
  config_path <- args[[2]]
}

source(file.path("R", "pipeline_config.R"))

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required.")
}

cfg <- load_pipeline_config(config_path)
sp <- cfg$pitcher$sp_skillz

ipb <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) return(out)
  whole <- floor(x[keep])
  frac <- round((x[keep] - whole) * 10)
  out[keep] <- whole + ifelse(frac == 1, 1 / 3, ifelse(frac == 2, 2 / 3, x[keep] - whole))
  out
}

read_year <- function(year) {
  path <- sprintf("/tmp/fg_skillz_%d.json", year)
  if (!file.exists(path)) {
    stop(sprintf("Missing Fangraphs cache file: %s", path))
  }
  d <- jsonlite::fromJSON(path, flatten = TRUE)$data
  x <- data.frame(
    player_id = as.integer(d$xMLBAMID),
    season = year,
    era = suppressWarnings(as.numeric(d$ERA)),
    whip = suppressWarnings(as.numeric(d$WHIP)),
    k_pct = suppressWarnings(as.numeric(d[["K%"]])),
    ip = ipb(d$IP),
    gs = suppressWarnings(as.numeric(d$GS)),
    start_ip = ipb(d[["Start-IP"]]),
    relief_ip = ipb(d[["Relief-IP"]]),
    siera = suppressWarnings(as.numeric(d$SIERA)),
    xfip = suppressWarnings(as.numeric(d$xFIP)),
    k_minus_bb_pct = suppressWarnings(as.numeric(d[["K-BB%"]])),
    contact_pct = suppressWarnings(as.numeric(d[["Contact%"]])),
    csw_pct = suppressWarnings(as.numeric(d[["C+SwStr%"]])),
    balls = suppressWarnings(as.numeric(d$Balls)),
    pitches = suppressWarnings(as.numeric(d$Pitches)),
    ball_pct = NA_real_,
    stuff_plus = suppressWarnings(as.numeric(d$sp_stuff)),
    pitching_plus = suppressWarnings(as.numeric(d$sp_pitching)),
    stringsAsFactors = FALSE
  )
  x$ball_pct <- ifelse(!is.na(x$pitches) & x$pitches > 0, 100 * x$balls / x$pitches, NA_real_)

  x <- x[order(x$player_id, -x$ip), , drop = FALSE]
  x <- x[!duplicated(x$player_id), , drop = FALSE]
  rownames(x) <- NULL

  has_split <- !is.na(x$start_ip) | !is.na(x$relief_ip)
  x$start_ip[has_split & is.na(x$start_ip)] <- 0
  x$relief_ip[has_split & is.na(x$relief_ip)] <- 0
  x$start_share <- ifelse(
    has_split & (x$start_ip + x$relief_ip) > 0,
    x$start_ip / (x$start_ip + x$relief_ip),
    NA_real_
  )
  x
}

years <- 2022:2025
all_years <- do.call(rbind, lapply(years, read_year))

pair_year <- function(base_year) {
  b <- all_years[all_years$season == base_year, , drop = FALSE]
  n <- all_years[all_years$season == (base_year + 1), c("player_id", "era", "whip", "k_pct", "ip"), drop = FALSE]
  names(n) <- c("player_id", "next_era", "next_whip", "next_k_pct", "next_ip")
  m <- merge(b, n, by = "player_id", all = FALSE)
  m$pair <- sprintf("%d->%d", base_year, base_year + 1)
  m
}

pairs <- do.call(rbind, lapply(2022:2024, pair_year))
pairs <- pairs[
  !is.na(pairs$start_share) &
    pairs$start_share >= sp$start_share_min &
    !is.na(pairs$gs) &
    pairs$gs > 5 &
    !is.na(pairs$next_ip) &
    pairs$next_ip >= 40,
  ,
  drop = FALSE
]

pairs$profile <- ifelse(
  !is.na(pairs$start_ip) & pairs$start_ip <= sp$low_ip_max_start_ip,
  "low_ip",
  ifelse(!is.na(pairs$start_ip) & pairs$start_ip >= sp$high_ip_min_start_ip, "high_ip", "blended_ip")
)

metrics <- c("siera", "xfip", "k_minus_bb_pct", "contact_pct", "csw_pct", "ball_pct", "stuff_plus", "pitching_plus")
targets <- c("next_era", "next_whip", "next_k_pct")
target_sign <- c(next_era = -1, next_whip = -1, next_k_pct = 1) # lower-better targets get -1
metric_quality_sign <- c(
  siera = -1,
  xfip = -1,
  k_minus_bb_pct = 1,
  contact_pct = -1,
  csw_pct = 1,
  ball_pct = -1,
  stuff_plus = 1,
  pitching_plus = 1
)

target_blend <- c(next_k_pct = 1, next_era = 1, next_whip = 1)
target_blend <- target_blend[names(target_sign)]
target_blend <- target_blend / sum(target_blend)

calc_one <- function(df, metric, target) {
  ok <- !is.na(df[[metric]]) & !is.na(df[[target]])
  n <- sum(ok)
  if (n < 20) {
    return(c(n = n, r = NA_real_, r2 = NA_real_, quality_aligned_r = NA_real_, sign_ok = NA_real_))
  }
  r <- suppressWarnings(stats::cor(df[[metric]][ok], df[[target]][ok]))
  aligned <- r * target_sign[[target]] * metric_quality_sign[[metric]]
  c(n = n, r = r, r2 = r^2, quality_aligned_r = aligned, sign_ok = as.numeric(aligned > 0))
}

rows <- list()
profiles <- c("low_ip", "high_ip")
for (profile in profiles) {
  d <- pairs[pairs$profile == profile, , drop = FALSE]
  for (target in targets) {
    for (metric in metrics) {
      v <- calc_one(d, metric, target)
      rows[[length(rows) + 1]] <- data.frame(
        profile = profile,
        target = target,
        metric = metric,
        n = as.integer(v[["n"]]),
        r = as.numeric(v[["r"]]),
        r2 = as.numeric(v[["r2"]]),
        quality_aligned_r = as.numeric(v[["quality_aligned_r"]]),
        sign_ok = as.numeric(v[["sign_ok"]]),
        stringsAsFactors = FALSE
      )
    }
  }
}

target_tbl <- do.call(rbind, rows)

target_tbl$strength <- target_tbl$r2
target_tbl$sign_penalty <- ifelse(is.na(target_tbl$quality_aligned_r), 0, ifelse(target_tbl$quality_aligned_r > 0, 1, 0.25))
target_tbl$effective_strength <- target_tbl$strength * target_tbl$sign_penalty

# Enforce command metric scope: ball% only informs WHIP-target contribution.
ball_non_whip <- target_tbl$metric == "ball_pct" & target_tbl$target != "next_whip"
target_tbl$effective_strength[ball_non_whip] <- 0

target_weights <- target_tbl
target_weights$raw_weight <- metric_quality_sign[target_weights$metric] * target_weights$effective_strength
target_weights$scaled_weight <- NA_real_

for (profile in profiles) {
  for (target in targets) {
    idx <- target_weights$profile == profile & target_weights$target == target
    den <- max(abs(target_weights$raw_weight[idx]), na.rm = TRUE)
    if (!is.finite(den) || den <= 0) {
      target_weights$scaled_weight[idx] <- 0
    } else {
      target_weights$scaled_weight[idx] <- (target_weights$raw_weight[idx] / den) * 2
    }
  }
}

composite_rows <- list()
for (profile in profiles) {
  for (metric in metrics) {
    sub <- target_tbl[target_tbl$profile == profile & target_tbl$metric == metric, , drop = FALSE]
    if (nrow(sub) == 0) next

    target_w <- target_blend[sub$target]
    if (identical(metric, "ball_pct")) {
      target_w[sub$target != "next_whip"] <- 0
    }
    if (sum(target_w, na.rm = TRUE) <= 0) {
      next
    }
    target_w <- target_w / sum(target_w)
    weighted_strength <- sum(sub$effective_strength * target_w, na.rm = TRUE)
    sign_consistency <- mean(sub$quality_aligned_r > 0, na.rm = TRUE)
    composite_strength <- weighted_strength * sign_consistency

    composite_rows[[length(composite_rows) + 1]] <- data.frame(
      profile = profile,
      metric = metric,
      weighted_strength = weighted_strength,
      sign_consistency = sign_consistency,
      composite_strength = composite_strength,
      raw_weight = metric_quality_sign[[metric]] * composite_strength,
      stringsAsFactors = FALSE
    )
  }
}

composite <- do.call(rbind, composite_rows)
composite$scaled_weight <- NA_real_
for (profile in profiles) {
  idx <- composite$profile == profile
  den <- max(abs(composite$raw_weight[idx]), na.rm = TRUE)
  if (!is.finite(den) || den <= 0) {
    composite$scaled_weight[idx] <- 0
  } else {
    composite$scaled_weight[idx] <- (composite$raw_weight[idx] / den) * 2
  }
}

composite$scaled_weight_round2 <- round(composite$scaled_weight, 2)

processed_dir <- cfg$paths$processed_dir
dir.create(processed_dir, recursive = TRUE, showWarnings = FALSE)

target_out <- file.path(processed_dir, "sp_skillz_target_correlations_and_weights.csv")
composite_out <- file.path(processed_dir, "sp_skillz_composite_weights.csv")
meta_out <- file.path(processed_dir, "sp_skillz_weight_derivation_metadata.csv")

utils::write.csv(target_weights, target_out, row.names = FALSE, na = "")
utils::write.csv(composite, composite_out, row.names = FALSE, na = "")

meta <- data.frame(
  run_at_utc = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  base_years = "2022-2024",
  next_years = "2023-2025",
  profiles = paste(profiles, collapse = ","),
  start_share_min = sp$start_share_min,
  low_ip_max_start_ip = sp$low_ip_max_start_ip,
  high_ip_min_start_ip = sp$high_ip_min_start_ip,
  next_ip_min = 40,
  ball_pct_target_scope = "next_whip_only",
  target_blend = paste(sprintf("%s=%.3f", names(target_blend), as.numeric(target_blend)), collapse = ","),
  stringsAsFactors = FALSE
)
utils::write.csv(meta, meta_out, row.names = FALSE, na = "")

fmt_profile <- function(profile_name) {
  x <- composite[composite$profile == profile_name, c("metric", "scaled_weight_round2"), drop = FALSE]
  x <- x[match(c("tbf", "ip_per_gs", metrics), c("tbf", "ip_per_gs", x$metric), nomatch = 0), , drop = FALSE]
  # add explicit zeros for the intentionally zero-weight metrics
  extra <- data.frame(metric = c("tbf", "ip_per_gs"), scaled_weight_round2 = c(0, 0), stringsAsFactors = FALSE)
  x <- rbind(extra, composite[composite$profile == profile_name, c("metric", "scaled_weight_round2"), drop = FALSE])
  x <- x[!duplicated(x$metric), , drop = FALSE]
  x <- x[match(c("tbf", "ip_per_gs", metrics), x$metric), , drop = FALSE]
  paste(sprintf("%s=%s", x$metric, format(x$scaled_weight_round2, trim = TRUE, scientific = FALSE)), collapse = ",")
}

low_text <- fmt_profile("low_ip")
high_text <- fmt_profile("high_ip")

cat("Saved target table: ", target_out, "\n", sep = "")
cat("Saved composite table: ", composite_out, "\n", sep = "")
cat("Saved derivation metadata: ", meta_out, "\n", sep = "")
cat("\nRecommended low_ip_weights:\n", low_text, "\n", sep = "")
cat("\nRecommended high_ip_weights:\n", high_text, "\n", sep = "")
