if (!exists("%||%")) source(file.path("R", "utils.R"))

# ---------------------------------------------------------------------------
# RP Skillz — reliever quality index
#
# Metrics (all scored relative to the qualifying RP pool):
#   velo_max    — max avg fastball-family velo (FA/FT/SI/FC)        [+]
#   stuff_plus  — FG Stuff+, (x-100)/10 scale                       [+]
#   pitching_plus — FG Pitching+, (x-100)/10 scale                  [+]
#   k_pct       — K%                                                 [+]
#   csw_pct     — Called Strikes + Whiff %                           [+]
#   sd_md_net   — Shutdowns minus Meltdowns                          [+]
#   gmli        — game entry Leverage Index (manager trust)          [+]
#
# All metrics carry equal weight (1.0) by default.  Reliability
# columns are computed for display but do NOT modify the composite
# score — samples are too small for reliability weighting to be
# meaningful within a single RP season.
# ---------------------------------------------------------------------------

RP_SKILLZ_METRICS <- c(
  "velo_max",
  "stuff_plus",
  "pitching_plus",
  "k_pct",
  "csw_pct",
  "sd_md_net",
  "gmli"
)

RP_PLUS_METRICS <- c("stuff_plus", "pitching_plus")

# Fastball-family pitch types whose velocity feeds velo_max.
# Ordered by typical usage; first non-NA value wins within each row.
RP_FASTBALL_VELO_COLS <- c("pfxvFA", "pfxvFT", "pfxvSI", "pfxvFC")

DEFAULT_RP_SKILLZ_WEIGHTS <- c(
  velo_max    = 1,
  stuff_plus  = 1,
  pitching_plus = 1,
  k_pct       = 1,
  csw_pct     = 1,
  sd_md_net   = 1,
  gmli        = 1
)

# Stabilization points for *display* reliability only (not used in scoring).
# Units: pitches for pitch-quality metrics, TBF for rate stats,
# relief appearances (relief_g) for leverage/outcome metrics.
DEFAULT_RP_SKILLZ_STABILIZATION_POINTS <- c(
  velo_max    = 50,    # pitches — stabilizes extremely fast
  stuff_plus  = 100,   # pitches
  pitching_plus = 300, # pitches — location needs more data
  k_pct       = 60,    # TBF — faster than BB%
  csw_pct     = 150,   # pitches
  sd_md_net   = 40,    # relief appearances (WPA-based; never fully stable)
  gmli        = 20     # relief appearances
)

# ---------------------------------------------------------------------------
# Helper functions — shared with sp_skillz.R via the same R/ environment
# (normalize_col_key, strip_html_tags, select_first_match, as_numeric_clean,
#  baseball_ip_to_decimal, z_score_vector, z_score_from_reference,
#  build_fg_api_url, compute_metric_reliability, plus_metric_scale are all
#  defined in sp_skillz.R and available when both are sourced)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Data ingestion
# ---------------------------------------------------------------------------

read_rp_skillz_raw <- function(leaderboard_url = "", local_csv_path = "") {
  if (nzchar(local_csv_path)) {
    if (!file.exists(local_csv_path)) {
      stop(sprintf("local_csv_path not found: %s", local_csv_path))
    }
    out <- utils::read.csv(local_csv_path, stringsAsFactors = FALSE, check.names = FALSE)
    if (nrow(out) == 0) stop("RP Skillz local CSV has zero rows.")
    return(out)
  }

  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required to fetch FanGraphs API data.")
  }

  # Use the shared build_fg_api_url() from sp_skillz.R
  api_url <- build_fg_api_url(leaderboard_url, page_items = 2000)
  payload <- tryCatch(
    jsonlite::fromJSON(api_url, flatten = TRUE),
    error = function(e) stop(sprintf("Failed reading FanGraphs API data: %s", conditionMessage(e)))
  )

  if (!is.list(payload) || !("data" %in% names(payload))) {
    stop("FanGraphs API response did not include a 'data' field.")
  }
  out <- as.data.frame(payload$data, stringsAsFactors = FALSE)
  if (nrow(out) == 0) stop("RP Skillz API returned zero rows.")
  out
}

# ---------------------------------------------------------------------------
# Standardize raw FanGraphs data into a clean RP Skillz schema
# ---------------------------------------------------------------------------

standardize_rp_skillz_input <- function(raw, lookback_season = NA_integer_) {
  # --- Identity ---
  player_id   <- as_numeric_clean(select_first_match(raw, c("xMLBAMID", "xmlbamid", "mlbamid", "playerid")))
  player_name <- strip_html_tags(select_first_match(raw, c("Name", "name", "player", "playername")))
  team        <- strip_html_tags(select_first_match(raw, c("Team", "team", "tm")))

  # --- Playing time ---
  g   <- as_numeric_clean(select_first_match(raw, c("G", "g", "games")))
  gs  <- as_numeric_clean(select_first_match(raw, c("GS", "gs", "games started")))
  ip  <- baseball_ip_to_decimal(select_first_match(raw, c("IP", "ip", "innings pitched")))
  tbf <- as_numeric_clean(select_first_match(raw, c("TBF", "tbf", "bf", "batters faced")))
  pitches <- as_numeric_clean(select_first_match(raw, c("Pitches", "pitches", "pit")))

  start_ip  <- baseball_ip_to_decimal(select_first_match(raw, c("Start-IP", "start-ip", "starter ip")))
  relief_ip <- baseball_ip_to_decimal(select_first_match(raw, c("Relief-IP", "relief-ip", "reliever ip")))

  # --- Core skill metrics ---
  k_pct   <- as_numeric_clean(select_first_match(raw, c("K%", "k%", "k pct", "so%")))
  csw_pct <- as_numeric_clean(select_first_match(raw, c("C+SwStr%", "c+swstr%", "csw%", "csw pct")))

  stuff_plus   <- as_numeric_clean(select_first_match(raw, c("sp_stuff",   "stuff+",   "stuff plus")))
  pitching_plus <- as_numeric_clean(select_first_match(raw, c("sp_pitching", "pitching+", "pitching plus")))

  # --- Velocity: max across fastball-family pitch types ---
  # Use pfx columns for per-type averages; any non-zero / non-NA column counts.
  velo_mat <- vapply(RP_FASTBALL_VELO_COLS, function(col) {
    v <- as_numeric_clean(select_first_match(raw, c(col, tolower(col))))
    v[!is.na(v) & v == 0] <- NA_real_   # 0 = pitch type not thrown
    v
  }, numeric(nrow(raw)))
  if (is.null(dim(velo_mat))) dim(velo_mat) <- c(length(velo_mat), 1)
  velo_max <- apply(velo_mat, 1, function(row) {
    vals <- row[!is.na(row)]
    if (length(vals) == 0) NA_real_ else max(vals)
  })

  # --- Outcome / role metrics ---
  sd_raw    <- as_numeric_clean(select_first_match(raw, c("SD", "sd", "shutdowns")))
  md_raw    <- as_numeric_clean(select_first_match(raw, c("MD", "md", "meltdowns")))
  sd_md_net <- ifelse(!is.na(sd_raw) & !is.na(md_raw), sd_raw - md_raw, NA_real_)
  gmli      <- as_numeric_clean(select_first_match(raw, c("gmLI", "gmli", "gm li", "game li")))

  out <- data.frame(
    player_id     = as.integer(round(player_id)),
    player_name   = trimws(player_name),
    team          = toupper(trimws(team)),
    g             = g,
    gs            = gs,
    ip            = ip,
    tbf           = tbf,
    pitches       = pitches,
    start_ip      = start_ip,
    relief_ip     = relief_ip,
    k_pct         = k_pct,
    csw_pct       = csw_pct,
    stuff_plus    = stuff_plus,
    pitching_plus = pitching_plus,
    velo_max      = velo_max,
    sd_raw        = sd_raw,
    md_raw        = md_raw,
    sd_md_net     = sd_md_net,
    gmli          = gmli,
    lookback_season = as.integer(lookback_season),
    stringsAsFactors = FALSE
  )

  # Drop rows with no player name
  out <- out[nzchar(out$player_name), , drop = FALSE]
  rownames(out) <- NULL

  # Derived columns
  out$relief_g <- pmax(0, out$g - out$gs)

  # relief_share: fraction of total IP pitched in relief
  start_ip_f  <- ifelse(is.na(out$start_ip),  0, out$start_ip)
  relief_ip_f <- ifelse(is.na(out$relief_ip), 0, out$relief_ip)
  total_split  <- start_ip_f + relief_ip_f
  out$relief_share <- ifelse(total_split > 0, relief_ip_f / total_split, NA_real_)

  out
}

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

validate_rp_skillz_weights <- function(weights = DEFAULT_RP_SKILLZ_WEIGHTS) {
  if (is.null(weights) || length(weights) == 0) stop("RP Skillz weights cannot be empty.")
  if (is.null(names(weights)) || any(!nzchar(names(weights)))) {
    stop("RP Skillz weights must be a named vector.")
  }
  missing <- setdiff(RP_SKILLZ_METRICS, names(weights))
  if (length(missing) > 0) {
    stop(sprintf("RP Skillz weights missing metrics: %s", paste(missing, collapse = ", ")))
  }
  out <- as.numeric(weights[RP_SKILLZ_METRICS])
  names(out) <- RP_SKILLZ_METRICS
  if (any(is.na(out))) stop("RP Skillz weights must be numeric.")
  out
}

validate_rp_skillz_stabilization_points <- function(stab = DEFAULT_RP_SKILLZ_STABILIZATION_POINTS) {
  if (is.null(stab) || length(stab) == 0) stop("RP Skillz stabilization_points cannot be empty.")
  missing <- setdiff(RP_SKILLZ_METRICS, names(stab))
  if (length(missing) > 0) {
    stop(sprintf("RP Skillz stabilization_points missing metrics: %s", paste(missing, collapse = ", ")))
  }
  out <- as.numeric(stab[RP_SKILLZ_METRICS])
  names(out) <- RP_SKILLZ_METRICS
  if (any(is.na(out))) stop("RP Skillz stabilization_points must be numeric.")
  if (any(out < 0))   stop("RP Skillz stabilization_points cannot be negative.")
  out
}

# ---------------------------------------------------------------------------
# Sample source for each metric (for reliability display)
# ---------------------------------------------------------------------------

rp_metric_sample_source_for <- function(metric) {
  switch(
    metric,
    velo_max      = "pitches",
    stuff_plus    = "pitches",
    pitching_plus = "pitches",
    k_pct         = "tbf",
    csw_pct       = "pitches",
    sd_md_net     = "relief_g",
    gmli          = "relief_g",
    "tbf"   # fallback
  )
}

build_rp_metric_sample_matrix <- function(skillz_data) {
  n   <- nrow(skillz_data)
  out <- matrix(NA_real_, nrow = n, ncol = length(RP_SKILLZ_METRICS))
  colnames(out) <- RP_SKILLZ_METRICS

  for (metric in RP_SKILLZ_METRICS) {
    src     <- rp_metric_sample_source_for(metric)
    samples <- suppressWarnings(as.numeric(skillz_data[[src]]))
    samples[!is.na(samples) & samples < 0] <- 0
    out[, metric] <- samples
  }
  out
}

# ---------------------------------------------------------------------------
# Standardized metric matrix (z-scores / plus scaling)
# ---------------------------------------------------------------------------

build_rp_metric_standardized_matrix <- function(skillz_data, reference_data = NULL) {
  out <- matrix(NA_real_, nrow = nrow(skillz_data), ncol = length(RP_SKILLZ_METRICS))
  colnames(out) <- RP_SKILLZ_METRICS

  for (metric in RP_SKILLZ_METRICS) {
    values <- as.numeric(skillz_data[[metric]])

    if (metric %in% RP_PLUS_METRICS) {
      out[, metric] <- plus_metric_scale(values)   # (x-100)/10
      next
    }

    if (is.null(reference_data)) {
      out[, metric] <- z_score_vector(values)
    } else {
      ref_vals <- as.numeric(reference_data[[metric]])
      out[, metric] <- z_score_from_reference(
        values,
        center = mean(ref_vals, na.rm = TRUE),
        spread = stats::sd(ref_vals, na.rm = TRUE)
      )
    }
  }

  out
}

# ---------------------------------------------------------------------------
# Main scoring function
# ---------------------------------------------------------------------------

compute_rp_skillz_scores <- function(
  skillz_data,
  weights                = DEFAULT_RP_SKILLZ_WEIGHTS,
  stabilization_points   = DEFAULT_RP_SKILLZ_STABILIZATION_POINTS,
  reliability_method     = "sample_over_sample_plus_stab",
  relief_share_min       = 0.50,
  min_relief_g           = 5,
  num_teams              = 15,
  rp_depth               = 3
) {
  weights              <- validate_rp_skillz_weights(weights)
  stabilization_points <- validate_rp_skillz_stabilization_points(stabilization_points)

  # --- Eligibility filter ---
  # Keep pitchers who work primarily in relief AND have a minimum workload.
  # relief_share NA → assume eligible if relief_g passes (catches pure RP
  # rows where start_ip / relief_ip were both missing).
  elig <- with(skillz_data, {
    share_ok <- is.na(relief_share) | (relief_share >= relief_share_min)
    g_ok     <- !is.na(relief_g) & (relief_g >= min_relief_g)
    share_ok & g_ok
  })

  if (sum(elig) == 0) {
    stop("No qualifying relievers after applying relief_share_min / min_relief_g filters.")
  }

  rp_pool <- skillz_data[elig, , drop = FALSE]
  rownames(rp_pool) <- NULL

  # --- z-score matrix (against the qualifying RP pool itself) ---
  z_mat <- build_rp_metric_standardized_matrix(rp_pool)

  # --- Flat weight vector (equal weights; no paradigm logic needed) ---
  w <- weights[colnames(z_mat)]

  # --- Composite score: weighted sum of z-scores ---
  score_raw <- as.numeric(z_mat %*% w)

  # --- Reliability for each metric (display only; not applied to score) ---
  sample_mat <- build_rp_metric_sample_matrix(rp_pool)
  rel_mat <- matrix(NA_real_, nrow = nrow(rp_pool), ncol = length(RP_SKILLZ_METRICS))
  colnames(rel_mat) <- RP_SKILLZ_METRICS
  for (metric in RP_SKILLZ_METRICS) {
    rel_mat[, metric] <- compute_metric_reliability(
      sample_mat[, metric],
      stabilization_points[metric],
      method = reliability_method
    )
  }

  # --- Assemble output ---
  out <- rp_pool
  out$rp_skillz_score <- round(score_raw, 4)
  out$rp_skillz_rank  <- rank(-score_raw, ties.method = "min", na.last = "keep")

  # Flag pitchers in the "rostered" RP pool (top num_teams × rp_depth)
  roster_cutoff <- num_teams * rp_depth
  out$rp_skillz_pool_flag <- out$rp_skillz_rank <= roster_cutoff

  # Per-metric z-scores and reliabilities
  for (metric in RP_SKILLZ_METRICS) {
    out[[paste0("z_", metric)]]   <- round(z_mat[, metric], 4)
    out[[paste0("rel_", metric)]] <- round(rel_mat[, metric], 3)
    out[[paste0("n_", metric)]]   <- sample_mat[, metric]
    out[[paste0("w_", metric)]]   <- w[metric]
  }

  out <- out[order(out$rp_skillz_rank, out$player_name, na.last = TRUE), , drop = FALSE]
  rownames(out) <- NULL
  out
}

# ---------------------------------------------------------------------------
# Convenience wrapper: fetch → standardize → score
# ---------------------------------------------------------------------------

fetch_and_score_rp_skillz <- function(
  leaderboard_url,
  local_csv_path       = "",
  lookback_season      = NA_integer_,
  weights              = DEFAULT_RP_SKILLZ_WEIGHTS,
  stabilization_points = DEFAULT_RP_SKILLZ_STABILIZATION_POINTS,
  reliability_method   = "sample_over_sample_plus_stab",
  relief_share_min     = 0.50,
  min_relief_g         = 5,
  num_teams            = 15,
  rp_depth             = 3
) {
  raw          <- read_rp_skillz_raw(leaderboard_url = leaderboard_url, local_csv_path = local_csv_path)
  standardized <- standardize_rp_skillz_input(raw, lookback_season = lookback_season)
  scores       <- compute_rp_skillz_scores(
    skillz_data          = standardized,
    weights              = weights,
    stabilization_points = stabilization_points,
    reliability_method   = reliability_method,
    relief_share_min     = relief_share_min,
    min_relief_g         = min_relief_g,
    num_teams            = num_teams,
    rp_depth             = rp_depth
  )

  list(
    raw          = raw,
    standardized = standardized,
    scores       = scores
  )
}
