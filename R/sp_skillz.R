if (!exists("%||%")) {
  `%||%` <- function(x, y) {
    if (is.null(x)) y else x
  }
}

SP_SKILLZ_METRICS <- c(
  "tbf",
  "ip_per_gs",
  "siera",
  "xfip",
  "k_minus_bb_pct",
  "contact_pct",
  "csw_pct",
  "ball_pct",
  "stuff_plus",
  "pitching_plus"
)

SP_PLUS_METRICS <- c("stuff_plus", "pitching_plus")

DEFAULT_SP_SKILLZ_WEIGHTS <- c(
  tbf = 0,
  ip_per_gs = 0,
  siera = -1,
  xfip = -1,
  k_minus_bb_pct = 1,
  contact_pct = -1,
  csw_pct = 1,
  ball_pct = -1,
  stuff_plus = 1.5,
  pitching_plus = 2
)

# Low-IP paradigm emphasizes metrics that held up earlier in-sample.
DEFAULT_LOW_IP_PARADIGM_WEIGHTS <- c(
  tbf = 0,
  ip_per_gs = 0,
  siera = -0.76,
  xfip = -0.42,
  k_minus_bb_pct = 1.23,
  contact_pct = -2.0,
  csw_pct = 1.26,
  ball_pct = -0.01,
  stuff_plus = 1.75,
  pitching_plus = 1.15
)

# High-IP paradigm leans into robust run-prevention and K-BB estimators.
DEFAULT_HIGH_IP_PARADIGM_WEIGHTS <- c(
  tbf = 0,
  ip_per_gs = 0,
  siera = -1.61,
  xfip = -1.34,
  k_minus_bb_pct = 2.0,
  contact_pct = -1.73,
  csw_pct = 1.19,
  ball_pct = -0.58,
  stuff_plus = 1.56,
  pitching_plus = 1.64
)

# Stabilization points by metric (unit depends on metric sample source).
# These are used to build per-metric reliability for stabilized scoring.
DEFAULT_SP_SKILLZ_STABILIZATION_POINTS <- c(
  tbf = 1,
  ip_per_gs = 1,
  siera = 30,
  xfip = 30,
  k_minus_bb_pct = 120,
  contact_pct = 150,
  csw_pct = 200,
  ball_pct = 150,
  stuff_plus = 100,
  pitching_plus = 300
)

normalize_col_key <- function(x) {
  x <- tolower(as.character(x))
  gsub("[^a-z0-9]", "", x)
}

strip_html_tags <- function(x) {
  x <- as.character(x)
  x <- gsub("<[^>]+>", "", x)
  x <- gsub("&amp;", "&", x, fixed = TRUE)
  x <- gsub("&nbsp;", " ", x, fixed = TRUE)
  trimws(x)
}

select_first_match <- function(data, candidates) {
  original_names <- names(data)
  normalized_names <- normalize_col_key(original_names)
  normalized_candidates <- normalize_col_key(candidates)

  idx <- match(normalized_candidates, normalized_names, nomatch = 0)
  idx <- idx[idx > 0]

  if (length(idx) == 0) {
    return(rep(NA, nrow(data)))
  }

  data[[original_names[idx[1]]]]
}

as_numeric_clean <- function(x) {
  if (is.numeric(x)) {
    return(as.numeric(x))
  }

  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x <- gsub(",", "", x, fixed = TRUE)
  x <- gsub("%", "", x, fixed = TRUE)
  suppressWarnings(as.numeric(x))
}

baseball_ip_to_decimal <- function(x) {
  x_num <- as_numeric_clean(x)
  out <- rep(NA_real_, length(x_num))
  keep <- !is.na(x_num)
  if (!any(keep)) {
    return(out)
  }

  whole <- floor(x_num[keep])
  frac_digit <- round((x_num[keep] - whole) * 10)
  frac_component <- ifelse(
    frac_digit == 1, 1 / 3,
    ifelse(frac_digit == 2, 2 / 3, x_num[keep] - whole)
  )
  out[keep] <- whole + frac_component
  out
}

z_score_vector <- function(x) {
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) {
    return(out)
  }

  x_keep <- x[keep]
  center <- mean(x_keep)
  spread <- stats::sd(x_keep)

  if (is.na(spread) || spread == 0) {
    out[keep] <- 0
    return(out)
  }

  out[keep] <- (x_keep - center) / spread
  out
}

z_score_from_reference <- function(x, center, spread) {
  out <- rep(NA_real_, length(x))
  keep <- !is.na(x)
  if (!any(keep)) {
    return(out)
  }

  if (is.na(spread) || spread == 0) {
    out[keep] <- 0
    return(out)
  }

  out[keep] <- (x[keep] - center) / spread
  out
}

parse_query_string <- function(query) {
  out <- character(0)
  if (!nzchar(query)) {
    return(out)
  }

  tokens <- strsplit(query, "&", fixed = TRUE)[[1]]
  for (token in tokens) {
    token <- trimws(token)
    if (!nzchar(token)) {
      next
    }
    if (grepl("=", token, fixed = TRUE)) {
      key <- sub("=.*$", "", token)
      val <- sub("^[^=]*=", "", token)
    } else {
      key <- token
      val <- ""
    }
    out[[key]] <- val
  }
  out
}

rebuild_query_string <- function(params) {
  if (length(params) == 0) {
    return("")
  }
  parts <- vapply(names(params), function(k) {
    v <- params[[k]]
    if (!nzchar(v)) {
      k
    } else {
      paste0(k, "=", v)
    }
  }, character(1))
  paste(parts, collapse = "&")
}

build_fg_api_url <- function(leaderboard_url, page_items = 2000) {
  if (!nzchar(leaderboard_url)) {
    stop("leaderboard_url cannot be empty.")
  }

  url_parts <- strsplit(leaderboard_url, "\\?", perl = TRUE)[[1]]
  query <- if (length(url_parts) > 1) paste(url_parts[-1], collapse = "?") else ""
  params <- parse_query_string(query)
  if (length(params) > 0 && "csv" %in% names(params)) {
    params <- params[names(params) != "csv"]
  }
  if (length(params) > 0 && "pageItems" %in% names(params)) {
    params <- params[names(params) != "pageItems"]
  }
  params[["pageitems"]] <- as.character(as.integer(page_items))

  query_out <- rebuild_query_string(params)
  base_url <- "https://www.fangraphs.com/api/leaders/major-league/data"
  if (nzchar(query_out)) {
    paste0(base_url, "?", query_out)
  } else {
    base_url
  }
}

read_sp_skillz_raw <- function(leaderboard_url = "", local_csv_path = "") {
  if (nzchar(local_csv_path)) {
    if (!file.exists(local_csv_path)) {
      stop(sprintf("local_csv_path not found: %s", local_csv_path))
    }
    out <- utils::read.csv(local_csv_path, stringsAsFactors = FALSE, check.names = FALSE)
    if (nrow(out) == 0) {
      stop("SP Skillz local CSV has zero rows.")
    }
    return(out)
  }

  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required to fetch Fangraphs API data.")
  }

  api_url <- build_fg_api_url(leaderboard_url, page_items = 2000)
  payload <- tryCatch(
    jsonlite::fromJSON(api_url, flatten = TRUE),
    error = function(e) {
      stop(sprintf("Failed reading Fangraphs API data: %s", conditionMessage(e)))
    }
  )

  if (!is.list(payload) || !("data" %in% names(payload))) {
    stop("Fangraphs API response did not include a 'data' field.")
  }
  out <- as.data.frame(payload$data, stringsAsFactors = FALSE)
  if (nrow(out) == 0) {
    stop("SP Skillz API returned zero rows.")
  }
  out
}

standardize_sp_skillz_input <- function(raw, lookback_season = NA_integer_) {
  out <- data.frame(
    player_id = as_numeric_clean(select_first_match(raw, c("xmlbamid", "mlbam id", "mlbamid", "playerid", "player_id"))),
    player_name = strip_html_tags(select_first_match(raw, c("name", "player", "playername"))),
    team = strip_html_tags(select_first_match(raw, c("team", "tm"))),
    tbf = as_numeric_clean(select_first_match(raw, c("tbf", "bf", "batters faced"))),
    ip = baseball_ip_to_decimal(select_first_match(raw, c("ip", "innings pitched", "inn"))),
    gs = as_numeric_clean(select_first_match(raw, c("gs", "games started"))),
    siera = as_numeric_clean(select_first_match(raw, c("siera"))),
    xfip = as_numeric_clean(select_first_match(raw, c("xfip"))),
    k_minus_bb_pct = as_numeric_clean(select_first_match(raw, c("k-bb%", "kbb%", "k-bb pct", "k-bb"))),
    contact_pct = as_numeric_clean(select_first_match(raw, c("contact%", "contact pct", "ct%"))),
    csw_pct = as_numeric_clean(select_first_match(raw, c("c+swstr%", "csw%", "csw pct"))),
    balls = as_numeric_clean(select_first_match(raw, c("balls"))),
    pitches = as_numeric_clean(select_first_match(raw, c("pitches", "pit"))),
    ball_pct_raw = as_numeric_clean(select_first_match(raw, c("ball%", "ball pct"))),
    stuff_plus = as_numeric_clean(select_first_match(raw, c("sp_stuff", "stuff+", "stuff plus"))),
    pitching_plus = as_numeric_clean(select_first_match(raw, c("sp_pitching", "pitching+", "pitching plus"))),
    start_ip = baseball_ip_to_decimal(select_first_match(raw, c("start-ip", "starter ip", "ip as sp", "sp ip"))),
    relief_ip = baseball_ip_to_decimal(select_first_match(raw, c("relief-ip", "reliever ip", "ip as rp", "rp ip"))),
    start_ip_relief_ip_raw = as_numeric_clean(select_first_match(raw, c("start-ip/relief-ip", "start ip/relief ip", "startip/reliefip", "sp ip ratio"))),
    stringsAsFactors = FALSE
  )

  out$player_name[is.na(out$player_name)] <- ""
  out$player_name <- trimws(out$player_name)
  out <- out[nzchar(out$player_name), , drop = FALSE]
  rownames(out) <- NULL

  out$team[is.na(out$team)] <- ""
  out$team <- toupper(trimws(out$team))
  out$player_id <- as.integer(round(out$player_id))

  out$ip_per_gs <- ifelse(!is.na(out$gs) & out$gs > 0 & !is.na(out$ip), out$ip / out$gs, NA_real_)

  out$ball_pct <- ifelse(
    !is.na(out$pitches) & out$pitches > 0 & !is.na(out$balls),
    (out$balls / out$pitches) * 100,
    out$ball_pct_raw
  )

  out$start_share <- NA_real_
  has_split_ip <- (!is.na(out$start_ip) | !is.na(out$relief_ip))
  out$start_ip_filled <- out$start_ip
  out$relief_ip_filled <- out$relief_ip
  out$start_ip_filled[has_split_ip & is.na(out$start_ip_filled)] <- 0
  out$relief_ip_filled[has_split_ip & is.na(out$relief_ip_filled)] <- 0

  valid_split <- has_split_ip & (out$start_ip_filled + out$relief_ip_filled) > 0
  if (any(valid_split)) {
    out$start_share[valid_split] <- out$start_ip_filled[valid_split] / (out$start_ip_filled[valid_split] + out$relief_ip_filled[valid_split])
  }

  needs_ratio_fallback <- is.na(out$start_share) & !is.na(out$start_ip_relief_ip_raw)
  if (any(needs_ratio_fallback)) {
    ratio <- out$start_ip_relief_ip_raw[needs_ratio_fallback]
    out$start_share[needs_ratio_fallback] <- ifelse(
      ratio > 1,
      ratio / (1 + ratio),
      ifelse(ratio > 0 & ratio <= 1, ratio, NA_real_)
    )
  }

  out$lookback_season <- as.integer(lookback_season)
  out
}

validate_sp_skillz_weights <- function(weights = DEFAULT_SP_SKILLZ_WEIGHTS) {
  if (is.null(weights) || length(weights) == 0) {
    stop("SP Skillz weights cannot be empty.")
  }
  if (is.null(names(weights)) || any(!nzchar(names(weights)))) {
    stop("SP Skillz weights must be a named vector.")
  }

  missing <- setdiff(SP_SKILLZ_METRICS, names(weights))
  if (length(missing) > 0) {
    stop(sprintf("SP Skillz weights missing metrics: %s", paste(missing, collapse = ", ")))
  }

  out <- as.numeric(weights[SP_SKILLZ_METRICS])
  names(out) <- SP_SKILLZ_METRICS
  if (any(is.na(out))) {
    stop("SP Skillz weights must be numeric.")
  }
  out
}

validate_sp_skillz_stabilization_points <- function(stabilization_points = DEFAULT_SP_SKILLZ_STABILIZATION_POINTS) {
  if (is.null(stabilization_points) || length(stabilization_points) == 0) {
    stop("SP Skillz stabilization_points cannot be empty.")
  }
  if (is.null(names(stabilization_points)) || any(!nzchar(names(stabilization_points)))) {
    stop("SP Skillz stabilization_points must be a named vector.")
  }

  missing <- setdiff(SP_SKILLZ_METRICS, names(stabilization_points))
  if (length(missing) > 0) {
    stop(sprintf("SP Skillz stabilization_points missing metrics: %s", paste(missing, collapse = ", ")))
  }

  out <- as.numeric(stabilization_points[SP_SKILLZ_METRICS])
  names(out) <- SP_SKILLZ_METRICS
  if (any(is.na(out))) {
    stop("SP Skillz stabilization_points must be numeric.")
  }
  if (any(out < 0)) {
    stop("SP Skillz stabilization_points cannot be negative.")
  }
  out
}

metric_sample_source_for <- function(metric) {
  switch(
    metric,
    tbf = "tbf",
    ip_per_gs = "gs",
    siera = "start_ip",
    xfip = "start_ip",
    k_minus_bb_pct = "tbf",
    contact_pct = "tbf",
    csw_pct = "pitches",
    ball_pct = "pitches",
    stuff_plus = "pitches",
    pitching_plus = "pitches",
    "tbf"
  )
}

build_metric_sample_matrix <- function(skillz_data) {
  n <- nrow(skillz_data)
  out <- matrix(NA_real_, nrow = n, ncol = length(SP_SKILLZ_METRICS))
  colnames(out) <- SP_SKILLZ_METRICS

  start_ip_or_ip <- suppressWarnings(as.numeric(skillz_data$start_ip))
  ip_vals <- suppressWarnings(as.numeric(skillz_data$ip))
  use_ip_fallback <- is.na(start_ip_or_ip) & !is.na(ip_vals)
  start_ip_or_ip[use_ip_fallback] <- ip_vals[use_ip_fallback]

  for (metric in SP_SKILLZ_METRICS) {
    source <- metric_sample_source_for(metric)
    samples <- suppressWarnings(as.numeric(skillz_data[[source]]))
    samples[!is.na(samples) & samples < 0] <- 0

    if (metric %in% c("siera", "xfip")) {
      samples <- start_ip_or_ip
      samples[!is.na(samples) & samples < 0] <- 0
    }

    out[, metric] <- samples
  }

  out
}

compute_metric_reliability <- function(samples, stabilization_point, method = "sample_over_sample_plus_stab") {
  out <- rep(NA_real_, length(samples))
  keep <- !is.na(samples)
  if (!any(keep)) {
    return(out)
  }

  if (!is.finite(stabilization_point) || stabilization_point <= 0) {
    out[keep] <- 1
    return(out)
  }

  s <- pmax(samples[keep], 0)
  if (identical(method, "linear_cap")) {
    out[keep] <- pmin(1, s / stabilization_point)
  } else if (identical(method, "sample_over_sample_plus_stab")) {
    out[keep] <- s / (s + stabilization_point)
  } else {
    stop(sprintf("Unknown reliability_method: %s", method))
  }
  out
}

plus_metric_scale <- function(x) {
  (x - 100) / 10
}

build_metric_standardized_matrix <- function(skillz_data, reference_data = NULL) {
  out <- matrix(NA_real_, nrow = nrow(skillz_data), ncol = length(SP_SKILLZ_METRICS))
  colnames(out) <- SP_SKILLZ_METRICS

  for (metric in SP_SKILLZ_METRICS) {
    values <- as.numeric(skillz_data[[metric]])
    if (metric %in% SP_PLUS_METRICS) {
      out[, metric] <- plus_metric_scale(values)
      next
    }

    if (is.null(reference_data)) {
      out[, metric] <- z_score_vector(values)
    } else {
      ref_values <- as.numeric(reference_data[[metric]])
      out[, metric] <- z_score_from_reference(values, center = mean(ref_values, na.rm = TRUE), spread = stats::sd(ref_values, na.rm = TRUE))
    }
  }

  out
}

build_effective_weight_matrix <- function(
  skillz_data,
  default_weights = DEFAULT_SP_SKILLZ_WEIGHTS,
  use_ip_paradigms = TRUE,
  low_ip_max_start_ip = 80,
  high_ip_min_start_ip = 100,
  low_ip_weights = DEFAULT_LOW_IP_PARADIGM_WEIGHTS,
  high_ip_weights = DEFAULT_HIGH_IP_PARADIGM_WEIGHTS,
  gs_fallback_threshold = 5
) {
  default_weights <- validate_sp_skillz_weights(default_weights)
  low_ip_weights <- validate_sp_skillz_weights(low_ip_weights)
  high_ip_weights <- validate_sp_skillz_weights(high_ip_weights)

  if (high_ip_min_start_ip < low_ip_max_start_ip) {
    stop("high_ip_min_start_ip must be greater than or equal to low_ip_max_start_ip.")
  }

  n <- nrow(skillz_data)
  weight_mat <- matrix(NA_real_, nrow = n, ncol = length(SP_SKILLZ_METRICS))
  colnames(weight_mat) <- SP_SKILLZ_METRICS
  profile <- rep("default", n)

  for (i in seq_len(n)) {
    row_weights <- default_weights
    row_profile <- "default"

    if (isTRUE(use_ip_paradigms)) {
      start_ip <- suppressWarnings(as.numeric(skillz_data$start_ip[i]))
      gs <- suppressWarnings(as.numeric(skillz_data$gs[i]))

      if (!is.na(start_ip)) {
        if (start_ip <= low_ip_max_start_ip) {
          row_weights <- low_ip_weights
          row_profile <- "low_ip"
        } else if (start_ip >= high_ip_min_start_ip) {
          row_weights <- high_ip_weights
          row_profile <- "high_ip"
        } else if (high_ip_min_start_ip > low_ip_max_start_ip) {
          alpha <- (start_ip - low_ip_max_start_ip) / (high_ip_min_start_ip - low_ip_max_start_ip)
          alpha <- min(max(alpha, 0), 1)
          row_weights <- ((1 - alpha) * low_ip_weights) + (alpha * high_ip_weights)
          row_profile <- "blended_ip"
        } else {
          row_weights <- high_ip_weights
          row_profile <- "high_ip"
        }
      } else if (!is.na(gs) && gs <= gs_fallback_threshold) {
        row_weights <- low_ip_weights
        row_profile <- "low_ip_gs_fallback"
      } else {
        row_weights <- high_ip_weights
        row_profile <- "high_ip_gs_fallback"
      }
    }

    weight_mat[i, ] <- as.numeric(row_weights[SP_SKILLZ_METRICS])
    profile[i] <- row_profile
  }

  list(
    weight_matrix = weight_mat,
    weight_profile = profile
  )
}

weighted_z_sum <- function(z_row, weight_row, target_abs_weight, denominator_weights = NULL) {
  keep <- !is.na(z_row) & !is.na(weight_row) & weight_row != 0
  if (!any(keep)) {
    return(NA_real_)
  }

  denom_vec <- if (is.null(denominator_weights)) weight_row else denominator_weights
  denom_keep <- !is.na(denom_vec) & denom_vec != 0 & keep
  if (!any(denom_keep)) {
    return(NA_real_)
  }

  denom <- sum(abs(denom_vec[denom_keep]))
  if (!is.finite(denom) || denom <= 0) {
    return(NA_real_)
  }

  standardized_sum <- sum(z_row[keep] * weight_row[keep]) / denom
  standardized_sum * target_abs_weight
}

compute_sp_skillz_scores <- function(
  skillz_data,
  weights = DEFAULT_SP_SKILLZ_WEIGHTS,
  num_teams = 15,
  sp_depth = 10,
  start_share_min = 2 / 3,
  use_ip_paradigms = TRUE,
  low_ip_max_start_ip = 80,
  high_ip_min_start_ip = 100,
  low_ip_weights = DEFAULT_LOW_IP_PARADIGM_WEIGHTS,
  high_ip_weights = DEFAULT_HIGH_IP_PARADIGM_WEIGHTS,
  gs_fallback_threshold = 5,
  stabilization_points = DEFAULT_SP_SKILLZ_STABILIZATION_POINTS,
  reliability_method = "sample_over_sample_plus_stab"
) {
  if (!is.numeric(num_teams) || length(num_teams) != 1 || is.na(num_teams) || num_teams < 1) {
    stop("num_teams must be a single positive number.")
  }
  if (!is.numeric(sp_depth) || length(sp_depth) != 1 || is.na(sp_depth) || sp_depth < 1) {
    stop("sp_depth must be a single positive number.")
  }
  if (!is.numeric(start_share_min) || length(start_share_min) != 1 || is.na(start_share_min) || start_share_min < 0 || start_share_min > 1) {
    stop("start_share_min must be between 0 and 1.")
  }

  weights <- validate_sp_skillz_weights(weights)
  low_ip_weights <- validate_sp_skillz_weights(low_ip_weights)
  high_ip_weights <- validate_sp_skillz_weights(high_ip_weights)
  stabilization_points <- validate_sp_skillz_stabilization_points(stabilization_points)

  required <- c("player_name", "team", "start_share", "start_ip", "gs", SP_SKILLZ_METRICS)
  missing <- setdiff(required, names(skillz_data))
  if (length(missing) > 0) {
    stop(sprintf("SP Skillz input missing required columns: %s", paste(missing, collapse = ", ")))
  }

  keep_mask <- !is.na(skillz_data$start_share) & skillz_data$start_share >= start_share_min
  out <- skillz_data[keep_mask, , drop = FALSE]
  rownames(out) <- NULL

  if (nrow(out) == 0) {
    stop("No rows remain after applying SP start-share filter.")
  }

  z_global <- build_metric_standardized_matrix(out, reference_data = NULL)

  weight_setup <- build_effective_weight_matrix(
    skillz_data = out,
    default_weights = weights,
    use_ip_paradigms = use_ip_paradigms,
    low_ip_max_start_ip = low_ip_max_start_ip,
    high_ip_min_start_ip = high_ip_min_start_ip,
    low_ip_weights = low_ip_weights,
    high_ip_weights = high_ip_weights,
    gs_fallback_threshold = gs_fallback_threshold
  )
  weight_mat <- weight_setup$weight_matrix
  out$sp_skillz_weight_profile <- as.character(weight_setup$weight_profile)

  sample_mat <- build_metric_sample_matrix(out)
  reliability_mat <- matrix(NA_real_, nrow = nrow(out), ncol = length(SP_SKILLZ_METRICS))
  colnames(reliability_mat) <- SP_SKILLZ_METRICS
  for (metric in SP_SKILLZ_METRICS) {
    reliability_mat[, metric] <- compute_metric_reliability(
      samples = sample_mat[, metric],
      stabilization_point = stabilization_points[[metric]],
      method = reliability_method
    )
  }
  effective_weight_mat <- weight_mat * reliability_mat

  row_abs_weight <- rowSums(abs(weight_mat), na.rm = TRUE)
  row_abs_effective_weight <- rowSums(abs(effective_weight_mat), na.rm = TRUE)
  row_reliability <- ifelse(row_abs_weight > 0, row_abs_effective_weight / row_abs_weight, NA_real_)

  target_abs_weight <- max(
    sum(abs(weights[weights != 0])),
    sum(abs(low_ip_weights[low_ip_weights != 0])),
    sum(abs(high_ip_weights[high_ip_weights != 0])),
    na.rm = TRUE
  )
  if (!is.finite(target_abs_weight) || target_abs_weight <= 0) {
    stop("SP Skillz weights must include at least one non-zero value.")
  }

  out$sp_skillz_score_initial <- vapply(
    seq_len(nrow(out)),
    function(i) weighted_z_sum(z_global[i, ], weight_mat[i, ], target_abs_weight),
    numeric(1)
  )
  out$sp_skillz_score_initial_stabilized <- vapply(
    seq_len(nrow(out)),
    function(i) weighted_z_sum(
      z_global[i, ],
      effective_weight_mat[i, ],
      target_abs_weight,
      denominator_weights = weight_mat[i, ]
    ),
    numeric(1)
  )
  out$sp_skillz_reliability_initial <- row_reliability
  out$sp_skillz_rank_initial <- rank(-out$sp_skillz_score_initial, ties.method = "min", na.last = "keep")
  out$sp_skillz_rank_initial_stabilized <- rank(-out$sp_skillz_score_initial_stabilized, ties.method = "min", na.last = "keep")

  starter_n <- min(as.integer(round(num_teams * sp_depth)), nrow(out))
  if (starter_n < 1) {
    stop("Starter pool size resolved to zero.")
  }

  order_pass1 <- order(out$sp_skillz_score_initial, decreasing = TRUE, na.last = NA)
  if (length(order_pass1) < starter_n) {
    stop("Not enough non-missing SP Skillz scores to build starter pool.")
  }
  starter_idx <- order_pass1[seq_len(starter_n)]
  starter_pool <- out[starter_idx, , drop = FALSE]

  ref <- data.frame(
    metric = SP_SKILLZ_METRICS,
    standardization_method = ifelse(SP_SKILLZ_METRICS %in% SP_PLUS_METRICS, "fixed_scale_(x-100)/10", "starter_pool_zscore"),
    overall_mean = vapply(SP_SKILLZ_METRICS, function(metric) mean(out[[metric]], na.rm = TRUE), numeric(1)),
    overall_sd = vapply(SP_SKILLZ_METRICS, function(metric) stats::sd(out[[metric]], na.rm = TRUE), numeric(1)),
    starter_pool_mean = vapply(SP_SKILLZ_METRICS, function(metric) mean(starter_pool[[metric]], na.rm = TRUE), numeric(1)),
    starter_pool_sd = vapply(SP_SKILLZ_METRICS, function(metric) stats::sd(starter_pool[[metric]], na.rm = TRUE), numeric(1)),
    sample_source = vapply(SP_SKILLZ_METRICS, metric_sample_source_for, character(1)),
    stabilization_point = as.numeric(stabilization_points[SP_SKILLZ_METRICS]),
    reliability_method = reliability_method,
    start_share_min = start_share_min,
    use_ip_paradigms = isTRUE(use_ip_paradigms),
    low_ip_max_start_ip = low_ip_max_start_ip,
    high_ip_min_start_ip = high_ip_min_start_ip,
    gs_fallback_threshold = gs_fallback_threshold,
    starter_pool_size = starter_n,
    num_teams = as.integer(round(num_teams)),
    sp_depth = sp_depth,
    stringsAsFactors = FALSE
  )

  z_pool <- build_metric_standardized_matrix(out, reference_data = starter_pool)

  out$sp_skillz_score <- vapply(
    seq_len(nrow(out)),
    function(i) weighted_z_sum(z_pool[i, ], weight_mat[i, ], target_abs_weight),
    numeric(1)
  )
  out$sp_skillz_score_stabilized <- vapply(
    seq_len(nrow(out)),
    function(i) weighted_z_sum(
      z_pool[i, ],
      effective_weight_mat[i, ],
      target_abs_weight,
      denominator_weights = weight_mat[i, ]
    ),
    numeric(1)
  )
  out$sp_skillz_reliability <- row_reliability
  out$sp_skillz_rank <- rank(-out$sp_skillz_score, ties.method = "min", na.last = "keep")
  out$sp_skillz_rank_stabilized <- rank(-out$sp_skillz_score_stabilized, ties.method = "min", na.last = "keep")
  out$sp_skillz_starter_pool_flag <- FALSE
  out$sp_skillz_starter_pool_flag[starter_idx] <- TRUE

  for (metric in SP_SKILLZ_METRICS) {
    out[[paste0(metric, "_z")]] <- z_pool[, metric]
    out[[paste0(metric, "_sample")]] <- sample_mat[, metric]
    out[[paste0(metric, "_rel")]] <- reliability_mat[, metric]
    out[[paste0("w_", metric)]] <- weight_mat[, metric]
    out[[paste0("w_eff_", metric)]] <- effective_weight_mat[, metric]
  }

  out <- out[order(out$sp_skillz_rank, out$player_name, na.last = TRUE), , drop = FALSE]
  rownames(out) <- NULL

  starter_pool <- out[out$sp_skillz_starter_pool_flag, , drop = FALSE]
  starter_pool <- starter_pool[order(starter_pool$sp_skillz_rank, starter_pool$player_name, na.last = TRUE), , drop = FALSE]
  rownames(starter_pool) <- NULL

  list(
    scores = out,
    reference = ref,
    starter_pool = starter_pool
  )
}

fetch_and_score_sp_skillz <- function(
  leaderboard_url,
  local_csv_path = "",
  lookback_season = NA_integer_,
  weights = DEFAULT_SP_SKILLZ_WEIGHTS,
  num_teams = 15,
  sp_depth = 10,
  start_share_min = 2 / 3,
  use_ip_paradigms = TRUE,
  low_ip_max_start_ip = 80,
  high_ip_min_start_ip = 100,
  low_ip_weights = DEFAULT_LOW_IP_PARADIGM_WEIGHTS,
  high_ip_weights = DEFAULT_HIGH_IP_PARADIGM_WEIGHTS,
  gs_fallback_threshold = 5,
  stabilization_points = DEFAULT_SP_SKILLZ_STABILIZATION_POINTS,
  reliability_method = "sample_over_sample_plus_stab"
) {
  raw <- read_sp_skillz_raw(
    leaderboard_url = leaderboard_url,
    local_csv_path = local_csv_path
  )
  standardized <- standardize_sp_skillz_input(raw, lookback_season = lookback_season)
  scored <- compute_sp_skillz_scores(
    skillz_data = standardized,
    weights = weights,
    num_teams = num_teams,
    sp_depth = sp_depth,
    start_share_min = start_share_min,
    use_ip_paradigms = use_ip_paradigms,
    low_ip_max_start_ip = low_ip_max_start_ip,
    high_ip_min_start_ip = high_ip_min_start_ip,
    low_ip_weights = low_ip_weights,
    high_ip_weights = high_ip_weights,
    gs_fallback_threshold = gs_fallback_threshold,
    stabilization_points = stabilization_points,
    reliability_method = reliability_method
  )

  list(
    raw = raw,
    standardized = standardized,
    scores = scored$scores,
    reference = scored$reference,
    starter_pool = scored$starter_pool
  )
}
