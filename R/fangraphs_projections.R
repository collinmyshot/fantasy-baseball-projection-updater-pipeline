FG_SYSTEM_CODES <- list(
  batx = c("thebatx", "batx"),
  steamer = c("steamer"),
  oopsy = c("oopsy"),
  atc = c("atc")
)

DEFAULT_PROJECTION_WEIGHTS <- c(
  batx = 3,
  steamer = 2,
  oopsy = 3,
  atc = 1
)

DEFAULT_CATEGORY_WEIGHTS <- c(
  pa = 2.3,
  hr = 1.35,
  sb = 1.0,
  r = 0.6,
  rbi = 0.6,
  h = 1.0
)

DEFAULT_NFBC_TEAMS <- 15
DEFAULT_NFBC_HITTER_STARTER_SLOTS <- c(
  c = 2,
  b1 = 1,
  b2 = 1,
  b3 = 1,
  ss = 1,
  of = 5,
  ci = 1,
  mi = 1,
  ut = 1
)
DEFAULT_NFBC_HITTER_STARTER_COUNT <- as.integer(DEFAULT_NFBC_TEAMS * sum(DEFAULT_NFBC_HITTER_STARTER_SLOTS))
DEFAULT_TOTAL_BUDGET <- 260
DEFAULT_HITTER_BUDGET_SHARE <- 0.70
DEFAULT_MIN_BID <- 1
DEFAULT_NFBC_ADP_DRAFT_TYPE <- 897
DEFAULT_NFBC_ADP_NUM_TEAMS <- 12
DEFAULT_NFBC_ADP_LOOKBACK_DAYS <- 14
NFBC_ADP_ENDPOINT <- "https://nfc.shgn.com/adp.data.php"

STANDARD_COLUMN_MAP <- list(
  playerid = c("playerid", "playeridfg", "fgplayerid", "idfg", "id"),
  name = c("playername", "name", "shortname"),
  team = c("team", "tm"),
  ab = c("ab", "atbats"),
  pa = c("pa", "plateappearances"),
  h = c("h", "hits"),
  r = c("r", "runs"),
  hr = c("hr", "homeruns"),
  rbi = c("rbi", "runsbattedin"),
  sb = c("sb", "stolenbases"),
  avg = c("avg", "battingaverage")
)

STANDARD_COLUMN_ORDER <- c(
  "projection_system", "projection_type", "season", "playerid", "name", "team",
  "ab", "pa", "h", "r", "hr", "rbi", "sb", "avg"
)

FG_API_BASE <- "https://www.fangraphs.com/api/projections"
FG_REQUEST_RETRIES <- 3L
FG_REQUEST_RETRY_SLEEP_SEC <- 1.5

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

normalize_name <- function(x) {
  gsub("[^a-z0-9]", "", tolower(x))
}

normalize_join_name <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- iconv(x, from = "", to = "ASCII//TRANSLIT")
  x[is.na(x)] <- ""
  x <- tolower(x)
  x <- gsub("[^a-z0-9]", "", x)
  x
}

normalize_team_abbrev <- function(x) {
  out <- toupper(trimws(as.character(x)))
  out[is.na(out)] <- ""
  out <- gsub("[^A-Z0-9]", "", out)

  # Map common Fangraphs/NFBC abbreviation differences to a shared key.
  mapped <- c(
    KCR = "KCR", KC = "KCR",
    SDP = "SDP", SD = "SDP",
    SFG = "SFG", SF = "SFG",
    TBR = "TBR", TB = "TBR",
    CHW = "CHW", CWS = "CHW",
    WSN = "WSN", WAS = "WSN",
    ARI = "ARZ", ARZ = "ARZ",
    MIL = "MIL", MLW = "MIL"
  )
  has_map <- out %in% names(mapped)
  out[has_map] <- mapped[out[has_map]]

  tolower(out)
}

build_fg_projection_url <- function(projection_type, season) {
  params <- list(
    pos = "all",
    stats = "bat",
    lg = "all",
    qual = "0",
    type = projection_type,
    season = as.character(season),
    month = "0",
    season1 = as.character(season),
    ind = "0",
    team = "0",
    rost = "0",
    age = "0",
    filter = "",
    players = "0",
    z = as.character(as.integer(Sys.time())),
    sort = "0,1"
  )

  query <- paste(
    paste0(names(params), "=", vapply(params, utils::URLencode, character(1), reserved = TRUE)),
    collapse = "&"
  )

  paste0(FG_API_BASE, "?", query)
}

as_data_frame <- function(raw) {
  if (is.null(raw)) {
    return(NULL)
  }

  if (is.list(raw) && "data" %in% names(raw)) {
    raw <- raw$data
  }

  if (is.data.frame(raw)) {
    return(raw)
  }

  if (is.list(raw)) {
    out <- tryCatch(as.data.frame(raw, stringsAsFactors = FALSE), error = function(e) NULL)
    return(out)
  }

  NULL
}

fetch_projection_raw <- function(system_name, season) {
  if (!system_name %in% names(FG_SYSTEM_CODES)) {
    stop(sprintf("Unknown system '%s'. Expected one of: %s", system_name, paste(names(FG_SYSTEM_CODES), collapse = ", ")))
  }

  attempts <- FG_SYSTEM_CODES[[system_name]]
  last_error <- NULL

  for (projection_type in attempts) {
    url <- build_fg_projection_url(projection_type = projection_type, season = season)
    for (attempt in seq_len(FG_REQUEST_RETRIES)) {
      raw <- tryCatch(jsonlite::fromJSON(url), error = function(e) {
        last_error <<- conditionMessage(e)
        NULL
      })

      data <- as_data_frame(raw)

      if (!is.null(data) && nrow(data) > 0) {
        data$projection_system <- system_name
        data$projection_type <- projection_type
        data$season <- as.integer(season)
        return(data)
      }

      if (attempt < FG_REQUEST_RETRIES) {
        Sys.sleep(FG_REQUEST_RETRY_SLEEP_SEC * attempt)
      }
    }
  }

  stop(sprintf("Failed to fetch data for system '%s'. Last error: %s", system_name, ifelse(is.null(last_error), "No rows returned", last_error)))
}

select_first_match <- function(data, candidates) {
  original_names <- names(data)
  normalized_names <- normalize_name(original_names)
  normalized_candidates <- normalize_name(candidates)

  idx <- match(normalized_candidates, normalized_names, nomatch = 0)
  idx <- idx[idx > 0]

  if (length(idx) == 0) {
    return(rep(NA, nrow(data)))
  }

  data[[original_names[idx[1]]]]
}

standardize_hitter_table <- function(data) {
  out <- data.frame(
    projection_system = as.character(data$projection_system),
    projection_type = as.character(data$projection_type),
    season = as.integer(data$season),
    stringsAsFactors = FALSE
  )

  for (target in names(STANDARD_COLUMN_MAP)) {
    out[[target]] <- select_first_match(data, STANDARD_COLUMN_MAP[[target]])
  }

  numeric_columns <- setdiff(
    STANDARD_COLUMN_ORDER,
    c("projection_system", "projection_type", "season", "name", "team")
  )
  for (column in numeric_columns) {
    out[[column]] <- suppressWarnings(as.numeric(out[[column]]))
  }

  out <- out[, STANDARD_COLUMN_ORDER]

  if (!all(is.na(out$playerid))) {
    out <- out[order(out$playerid, out$name, na.last = TRUE), ]
  }

  rownames(out) <- NULL
  out
}

add_per_pa_rates <- function(standard_data) {
  out <- standard_data

  out$ab_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$ab / out$pa, NA_real_)
  out$r_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$r / out$pa, NA_real_)
  out$hr_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$hr / out$pa, NA_real_)
  out$rbi_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$rbi / out$pa, NA_real_)
  out$sb_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$sb / out$pa, NA_real_)
  out$h_per_pa <- ifelse(!is.na(out$pa) & out$pa > 0, out$h / out$pa, NA_real_)

  out
}

weighted_mean_available <- function(values, weights) {
  keep <- !is.na(values) & !is.na(weights)
  if (!any(keep)) {
    return(NA_real_)
  }
  sum(values[keep] * weights[keep]) / sum(weights[keep])
}

validate_projection_weights <- function(weights, required_systems = names(FG_SYSTEM_CODES)) {
  if (is.null(weights) || length(weights) == 0) {
    stop("Projection weights cannot be empty.")
  }
  if (is.null(names(weights)) || any(!nzchar(names(weights)))) {
    stop("Projection weights must be a named vector.")
  }

  missing_systems <- setdiff(required_systems, names(weights))
  if (length(missing_systems) > 0) {
    stop(sprintf("Missing weights for systems: %s", paste(missing_systems, collapse = ", ")))
  }

  cleaned <- as.numeric(weights[required_systems])
  names(cleaned) <- required_systems

  if (any(is.na(cleaned)) || any(cleaned < 0)) {
    stop("Projection weights must be numeric and non-negative.")
  }
  if (sum(cleaned) <= 0) {
    stop("Projection weights must sum to a value greater than 0.")
  }

  cleaned
}

mode_team <- function(x) {
  x <- x[!is.na(x) & nzchar(x)]
  if (length(x) == 0) {
    return(NA_character_)
  }
  names(sort(table(x), decreasing = TRUE))[1]
}

weighted_aggregate_per_pa <- function(per_pa_data, weights = DEFAULT_PROJECTION_WEIGHTS) {
  weights <- validate_projection_weights(weights)

  key_player <- ifelse(
    !is.na(per_pa_data$playerid),
    paste0("id:", per_pa_data$playerid),
    paste0("name:", tolower(trimws(per_pa_data$name)))
  )

  split_rows <- split(
    per_pa_data,
    key_player
  )

  out_list <- lapply(split_rows, function(player_rows) {
    w <- as.numeric(weights[player_rows$projection_system])
    systems_present <- unique(player_rows$projection_system[!is.na(player_rows$projection_system)])
    systems_present <- names(weights)[names(weights) %in% systems_present]

    weighted_pa <- weighted_mean_available(player_rows$pa, w)
    weighted_ab_per_pa <- weighted_mean_available(player_rows$ab_per_pa, w)
    weighted_h_per_pa <- weighted_mean_available(player_rows$h_per_pa, w)
    weighted_r_per_pa <- weighted_mean_available(player_rows$r_per_pa, w)
    weighted_hr_per_pa <- weighted_mean_available(player_rows$hr_per_pa, w)
    weighted_rbi_per_pa <- weighted_mean_available(player_rows$rbi_per_pa, w)
    weighted_sb_per_pa <- weighted_mean_available(player_rows$sb_per_pa, w)

    weighted_avg <- ifelse(
      !is.na(weighted_ab_per_pa) && weighted_ab_per_pa > 0,
      weighted_h_per_pa / weighted_ab_per_pa,
      NA_real_
    )

    data.frame(
      season = player_rows$season[1],
      playerid = suppressWarnings(as.numeric(player_rows$playerid[1])),
      name = player_rows$name[1],
      team = mode_team(player_rows$team),
      systems_used = length(systems_present),
      systems_included = paste(systems_present, collapse = ","),
      weights_used = sum(w[!is.na(player_rows$pa)]),
      weighted_pa = weighted_pa,
      weighted_ab_per_pa = weighted_ab_per_pa,
      weighted_h_per_pa = weighted_h_per_pa,
      weighted_r_per_pa = weighted_r_per_pa,
      weighted_hr_per_pa = weighted_hr_per_pa,
      weighted_rbi_per_pa = weighted_rbi_per_pa,
      weighted_sb_per_pa = weighted_sb_per_pa,
      weighted_avg = weighted_avg,
      stringsAsFactors = FALSE
    )
  })

  out <- do.call(rbind, out_list)
  if (!all(is.na(out$playerid))) {
    out <- out[order(out$playerid, out$name, na.last = TRUE), ]
  }
  rownames(out) <- NULL
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

validate_category_weights <- function(weights) {
  required <- c("pa", "hr", "sb", "r", "rbi", "h")
  if (is.null(weights) || length(weights) == 0) {
    stop("Category weights cannot be empty.")
  }
  if (is.null(names(weights)) || any(!nzchar(names(weights)))) {
    stop("Category weights must be a named vector.")
  }

  missing <- setdiff(required, names(weights))
  if (length(missing) > 0) {
    stop(sprintf("Missing category weights: %s", paste(missing, collapse = ", ")))
  }

  out <- as.numeric(weights[required])
  names(out) <- required
  if (any(is.na(out)) || any(out < 0)) {
    stop("Category weights must be numeric and non-negative.")
  }
  if (sum(out) <= 0) {
    stop("Category weights must sum to a value greater than 0.")
  }

  out
}

build_baseline_reference <- function(data, baseline, pa_floor, starter_count = NA_integer_, starter_rank_metric = NA_character_) {
  metrics <- c(
    PA = "weighted_pa",
    R_per_PA = "weighted_r_per_pa",
    RBI_per_PA = "weighted_rbi_per_pa",
    HR_per_PA = "weighted_hr_per_pa",
    SB_per_PA_LOG = "weighted_sb_per_pa_log",
    H_per_PA = "weighted_h_per_pa",
    AVG = "weighted_avg"
  )

  means <- vapply(metrics, function(col) mean(data[[col]], na.rm = TRUE), numeric(1))
  sds <- vapply(metrics, function(col) stats::sd(data[[col]], na.rm = TRUE), numeric(1))

  data.frame(
    baseline = baseline,
    category = names(metrics),
    source_column = unname(metrics),
    mean = as.numeric(means),
    sd = as.numeric(sds),
    pa_floor = pa_floor,
    player_count = nrow(data),
    starter_count = rep(starter_count, length(metrics)),
    starter_rank_metric = rep(starter_rank_metric, length(metrics)),
    stringsAsFactors = FALSE
  )
}

compute_zscores_per_pa <- function(
  weighted_aggregate,
  pa_floor = 200,
  category_weights = DEFAULT_CATEGORY_WEIGHTS,
  starter_count = DEFAULT_NFBC_HITTER_STARTER_COUNT,
  starter_rank_metric = "z_total_custom"
) {
  if (!is.numeric(pa_floor) || length(pa_floor) != 1 || is.na(pa_floor) || pa_floor < 0) {
    stop("pa_floor must be a single non-negative number.")
  }
  if (!is.numeric(starter_count) || length(starter_count) != 1 || is.na(starter_count) || starter_count < 1) {
    stop("starter_count must be a single positive number.")
  }

  category_weights <- validate_category_weights(category_weights)

  required <- c(
    "weighted_pa",
    "weighted_r_per_pa",
    "weighted_rbi_per_pa",
    "weighted_hr_per_pa",
    "weighted_sb_per_pa",
    "weighted_h_per_pa"
  )
  missing_cols <- setdiff(required, names(weighted_aggregate))
  if (length(missing_cols) > 0) {
    stop(sprintf("Missing required columns for z-scores: %s", paste(missing_cols, collapse = ", ")))
  }

  out <- weighted_aggregate[
    !is.na(weighted_aggregate$weighted_pa) & weighted_aggregate$weighted_pa >= pa_floor,
    ,
    drop = FALSE
  ]

  if (nrow(out) == 0) {
    stop(sprintf("No players remain after applying pa_floor >= %s.", pa_floor))
  }

  out$weighted_sb_per_pa_log <- log1p(out$weighted_sb_per_pa)

  out$z_pa <- z_score_vector(out$weighted_pa)
  out$z_r <- z_score_vector(out$weighted_r_per_pa)
  out$z_rbi <- z_score_vector(out$weighted_rbi_per_pa)
  out$z_hr <- z_score_vector(out$weighted_hr_per_pa)
  out$z_sb <- z_score_vector(out$weighted_sb_per_pa_log)
  out$z_h <- z_score_vector(out$weighted_h_per_pa)
  out$z_avg <- z_score_vector(out$weighted_avg)

  # Raw total uses the same stat set as custom total, but with equal weights.
  out$z_total_equal <- out$z_pa + out$z_r + out$z_rbi + out$z_hr + out$z_sb + out$z_h
  out$z_total_custom <- (
    category_weights[["pa"]] * out$z_pa +
      category_weights[["hr"]] * out$z_hr +
      category_weights[["sb"]] * out$z_sb +
      category_weights[["r"]] * out$z_r +
      category_weights[["rbi"]] * out$z_rbi +
      category_weights[["h"]] * out$z_h
  )

  # Backward compatibility for older metric names before SB standard-path removal.
  if (starter_rank_metric %in% c("z_total_equal_sb_std", "z_total_equal_sb_log")) {
    starter_rank_metric <- "z_total_equal"
  }

  rank_metric_options <- c("z_total_equal", "z_total_custom")
  if (!starter_rank_metric %in% rank_metric_options) {
    stop(sprintf("starter_rank_metric must be one of: %s", paste(rank_metric_options, collapse = ", ")))
  }

  starter_pool <- out[!is.na(out[[starter_rank_metric]]), , drop = FALSE]
  if (nrow(starter_pool) == 0) {
    stop(sprintf("No players have non-missing values for starter_rank_metric '%s'.", starter_rank_metric))
  }

  starter_pool <- starter_pool[order(starter_pool[[starter_rank_metric]], decreasing = TRUE), , drop = FALSE]
  starter_n <- min(as.integer(round(starter_count)), nrow(starter_pool))
  starter_pool <- starter_pool[seq_len(starter_n), , drop = FALSE]
  starter_pool$starter_pool_rank <- seq_len(nrow(starter_pool))

  overall_ref <- build_baseline_reference(out, "overall_pool", pa_floor = pa_floor)
  starter_ref <- build_baseline_reference(
    starter_pool,
    "starter_pool",
    pa_floor = pa_floor,
    starter_count = starter_n,
    starter_rank_metric = starter_rank_metric
  )

  starter_center <- setNames(starter_ref$mean, starter_ref$category)
  starter_spread <- setNames(starter_ref$sd, starter_ref$category)

  out$z_pa_starter <- z_score_from_reference(out$weighted_pa, starter_center[["PA"]], starter_spread[["PA"]])
  out$z_r_starter <- z_score_from_reference(out$weighted_r_per_pa, starter_center[["R_per_PA"]], starter_spread[["R_per_PA"]])
  out$z_rbi_starter <- z_score_from_reference(out$weighted_rbi_per_pa, starter_center[["RBI_per_PA"]], starter_spread[["RBI_per_PA"]])
  out$z_hr_starter <- z_score_from_reference(out$weighted_hr_per_pa, starter_center[["HR_per_PA"]], starter_spread[["HR_per_PA"]])
  out$z_sb_starter <- z_score_from_reference(out$weighted_sb_per_pa_log, starter_center[["SB_per_PA_LOG"]], starter_spread[["SB_per_PA_LOG"]])
  out$z_h_starter <- z_score_from_reference(out$weighted_h_per_pa, starter_center[["H_per_PA"]], starter_spread[["H_per_PA"]])
  out$z_avg_starter <- z_score_from_reference(out$weighted_avg, starter_center[["AVG"]], starter_spread[["AVG"]])

  out$z_total_equal_starter <- out$z_pa_starter + out$z_r_starter + out$z_rbi_starter + out$z_hr_starter + out$z_sb_starter + out$z_h_starter
  out$z_total_custom_starter <- (
    category_weights[["pa"]] * out$z_pa_starter +
      category_weights[["hr"]] * out$z_hr_starter +
      category_weights[["sb"]] * out$z_sb_starter +
      category_weights[["r"]] * out$z_r_starter +
      category_weights[["rbi"]] * out$z_rbi_starter +
      category_weights[["h"]] * out$z_h_starter
  )

  make_rank <- function(x) {
    rank(-x, ties.method = "min", na.last = "keep")
  }

  out$rank_equal <- make_rank(out$z_total_equal)
  out$rank_custom <- make_rank(out$z_total_custom)
  out$rank_equal_starter <- make_rank(out$z_total_equal_starter)
  out$rank_custom_starter <- make_rank(out$z_total_custom_starter)

  out <- out[order(out$z_total_custom_starter, decreasing = TRUE), ]
  rownames(out) <- NULL

  list(
    z_scores = out,
    zscore_reference = rbind(overall_ref, starter_ref),
    starter_pool = starter_pool
  )
}

compute_dollar_values <- function(
  z_scores,
  starter_pool,
  starter_count,
  num_teams = DEFAULT_NFBC_TEAMS,
  total_budget = DEFAULT_TOTAL_BUDGET,
  hitter_budget_share = DEFAULT_HITTER_BUDGET_SHARE,
  min_bid = DEFAULT_MIN_BID
) {
  if (!is.numeric(total_budget) || length(total_budget) != 1 || is.na(total_budget) || total_budget <= 0) {
    stop("total_budget must be a single positive number.")
  }
  if (!is.numeric(hitter_budget_share) || length(hitter_budget_share) != 1 || is.na(hitter_budget_share) || hitter_budget_share <= 0 || hitter_budget_share >= 1) {
    stop("hitter_budget_share must be between 0 and 1.")
  }
  if (!is.numeric(min_bid) || length(min_bid) != 1 || is.na(min_bid) || min_bid < 0) {
    stop("min_bid must be a single non-negative number.")
  }
  if (!is.numeric(starter_count) || length(starter_count) != 1 || is.na(starter_count) || starter_count < 1) {
    stop("starter_count must be a single positive number.")
  }
  if (!is.numeric(num_teams) || length(num_teams) != 1 || is.na(num_teams) || num_teams < 1) {
    stop("num_teams must be a single positive number.")
  }

  required_metrics <- c("z_total_equal", "z_total_custom")
  missing_from_z <- setdiff(required_metrics, names(z_scores))
  if (length(missing_from_z) > 0) {
    stop(sprintf("Missing z-score columns for dollar conversion: %s", paste(missing_from_z, collapse = ", ")))
  }
  missing_from_starters <- setdiff(required_metrics, names(starter_pool))
  if (length(missing_from_starters) > 0) {
    stop(sprintf("Missing starter-pool columns for dollar conversion: %s", paste(missing_from_starters, collapse = ", ")))
  }

  budget_bats <- total_budget * hitter_budget_share * as.numeric(num_teams)
  numstartingbats <- min(as.numeric(starter_count), nrow(starter_pool))
  if (numstartingbats < 1) {
    stop("starter_pool has no rows for dollar conversion.")
  }
  dollars_per_slot <- budget_bats / numstartingbats

  avg_z_equal <- mean(starter_pool$z_total_equal, na.rm = TRUE)
  avg_z_custom <- mean(starter_pool$z_total_custom, na.rm = TRUE)

  dollar_formula <- function(z_value, avg_z_value) {
    if (is.na(avg_z_value) || abs(avg_z_value) < .Machine$double.eps^0.5) {
      return(rep(NA_real_, length(z_value)))
    }
    (dollars_per_slot * (z_value / avg_z_value)) + min_bid
  }

  out <- z_scores
  out$dollar_value_raw <- dollar_formula(out$z_total_equal, avg_z_equal)
  out$dollar_value_weights <- dollar_formula(out$z_total_custom, avg_z_custom)

  reference <- data.frame(
    metric = c("z_total_equal", "z_total_custom"),
    avg_z_sum = c(avg_z_equal, avg_z_custom),
    budget_bats = budget_bats,
    numstartingbats = numstartingbats,
    dollars_per_slot = dollars_per_slot,
    total_budget = total_budget,
    num_teams = num_teams,
    hitter_budget_share = hitter_budget_share,
    min_bid = min_bid,
    stringsAsFactors = FALSE
  )

  list(
    z_scores = out,
    dollar_reference = reference
  )
}

empty_nfbc_adp_table <- function() {
  data.frame(
    nfbc_rank = numeric(0),
    nfbc_player_id = numeric(0),
    player_name = character(0),
    team = character(0),
    positions = character(0),
    adp = numeric(0),
    adp_min_pick = numeric(0),
    adp_max_pick = numeric(0),
    adp_diff = numeric(0),
    adp_picks = numeric(0),
    name_key = character(0),
    team_key = character(0),
    stringsAsFactors = FALSE
  )
}

nfbc_player_name_to_first_last <- function(x) {
  out <- as.character(x)
  out[is.na(out)] <- ""
  out <- trimws(out)

  has_comma <- grepl(",", out, fixed = TRUE)
  if (any(has_comma)) {
    out[has_comma] <- vapply(strsplit(out[has_comma], ",", fixed = TRUE), function(parts) {
      parts <- trimws(parts)
      if (length(parts) < 2) {
        return(parts[1])
      }
      last <- parts[1]
      first <- paste(parts[-1], collapse = " ")
      if (!nzchar(first)) {
        return(last)
      }
      paste(first, last)
    }, character(1))
  }

  out
}

parse_nfbc_adp_tsv <- function(tsv_path) {
  if (!nzchar(tsv_path) || !file.exists(tsv_path)) {
    return(empty_nfbc_adp_table())
  }

  raw <- tryCatch(
    utils::read.delim(tsv_path, sep = "\t", stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) NULL
  )

  if (is.null(raw) || nrow(raw) == 0) {
    return(empty_nfbc_adp_table())
  }

  value_or_na <- function(data, i) {
    if (ncol(data) >= i) {
      return(data[[i]])
    }
    rep(NA, nrow(data))
  }

  out <- data.frame(
    nfbc_rank = suppressWarnings(as.numeric(value_or_na(raw, 1))),
    nfbc_player_id = suppressWarnings(as.numeric(value_or_na(raw, 2))),
    player_name = nfbc_player_name_to_first_last(value_or_na(raw, 3)),
    team = trimws(as.character(value_or_na(raw, 4))),
    positions = trimws(as.character(value_or_na(raw, 5))),
    adp = suppressWarnings(as.numeric(value_or_na(raw, 6))),
    adp_min_pick = suppressWarnings(as.numeric(value_or_na(raw, 7))),
    adp_max_pick = suppressWarnings(as.numeric(value_or_na(raw, 8))),
    adp_diff = suppressWarnings(as.numeric(value_or_na(raw, 9))),
    adp_picks = suppressWarnings(as.numeric(value_or_na(raw, 10))),
    stringsAsFactors = FALSE
  )

  hitter_positions <- c("c", "1b", "2b", "3b", "ss", "of", "ci", "mi", "ut")
  is_hitter <- vapply(seq_len(nrow(out)), function(i) {
    pos <- out$positions[i]
    name_key <- normalize_join_name(out$player_name[i])

    if (is.na(pos) || !nzchar(pos)) {
      return(FALSE)
    }

    # ADP position strings can be comma- or slash-delimited.
    tokens <- tolower(unlist(strsplit(pos, "[,/]", perl = TRUE)))
    tokens <- trimws(tokens)
    tokens <- tokens[nzchar(tokens)]

    has_hitter_pos <- any(tokens %in% hitter_positions)
    if (has_hitter_pos) {
      return(TRUE)
    }

    # Explicit exception: keep Shohei even if listed as P-only.
    identical(name_key, "shoheiohtani")
  }, logical(1))

  out <- out[is_hitter, , drop = FALSE]
  if (nrow(out) == 0) {
    return(empty_nfbc_adp_table())
  }

  out$name_key <- normalize_join_name(out$player_name)
  out$team_key <- normalize_team_abbrev(out$team)

  out <- out[order(out$nfbc_rank, out$adp, na.last = TRUE), , drop = FALSE]
  out <- out[!duplicated(paste(out$name_key, out$team_key, sep = "|")), , drop = FALSE]
  rownames(out) <- NULL
  out
}

download_nfbc_adp_tsv <- function(
  output_path,
  from_date,
  to_date,
  draft_type = DEFAULT_NFBC_ADP_DRAFT_TYPE,
  num_teams = DEFAULT_NFBC_ADP_NUM_TEAMS
) {
  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) {
    stop("curl is required to download NFBC ADP data.")
  }

  payload <- paste(
    paste0("team_id=", utils::URLencode("0", reserved = TRUE)),
    paste0("from_date=", utils::URLencode(as.character(from_date), reserved = TRUE)),
    paste0("to_date=", utils::URLencode(as.character(to_date), reserved = TRUE)),
    paste0("num_teams=", utils::URLencode(as.character(num_teams), reserved = TRUE)),
    paste0("draft_type=", utils::URLencode(as.character(draft_type), reserved = TRUE)),
    paste0("position=", utils::URLencode("", reserved = TRUE)),
    paste0("league_teams=", utils::URLencode("", reserved = TRUE)),
    paste0("sport=", utils::URLencode("mlb", reserved = TRUE)),
    paste0("download=", utils::URLencode("Download", reserved = TRUE)),
    sep = "&"
  )

  cmd_out <- tryCatch(
    suppressWarnings(system2(
      curl_bin,
      args = c("-sL", NFBC_ADP_ENDPOINT, "--data", payload, "-o", output_path),
      stdout = TRUE,
      stderr = TRUE
    )),
    error = function(e) {
      stop(sprintf("NFBC ADP curl command failed to execute: %s", conditionMessage(e)))
    }
  )
  status <- attr(cmd_out, "status")
  if (is.null(status)) {
    status <- 0
  }
  if (status != 0) {
    details <- ""
    if (length(cmd_out) > 0) {
      details <- paste(utils::head(cmd_out, 3), collapse = " | ")
    }
    stop(sprintf(
      "NFBC ADP download failed with exit status %s. %s",
      status,
      details
    ))
  }
  if (!file.exists(output_path) || isTRUE(file.info(output_path)$size <= 0)) {
    stop("NFBC ADP download did not produce a file.")
  }

  header_line <- readLines(output_path, n = 1, warn = FALSE)
  if (!length(header_line) || !grepl("^Rank\tPlayer ID\t", header_line[1])) {
    stop("NFBC ADP download returned unexpected content.")
  }

  output_path
}

fetch_nfbc_adp <- function(
  raw_output_path,
  local_tsv_path = "",
  lookback_days = DEFAULT_NFBC_ADP_LOOKBACK_DAYS,
  draft_type = DEFAULT_NFBC_ADP_DRAFT_TYPE,
  num_teams = DEFAULT_NFBC_ADP_NUM_TEAMS
) {
  if (nzchar(local_tsv_path) && file.exists(local_tsv_path)) {
    adp <- parse_nfbc_adp_tsv(local_tsv_path)
    return(list(
      adp = adp,
      source = "local_file",
      source_path = local_tsv_path,
      from_date = NA_character_,
      to_date = NA_character_,
      draft_type = draft_type,
      num_teams = num_teams
    ))
  }

  to_date <- Sys.Date()
  from_date <- as.Date(to_date) - as.integer(lookback_days)

  if (!nzchar(raw_output_path)) {
    raw_output_path <- tempfile(fileext = ".tsv")
  }

  download_nfbc_adp_tsv(
    output_path = raw_output_path,
    from_date = from_date,
    to_date = to_date,
    draft_type = draft_type,
    num_teams = num_teams
  )
  adp <- parse_nfbc_adp_tsv(raw_output_path)

  list(
    adp = adp,
    source = "nfbc_download",
    source_path = raw_output_path,
    from_date = as.character(from_date),
    to_date = as.character(to_date),
    draft_type = draft_type,
    num_teams = num_teams
  )
}

empty_player_match_overrides <- function() {
  data.frame(
    projection_name = character(0),
    projection_team = character(0),
    adp_name = character(0),
    adp_team = character(0),
    notes = character(0),
    projection_name_key = character(0),
    projection_team_key = character(0),
    adp_name_key = character(0),
    adp_team_key = character(0),
    stringsAsFactors = FALSE
  )
}

load_player_match_overrides <- function(path = "") {
  if (!nzchar(path) || !file.exists(path)) {
    return(empty_player_match_overrides())
  }

  raw <- tryCatch(
    utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE),
    error = function(e) NULL
  )
  if (is.null(raw) || nrow(raw) == 0) {
    return(empty_player_match_overrides())
  }

  required <- c("projection_name", "projection_team", "adp_name", "adp_team")
  missing <- setdiff(required, names(raw))
  if (length(missing) > 0) {
    warning(sprintf(
      "Player match overrides file is missing required columns: %s",
      paste(missing, collapse = ", ")
    ))
    return(empty_player_match_overrides())
  }

  raw$projection_name <- trimws(as.character(raw$projection_name))
  raw$projection_team <- trimws(as.character(raw$projection_team))
  raw$adp_name <- trimws(as.character(raw$adp_name))
  raw$adp_team <- trimws(as.character(raw$adp_team))
  if (!"notes" %in% names(raw)) {
    raw$notes <- ""
  }
  raw$notes <- trimws(as.character(raw$notes))

  raw <- raw[nzchar(raw$projection_name) & nzchar(raw$adp_name), , drop = FALSE]
  if (nrow(raw) == 0) {
    return(empty_player_match_overrides())
  }

  raw$projection_name_key <- normalize_join_name(raw$projection_name)
  raw$projection_team_key <- normalize_team_abbrev(raw$projection_team)
  raw$adp_name_key <- normalize_join_name(raw$adp_name)
  raw$adp_team_key <- normalize_team_abbrev(raw$adp_team)

  keep_cols <- c(
    "projection_name", "projection_team", "adp_name", "adp_team", "notes",
    "projection_name_key", "projection_team_key", "adp_name_key", "adp_team_key"
  )
  raw <- raw[, keep_cols, drop = FALSE]
  raw <- raw[!duplicated(paste(raw$projection_name_key, raw$projection_team_key, raw$adp_name_key, raw$adp_team_key, sep = "|")), , drop = FALSE]
  rownames(raw) <- NULL
  raw
}

empty_adp_match_audit <- function() {
  list(
    summary = data.frame(
      projection_rows = 0,
      adp_rows = 0,
      matched_rows = 0,
      match_rate = NA_real_,
      direct_match_rows = 0,
      name_unique_match_rows = 0,
      override_match_rows = 0,
      filled_with_max_adp_rows = 0,
      stringsAsFactors = FALSE
    ),
    projection_unmatched = data.frame(stringsAsFactors = FALSE),
    adp_unmatched = data.frame(stringsAsFactors = FALSE),
    matches = data.frame(stringsAsFactors = FALSE)
  )
}

apply_nfbc_adp_matches <- function(
  output_table,
  nfbc_adp = NULL,
  player_match_overrides = NULL,
  fill_missing_with_max = TRUE
) {
  out <- output_table
  out$name_key <- normalize_join_name(out$player_name)
  out$team_key <- normalize_team_abbrev(out$team)
  out$adp <- NA_real_
  out$position <- NA_character_
  out$adp_match_method <- NA_character_
  out$adp_override_notes <- NA_character_
  out$adp_source_index <- NA_integer_

  if (is.null(nfbc_adp) || nrow(nfbc_adp) == 0) {
    out <- out[, setdiff(names(out), c("name_key", "team_key")), drop = FALSE]
    return(list(output = out, audit = empty_adp_match_audit()))
  }

  key_out <- paste(out$name_key, out$team_key, sep = "|")
  key_adp <- paste(nfbc_adp$name_key, nfbc_adp$team_key, sep = "|")

  # Pass 1: strict name + team match.
  idx_direct <- match(key_out, key_adp)
  has_direct <- !is.na(idx_direct)
  if (any(has_direct)) {
    out$adp[has_direct] <- nfbc_adp$adp[idx_direct[has_direct]]
    out$position[has_direct] <- nfbc_adp$positions[idx_direct[has_direct]]
    out$adp_match_method[has_direct] <- "direct"
    out$adp_source_index[has_direct] <- idx_direct[has_direct]
  }

  # Pass 2: unique-name fallback.
  missing <- is.na(out$adp)
  if (any(missing)) {
    out_name_counts <- table(out$name_key)
    adp_name_counts <- table(nfbc_adp$name_key)
    unique_name_pool <- names(adp_name_counts[adp_name_counts == 1])
    fallback_ok <- missing &
      out$name_key %in% unique_name_pool &
      out_name_counts[out$name_key] == 1

    if (any(fallback_ok)) {
      idx_name <- match(out$name_key[fallback_ok], nfbc_adp$name_key)
      out$adp[fallback_ok] <- nfbc_adp$adp[idx_name]
      out$position[fallback_ok] <- nfbc_adp$positions[idx_name]
      out$adp_match_method[fallback_ok] <- "name_unique"
      out$adp_source_index[fallback_ok] <- idx_name
    }
  }

  # Pass 3: explicit manual overrides for edge cases.
  overrides <- if (is.null(player_match_overrides)) empty_player_match_overrides() else player_match_overrides
  missing <- is.na(out$adp)
  if (nrow(overrides) > 0 && any(missing)) {
    miss_idx <- which(missing)
    for (i in miss_idx) {
      candidates <- overrides[
        overrides$projection_name_key == out$name_key[i] &
          (overrides$projection_team_key == "" | overrides$projection_team_key == out$team_key[i]),
        ,
        drop = FALSE
      ]
      if (nrow(candidates) == 0) {
        next
      }

      exact_team <- candidates$projection_team_key == out$team_key[i] & nzchar(candidates$projection_team_key)
      if (any(exact_team)) {
        candidates <- candidates[exact_team, , drop = FALSE]
      }

      override_row <- candidates[1, , drop = FALSE]
      adp_idx <- NA_integer_

      if (nzchar(override_row$adp_team_key)) {
        adp_idx <- match(
          paste(override_row$adp_name_key, override_row$adp_team_key, sep = "|"),
          key_adp
        )
      } else {
        adp_idx <- match(override_row$adp_name_key, nfbc_adp$name_key)
      }

      if (!is.na(adp_idx) && adp_idx > 0) {
        out$adp[i] <- nfbc_adp$adp[adp_idx]
        out$position[i] <- nfbc_adp$positions[adp_idx]
        out$adp_match_method[i] <- "manual_override"
        out$adp_override_notes[i] <- override_row$notes
        out$adp_source_index[i] <- adp_idx
      }
    }
  }

  if (isTRUE(fill_missing_with_max)) {
    max_adp <- suppressWarnings(max(nfbc_adp$adp, na.rm = TRUE))
    if (is.finite(max_adp)) {
      still_missing <- is.na(out$adp)
      if (any(still_missing)) {
        out$adp[still_missing] <- max_adp
        out$adp_match_method[still_missing] <- "filled_max_adp"
      }
    }
  }

  out$adp <- round(out$adp, 2)

  matched_mask <- out$adp_match_method %in% c("direct", "name_unique", "manual_override")
  match_rate <- if (nrow(out) > 0) sum(matched_mask, na.rm = TRUE) / nrow(out) else NA_real_

  used_adp_idx <- unique(out$adp_source_index[!is.na(out$adp_source_index)])
  adp_unmatched <- nfbc_adp[setdiff(seq_len(nrow(nfbc_adp)), used_adp_idx), , drop = FALSE]
  projection_unmatched <- out[!matched_mask, c("player_name", "team", "pa", "hr", "sb", "r", "rbi", "avg", "adp", "adp_match_method"), drop = FALSE]
  projection_unmatched <- projection_unmatched[order(projection_unmatched$pa, decreasing = TRUE, na.last = TRUE), , drop = FALSE]
  matches <- out[, c("player_name", "team", "position", "adp", "adp_match_method", "adp_override_notes"), drop = FALSE]

  audit <- list(
    summary = data.frame(
      projection_rows = nrow(out),
      adp_rows = nrow(nfbc_adp),
      matched_rows = sum(matched_mask, na.rm = TRUE),
      match_rate = round(match_rate, 4),
      direct_match_rows = sum(out$adp_match_method == "direct", na.rm = TRUE),
      name_unique_match_rows = sum(out$adp_match_method == "name_unique", na.rm = TRUE),
      override_match_rows = sum(out$adp_match_method == "manual_override", na.rm = TRUE),
      filled_with_max_adp_rows = sum(out$adp_match_method == "filled_max_adp", na.rm = TRUE),
      stringsAsFactors = FALSE
    ),
    projection_unmatched = projection_unmatched,
    adp_unmatched = adp_unmatched,
    matches = matches
  )

  out <- out[, setdiff(names(out), c("name_key", "team_key", "adp_source_index")), drop = FALSE]
  list(output = out, audit = audit)
}

extract_atc_pa_lookup <- function(atc_standard) {
  if (is.null(atc_standard) || !is.data.frame(atc_standard) || nrow(atc_standard) == 0) {
    return(list(by_id = data.frame(), by_name = data.frame()))
  }

  atc <- atc_standard
  atc$playerid <- suppressWarnings(as.numeric(atc$playerid))
  atc$name_key <- tolower(trimws(as.character(atc$name)))

  by_id <- atc[!is.na(atc$playerid) & !is.na(atc$pa), c("playerid", "pa")]
  if (nrow(by_id) > 0) {
    by_id <- stats::aggregate(pa ~ playerid, data = by_id, FUN = function(x) mean(x, na.rm = TRUE))
  }

  by_name <- atc[is.na(atc$playerid) & nzchar(atc$name_key) & !is.na(atc$pa), c("name_key", "pa")]
  if (nrow(by_name) > 0) {
    by_name <- stats::aggregate(pa ~ name_key, data = by_name, FUN = function(x) mean(x, na.rm = TRUE))
  }

  list(by_id = by_id, by_name = by_name)
}

build_z_scored_aggregate_projection_output_table <- function(
  z_scores,
  atc_standard,
  nfbc_adp = NULL,
  player_match_overrides = NULL,
  fill_missing_adp_with_max = TRUE
) {
  out <- z_scores
  out$playerid <- suppressWarnings(as.numeric(out$playerid))
  out$name_key <- tolower(trimws(as.character(out$name)))

  lookup <- extract_atc_pa_lookup(atc_standard)

  out$atc_pa <- NA_real_
  if (nrow(lookup$by_id) > 0) {
    idx <- match(out$playerid, lookup$by_id$playerid)
    out$atc_pa <- lookup$by_id$pa[idx]
  }

  # Name fallback where playerid is missing in either table.
  missing_atc <- is.na(out$atc_pa)
  if (any(missing_atc) && nrow(lookup$by_name) > 0) {
    idx_name <- match(out$name_key[missing_atc], lookup$by_name$name_key)
    out$atc_pa[missing_atc] <- lookup$by_name$pa[idx_name]
  }

  out$pa_proj <- ifelse(!is.na(out$atc_pa), out$atc_pa, out$weighted_pa)
  out$hr_proj <- out$weighted_hr_per_pa * out$pa_proj
  out$sb_proj <- out$weighted_sb_per_pa * out$pa_proj
  out$r_proj <- out$weighted_r_per_pa * out$pa_proj
  out$rbi_proj <- out$weighted_rbi_per_pa * out$pa_proj
  out$avg_proj <- out$weighted_avg

  output <- data.frame(
    player_name = out$name,
    team = out$team,
    pa = round(out$pa_proj, 1),
    hr = round(out$hr_proj, 1),
    sb = round(out$sb_proj, 1),
    r = round(out$r_proj, 1),
    rbi = round(out$rbi_proj, 1),
    avg = round(out$avg_proj, 3),
    pa_z = round(out$z_pa_starter, 3),
    hr_z = round(out$z_hr_starter, 3),
    sb_z = round(out$z_sb_starter, 3),
    r_z = round(out$z_r_starter, 3),
    rbi_z = round(out$z_rbi_starter, 3),
    avg_z = round(out$z_avg_starter, 3),
    z_total_raw = round(out$z_total_equal, 3),
    z_total_weights = round(out$z_total_custom, 3),
    dollars_raw = round(out$dollar_value_raw, 2),
    dollars_weights = round(out$dollar_value_weights, 2),
    stringsAsFactors = FALSE
  )

  match_result <- apply_nfbc_adp_matches(
    output,
    nfbc_adp = nfbc_adp,
    player_match_overrides = player_match_overrides,
    fill_missing_with_max = fill_missing_adp_with_max
  )
  output <- match_result$output
  output$dollars_adp <- ifelse(
    !is.na(output$adp) & output$adp > 0,
    round(-9.8 * log(output$adp) + 57.8, 2),
    NA_real_
  )

  ordered_cols <- c(
    "player_name", "position", "adp", "team",
    "pa", "hr", "sb", "r", "rbi", "avg",
    "pa_z", "hr_z", "sb_z", "r_z", "rbi_z", "avg_z",
    "z_total_raw", "z_total_weights",
    "dollars_raw", "dollars_weights", "dollars_adp"
  )
  output <- output[, ordered_cols, drop = FALSE]

  output <- output[order(output$dollars_weights, decreasing = TRUE), ]
  rownames(output) <- NULL
  list(
    output = output,
    adp_match_audit = match_result$audit
  )
}

validate_pipeline_quality <- function(
  raw_list,
  weighted_aggregate,
  z_scores,
  z_scored_aggregate_projection_output,
  adp_match_audit = NULL,
  quality = list()
) {
  min_rows_per_system <- as.integer(quality$min_rows_per_system %||% 0)
  min_weighted_rows <- as.integer(quality$min_weighted_rows %||% 0)
  min_zscore_rows <- as.integer(quality$min_zscore_rows %||% 0)
  min_adp_match_rate <- as.numeric(quality$min_adp_match_rate %||% 0)
  fail_on_adp_match_rate <- isTRUE(quality$fail_on_adp_match_rate)

  for (nm in names(raw_list)) {
    n <- nrow(raw_list[[nm]])
    if (!is.na(min_rows_per_system) && n < min_rows_per_system) {
      stop(sprintf(
        "Quality check failed: system '%s' has %s rows (< %s).",
        nm, n, min_rows_per_system
      ))
    }
  }

  if (nrow(weighted_aggregate) < min_weighted_rows) {
    stop(sprintf(
      "Quality check failed: weighted aggregate has %s rows (< %s).",
      nrow(weighted_aggregate), min_weighted_rows
    ))
  }
  if (nrow(z_scores) < min_zscore_rows) {
    stop(sprintf(
      "Quality check failed: z-score table has %s rows (< %s).",
      nrow(z_scores), min_zscore_rows
    ))
  }

  if (nrow(z_scored_aggregate_projection_output) == 0) {
    stop("Quality check failed: final output table is empty.")
  }

  if (any(!is.finite(z_scored_aggregate_projection_output$dollars_raw), na.rm = TRUE) ||
      any(!is.finite(z_scored_aggregate_projection_output$dollars_weights), na.rm = TRUE)) {
    stop("Quality check failed: dollars columns contain non-finite values.")
  }

  if (!is.null(adp_match_audit) && is.list(adp_match_audit) && !is.null(adp_match_audit$summary)) {
    match_rate <- suppressWarnings(as.numeric(adp_match_audit$summary$match_rate[1]))
    if (is.finite(match_rate) && is.finite(min_adp_match_rate) && match_rate < min_adp_match_rate) {
      msg <- sprintf(
        "ADP match rate %.3f is below configured threshold %.3f.",
        match_rate, min_adp_match_rate
      )
      if (fail_on_adp_match_rate) {
        stop(sprintf("Quality check failed: %s", msg))
      }
      warning(sprintf("Quality check warning: %s", msg))
    }
  }

  invisible(TRUE)
}

fetch_all_systems <- function(
  systems = names(FG_SYSTEM_CODES),
  season = as.integer(format(Sys.Date(), "%Y")),
  weights = DEFAULT_PROJECTION_WEIGHTS,
  pa_floor = 200,
  category_weights = DEFAULT_CATEGORY_WEIGHTS,
  starter_count = DEFAULT_NFBC_HITTER_STARTER_COUNT,
  starter_rank_metric = "z_total_custom",
  num_teams = DEFAULT_NFBC_TEAMS,
  total_budget = DEFAULT_TOTAL_BUDGET,
  hitter_budget_share = DEFAULT_HITTER_BUDGET_SHARE,
  min_bid = DEFAULT_MIN_BID,
  adp_local_tsv_path = "",
  adp_fill_missing_with_max = TRUE,
  adp_lookback_days = DEFAULT_NFBC_ADP_LOOKBACK_DAYS,
  adp_draft_type = DEFAULT_NFBC_ADP_DRAFT_TYPE,
  adp_num_teams = DEFAULT_NFBC_ADP_NUM_TEAMS,
  adp_raw_output_path = "",
  player_match_overrides_path = "",
  player_match_overrides = NULL,
  quality = list()
) {
  weights <- validate_projection_weights(weights)
  category_weights <- validate_category_weights(category_weights)
  missing_from_run <- setdiff(systems, names(weights))
  if (length(missing_from_run) > 0) {
    stop(sprintf("No weights configured for selected systems: %s", paste(missing_from_run, collapse = ", ")))
  }

  raw_list <- list()
  standard_list <- list()
  per_pa_list <- list()

  for (system_name in systems) {
    message(sprintf("Fetching %s...", system_name))
    raw <- fetch_projection_raw(system_name = system_name, season = season)
    standard <- standardize_hitter_table(raw)
    per_pa <- add_per_pa_rates(standard)

    raw_list[[system_name]] <- raw
    standard_list[[system_name]] <- standard
    per_pa_list[[system_name]] <- per_pa
  }

  combined_standard <- do.call(rbind, standard_list)
  combined_standard$season <- as.integer(combined_standard$season)
  rownames(combined_standard) <- NULL

  combined_per_pa <- do.call(rbind, per_pa_list)
  combined_per_pa$season <- as.integer(combined_per_pa$season)
  rownames(combined_per_pa) <- NULL

  weighted_aggregate <- weighted_aggregate_per_pa(combined_per_pa, weights = weights[systems])
  zscore_result <- compute_zscores_per_pa(
    weighted_aggregate,
    pa_floor = pa_floor,
    category_weights = category_weights,
    starter_count = starter_count,
    starter_rank_metric = starter_rank_metric
  )
  dollar_result <- compute_dollar_values(
    z_scores = zscore_result$z_scores,
    starter_pool = zscore_result$starter_pool,
    starter_count = starter_count,
    num_teams = num_teams,
    total_budget = total_budget,
    hitter_budget_share = hitter_budget_share,
    min_bid = min_bid
  )
  atc_standard <- if ("atc" %in% names(standard_list)) standard_list[["atc"]] else NULL
  adp_raw_path <- if (nzchar(adp_raw_output_path)) {
    adp_raw_output_path
  } else {
    file.path("data", "raw", sprintf("%s_nfbc_adp.tsv", season))
  }
  adp_result <- tryCatch(
    fetch_nfbc_adp(
      raw_output_path = adp_raw_path,
      local_tsv_path = adp_local_tsv_path,
      lookback_days = adp_lookback_days,
      draft_type = adp_draft_type,
      num_teams = adp_num_teams
    ),
    error = function(e) {
      warning(sprintf("NFBC ADP fetch unavailable: %s", conditionMessage(e)))
      list(
        adp = empty_nfbc_adp_table(),
        source = "unavailable",
        source_path = NA_character_,
        from_date = NA_character_,
        to_date = NA_character_,
        draft_type = adp_draft_type,
        num_teams = adp_num_teams
      )
    }
  )
  overrides <- player_match_overrides
  if (is.null(overrides)) {
    overrides <- load_player_match_overrides(player_match_overrides_path)
  }

  z_output_result <- build_z_scored_aggregate_projection_output_table(
    dollar_result$z_scores,
    atc_standard = atc_standard,
    nfbc_adp = adp_result$adp,
    player_match_overrides = overrides,
    fill_missing_adp_with_max = adp_fill_missing_with_max
  )
  z_scored_aggregate_projection_output <- z_output_result$output

  validate_pipeline_quality(
    raw_list = raw_list,
    weighted_aggregate = weighted_aggregate,
    z_scores = dollar_result$z_scores,
    z_scored_aggregate_projection_output = z_scored_aggregate_projection_output,
    adp_match_audit = z_output_result$adp_match_audit,
    quality = quality
  )

  list(
    season = season,
    pa_floor = pa_floor,
    category_weights = category_weights,
    starter_count = starter_count,
    starter_rank_metric = starter_rank_metric,
    num_teams = num_teams,
    total_budget = total_budget,
    hitter_budget_share = hitter_budget_share,
    min_bid = min_bid,
    raw = raw_list,
    standard = standard_list,
    per_pa = per_pa_list,
    combined_standard = combined_standard,
    combined_per_pa = combined_per_pa,
    weighted_aggregate = weighted_aggregate,
    z_scores = dollar_result$z_scores,
    zscore_reference = zscore_result$zscore_reference,
    starter_pool = zscore_result$starter_pool,
    dollar_reference = dollar_result$dollar_reference,
    nfbc_adp = adp_result$adp,
    nfbc_adp_source = adp_result$source,
    nfbc_adp_source_path = adp_result$source_path,
    nfbc_adp_from_date = adp_result$from_date,
    nfbc_adp_to_date = adp_result$to_date,
    nfbc_adp_draft_type = adp_result$draft_type,
    nfbc_adp_num_teams = adp_result$num_teams,
    adp_match_audit = z_output_result$adp_match_audit,
    player_match_overrides = overrides,
    player_match_overrides_path = player_match_overrides_path,
    z_scored_aggregate_projection_output = z_scored_aggregate_projection_output,
    # Backward-compatible alias for older script references.
    draft_board = z_scored_aggregate_projection_output
  )
}
