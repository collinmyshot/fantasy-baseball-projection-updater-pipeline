`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

is_named_list <- function(x) {
  is.list(x) && !is.null(names(x)) && all(nzchar(names(x)))
}

deep_merge <- function(base, override) {
  if (!is.list(base) || !is.list(override)) {
    return(override)
  }

  out <- base
  for (nm in names(override)) {
    if (nm %in% names(out) && is_named_list(out[[nm]]) && is_named_list(override[[nm]])) {
      out[[nm]] <- deep_merge(out[[nm]], override[[nm]])
    } else {
      out[[nm]] <- override[[nm]]
    }
  }
  out
}

ensure_scalar_numeric <- function(x, fallback) {
  v <- suppressWarnings(as.numeric(x))
  if (length(v) != 1 || is.na(v)) {
    return(fallback)
  }
  v
}

default_pipeline_config <- function() {
  list(
    season = as.integer(format(Sys.Date(), "%Y")),
    paths = list(
      raw_dir = "data/raw",
      processed_dir = "data/processed",
      match_overrides_csv = "data/manual/player_match_overrides.csv"
    ),
    projection = list(
      systems = c("batx", "steamer", "oopsy", "atc"),
      system_weights = list(batx = 3, steamer = 2, oopsy = 3, atc = 1),
      category_weights = list(pa = 2.3, hr = 1.35, sb = 1.0, r = 0.6, rbi = 0.6, h = 1.0),
      pa_floor = 200,
      starter_rank_metric = "z_total_custom",
      league = list(
        num_teams = 15,
        hitter_slots = list(c = 2, b1 = 1, b2 = 1, b3 = 1, ss = 1, of = 5, ci = 1, mi = 1, ut = 1)
      ),
      auction = list(total_budget = 260, hitter_budget_share = 0.70, min_bid = 1)
    ),
    adp = list(
      local_tsv = "data/raw/ADP.tsv",
      draft_type = 897,
      num_teams = 12,
      lookback_days = 14,
      fill_missing_with_max = TRUE
    ),
    google_sheets = list(
      workbook_url = "",
      source_ranks_url = "",
      projection_tab = "Projections_Bats",
      adp_tab = "ADP",
      rbll_tab = "RBLL Team & Targets"
    ),
    quality = list(
      min_rows_per_system = 500,
      min_weighted_rows = 300,
      min_zscore_rows = 150,
      min_adp_match_rate = 0.35,
      fail_on_adp_match_rate = FALSE
    )
  )
}

normalize_pipeline_config <- function(cfg) {
  cfg$season <- as.integer(ensure_scalar_numeric(cfg$season, as.integer(format(Sys.Date(), "%Y"))))

  cfg$projection$systems <- unique(tolower(as.character(unlist(cfg$projection$systems))))
  cfg$projection$systems <- cfg$projection$systems[nzchar(cfg$projection$systems)]

  system_weights <- unlist(cfg$projection$system_weights, recursive = TRUE, use.names = TRUE)
  cfg$projection$system_weights <- as.numeric(system_weights)
  names(cfg$projection$system_weights) <- names(system_weights)

  category_weights <- unlist(cfg$projection$category_weights, recursive = TRUE, use.names = TRUE)
  cfg$projection$category_weights <- as.numeric(category_weights)
  names(cfg$projection$category_weights) <- names(category_weights)

  cfg$projection$pa_floor <- ensure_scalar_numeric(cfg$projection$pa_floor, 200)
  cfg$projection$league$num_teams <- ensure_scalar_numeric(cfg$projection$league$num_teams, 15)

  hitter_slots_raw <- unlist(cfg$projection$league$hitter_slots, recursive = TRUE, use.names = TRUE)
  hitter_slots <- as.numeric(hitter_slots_raw)
  names(hitter_slots) <- names(hitter_slots_raw)
  cfg$projection$league$hitter_slots <- hitter_slots

  cfg$projection$starter_count <- as.integer(round(cfg$projection$league$num_teams * sum(hitter_slots, na.rm = TRUE)))

  cfg$projection$auction$total_budget <- ensure_scalar_numeric(cfg$projection$auction$total_budget, 260)
  cfg$projection$auction$hitter_budget_share <- ensure_scalar_numeric(cfg$projection$auction$hitter_budget_share, 0.70)
  cfg$projection$auction$min_bid <- ensure_scalar_numeric(cfg$projection$auction$min_bid, 1)

  cfg$adp$draft_type <- as.integer(round(ensure_scalar_numeric(cfg$adp$draft_type, 897)))
  cfg$adp$num_teams <- as.integer(round(ensure_scalar_numeric(cfg$adp$num_teams, 12)))
  cfg$adp$lookback_days <- as.integer(round(ensure_scalar_numeric(cfg$adp$lookback_days, 14)))
  cfg$adp$fill_missing_with_max <- isTRUE(cfg$adp$fill_missing_with_max)

  cfg$quality$min_rows_per_system <- as.integer(round(ensure_scalar_numeric(cfg$quality$min_rows_per_system, 500)))
  cfg$quality$min_weighted_rows <- as.integer(round(ensure_scalar_numeric(cfg$quality$min_weighted_rows, 300)))
  cfg$quality$min_zscore_rows <- as.integer(round(ensure_scalar_numeric(cfg$quality$min_zscore_rows, 150)))
  cfg$quality$min_adp_match_rate <- ensure_scalar_numeric(cfg$quality$min_adp_match_rate, 0.35)
  cfg$quality$fail_on_adp_match_rate <- isTRUE(cfg$quality$fail_on_adp_match_rate)

  cfg
}

load_pipeline_config <- function(config_path = file.path("config", "pipeline.yml"), overrides = list()) {
  cfg <- default_pipeline_config()

  if (nzchar(config_path) && file.exists(config_path)) {
    if (!requireNamespace("yaml", quietly = TRUE)) {
      stop("Package 'yaml' is required to read config files. Install with install.packages('yaml').")
    }
    loaded <- yaml::read_yaml(config_path)
    if (is.list(loaded)) {
      cfg <- deep_merge(cfg, loaded)
    }
  }

  if (is.list(overrides) && length(overrides) > 0) {
    cfg <- deep_merge(cfg, overrides)
  }

  normalize_pipeline_config(cfg)
}
