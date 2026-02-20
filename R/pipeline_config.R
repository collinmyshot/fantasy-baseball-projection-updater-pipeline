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
    pitcher = list(
      projection = list(
        system = "oopsy"
      ),
      google_sheets = list(
        workbook_url = "",
        integrated_tab = "Pitcher Integrated",
        sp_skillz_tab = "Pitcher SP Skillz",
        auto_export = FALSE
      ),
      sp_skillz = list(
        source_url = "https://www.fangraphs.com/leaders/major-league?pos=all&stats=pit&lg=all&type=c%2C7%2C8%2C14%2C13%2C55%2C57%2C-1%2C6%2C62%2C122%2C42%2C-1%2C120%2C331%2C121%2C29%2C31%2C-1%2C105%2C110%2C-1%2C386%2C387%2C388&month=0&ind=0&team=0&rost=0&age=0&filter&players=0&startdate&enddate&v_cr=legacy&qual=5&season1=2025&season=2025",
        lookback_season = 2025,
        num_teams = 15,
        sp_depth = 10,
        start_share_min = 0.67,
        use_ip_paradigms = TRUE,
        low_ip_max_start_ip = 80,
        high_ip_min_start_ip = 100,
        gs_fallback_threshold = 5,
        weights = list(
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
        ),
        low_ip_weights = list(
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
        ),
        high_ip_weights = list(
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
        ),
        stabilization_points = list(
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
        ),
        reliability_method = "sample_over_sample_plus_stab",
        eno = list(
          sheet_url = "https://docs.google.com/spreadsheets/d/1daR9RNic3GcfDb6FLsm2OZRBS8VkqucOqHSnIS7ru5c/edit?usp=sharing",
          sheet_tab = "Feb 3 Ranks",
          include_2026_stuff_plus = TRUE
        )
      )
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
      run_data_tab = "Run Data",
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

  cfg$pitcher$projection$system <- tolower(as.character(cfg$pitcher$projection$system %||% "oopsy"))
  if (!nzchar(cfg$pitcher$projection$system)) {
    cfg$pitcher$projection$system <- "oopsy"
  }
  allowed_projection_systems <- c("steamer", "oopsy", "atc", "thebat", "zips")
  if (!cfg$pitcher$projection$system %in% allowed_projection_systems) {
    stop(sprintf("Unsupported pitcher projection system '%s'. Allowed: %s", cfg$pitcher$projection$system, paste(allowed_projection_systems, collapse = ", ")))
  }
  cfg$pitcher$google_sheets$workbook_url <- as.character(cfg$pitcher$google_sheets$workbook_url %||% "")
  cfg$pitcher$google_sheets$integrated_tab <- as.character(cfg$pitcher$google_sheets$integrated_tab %||% "Pitcher Integrated")
  cfg$pitcher$google_sheets$sp_skillz_tab <- as.character(cfg$pitcher$google_sheets$sp_skillz_tab %||% "Pitcher SP Skillz")
  cfg$pitcher$google_sheets$auto_export <- isTRUE(cfg$pitcher$google_sheets$auto_export)

  cfg$pitcher$sp_skillz$lookback_season <- as.integer(round(ensure_scalar_numeric(cfg$pitcher$sp_skillz$lookback_season, cfg$season - 1)))
  cfg$pitcher$sp_skillz$num_teams <- as.integer(round(ensure_scalar_numeric(cfg$pitcher$sp_skillz$num_teams, cfg$projection$league$num_teams)))
  cfg$pitcher$sp_skillz$sp_depth <- ensure_scalar_numeric(cfg$pitcher$sp_skillz$sp_depth, 10)
  cfg$pitcher$sp_skillz$start_share_min <- ensure_scalar_numeric(cfg$pitcher$sp_skillz$start_share_min, 0.67)
  cfg$pitcher$sp_skillz$use_ip_paradigms <- isTRUE(cfg$pitcher$sp_skillz$use_ip_paradigms)
  cfg$pitcher$sp_skillz$low_ip_max_start_ip <- ensure_scalar_numeric(cfg$pitcher$sp_skillz$low_ip_max_start_ip, 80)
  cfg$pitcher$sp_skillz$high_ip_min_start_ip <- ensure_scalar_numeric(cfg$pitcher$sp_skillz$high_ip_min_start_ip, 100)
  cfg$pitcher$sp_skillz$gs_fallback_threshold <- ensure_scalar_numeric(cfg$pitcher$sp_skillz$gs_fallback_threshold, 5)
  cfg$pitcher$sp_skillz$source_url <- as.character(cfg$pitcher$sp_skillz$source_url %||% "")

  weights_raw <- unlist(cfg$pitcher$sp_skillz$weights, recursive = TRUE, use.names = TRUE)
  weights <- as.numeric(weights_raw)
  names(weights) <- names(weights_raw)
  cfg$pitcher$sp_skillz$weights <- weights

  low_ip_weights_raw <- unlist(cfg$pitcher$sp_skillz$low_ip_weights, recursive = TRUE, use.names = TRUE)
  low_ip_weights <- as.numeric(low_ip_weights_raw)
  names(low_ip_weights) <- names(low_ip_weights_raw)
  cfg$pitcher$sp_skillz$low_ip_weights <- low_ip_weights

  high_ip_weights_raw <- unlist(cfg$pitcher$sp_skillz$high_ip_weights, recursive = TRUE, use.names = TRUE)
  high_ip_weights <- as.numeric(high_ip_weights_raw)
  names(high_ip_weights) <- names(high_ip_weights_raw)
  cfg$pitcher$sp_skillz$high_ip_weights <- high_ip_weights

  stabilization_points_raw <- unlist(cfg$pitcher$sp_skillz$stabilization_points, recursive = TRUE, use.names = TRUE)
  stabilization_points <- as.numeric(stabilization_points_raw)
  names(stabilization_points) <- names(stabilization_points_raw)
  cfg$pitcher$sp_skillz$stabilization_points <- stabilization_points
  cfg$pitcher$sp_skillz$reliability_method <- as.character(cfg$pitcher$sp_skillz$reliability_method %||% "sample_over_sample_plus_stab")

  cfg$pitcher$sp_skillz$eno$sheet_url <- as.character(cfg$pitcher$sp_skillz$eno$sheet_url %||% "")
  cfg$pitcher$sp_skillz$eno$sheet_tab <- as.character(cfg$pitcher$sp_skillz$eno$sheet_tab %||% "Feb 3 Ranks")
  cfg$pitcher$sp_skillz$eno$include_2026_stuff_plus <- isTRUE(cfg$pitcher$sp_skillz$eno$include_2026_stuff_plus)

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
