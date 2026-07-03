source(file.path("R", "utils.R"))

slugify_text <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- tolower(trimws(x))
  x <- gsub("[^a-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x
}

normalize_team_abbrev_pf <- function(x) {
  out <- toupper(trimws(as.character(x)))
  out[is.na(out)] <- ""
  out <- gsub("[^A-Z0-9]", "", out)

  mapped <- c(
    ARI = "AZ", ARZ = "AZ",
    ATL = "ATL",
    BAL = "BAL",
    BOS = "BOS",
    CHC = "CHC",
    CHW = "CHW", CWS = "CHW",
    CIN = "CIN",
    CLE = "CLE",
    COL = "COL",
    DET = "DET",
    HOU = "HOU",
    KCR = "KCR", KC = "KCR",
    LAA = "LAA", ANA = "LAA",
    LAD = "LAD",
    MIA = "MIA", FLA = "MIA",
    MIL = "MIL",
    MIN = "MIN",
    NYM = "NYM",
    NYY = "NYY",
    ATH = "ATH", OAK = "ATH", SAC = "ATH",
    PHI = "PHI",
    PIT = "PIT",
    SDP = "SDP", SD = "SDP",
    SEA = "SEA",
    SFG = "SFG", SF = "SFG",
    STL = "STL",
    TBR = "TBR", TB = "TBR",
    TEX = "TEX",
    TOR = "TOR",
    WSH = "WSH", WSN = "WSH", WAS = "WSH"
  )

  hit <- out %in% names(mapped)
  out[hit] <- mapped[out[hit]]
  out
}

first_present_column <- function(data, candidates) {
  if (!is.data.frame(data)) {
    return(NULL)
  }
  nms <- names(data)
  idx <- match(tolower(candidates), tolower(nms), nomatch = 0)
  idx <- idx[idx > 0]
  if (length(idx) == 0) {
    return(rep(NA, nrow(data)))
  }
  data[[nms[idx[1]]]]
}

as_date_safe <- function(x) {
  if (inherits(x, "Date")) {
    return(x)
  }
  x <- as.character(x)
  x <- substr(x, 1, 10)
  suppressWarnings(as.Date(x))
}

weighted_mean <- function(x, w) {
  keep <- !is.na(x) & !is.na(w)
  if (!any(keep)) {
    return(NA_real_)
  }
  sum(x[keep] * w[keep]) / sum(w[keep])
}

mode_value <- function(x) {
  x <- x[!is.na(x) & nzchar(as.character(x))]
  if (length(x) == 0) {
    return(NA_character_)
  }
  names(sort(table(x), decreasing = TRUE))[1]
}

build_contact_shape_keys <- function(data) {
  ev_bin <- ifelse(
    is.finite(data$launch_speed),
    as.integer(floor(data$launch_speed / 2) * 2),
    NA_integer_
  )
  la_bin <- ifelse(
    is.finite(data$launch_angle),
    as.integer(floor(data$launch_angle / 2) * 2),
    NA_integer_
  )
  spray_bin <- ifelse(
    is.finite(data$spray_angle),
    as.integer(floor(data$spray_angle / 5) * 5),
    NA_integer_
  )
  stand_bin <- ifelse(data$stand %in% c("L", "R", "S"), data$stand, "U")

  key_full <- ifelse(
    is.finite(ev_bin) & is.finite(la_bin) & is.finite(spray_bin),
    paste(ev_bin, la_bin, spray_bin, stand_bin, sep = "|"),
    NA_character_
  )
  key_evla_stand <- ifelse(
    is.finite(ev_bin) & is.finite(la_bin),
    paste(ev_bin, la_bin, stand_bin, sep = "|"),
    NA_character_
  )
  key_evla <- ifelse(
    is.finite(ev_bin) & is.finite(la_bin),
    paste(ev_bin, la_bin, sep = "|"),
    NA_character_
  )

  data.frame(
    key_full = key_full,
    key_evla_stand = key_evla_stand,
    key_evla = key_evla,
    stringsAsFactors = FALSE
  )
}

build_smoothed_lookup <- function(outcome, key, prior_rate, prior_n) {
  keep <- is.finite(outcome) & !is.na(key) & nzchar(key)
  if (!any(keep)) {
    return(list(rate = numeric(0), n = integer(0)))
  }

  d <- data.frame(
    key = as.character(key[keep]),
    y = as.numeric(outcome[keep]),
    one = 1L,
    stringsAsFactors = FALSE
  )
  agg <- stats::aggregate(d[, c("y", "one")], by = list(key = d$key), FUN = sum)
  agg$rate <- (agg$y + prior_rate * prior_n) / (agg$one + prior_n)

  list(
    rate = stats::setNames(agg$rate, agg$key),
    n = stats::setNames(as.integer(agg$one), agg$key)
  )
}

expected_from_contact_shape <- function(outcome, keys, min_bin_n = 50L, prior_n = 200L) {
  overall <- mean(outcome, na.rm = TRUE)
  if (!is.finite(overall)) {
    overall <- 0
  }

  lk1 <- build_smoothed_lookup(outcome, keys$key_full, overall, prior_n)
  lk2 <- build_smoothed_lookup(outcome, keys$key_evla_stand, overall, prior_n)
  lk3 <- build_smoothed_lookup(outcome, keys$key_evla, overall, prior_n)

  pred <- rep(NA_real_, length(outcome))

  k1 <- keys$key_full
  if (length(lk1$rate) > 0) {
    p1 <- lk1$rate[k1]
    n1 <- lk1$n[k1]
    use1 <- !is.na(p1) & !is.na(n1) & n1 >= min_bin_n
    pred[use1] <- p1[use1]
  }

  k2 <- keys$key_evla_stand
  need2 <- is.na(pred)
  if (any(need2) && length(lk2$rate) > 0) {
    p2 <- lk2$rate[k2]
    n2 <- lk2$n[k2]
    use2 <- need2 & !is.na(p2) & !is.na(n2) & n2 >= min_bin_n
    pred[use2] <- p2[use2]
  }

  k3 <- keys$key_evla
  need3 <- is.na(pred)
  if (any(need3) && length(lk3$rate) > 0) {
    p3 <- lk3$rate[k3]
    n3 <- lk3$n[k3]
    use3 <- need3 & !is.na(p3) & !is.na(n3) & n3 >= min_bin_n
    pred[use3] <- p3[use3]
  }

  pred[is.na(pred)] <- overall
  pmin(pmax(pred, 0), 1)
}

add_contact_event_residuals <- function(bbe, min_bin_n = 50L, prior_n = 200L) {
  events_lc <- tolower(trimws(bbe$events))

  bbe$single_on_contact <- ifelse(events_lc == "single", 1, 0)
  bbe$double_on_contact <- ifelse(events_lc == "double", 1, 0)
  bbe$triple_on_contact <- ifelse(events_lc == "triple", 1, 0)
  bbe$hr_on_contact <- ifelse(events_lc == "home_run", 1, 0)
  bbe$hit_on_contact <- ifelse(events_lc %in% c("single", "double", "triple", "home_run"), 1, 0)
  bbe$xbh_on_contact <- ifelse(events_lc %in% c("double", "triple", "home_run"), 1, 0)

  keys <- build_contact_shape_keys(bbe)

  bbe$xhit_contact <- expected_from_contact_shape(bbe$hit_on_contact, keys, min_bin_n = min_bin_n, prior_n = prior_n)
  bbe$xhr_contact <- expected_from_contact_shape(bbe$hr_on_contact, keys, min_bin_n = min_bin_n, prior_n = prior_n)
  bbe$x2b_contact <- expected_from_contact_shape(bbe$double_on_contact, keys, min_bin_n = min_bin_n, prior_n = prior_n)
  bbe$x3b_contact <- expected_from_contact_shape(bbe$triple_on_contact, keys, min_bin_n = min_bin_n, prior_n = prior_n)
  bbe$xxbh_contact <- expected_from_contact_shape(bbe$xbh_on_contact, keys, min_bin_n = min_bin_n, prior_n = prior_n)

  bbe$xba_con <- pmin(pmax(bbe$xba_con, 0), 1)
  bbe$xba_filled <- ifelse(is.finite(bbe$xba_con), bbe$xba_con, bbe$xhit_contact)

  bbe$bacon_resid <- bbe$hit_on_contact - bbe$xba_filled
  bbe$hr_resid <- bbe$hr_on_contact - bbe$xhr_contact
  bbe$double_resid <- bbe$double_on_contact - bbe$x2b_contact
  bbe$triple_resid <- bbe$triple_on_contact - bbe$x3b_contact
  bbe$xbh_resid <- bbe$xbh_on_contact - bbe$xxbh_contact

  # Ottoneu FanGraphs Points on contact, from the scoring constants in
  # R/fangraphs_projections.R (OTTONEU_FG_HITTING_POINTS): AB -1.0, H +5.6,
  # 2B +2.9, 3B +5.7, HR +9.4. Every BBE is treated as an AB, which slightly
  # overpenalizes sacrifice flies (~1% of BBE). The expectation reuses the
  # smoothed contact-shape event probabilities so actual and expected share
  # one methodology.
  pts_from_events <- function(hit, dbl, tpl, hr) {
    -1.0 + 5.6 * hit + 2.9 * dbl + 5.7 * tpl + 9.4 * hr
  }
  bbe$pts_on_contact <- pts_from_events(bbe$hit_on_contact, bbe$double_on_contact, bbe$triple_on_contact, bbe$hr_on_contact)
  bbe$xpts_contact <- pts_from_events(bbe$xhit_contact, bbe$x2b_contact, bbe$x3b_contact, bbe$xhr_contact)
  bbe$pts_resid <- bbe$pts_on_contact - bbe$xpts_contact

  # Carry outcome: projected landing distance for balls hit in the air.
  # Grounder "distance" is roll- and defense-dependent, so it is excluded by
  # leaving it NA (component fits drop non-finite outcomes).
  if (!"hit_distance" %in% names(bbe)) {
    bbe$hit_distance <- NA_real_
  }
  bb_type_lc <- tolower(trimws(as.character(bbe$bb_type %||% "")))
  bbe$hit_distance_air <- ifelse(
    bb_type_lc %in% c("fly_ball", "line_drive") & is.finite(bbe$hit_distance),
    bbe$hit_distance,
    NA_real_
  )

  bbe
}

standardize_bbe_columns <- function(raw) {
  out <- data.frame(.row_id = seq_len(nrow(raw)), stringsAsFactors = FALSE)

  out$game_pk <- suppressWarnings(as.numeric(first_present_column(raw, c("game_pk", "gamepk"))))
  out$game_date <- as_date_safe(first_present_column(raw, c("game_date", "date", "gameDate")))
  out$game_type <- as.character(first_present_column(raw, c("game_type", "gameType")))
  out$home_team <- normalize_team_abbrev_pf(first_present_column(raw, c("home_team", "home")))
  out$away_team <- normalize_team_abbrev_pf(first_present_column(raw, c("away_team", "away")))
  out$inning_topbot <- as.character(first_present_column(raw, c("inning_topbot", "inning_topbot_description", "inning_half")))

  out$batter <- suppressWarnings(as.numeric(first_present_column(raw, c("batter", "batter_id"))))
  out$pitcher <- suppressWarnings(as.numeric(first_present_column(raw, c("pitcher", "pitcher_id"))))
  out$stand <- toupper(trimws(as.character(first_present_column(raw, c("stand", "batter_stand", "bat_side")))))

  out$woba_con <- suppressWarnings(as.numeric(first_present_column(raw, c("woba_con", "wobacon", "woba_value", "woba"))))
  out$xwoba_con <- suppressWarnings(as.numeric(first_present_column(raw, c("xwoba_con", "xwobacon", "estimated_woba_using_speedangle", "xwoba", "xwoba_value"))))
  out$xba_con <- suppressWarnings(as.numeric(first_present_column(raw, c("estimated_ba_using_speedangle", "xba", "xba_value"))))
  out$events <- as.character(first_present_column(raw, c("events", "event")))

  out$launch_speed <- suppressWarnings(as.numeric(first_present_column(raw, c("launch_speed", "exit_velocity", "ev"))))
  out$launch_angle <- suppressWarnings(as.numeric(first_present_column(raw, c("launch_angle", "la"))))
  out$hit_distance <- suppressWarnings(as.numeric(first_present_column(raw, c("hit_distance", "hit_distance_sc"))))
  out$xslg_con <- suppressWarnings(as.numeric(first_present_column(raw, c("xslg_con", "estimated_slg_using_speedangle"))))
  out$bb_type <- as.character(first_present_column(raw, c("bb_type", "batted_ball_type")))
  out$hc_x <- suppressWarnings(as.numeric(first_present_column(raw, c("hc_x"))))
  out$hc_y <- suppressWarnings(as.numeric(first_present_column(raw, c("hc_y"))))

  out$spray_angle <- suppressWarnings(as.numeric(first_present_column(raw, c("spray_angle", "hc_angle"))))
  # Derive spray angle from Statcast hit coordinates when absent (the BBE store
  # carries hc_x/hc_y only). Home plate sits at (125.42, 198.27) in hit
  # coordinate space; 0.75 converts plot-space angle to field bearing
  # (baseballr convention). Negative = left field, positive = right field.
  need_spray <- !is.finite(out$spray_angle) & is.finite(out$hc_x) & is.finite(out$hc_y)
  out$spray_angle[need_spray] <- round(
    atan((out$hc_x[need_spray] - 125.42) / (198.27 - out$hc_y[need_spray])) * 180 / pi * 0.75,
    1
  )

  out$temp <- suppressWarnings(as.numeric(first_present_column(raw, c("temp", "temperature", "game_temperature"))))
  out$wind_speed <- suppressWarnings(as.numeric(first_present_column(raw, c("wind_speed", "windspeed"))))
  out$humidity <- suppressWarnings(as.numeric(first_present_column(raw, c("humidity", "rel_humidity"))))
  out$drag <- suppressWarnings(as.numeric(first_present_column(raw, c("drag", "drag_coefficient", "drag_daily"))))

  out$venue_id <- suppressWarnings(as.numeric(first_present_column(raw, c("venue_id", "home_venue_id", "park_id"))))
  out$venue_name <- as.character(first_present_column(raw, c("venue_name", "park_name", "stadium", "venue")))

  out$home_team[is.na(out$home_team)] <- ""
  out$away_team[is.na(out$away_team)] <- ""
  out$stand[!out$stand %in% c("L", "R", "S")] <- "U"
  out$.row_id <- NULL
  out
}

extract_team_from_schedule <- function(team_obj) {
  if (is.null(team_obj) || !is.list(team_obj)) {
    return(NA_character_)
  }
  candidates <- c("abbreviation", "teamCode", "fileCode", "name")
  for (nm in candidates) {
    val <- team_obj[[nm]]
    if (!is.null(val) && length(val) > 0 && nzchar(as.character(val[[1]]))) {
      return(as.character(val[[1]]))
    }
  }
  NA_character_
}

fetch_schedule_venues <- function(seasons, game_type = "R", sleep_sec = 0.25) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required for schedule fetch.")
  }

  seasons <- sort(unique(as.integer(seasons)))
  seasons <- seasons[!is.na(seasons)]
  if (length(seasons) == 0) {
    return(data.frame())
  }

  rows <- vector("list", 0)

  for (season in seasons) {
    url <- sprintf(
      "https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=%s&gameType=%s",
      season,
      utils::URLencode(game_type, reserved = TRUE)
    )

    payload <- tryCatch(
      jsonlite::fromJSON(url, simplifyVector = FALSE),
      error = function(e) {
        warning(sprintf("Schedule fetch failed for season %s: %s", season, conditionMessage(e)))
        NULL
      }
    )
    if (is.null(payload) || is.null(payload$dates)) {
      Sys.sleep(sleep_sec)
      next
    }

    for (d in payload$dates) {
      games <- d$games %||% list()
      if (length(games) == 0) {
        next
      }
      for (g in games) {
        home_team <- extract_team_from_schedule(g$teams$home$team)
        away_team <- extract_team_from_schedule(g$teams$away$team)
        venue_id <- suppressWarnings(as.numeric((g$venue$id %||% NA)))
        venue_name <- as.character(g$venue$name %||% NA_character_)
        game_pk <- suppressWarnings(as.numeric((g$gamePk %||% NA)))
        game_date <- as_date_safe(g$gameDate %||% d$date %||% NA_character_)

        rows[[length(rows) + 1L]] <- data.frame(
          game_pk = game_pk,
          game_date = game_date,
          season = as.integer(season),
          game_type = as.character(g$gameType %||% game_type),
          home_team = normalize_team_abbrev_pf(home_team),
          away_team = normalize_team_abbrev_pf(away_team),
          venue_id = venue_id,
          venue_name = venue_name,
          stringsAsFactors = FALSE
        )
      }
    }

    Sys.sleep(sleep_sec)
  }

  if (length(rows) == 0) {
    return(data.frame())
  }

  out <- do.call(rbind, rows)
  out <- out[!is.na(out$game_pk), ]
  out <- out[!duplicated(out$game_pk), ]
  rownames(out) <- NULL
  out
}

merge_schedule_venues <- function(bbe_data, schedule_data) {
  if (!is.data.frame(schedule_data) || nrow(schedule_data) == 0) {
    return(bbe_data)
  }

  schedule_keep <- schedule_data[, c("game_pk", "venue_id", "venue_name")]
  merged <- merge(
    bbe_data,
    schedule_keep,
    by = "game_pk",
    all.x = TRUE,
    suffixes = c("", "_sched")
  )

  if (!"venue_id" %in% names(merged)) {
    merged$venue_id <- merged$venue_id_sched
  } else {
    merged$venue_id <- ifelse(!is.na(merged$venue_id), merged$venue_id, merged$venue_id_sched)
  }

  if (!"venue_name" %in% names(merged)) {
    merged$venue_name <- merged$venue_name_sched
  } else {
    missing_name <- is.na(merged$venue_name) | !nzchar(trimws(merged$venue_name))
    merged$venue_name[missing_name] <- merged$venue_name_sched[missing_name]
  }

  merged$venue_id_sched <- NULL
  merged$venue_name_sched <- NULL
  merged
}

load_park_events <- function(path) {
  if (!nzchar(path) || !file.exists(path)) {
    return(data.frame())
  }

  events <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
  required <- c("event_id", "team", "venue_id", "park_name_regex", "start_date", "end_date", "era_suffix")
  missing <- setdiff(required, names(events))
  if (length(missing) > 0) {
    stop(sprintf("Park events file missing required columns: %s", paste(missing, collapse = ", ")))
  }

  events$team <- normalize_team_abbrev_pf(events$team)
  events$venue_id <- suppressWarnings(as.numeric(events$venue_id))
  events$start_date <- as_date_safe(events$start_date)
  events$end_date <- as_date_safe(events$end_date)
  events$park_name_regex <- as.character(events$park_name_regex)
  events$era_suffix <- slugify_text(events$era_suffix)
  events
}

apply_park_events <- function(model_data, events) {
  if (!is.data.frame(events) || nrow(events) == 0) {
    model_data$era_suffix <- "base"
    model_data$park_era_id <- paste0(model_data$base_park_id, "__", model_data$era_suffix)
    return(model_data)
  }

  model_data$era_suffix <- "base"

  for (i in seq_len(nrow(events))) {
    e <- events[i, ]
    match_team <- if (!is.na(e$team) && nzchar(e$team)) model_data$home_team == e$team else rep(TRUE, nrow(model_data))
    match_venue <- if (!is.na(e$venue_id)) model_data$venue_id == e$venue_id else rep(TRUE, nrow(model_data))

    if (!is.na(e$park_name_regex) && nzchar(e$park_name_regex)) {
      venue_name <- as.character(model_data$venue_name %||% "")
      venue_name[is.na(venue_name)] <- ""
      has_usable_name <- nzchar(trimws(venue_name)) & tolower(trimws(venue_name)) != "unknown_venue"
      name_match_raw <- grepl(e$park_name_regex, venue_name, ignore.case = TRUE)
      # When venue names are missing in BBE rows, fall back to team/date era split.
      match_name <- ifelse(has_usable_name, name_match_raw, TRUE)
    } else {
      match_name <- rep(TRUE, nrow(model_data))
    }

    start_date <- e$start_date
    end_date <- e$end_date
    if (is.na(start_date)) {
      start_date <- as.Date("1900-01-01")
    }
    if (is.na(end_date)) {
      end_date <- as.Date("2999-12-31")
    }
    match_date <- !is.na(model_data$game_date) & model_data$game_date >= start_date & model_data$game_date <= end_date

    hit <- match_team & match_venue & match_name & match_date
    model_data$era_suffix[hit] <- e$era_suffix
  }

  model_data$park_era_id <- paste0(model_data$base_park_id, "__", model_data$era_suffix)
  model_data
}

build_defense_composite <- function(defense_raw) {
  if (!is.data.frame(defense_raw) || nrow(defense_raw) == 0) {
    return(data.frame())
  }

  team <- first_present_column(defense_raw, c("team", "tm"))
  season <- suppressWarnings(as.integer(first_present_column(defense_raw, c("season", "year"))))
  oaa <- suppressWarnings(as.numeric(first_present_column(defense_raw, c("oaa", "OAA"))))
  drs <- suppressWarnings(as.numeric(first_present_column(defense_raw, c("drs", "DRS"))))
  uzr <- suppressWarnings(as.numeric(first_present_column(defense_raw, c("uzr", "UZR"))))

  out <- data.frame(
    team = normalize_team_abbrev_pf(team),
    season = season,
    oaa = oaa,
    drs = drs,
    uzr = uzr,
    stringsAsFactors = FALSE
  )

  out <- out[!is.na(out$season) & nzchar(out$team), ]
  if (nrow(out) == 0) {
    return(data.frame())
  }

  z_by_season <- function(x, season) {
    ave(x, season, FUN = function(v) {
      if (all(is.na(v))) {
        return(rep(NA_real_, length(v)))
      }
      s <- stats::sd(v, na.rm = TRUE)
      m <- mean(v, na.rm = TRUE)
      if (is.na(s) || s == 0) {
        return(rep(0, length(v)))
      }
      (v - m) / s
    })
  }

  out$z_oaa <- z_by_season(out$oaa, out$season)
  out$z_drs <- z_by_season(out$drs, out$season)
  out$z_uzr <- z_by_season(out$uzr, out$season)

  z_mat <- cbind(out$z_oaa, out$z_drs, out$z_uzr)
  out$defense_composite <- rowMeans(z_mat, na.rm = TRUE)
  out$defense_composite[!is.finite(out$defense_composite)] <- NA_real_

  out <- out[, c("team", "season", "oaa", "drs", "uzr", "defense_composite")]
  out <- out[!duplicated(out[, c("team", "season")]), ]
  rownames(out) <- NULL
  out
}

prepare_bbe_model_data <- function(
  bbe_raw,
  schedule_data = NULL,
  park_events = data.frame(),
  defense_data = data.frame(),
  drag_data = data.frame(),
  min_season = 2015,
  exclude_seasons = c(2020),
  regular_season_only = TRUE
) {
  bbe <- standardize_bbe_columns(bbe_raw)

  required_cols <- c("game_pk", "game_date", "home_team", "away_team", "inning_topbot", "batter", "pitcher", "woba_con", "xwoba_con")
  missing_required <- required_cols[!required_cols %in% names(bbe)]
  if (length(missing_required) > 0) {
    stop(sprintf("Missing required BBE columns after standardization: %s", paste(missing_required, collapse = ", ")))
  }

  bbe <- bbe[!is.na(bbe$game_date), ]
  bbe$season <- as.integer(format(bbe$game_date, "%Y"))
  bbe <- bbe[!is.na(bbe$season) & bbe$season >= as.integer(min_season), ]
  bbe <- bbe[!bbe$season %in% as.integer(exclude_seasons), ]

  if (regular_season_only && "game_type" %in% names(bbe)) {
    keep_game_type <- is.na(bbe$game_type) | bbe$game_type == "" | toupper(bbe$game_type) == "R"
    bbe <- bbe[keep_game_type, ]
  }

  is_bbe <- !is.na(bbe$launch_speed) & !is.na(bbe$launch_angle)
  bbe <- bbe[is_bbe, ]

  bbe <- bbe[is.finite(bbe$woba_con) & is.finite(bbe$xwoba_con), ]
  bbe$resid <- bbe$woba_con - bbe$xwoba_con
  bbe <- bbe[is.finite(bbe$resid), ]

  bbe <- add_contact_event_residuals(bbe)

  bbe$month <- as.integer(format(bbe$game_date, "%m"))
  # October belongs to 2H: the regular season runs into early October and the
  # regular_season_only filter above already removes postseason games.
  bbe$half <- ifelse(bbe$month >= 3 & bbe$month <= 6, "1H", ifelse(bbe$month >= 7 & bbe$month <= 10, "2H", NA_character_))
  bbe <- bbe[!is.na(bbe$half), ]

  inning_half <- tolower(trimws(bbe$inning_topbot))
  is_top <- grepl("top", inning_half)
  bbe$batting_team <- ifelse(is_top, bbe$away_team, bbe$home_team)
  bbe$fielding_team <- ifelse(is_top, bbe$home_team, bbe$away_team)
  bbe$batting_team <- normalize_team_abbrev_pf(bbe$batting_team)
  bbe$fielding_team <- normalize_team_abbrev_pf(bbe$fielding_team)

  need_venue_join <- !"venue_id" %in% names(bbe) || !"venue_name" %in% names(bbe) || any(is.na(bbe$venue_id))
  if (need_venue_join && is.data.frame(schedule_data) && nrow(schedule_data) > 0) {
    bbe <- merge_schedule_venues(bbe, schedule_data)
  }

  bbe$venue_id <- suppressWarnings(as.numeric(bbe$venue_id))
  bbe$venue_name <- as.character(bbe$venue_name)
  bbe$venue_name[is.na(bbe$venue_name)] <- ""

  bbe$base_park_id <- ifelse(
    !is.na(bbe$venue_id),
    paste0("venue_", bbe$venue_id),
    paste0("home_", bbe$home_team)
  )
  bbe$base_park_id <- slugify_text(bbe$base_park_id)

  bbe <- apply_park_events(bbe, park_events)

  bbe$batter_season_id <- paste0("b", bbe$batter, "_", bbe$season)
  bbe$pitcher_season_id <- paste0("p", bbe$pitcher, "_", bbe$season)
  bbe$fielding_team_season_id <- paste0(bbe$fielding_team, "_", bbe$season)
  bbe$batting_team_season_id <- paste0(bbe$batting_team, "_", bbe$season)
  bbe$park_era_half_id <- paste0(bbe$park_era_id, "__", bbe$half)

  bbe$measurement_era <- ifelse(
    bbe$season <= 2019,
    "trackman",
    ifelse(bbe$season >= 2021, "hawkeye", "other")
  )
  bbe$measurement_era <- factor(bbe$measurement_era, levels = c("trackman", "hawkeye", "other"))

  # Slightly downweight pre-Hawk-Eye years due known quality differences.
  bbe$quality_weight <- ifelse(bbe$season <= 2019, 0.95, 1.00)

  if (is.data.frame(defense_data) && nrow(defense_data) > 0) {
    defense_key <- defense_data[, c("team", "season", "defense_composite")]
    names(defense_key) <- c("fielding_team", "season", "defense_composite")
    bbe <- merge(bbe, defense_key, by = c("fielding_team", "season"), all.x = TRUE)
  } else {
    bbe$defense_composite <- NA_real_
  }

  # Daily league-wide ball drag coefficient (Savant drag dashboard). League-wide
  # by construction — Savant's Cd estimate adjusts for environmental conditions,
  # so it is a ball property, not a park property (it cannot absorb e.g. Coors
  # altitude). Joined by game date.
  bbe$league_cd <- NA_real_
  if (is.data.frame(drag_data) && nrow(drag_data) > 0) {
    drag_key <- data.frame(
      game_date = as_date_safe(first_present_column(drag_data, c("game_date", "date"))),
      league_cd_join = suppressWarnings(as.numeric(first_present_column(drag_data, c("mean_cd", "league_cd", "drag")))),
      stringsAsFactors = FALSE
    )
    drag_key <- drag_key[!is.na(drag_key$game_date) & is.finite(drag_key$league_cd_join), ]
    drag_key <- drag_key[!duplicated(drag_key$game_date), ]
    if (nrow(drag_key) > 0) {
      bbe <- merge(bbe, drag_key, by = "game_date", all.x = TRUE)
      bbe$league_cd <- bbe$league_cd_join
      bbe$league_cd_join <- NULL
    }
  }

  # Centered value + missing indicator so lmer keeps rows without coverage
  # (2015 and early 2016 predate the drag series; 2026 team defense is not yet
  # in the manual file) instead of silently dropping them via NA handling.
  # Missing rows sit at the covariate mean and the indicator absorbs any level
  # difference of the uncovered stratum.
  center_with_indicator <- function(x) {
    ok <- is.finite(x)
    ctr <- if (any(ok)) mean(x[ok]) else 0
    list(
      centered = ifelse(ok, x - ctr, 0),
      missing = ifelse(ok, 0, 1),
      center = ctr
    )
  }

  drag_ci <- center_with_indicator(bbe$league_cd)
  bbe$drag_c <- drag_ci$centered
  bbe$drag_missing <- drag_ci$missing
  attr(bbe, "drag_center") <- drag_ci$center

  def_ci <- center_with_indicator(bbe$defense_composite)
  bbe$defense_c <- def_ci$centered
  bbe$defense_missing <- def_ci$missing

  rownames(bbe) <- NULL
  bbe
}

fit_park_factor_model <- function(model_data, include_measurement_era = TRUE, quiet = FALSE) {
  if (!requireNamespace("lme4", quietly = TRUE)) {
    stop("Package 'lme4' is required. Install with install.packages('lme4').")
  }

  d <- model_data
  d <- d[is.finite(d$resid), ]

  fixed_terms <- character(0)
  if ("half" %in% names(d) && length(unique(stats::na.omit(d$half))) > 1) {
    fixed_terms <- c(fixed_terms, "half")
  }
  if (isTRUE(include_measurement_era) &&
      "measurement_era" %in% names(d) &&
      length(unique(stats::na.omit(d$measurement_era))) > 1) {
    fixed_terms <- c(fixed_terms, "measurement_era")
  }
  for (nm in c("temp", "wind_speed", "humidity", "drag_c", "drag_missing", "defense_c", "defense_missing")) {
    if (nm %in% names(d) && any(is.finite(d[[nm]]))) {
      vals <- d[[nm]][is.finite(d[[nm]])]
      if (length(unique(vals)) < 2) {
        next
      }
      fixed_terms <- c(fixed_terms, nm)
    }
  }

  rhs <- c(
    fixed_terms,
    "(1 | park_era_id)",
    "(1 | park_era_half_id)",
    "(1 | batter_season_id)",
    "(1 | pitcher_season_id)",
    "(1 | fielding_team_season_id)",
    "(1 | batting_team_season_id)"
  )

  form <- stats::as.formula(paste("resid ~", paste(rhs, collapse = " + ")))

  if (!quiet) {
    message("Fitting model with formula: ", deparse(form))
    message("Rows: ", nrow(d))
  }

  fit <- lme4::lmer(
    formula = form,
    data = d,
    weights = d$quality_weight,
    REML = TRUE,
    control = lme4::lmerControl(optimizer = "bobyqa", calc.derivs = FALSE)
  )

  list(
    fit = fit,
    formula = form,
    data = d,
    baseline_xwoba = mean(d$xwoba_con, na.rm = TRUE)
  )
}

fit_component_model <- function(
  model_data,
  outcome_col,
  include_measurement_era = TRUE,
  include_contact_shape = FALSE,
  quiet = FALSE
) {
  if (!requireNamespace("lme4", quietly = TRUE)) {
    stop("Package 'lme4' is required. Install with install.packages('lme4').")
  }
  if (!outcome_col %in% names(model_data)) {
    stop(sprintf("Outcome column '%s' not found in model_data.", outcome_col))
  }

  d <- model_data
  d <- d[is.finite(d[[outcome_col]]), ]
  if (nrow(d) == 0) {
    stop(sprintf("No rows with finite '%s' values.", outcome_col))
  }

  fixed_terms <- character(0)
  if ("half" %in% names(d) && length(unique(stats::na.omit(d$half))) > 1) {
    fixed_terms <- c(fixed_terms, "half")
  }
  if (isTRUE(include_measurement_era) &&
      "measurement_era" %in% names(d) &&
      length(unique(stats::na.omit(d$measurement_era))) > 1) {
    fixed_terms <- c(fixed_terms, "measurement_era")
  }

  for (nm in c("temp", "wind_speed", "humidity", "drag_c", "drag_missing", "defense_c", "defense_missing")) {
    if (nm %in% names(d) && any(is.finite(d[[nm]]))) {
      vals <- d[[nm]][is.finite(d[[nm]])]
      if (length(unique(vals)) < 2) {
        next
      }
      fixed_terms <- c(fixed_terms, nm)
    }
  }

  if (isTRUE(include_contact_shape)) {
    if ("launch_speed" %in% names(d) && any(is.finite(d$launch_speed))) {
      fixed_terms <- c(fixed_terms, "launch_speed", "I(launch_speed^2)")
    }
    if ("launch_angle" %in% names(d) && any(is.finite(d$launch_angle))) {
      fixed_terms <- c(fixed_terms, "launch_angle", "I(launch_angle^2)")
    }
  }

  rhs <- c(
    fixed_terms,
    "(1 | park_era_id)",
    "(1 | park_era_half_id)",
    "(1 | batter_season_id)",
    "(1 | pitcher_season_id)",
    "(1 | fielding_team_season_id)",
    "(1 | batting_team_season_id)"
  )

  form <- stats::as.formula(paste(outcome_col, "~", paste(rhs, collapse = " + ")))

  if (!quiet) {
    message("Fitting component model with formula: ", deparse(form))
    message("Rows: ", nrow(d))
  }

  fit <- lme4::lmer(
    formula = form,
    data = d,
    weights = d$quality_weight,
    REML = TRUE,
    control = lme4::lmerControl(optimizer = "bobyqa", calc.derivs = FALSE)
  )

  list(
    fit = fit,
    formula = form,
    data = d,
    baseline = mean(d[[outcome_col]], na.rm = TRUE),
    outcome_col = outcome_col
  )
}

extract_random_effects_with_se <- function(fit, group_name) {
  re <- lme4::ranef(fit, condVar = TRUE)
  if (!group_name %in% names(re)) {
    return(data.frame())
  }

  obj <- re[[group_name]]
  vals <- as.numeric(obj[, 1])
  lvls <- rownames(obj)

  pv <- attr(obj, "postVar")
  se <- rep(NA_real_, length(vals))
  if (!is.null(pv) && length(dim(pv)) == 3) {
    se <- sqrt(pv[1, 1, ])
  }

  data.frame(level = lvls, effect = vals, se = se, stringsAsFactors = FALSE)
}

extract_park_factors <- function(model_fit, model_data) {
  fit <- model_fit$fit
  baseline <- model_fit$baseline_xwoba %||% mean(model_data$xwoba_con, na.rm = TRUE)

  park_re <- extract_random_effects_with_se(fit, "park_era_id")
  names(park_re) <- c("park_era_id", "park_effect", "park_se")

  half_re <- extract_random_effects_with_se(fit, "park_era_half_id")
  names(half_re) <- c("park_era_half_id", "half_effect", "half_se")

  counts <- stats::aggregate(
    rep(1, nrow(model_data)),
    by = list(park_era_id = model_data$park_era_id, half = model_data$half),
    FUN = sum
  )
  names(counts)[3] <- "n_bbe"

  meta <- stats::aggregate(
    model_data[, c("venue_id", "venue_name", "home_team")],
    by = list(park_era_id = model_data$park_era_id),
    FUN = mode_value
  )

  out <- merge(counts, meta, by = "park_era_id", all.x = TRUE)
  out$park_era_half_id <- paste0(out$park_era_id, "__", out$half)

  out <- merge(out, park_re, by = "park_era_id", all.x = TRUE)
  out <- merge(out, half_re, by = "park_era_half_id", all.x = TRUE)

  out$park_effect[is.na(out$park_effect)] <- 0
  out$half_effect[is.na(out$half_effect)] <- 0

  out$delta_woba_over_xwoba_overall <- out$park_effect
  out$delta_woba_over_xwoba_half <- out$park_effect + out$half_effect

  out$pf_index_overall <- 100 * (1 + out$delta_woba_over_xwoba_overall / baseline)
  out$pf_index_half <- 100 * (1 + out$delta_woba_over_xwoba_half / baseline)

  out$overall_se <- out$park_se
  out$half_se_combined <- sqrt((out$park_se %||% 0)^2 + (out$half_se %||% 0)^2)

  out <- out[order(out$pf_index_half, decreasing = TRUE), ]
  rownames(out) <- NULL
  out
}

extract_component_park_factors <- function(component_fit, model_data, label) {
  fit <- component_fit$fit
  baseline <- component_fit$baseline %||% mean(component_fit$data[[component_fit$outcome_col]], na.rm = TRUE)
  if (!is.finite(baseline) || baseline == 0) {
    baseline <- 1e-6
  }

  park_re <- extract_random_effects_with_se(fit, "park_era_id")
  names(park_re) <- c("park_era_id", "park_effect", "park_se")

  half_re <- extract_random_effects_with_se(fit, "park_era_half_id")
  names(half_re) <- c("park_era_half_id", "half_effect", "half_se")

  counts <- stats::aggregate(
    rep(1, nrow(model_data)),
    by = list(park_era_id = model_data$park_era_id, half = model_data$half),
    FUN = sum
  )
  names(counts)[3] <- "n_bbe"

  meta <- stats::aggregate(
    model_data[, c("venue_id", "venue_name", "home_team")],
    by = list(park_era_id = model_data$park_era_id),
    FUN = mode_value
  )

  out <- merge(counts, meta, by = "park_era_id", all.x = TRUE)
  out$park_era_half_id <- paste0(out$park_era_id, "__", out$half)
  out <- merge(out, park_re, by = "park_era_id", all.x = TRUE)
  out <- merge(out, half_re, by = "park_era_half_id", all.x = TRUE)

  out$park_effect[is.na(out$park_effect)] <- 0
  out$half_effect[is.na(out$half_effect)] <- 0

  out$delta_overall <- out$park_effect
  out$delta_half <- out$park_effect + out$half_effect
  out$pf_index_overall <- 100 * (1 + out$delta_overall / baseline)
  out$pf_index_half <- 100 * (1 + out$delta_half / baseline)
  out$component <- label

  out <- out[order(out$pf_index_half, decreasing = TRUE), ]
  rownames(out) <- NULL
  out
}

predict_park_effect_rows <- function(fit, new_data) {
  park_re <- extract_random_effects_with_se(fit, "park_era_id")
  half_re <- extract_random_effects_with_se(fit, "park_era_half_id")
  park_map <- stats::setNames(park_re$effect, park_re$level)
  half_map <- stats::setNames(half_re$effect, half_re$level)

  park_val <- park_map[new_data$park_era_id]
  half_val <- half_map[new_data$park_era_half_id]

  park_val[is.na(park_val)] <- 0
  half_val[is.na(half_val)] <- 0

  as.numeric(park_val + half_val)
}

# Map season-keyed random-effect levels ("b661388_2024", "NYY_2023") to their
# entity's most recent training-season effect, so player/team skill estimated
# in training can be carried into a holdout season the fit never saw.
latest_entity_effect_map <- function(fit, group_name) {
  re <- extract_random_effects_with_se(fit, group_name)
  if (nrow(re) == 0) {
    return(stats::setNames(numeric(0), character(0)))
  }
  entity <- sub("_[0-9]{4}$", "", re$level)
  season <- suppressWarnings(as.integer(sub("^.*_", "", re$level)))
  ord <- order(entity, season)
  re <- re[ord, ]
  entity <- entity[ord]
  keep <- !duplicated(entity, fromLast = TRUE)
  stats::setNames(re$effect[keep], entity[keep])
}

# Player/team composition ("nuisance") effect per row. exact_season = TRUE
# looks up the row's own season-keyed level (for rows the fit was trained on);
# FALSE carries each entity's most recent training-season effect forward (for
# holdout rows whose season-keyed levels are unseen by construction).
predict_nuisance_effect_rows <- function(fit, new_data, exact_season = FALSE) {
  groups <- c(
    batter = "batter_season_id",
    pitcher = "pitcher_season_id",
    fielding = "fielding_team_season_id",
    batting = "batting_team_season_id"
  )

  total <- rep(0, nrow(new_data))
  for (g in groups) {
    if (exact_season) {
      re <- extract_random_effects_with_se(fit, g)
      if (nrow(re) == 0) next
      m <- stats::setNames(re$effect, re$level)
      v <- m[new_data[[g]]]
    } else {
      m <- latest_entity_effect_map(fit, g)
      if (length(m) == 0) next
      entity_key <- sub("_[0-9]{4}$", "", new_data[[g]])
      v <- m[entity_key]
    }
    v[is.na(v)] <- 0
    total <- total + as.numeric(v)
  }
  total
}

rolling_validate_park_factors <- function(
  model_data,
  train_window = 3L,
  min_train_seasons = 3L,
  include_measurement_era = TRUE,
  verbose = FALSE
) {
  seasons <- sort(unique(model_data$season))
  seasons <- seasons[!is.na(seasons)]

  if (length(seasons) < (min_train_seasons + 1L)) {
    return(list(summary = data.frame(), detail = data.frame()))
  }

  summary_rows <- vector("list", 0)
  detail_rows <- vector("list", 0)

  for (i in seq_along(seasons)) {
    target <- seasons[[i]]
    prior <- seasons[seasons < target]
    if (length(prior) < min_train_seasons) {
      next
    }

    train_seasons <- tail(prior, train_window)
    train <- model_data[model_data$season %in% train_seasons, ]
    hold <- model_data[model_data$season == target, ]

    if (nrow(train) < 500 || nrow(hold) < 200) {
      next
    }

    if (isTRUE(verbose)) {
      message(sprintf(
        "Validation fold: target=%s train=%s-%s rows(train=%s, hold=%s) include_measurement_era=%s",
        target,
        min(train_seasons),
        max(train_seasons),
        nrow(train),
        nrow(hold),
        include_measurement_era
      ))
    }

    fit_obj <- tryCatch(
      fit_park_factor_model(train, include_measurement_era = include_measurement_era, quiet = TRUE),
      error = function(e) {
        warning(sprintf("Validation fit failed for target %s: %s", target, conditionMessage(e)))
        NULL
      }
    )
    if (is.null(fit_obj)) {
      next
    }

    hold$pred_effect <- predict_park_effect_rows(fit_obj$fit, hold)

    # Composition-adjusted realized values: subtract the training fit's
    # player/team effects from holdout residuals (entities carried forward by
    # most recent training season). The raw prev-mean baseline "wins" on raw
    # residuals partly by predicting persistent home-team quality; adjusting
    # both sides compares them on the park quantity itself.
    hold$nuisance_effect <- predict_nuisance_effect_rows(fit_obj$fit, hold, exact_season = FALSE)
    hold$resid_adj <- hold$resid - hold$nuisance_effect

    by_key <- list(park_era_id = hold$park_era_id, half = hold$half)
    detail_pred <- stats::aggregate(hold$pred_effect, by = by_key, FUN = mean)
    names(detail_pred)[3] <- "pred_effect"
    detail_real <- stats::aggregate(hold$resid, by = by_key, FUN = mean)
    names(detail_real)[3] <- "realized"
    detail_real_adj <- stats::aggregate(hold$resid_adj, by = by_key, FUN = mean)
    names(detail_real_adj)[3] <- "realized_adj"
    detail_n <- stats::aggregate(rep(1, nrow(hold)), by = by_key, FUN = sum)
    names(detail_n)[3] <- "n"
    detail <- merge(detail_pred, detail_real, by = c("park_era_id", "half"), all = TRUE)
    detail <- merge(detail, detail_real_adj, by = c("park_era_id", "half"), all = TRUE)
    detail <- merge(detail, detail_n, by = c("park_era_id", "half"), all = TRUE)

    train$nuisance_exact <- predict_nuisance_effect_rows(fit_obj$fit, train, exact_season = TRUE)
    train$resid_adj <- train$resid - train$nuisance_exact
    prev <- stats::aggregate(
      train$resid,
      by = list(park_era_id = train$park_era_id, half = train$half),
      FUN = mean
    )
    names(prev)[3] <- "prev_mean"
    prev_adj <- stats::aggregate(
      train$resid_adj,
      by = list(park_era_id = train$park_era_id, half = train$half),
      FUN = mean
    )
    names(prev_adj)[3] <- "prev_mean_adj"
    detail <- merge(detail, prev, by = c("park_era_id", "half"), all.x = TRUE)
    detail <- merge(detail, prev_adj, by = c("park_era_id", "half"), all.x = TRUE)
    detail$prev_mean[is.na(detail$prev_mean)] <- 0
    detail$prev_mean_adj[is.na(detail$prev_mean_adj)] <- 0

    rmse_model <- sqrt(weighted_mean((detail$realized - detail$pred_effect)^2, detail$n))
    rmse_zero <- sqrt(weighted_mean((detail$realized - 0)^2, detail$n))
    rmse_prev <- sqrt(weighted_mean((detail$realized - detail$prev_mean)^2, detail$n))
    rmse_model_adj <- sqrt(weighted_mean((detail$realized_adj - detail$pred_effect)^2, detail$n))
    rmse_prev_adj <- sqrt(weighted_mean((detail$realized_adj - detail$prev_mean_adj)^2, detail$n))

    corr <- suppressWarnings(stats::cor(detail$pred_effect, detail$realized, use = "complete.obs"))
    corr_prev <- suppressWarnings(stats::cor(detail$prev_mean, detail$realized, use = "complete.obs"))
    corr_adj <- suppressWarnings(stats::cor(detail$pred_effect, detail$realized_adj, use = "complete.obs"))
    corr_prev_adj <- suppressWarnings(stats::cor(detail$prev_mean_adj, detail$realized_adj, use = "complete.obs"))

    slope <- NA_real_
    if (nrow(detail) >= 5 && stats::sd(detail$pred_effect, na.rm = TRUE) > 0) {
      slope_fit <- tryCatch(stats::lm(realized ~ pred_effect, data = detail, weights = n), error = function(e) NULL)
      if (!is.null(slope_fit)) {
        slope <- stats::coef(slope_fit)[["pred_effect"]] %||% NA_real_
      }
    }

    detail$season <- target
    detail_rows[[length(detail_rows) + 1L]] <- detail

    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      season = target,
      train_start = min(train_seasons),
      train_end = max(train_seasons),
      n_park_half = nrow(detail),
      rmse_model = rmse_model,
      rmse_zero = rmse_zero,
      rmse_prev = rmse_prev,
      rmse_model_adj = rmse_model_adj,
      rmse_prev_adj = rmse_prev_adj,
      corr_model_vs_realized = corr,
      corr_prev_vs_realized = corr_prev,
      corr_model_vs_realized_adj = corr_adj,
      corr_prev_vs_realized_adj = corr_prev_adj,
      calibration_slope = slope,
      stringsAsFactors = FALSE
    )

    if (isTRUE(verbose)) {
      message(sprintf(
        "Validation fold complete: target=%s rmse_model=%.6f rmse_prev=%.6f corr=%.4f slope=%.4f",
        target,
        rmse_model,
        rmse_prev,
        corr,
        slope
      ))
    }
  }

  summary_out <- if (length(summary_rows) > 0) do.call(rbind, summary_rows) else data.frame()
  detail_out <- if (length(detail_rows) > 0) do.call(rbind, detail_rows) else data.frame()

  list(summary = summary_out, detail = detail_out)
}

compare_measurement_era_models <- function(
  model_data,
  train_window = 3L,
  min_train_seasons = 3L,
  verbose = FALSE
) {
  if (isTRUE(verbose)) {
    message("Comparing validation with measurement-era term ON")
  }
  with_era <- rolling_validate_park_factors(
    model_data = model_data,
    train_window = train_window,
    min_train_seasons = min_train_seasons,
    include_measurement_era = TRUE,
    verbose = verbose
  )

  if (isTRUE(verbose)) {
    message("Comparing validation with measurement-era term OFF")
  }
  without_era <- rolling_validate_park_factors(
    model_data = model_data,
    train_window = train_window,
    min_train_seasons = min_train_seasons,
    include_measurement_era = FALSE,
    verbose = verbose
  )

  safe_mean <- function(x) {
    if (length(x) == 0 || all(is.na(x))) {
      return(NA_real_)
    }
    mean(x, na.rm = TRUE)
  }

  summary_tbl <- data.frame(
    model = c("with_measurement_era", "without_measurement_era"),
    mean_rmse_model = c(
      safe_mean(with_era$summary$rmse_model),
      safe_mean(without_era$summary$rmse_model)
    ),
    mean_corr_model_vs_realized = c(
      safe_mean(with_era$summary$corr_model_vs_realized),
      safe_mean(without_era$summary$corr_model_vs_realized)
    ),
    mean_calibration_slope = c(
      safe_mean(with_era$summary$calibration_slope),
      safe_mean(without_era$summary$calibration_slope)
    ),
    seasons_evaluated = c(
      nrow(with_era$summary),
      nrow(without_era$summary)
    ),
    stringsAsFactors = FALSE
  )

  list(
    summary = summary_tbl,
    with_era = with_era,
    without_era = without_era
  )
}

summarize_team_park_eras <- function(model_data) {
  if (!is.data.frame(model_data) || nrow(model_data) == 0) {
    empty <- data.frame()
    return(list(team_year = empty, primary = empty, transitions = empty))
  }

  key <- data.frame(
    season = as.integer(model_data$season),
    home_team = as.character(model_data$home_team),
    venue_id = suppressWarnings(as.numeric(model_data$venue_id)),
    venue_name = as.character(model_data$venue_name),
    park_era_id = as.character(model_data$park_era_id),
    n_bbe = 1L,
    stringsAsFactors = FALSE
  )

  # When schedule enrichment is unavailable, venue_id can be entirely NA; keep rows by imputing a stable key.
  key$venue_id[is.na(key$venue_id)] <- -1
  bad_name <- is.na(key$venue_name) | !nzchar(trimws(key$venue_name))
  key$venue_name[bad_name] <- "unknown_venue"

  team_year <- stats::aggregate(
    n_bbe ~ season + home_team + venue_id + venue_name + park_era_id,
    data = key,
    FUN = sum
  )

  if (nrow(team_year) == 0) {
    empty <- data.frame()
    return(list(team_year = empty, primary = empty, transitions = empty))
  }

  team_year$n_bbe <- suppressWarnings(as.numeric(team_year$n_bbe))
  team_year$n_bbe[is.na(team_year$n_bbe)] <- 0

  team_year <- team_year[order(team_year$home_team, team_year$season, -team_year$n_bbe), ]
  rownames(team_year) <- NULL

  primary <- do.call(rbind, lapply(
    split(team_year, list(team_year$home_team, team_year$season), drop = TRUE),
    function(df) df[which.max(df$n_bbe), , drop = FALSE]
  ))
  rownames(primary) <- NULL
  primary <- primary[order(primary$home_team, primary$season), ]

  transitions <- primary
  transitions$prior_park_era_id <- ave(
    transitions$park_era_id,
    transitions$home_team,
    FUN = function(x) c(NA_character_, x[-length(x)])
  )
  transitions$changed_from_prior <- transitions$park_era_id != transitions$prior_park_era_id
  transitions$changed_from_prior[is.na(transitions$changed_from_prior)] <- FALSE

  list(team_year = team_year, primary = primary, transitions = transitions)
}

compute_invariance_checks <- function(model_data, park_factors) {
  if (!is.data.frame(park_factors) || nrow(park_factors) == 0) {
    return(data.frame())
  }

  home_off <- stats::aggregate(
    model_data$xwoba_con,
    by = list(season = model_data$season, team = model_data$batting_team),
    FUN = mean
  )
  names(home_off)[3] <- "team_xwoba_con"

  park_home <- stats::aggregate(
    rep(1, nrow(model_data)),
    by = list(park_era_id = model_data$park_era_id, home_team = model_data$home_team, season = model_data$season),
    FUN = sum
  )

  park_overall <- stats::aggregate(
    park_factors$delta_woba_over_xwoba_overall,
    by = list(park_era_id = park_factors$park_era_id),
    FUN = mean
  )
  names(park_overall)[2] <- "park_effect"

  chk <- merge(park_home, park_overall, by = "park_era_id", all.x = TRUE)
  chk <- merge(chk, home_off, by.x = c("season", "home_team"), by.y = c("season", "team"), all.x = TRUE)

  offense_corr <- suppressWarnings(stats::cor(chk$park_effect, chk$team_xwoba_con, use = "complete.obs"))

  defense_corr <- NA_real_
  if ("defense_composite" %in% names(model_data) && any(is.finite(model_data$defense_composite))) {
    defense_team <- stats::aggregate(
      model_data$defense_composite,
      by = list(season = model_data$season, team = model_data$fielding_team),
      FUN = mean
    )
    names(defense_team)[3] <- "team_defense"
    chk2 <- merge(chk, defense_team, by.x = c("season", "home_team"), by.y = c("season", "team"), all.x = TRUE)
    defense_corr <- suppressWarnings(stats::cor(chk2$park_effect, chk2$team_defense, use = "complete.obs"))
  }

  data.frame(
    metric = c("corr_park_effect_vs_home_team_xwoba_con", "corr_park_effect_vs_home_team_defense"),
    value = c(offense_corr, defense_corr),
    stringsAsFactors = FALSE
  )
}
