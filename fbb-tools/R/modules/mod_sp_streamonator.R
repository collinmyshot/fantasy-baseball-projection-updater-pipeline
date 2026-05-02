suppressPackageStartupMessages({
  library(DT)
  library(jsonlite)
})

# ── Constants ──────────────────────────────────────────────────────────────────

STREAM_PROBABLES_URL    <- "https://www.fangraphs.com/api/roster-resource/probables-grid/data"
STREAM_FG_USER_AGENT    <- "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
STREAM_MLB_SCHEDULE_URL <- "https://statsapi.mlb.com/api/v1/schedule"

# MLB team ID → canonical abbreviation (used by both SP Streamonator and SP Outlook)
STREAM_MLB_ID_TO_ABR <- c(
  `108`="LAA", `109`="AZ",  `110`="BAL", `111`="BOS", `112`="CHC",
  `113`="CIN", `114`="CLE", `115`="COL", `116`="DET", `117`="HOU",
  `118`="KCR", `119`="LAD", `120`="WSH", `121`="NYM", `133`="ATH",
  `134`="PIT", `135`="SDP", `136`="SEA", `137`="SFG", `138`="STL",
  `139`="TBR", `140`="TEX", `141`="TOR", `142`="MIN", `143`="PHI",
  `144`="ATL", `145`="CHW", `146`="MIA", `147`="NYY", `158`="MIL"
)

STREAM_PF_FILE       <- "data/processed/park_factors/park_factors_savant_style_clean_2026_with_id.csv"
STREAM_SPZ_2025_FILE <- "data/processed/2025_sp_skillz_scores_2026_plus_model.csv"
STREAM_SPZ_2026_FILES <- list(
  std = "data/processed/2026_sp_skillz_scores_std.csv",
  l30 = "data/processed/2026_sp_skillz_scores_l30.csv"
)

# Team abbrev map: FanGraphs → park factor team_norm
STREAM_TEAM_MAP <- c(
  ARI="AZ", ARZ="AZ", AZ="AZ",
  ATH="ATH", OAK="ATH",
  BAL="BAL", BOS="BOS",
  CHC="CHC", CHN="CHC",
  CHW="CHW", CHA="CHW", CWS="CHW",
  CIN="CIN", CLE="CLE", CLV="CLE",
  COL="COL", DET="DET", HOU="HOU",
  KC="KCR", KCR="KCR",
  LAA="LAA", LAD="LAD", LA="LAD",
  MIA="MIA", FLA="MIA",
  MIL="MIL", MIN="MIN",
  NYM="NYM", NYY="NYY", PHI="PHI", PIT="PIT",
  SD="SDP", SDP="SDP",
  SEA="SEA",
  SF="SFG", SFG="SFG",
  STL="STL",
  TB="TBR", TBR="TBR", TAM="TBR",
  TEX="TEX", TOR="TOR",
  WSH="WSH", WSN="WSH", WAS="WSH"
)

# ── Utilities ──────────────────────────────────────────────────────────────────

stream_norm_team <- function(x) {
  x <- toupper(trimws(as.character(x)))
  out <- STREAM_TEAM_MAP[x]
  out[is.na(out)] <- x[is.na(out)]
  unname(out)
}

# Display-level alias lookup: lowercase alt name → canonical display name.
# Sourced from the same player_match_overrides.csv used by player_nk().
.STREAM_ALIAS <- local({
  df <- tryCatch(
    read.csv("data/manual/player_match_overrides.csv", stringsAsFactors = FALSE),
    error = function(e) NULL)
  if (is.null(df) || !all(c("alt_name", "canonical_name") %in% names(df)))
    return(character(0))
  setNames(trimws(df$canonical_name), tolower(trimws(df$alt_name)))
})

stream_norm_name <- function(x) {
  x <- iconv(as.character(x), to = "ASCII//TRANSLIT", sub = "")
  x_lwr <- tolower(trimws(x))
  # Substitute known alt names with their canonical display form before stripping
  if (length(.STREAM_ALIAS) > 0) {
    hits <- x_lwr %in% names(.STREAM_ALIAS)
    if (any(hits)) x[hits] <- .STREAM_ALIAS[x_lwr[hits]]
  }
  gsub("[^a-z ]", "", tolower(trimws(x)))
}

stream_match_names <- function(from, to) {
  fn <- stream_norm_name(from)
  tn <- stream_norm_name(to)
  idx <- match(fn, tn)
  # Last-name fallback for unmatched
  unmatched <- which(is.na(idx))
  if (length(unmatched)) {
    fl <- vapply(strsplit(fn, " "), function(p) if (length(p)) p[length(p)] else "", character(1))
    tl <- vapply(strsplit(tn, " "), function(p) if (length(p)) p[length(p)] else "", character(1))
    for (i in unmatched) {
      if (!nzchar(fl[i])) next
      hits <- which(tl == fl[i])
      if (length(hits) == 1) idx[i] <- hits[1]
    }
  }
  idx
}

# ── Week helpers ───────────────────────────────────────────────────────────────

stream_week_monday <- function(date, offset = 0L) {
  date <- as.Date(date)
  date - (as.integer(format(date, "%u")) - 1L) + (as.integer(offset) * 7L)
}

stream_week_range <- function(today = Sys.Date(), which = "current") {
  mon <- stream_week_monday(today, if (which == "next") 1L else 0L)
  sun <- mon + 6L
  # Always start from Monday (not clipped to today) — shinyapps.io runs UTC so
  # Sys.Date() can be 1 day ahead of the user's local date; the probables data
  # itself won't contain past days, so there's nothing to exclude.
  list(start = as.Date(mon), end = as.Date(sun),
       label = sprintf("%s \u2013 %s", format(mon, "%b %d"), format(sun, "%b %d, %Y")))
}

# ── Data loaders ───────────────────────────────────────────────────────────────

stream_load_pf <- function() {
  if (!file.exists(STREAM_PF_FILE)) return(NULL)
  pf <- tryCatch(read.csv(STREAM_PF_FILE, stringsAsFactors = FALSE, check.names = FALSE),
                 error = function(e) NULL)
  if (is.null(pf) || !all(c("team_norm","overall_pf_idx_100") %in% names(pf))) return(NULL)
  pf[, intersect(c("team_norm","park","overall_pf_idx_100"), names(pf)), drop = FALSE]
}

stream_load_spz <- function(year = "2025", period = "std") {
  path <- if (year == "2025") STREAM_SPZ_2025_FILE else STREAM_SPZ_2026_FILES[[period]]
  if (is.null(path) || !file.exists(path)) return(NULL)
  tryCatch(read.csv(path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
}

stream_index_spz <- function(spz, col = "sp_skillz_score_stabilized") {
  if (is.null(spz) || !col %in% names(spz)) return(spz)
  s     <- suppressWarnings(as.numeric(spz[[col]]))
  mu    <- mean(s, na.rm = TRUE)
  sigma <- sd(s, na.rm = TRUE)
  spz$sp_skillz_index <- if (is.na(sigma) || sigma == 0)
    ifelse(is.na(s), NA_real_, 100)
  else
    round(100 + (s - mu) / sigma * 10, 1)
  spz
}

# ── FanGraphs JSON fetch (generic, used by other FG endpoints) ─────────────────

stream_fg_fetch <- function(url) {
  payload <- tryCatch(jsonlite::fromJSON(url, simplifyVector = TRUE), error = function(e) NULL)
  if (!is.null(payload)) return(list(ok = TRUE, payload = payload))

  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) return(list(ok = FALSE, error = "jsonlite failed and curl not found"))

  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  args <- c("-sS","-L","--fail","--compressed",
            "--max-time","30","--connect-timeout","10",
            "--retry","2","--retry-delay","1",
            "-A", STREAM_FG_USER_AGENT,
            "-H","Accept: application/json, text/plain, */*",
            "-H","Referer: https://www.fangraphs.com/roster-resource/probables-grid",
            url, "-o", tmp)
  result <- tryCatch(system2(curl_bin, args = args, stdout = TRUE, stderr = TRUE),
                     error = function(e) NULL)
  if (is.null(result)) return(list(ok = FALSE, error = "curl execution failed"))
  status <- attr(result, "status")
  if (!is.null(status) && as.integer(status) != 0L)
    return(list(ok = FALSE, error = paste("curl exit status:", status)))
  payload <- tryCatch(jsonlite::fromJSON(tmp, simplifyVector = TRUE), error = function(e) NULL)
  if (!is.null(payload)) return(list(ok = TRUE, payload = payload))
  list(ok = FALSE, error = "JSON parse failed after curl")
}

# ── MLB Stats API probable starters fetch ─────────────────────────────────────
# Replaces FanGraphs probables-grid for SP Streamonator + SP Outlook.
# statsapi.mlb.com is fully open — no Cloudflare, no auth, same API used for schedules.
# Returns list(data = data.frame, diag = chr); data columns match stream_parse_probables output:
#   date (Date), pitcher_name (chr), pitcher_id (int), pitcher_team (chr),
#   opponent_team (chr), home_away (chr), pitcher_throws (chr)

stream_mlb_fetch_probables <- function(from = Sys.Date(), days = 14L) {
  to  <- from + as.integer(days)
  url <- sprintf(
    "%s?sportId=1&startDate=%s&endDate=%s&gameType=R&hydrate=probablePitcher",
    STREAM_MLB_SCHEDULE_URL,
    format(from, "%Y-%m-%d"),
    format(to,   "%Y-%m-%d")
  )

  payload <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(payload) || is.null(payload$dates))
    stop("Fetch failed: MLB Stats API returned no schedule data")

  rows <- list()
  for (day_obj in payload$dates) {
    d <- as.Date(day_obj$date)
    for (game in day_obj$games) {
      for (ha in c("home", "away")) {
        opp_ha <- if (ha == "home") "away" else "home"
        side   <- game$teams[[ha]]
        pp     <- side$probablePitcher
        nm     <- tryCatch(trimws(pp$fullName %||% ""), error = function(e) "")
        if (!nzchar(nm)) next

        team_id  <- as.character(tryCatch(side$team$id,              error = function(e) NA))
        opp_id   <- as.character(tryCatch(game$teams[[opp_ha]]$team$id, error = function(e) NA))
        team_abr <- unname(STREAM_MLB_ID_TO_ABR[team_id])
        opp_abr  <- unname(STREAM_MLB_ID_TO_ABR[opp_id])
        if (is.na(team_abr) || is.na(opp_abr)) next

        throws <- tryCatch({
          t <- pp$pitchHand$code
          if (is.null(t) || !nzchar(t %||% "")) NA_character_ else toupper(t)
        }, error = function(e) NA_character_)

        rows[[length(rows) + 1L]] <- data.frame(
          date           = d,
          pitcher_name   = nm,
          pitcher_id     = suppressWarnings(as.integer(pp$id %||% NA)),
          pitcher_team   = stream_norm_team(team_abr),
          opponent_team  = stream_norm_team(opp_abr),
          home_away      = if (ha == "home") "H" else "A",
          pitcher_throws = throws,
          stringsAsFactors = FALSE
        )
      }
    }
  }

  if (!length(rows)) stop("Fetch failed: no probable pitchers in MLB Stats API response")

  df <- do.call(rbind, rows)
  rownames(df) <- NULL
  diag <- sprintf("MLB Stats API: %d pitcher-starts, %s\u2013%s",
                  nrow(df), format(from, "%m/%d"), format(to, "%m/%d"))
  list(data = df, diag = diag)
}

# ── Probables grid parser ──────────────────────────────────────────────────────
# Returns a diagnostic string AND the parsed data frame.
# Output df columns: date, pitcher_name, pitcher_id, pitcher_team, opponent_team, home_away

stream_parse_probables <- function(result) {
  if (!isTRUE(result$ok) || is.null(result$payload))
    stop("Fetch failed: ", result$error %||% "unknown error")

  p   <- result$payload
  diag <- build_payload_diag(p)

  # Strategy 1: flat data frame — try pitcher-level first, then game-level
  if (is.data.frame(p)) {
    df <- try_parse_pitcher_level(p)
    if (is.null(df)) df <- try_parse_game_level(p)
    if (is.null(df)) stop("Could not parse flat probables response. Diag:\n", diag)
    return(list(data = df, diag = diag))
  }

  # Strategy 2: nested {games: [...]} with team.sp / opponent objects (2026+ FG format)
  if (is.list(p) && "games" %in% names(p) && is.data.frame(p$games)) {
    df <- try_parse_nested_games(p$games)
    if (!is.null(df)) return(list(data = df, diag = diag))
  }

  # Strategy 3: list of team objects (team-centric, date-keyed sub-arrays)
  if (is.list(p)) {
    rows <- Filter(function(x) !is.null(x) && is.data.frame(x) && nrow(x) > 0,
                   lapply(p, parse_team_obj))
    if (length(rows)) return(list(data = do.call(rbind, rows), diag = diag))
    stop("No parseable entries in probables list response. Diag:\n", diag)
  }

  stop("Unexpected probables payload class: ", paste(class(p), collapse=", "))
}

# Build a human-readable diagnostic string for debugging
build_payload_diag <- function(p) {
  if (is.data.frame(p)) {
    paste0(
      "Shape: flat data.frame (", nrow(p), " rows x ", ncol(p), " cols)\n",
      "Columns: ", paste(names(p), collapse=", "), "\n",
      if (nrow(p) > 0) paste0("Row 1 values: ", paste(as.character(unlist(p[1,])), collapse=" | ")) else ""
    )
  } else if (is.list(p)) {
    e1 <- p[[1]]
    paste0(
      "Shape: list (", length(p), " elements)\n",
      "Element 1 names: ", paste(names(e1), collapse=", "), "\n",
      if (is.list(e1)) {
        sub_fields <- lapply(names(e1), function(nm) {
          v <- e1[[nm]]
          sprintf("  $%s [%s] len=%d", nm, class(v)[1], length(v))
        })
        paste(sub_fields, collapse="\n")
      } else ""
    )
  } else {
    paste0("Class: ", paste(class(p), collapse=", "))
  }
}

# ── Parse attempt: pitcher-level flat DF ──
# Expected: one row per pitcher start, with pitcher/team/opp/date columns
try_parse_pitcher_level <- function(df) {
  cn <- tolower(names(df))
  col1 <- function(...) {
    hit <- match(c(...), cn)
    hit <- hit[!is.na(hit)]
    if (!length(hit)) return(rep(NA_character_, nrow(df)))
    as.character(df[[hit[1]]])
  }

  p_name   <- col1("teamspplayername","startername","starter_name","sp","pitcher","pitchername","name","playername","sp_name")
  p_team   <- col1("abbname","teamabbrev","team_abbrev","team","teamabbr","teamname","tm","teamshort","shortname")
  opp_raw  <- col1("opponentabbname","opp","opponent","oppabbrev","opp_abbrev","oppteam","opp_team","opposingteam","opposing_team")
  ha_raw   <- col1("ishome","homeaway","home_away","ha","location","home")
  date_raw <- col1("gamedate","date","game_date","startdate","start_date")
  p_id     <- suppressWarnings(as.integer(as.numeric(col1("teamspplayerid","playerid","player_id","starterid","starter_id","mlbamid"))))
  throws   <- toupper(trimws(col1("throws","throw","pitchhand","pitching_hand","hand","p_throws")))

  # Require at least pitcher name AND team to consider this a pitcher-level frame
  if (all(is.na(p_name)) || all(is.na(p_team))) return(NULL)

  if (all(is.na(ha_raw))) {
    home_away <- ifelse(grepl("^@", opp_raw), "A", "H")
  } else {
    ha_up <- toupper(trimws(ha_raw))
    home_away <- ifelse(ha_up %in% c("H","HOME","1","TRUE"), "H",
                 ifelse(ha_up %in% c("A","AWAY","0","FALSE"), "A", ha_up))
  }

  # Handle ISO datetime strings like "2026-03-26T00:00:00"
  date_parsed <- suppressWarnings(as.Date(sub("T.*$", "", date_raw)))

  out <- data.frame(
    date           = date_parsed,
    pitcher_name   = trimws(p_name),
    pitcher_id     = p_id,
    pitcher_team   = stream_norm_team(p_team),
    opponent_team  = stream_norm_team(sub("^@","", opp_raw)),
    home_away      = home_away,
    pitcher_throws = throws,
    stringsAsFactors = FALSE
  )
  out <- out[!is.na(out$date) & !is.na(out$pitcher_name) & nzchar(out$pitcher_name), , drop = FALSE]
  if (nrow(out) == 0) return(NULL)
  rownames(out) <- NULL
  out
}

# ── Parse attempt: game-level flat DF ──
# Expected: one row per GAME with separate home/away pitcher columns.
# Produces two rows per game (home starter row + away starter row).
try_parse_game_level <- function(df) {
  cn <- tolower(names(df))
  col1 <- function(...) {
    hit <- match(c(...), cn)
    hit <- hit[!is.na(hit)]
    if (!length(hit)) return(rep(NA_character_, nrow(df)))
    as.character(df[[hit[1]]])
  }
  col1i <- function(...) suppressWarnings(as.integer(as.numeric(col1(...))))

  # Date
  date_raw <- col1("date","gamedate","game_date","startdate","start_date")

  # Home team / pitcher
  home_team  <- col1("hometeam","home_team","home","hometeamabbrev","home_abbrev","hteam")
  home_sp    <- col1("homesp","home_sp","homepitcher","home_pitcher","homestarterName","homestarter","home_starter","homestarter_name")
  home_id    <- col1i("homeplayerid","home_playerid","homepitcherid","home_pitcher_id","homeid","home_id")

  # Away team / pitcher
  away_team  <- col1("awayteam","away_team","away","awayteamabbrev","away_abbrev","ateam","visitor","visitorteam")
  away_sp    <- col1("awaysp","away_sp","awaypitcher","away_pitcher","awaystarterName","awaystarter","away_starter","awaystarter_name")
  away_id    <- col1i("awayplayerid","away_playerid","awaypitcherid","away_pitcher_id","awayid","away_id")

  # Require at least date + home team + away team to proceed
  if (all(is.na(date_raw)) || all(is.na(home_team)) || all(is.na(away_team))) return(NULL)

  n    <- nrow(df)
  dates <- suppressWarnings(as.Date(date_raw))

  # Build home-pitcher rows
  home_rows <- data.frame(
    date          = dates,
    pitcher_name  = trimws(home_sp),
    pitcher_id    = home_id,
    pitcher_team  = stream_norm_team(home_team),
    opponent_team = stream_norm_team(away_team),
    home_away     = "H",
    stringsAsFactors = FALSE
  )
  # Build away-pitcher rows
  away_rows <- data.frame(
    date          = dates,
    pitcher_name  = trimws(away_sp),
    pitcher_id    = away_id,
    pitcher_team  = stream_norm_team(away_team),
    opponent_team = stream_norm_team(home_team),
    home_away     = "A",
    stringsAsFactors = FALSE
  )

  out <- rbind(home_rows, away_rows)
  out <- out[!is.na(out$date) & !is.na(out$pitcher_name) & nzchar(out$pitcher_name), , drop = FALSE]
  if (nrow(out) == 0) return(NULL)
  rownames(out) <- NULL
  out
}

# ── Parse attempt: nested {games} format (2026+ FG API) ──
# Each row: teamId, abbName, gameDate, isHome, team$sp{playerId,name,throws}, opponent{teamId,abbName,sp{...}}
try_parse_nested_games <- function(games_df) {
  needed <- c("abbName", "gameDate", "isHome", "team", "opponent")
  if (!all(needed %in% names(games_df))) return(NULL)

  n <- nrow(games_df)
  rows <- vector("list", n)
  k <- 0L

  opp_df <- games_df$opponent
  tm_df  <- games_df$team

  for (i in seq_len(n)) {
    gdate   <- suppressWarnings(as.Date(sub("T.*$", "", games_df$gameDate[i])))
    is_home <- isTRUE(games_df$isHome[i])
    tm_abbr <- as.character(games_df$abbName[i])

    opp_abbr <- if (is.data.frame(opp_df) && "abbName" %in% names(opp_df))
                  as.character(opp_df$abbName[i])
                else NA_character_

    # Team's SP only — opponent's row will appear separately in the response
    if (!is.data.frame(tm_df) || !"sp" %in% names(tm_df)) next
    sp <- tm_df$sp
    sp_name   <- if (is.data.frame(sp) && "name" %in% names(sp)) as.character(sp$name[i]) else NA_character_
    sp_id     <- if (is.data.frame(sp) && "playerId" %in% names(sp)) suppressWarnings(as.integer(sp$playerId[i])) else NA_integer_
    sp_throws <- if (is.data.frame(sp) && "throws" %in% names(sp)) toupper(as.character(sp$throws[i])) else NA_character_

    if (!is.na(sp_name) && nzchar(sp_name)) {
      k <- k + 1L
      rows[[k]] <- data.frame(
        date           = gdate,
        pitcher_name   = trimws(sp_name),
        pitcher_id     = sp_id,
        pitcher_team   = stream_norm_team(tm_abbr),
        opponent_team  = stream_norm_team(opp_abbr),
        home_away      = if (is_home) "H" else "A",
        pitcher_throws = sp_throws,
        stringsAsFactors = FALSE
      )
    }
  }

  rows <- rows[seq_len(k)]
  if (!length(rows)) return(NULL)
  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  out
}

# ── Parse attempt: team-object (nested list with date sub-arrays) ──
parse_team_obj <- function(obj) {
  if (!is.list(obj)) return(NULL)

  # Find team abbrev
  team_fields <- c("teamabbrev","TeamAbbrev","teamabbr","TeamAbbr","team","Team","abbrev","Abbrev","shortname","ShortName")
  pitcher_team <- NA_character_
  for (f in team_fields) {
    if (!is.null(obj[[f]]) && nzchar(obj[[f]])) { pitcher_team <- as.character(obj[[f]]); break }
  }

  # Find the games/dates sub-array
  games_fields <- c("games","Games","dates","Dates","schedule","Schedule","data","Data","starts","Starts")
  games <- NULL
  for (f in games_fields) { if (!is.null(obj[[f]])) { games <- obj[[f]]; break } }

  # Fallback: look for any list/df element that contains "date" or "sp"
  if (is.null(games)) {
    for (nm in names(obj)) {
      v <- obj[[nm]]
      if (is.data.frame(v) || (is.list(v) && length(v) > 0)) {
        vnames <- if (is.data.frame(v)) names(v) else names(v[[1]])
        if (any(tolower(vnames) %in% c("date","sp","pitcher","startername"))) {
          games <- v; break
        }
      }
    }
  }

  if (is.null(games)) return(NULL)

  sub_df <- if (is.data.frame(games)) {
    games
  } else {
    tryCatch(as.data.frame(do.call(rbind, lapply(games, as.data.frame)), stringsAsFactors = FALSE),
             error = function(e) NULL)
  }
  if (is.null(sub_df) || nrow(sub_df) == 0) return(NULL)

  # Inject team column for pitcher-level parse
  if (!any(tolower(names(sub_df)) %in% c("teamabbrev","team_abbrev","team","teamname","tm","teamabbr"))) {
    sub_df$team <- pitcher_team
  }

  # Try pitcher-level first, then game-level
  df <- try_parse_pitcher_level(sub_df)
  if (is.null(df)) df <- try_parse_game_level(sub_df)
  df
}

# ── Streamer Score ─────────────────────────────────────────────────────────────
# Weighted mean of all non-NA components with positive weight

stream_score <- function(..., weights) {
  comps <- list(...)
  n <- length(comps[[1]])
  vapply(seq_len(n), function(i) {
    v  <- vapply(comps, `[[`, numeric(1), i)
    w  <- weights
    ok <- !is.na(v) & w > 0
    if (!any(ok)) return(NA_real_)
    round(sum(v[ok] * w[ok]) / sum(w[ok]), 1)
  }, numeric(1))
}

# ── Main build ─────────────────────────────────────────────────────────────────

stream_build <- function(probables, spz_std, spz_l30 = NULL, pf, tr_data = NULL,
                          week_start = NULL, week_end = NULL,
                          w_sp    = 6,
                          w_spz_std = 1, w_spz_l30 = 0,
                          w_tr    = 3,
                          w_tr_std = 1, w_tr_l30 = 1, w_tr_vhand = 1,
                          w_pf    = 1) {
  if (is.null(probables) || nrow(probables) == 0) return(NULL)
  df <- probables
  if (!is.null(week_start)) df <- df[!is.na(df$date) & df$date >= as.Date(week_start), ]
  if (!is.null(week_end))   df <- df[!is.na(df$date) & df$date <= as.Date(week_end),   ]
  if (nrow(df) == 0) return(NULL)

  if (!"pitcher_throws" %in% names(df)) df$pitcher_throws <- NA_character_

  # ── SP Skillz: composite of Season-to-Date and Last-30 sources ──────────────
  spz_idx <- function(spz) {
    if (is.null(spz) || nrow(spz) == 0 || !"player_name" %in% names(spz))
      return(rep(NA_real_, nrow(df)))
    spz <- stream_index_spz(spz)
    spz$sp_skillz_index[stream_match_names(df$pitcher_name, spz$player_name)]
  }
  std_spz <- spz_idx(spz_std)
  l30_spz <- spz_idx(spz_l30)
  df$sp_skillz_index <- stream_score(std_spz, l30_spz,
                                     weights = c(w_spz_std, w_spz_l30))
  # Flag pitchers not found in SP Skillz and default to 100 (league avg)
  df$spz_placeholder <- is.na(df$sp_skillz_index)
  df$sp_skillz_index[df$spz_placeholder] <- 100

  # ── Park Factor join ─────────────────────────────────────────────────────────
  if (!is.null(pf) && nrow(pf) > 0) {
    lookup         <- ifelse(df$home_away == "A", df$opponent_team, df$pitcher_team)
    pi             <- match(lookup, pf$team_norm)
    df$park_factor <- round(pf$overall_pf_idx_100[pi], 1)
    df$park_name   <- if ("park" %in% names(pf)) pf$park[pi] else NA_character_
  } else {
    df$park_factor <- NA_real_
    df$park_name   <- NA_character_
  }

  # ── Team Rater: 3-component composite ───────────────────────────────────────
  get_tr_idx <- function(tbl) {
    if (is.null(tbl) || nrow(tbl) == 0) return(rep(NA_real_, nrow(df)))
    tbl$team_rater_index[match(df$opponent_team, stream_norm_team(tbl$abbr))]
  }
  tr_vhand_idx <- vapply(seq_len(nrow(df)), function(i) {
    hand <- toupper(trimws(df$pitcher_throws[i] %||% ""))
    if (is.na(hand) || !nzchar(hand)) return(NA_real_)
    tbl  <- if (hand == "L") tr_data$vlhp else tr_data$vrhp
    if (is.null(tbl) || nrow(tbl) == 0) return(NA_real_)
    idx  <- match(df$opponent_team[i], stream_norm_team(tbl$abbr))
    if (is.na(idx)) NA_real_ else tbl$team_rater_index[idx]
  }, numeric(1))
  df$team_rater <- stream_score(
    get_tr_idx(tr_data$std), get_tr_idx(tr_data$l30), tr_vhand_idx,
    weights = c(w_tr_std, w_tr_l30, w_tr_vhand)
  )
  # Flag teams not found in Team Rater and default to 100 (league avg)
  df$tr_placeholder <- is.na(df$team_rater)
  df$team_rater[df$tr_placeholder] <- 100

  # ── Final Streamer Score ─────────────────────────────────────────────────────
  # SP Skillz: higher = better (keep as-is)
  # Team Rater: higher = stronger offense = worse for pitcher → invert with 200-x
  # Park Factor: higher = hitter-friendly = worse for pitcher → invert with 200-x
  df$streamer_score <- stream_score(
    df$sp_skillz_index,
    200 - df$team_rater,
    ifelse(is.na(df$park_factor), NA_real_, 200 - df$park_factor),
    weights = c(w_sp, w_tr, w_pf)
  )

  # ── 2-start flags ────────────────────────────────────────────────────────────
  cnt <- table(df$pitcher_name)
  df$two_start_flag <- df$pitcher_name %in% names(cnt[cnt >= 2])

  # Suspicious 2-start: starts < 4 days apart (e.g. Fri+Sun = 2 days = 1 day rest)
  df$suspicious_rest <- FALSE
  for (p in names(cnt[cnt >= 2])) {
    rows  <- which(df$pitcher_name == p)
    dates <- sort(df$date[rows])
    if (length(dates) >= 2 && any(as.integer(diff(dates)) < 4)) {
      df$suspicious_rest[rows] <- TRUE
    }
  }

  df[order(df$date, -ifelse(is.na(df$streamer_score), -Inf, df$streamer_score)), , drop = FALSE]
}

# ── Display formatter ──────────────────────────────────────────────────────────
# Column order: Date | Pitcher | Team | Opp | Park | SP Skillz | Team Rater |
#               Park Factor | Streamer Score | Pitcher (repeat)

stream_format_display <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)
  susp        <- !is.na(df$suspicious_rest) & df$suspicious_rest
  two         <- !is.na(df$two_start_flag)  & df$two_start_flag
  spz_ph      <- !is.na(df$spz_placeholder) & df$spz_placeholder
  tr_ph       <- !is.na(df$tr_placeholder)  & df$tr_placeholder
  # Star-wrap 2-start pitchers; append dagger for suspicious rest
  p_name  <- ifelse(two,  paste0("\u2605 ", df$pitcher_name, " \u2605"), df$pitcher_name)
  p_label <- ifelse(susp, paste0(p_name, " \u2020"), p_name)
  # Opp column: "@ OPP" when pitcher is away, "vs OPP" when pitcher is home
  opp_label <- ifelse(df$home_away == "A",
                      paste0("@ ",  df$opponent_team),
                      paste0("vs ", df$opponent_team))
  # Append asterisk for placeholder (avg default) values
  spz_display <- ifelse(spz_ph, paste0(df$sp_skillz_index, "*"), as.character(df$sp_skillz_index))
  tr_display  <- ifelse(tr_ph,  paste0(df$team_rater, "*"),      as.character(df$team_rater))
  data.frame(
    Date             = format(df$date, "%a %b %d"),
    Pitcher          = p_label,
    Team             = df$pitcher_team,
    Opp              = opp_label,
    `SP Skillz`      = spz_display,
    `Team Rater`     = tr_display,
    `Park Factor`    = df$park_factor,
    `Streamer Score` = df$streamer_score,
    `Pitcher`        = p_label,
    `.susp`          = as.integer(susp),
    # Hidden numeric sort columns — keeps "100*" display but lets DT sort correctly
    `.spz_sort`      = as.numeric(df$sp_skillz_index),
    `.tr_sort`       = as.numeric(df$team_rater),
    check.names      = FALSE,
    stringsAsFactors = FALSE
  )
}

stream_render_dt <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    return(datatable(
      data.frame(Message = "No data. Fetch probables to populate."),
      rownames = FALSE, options = list(dom = "t", ordering = FALSE)
    ))
  }

  has_susp     <- ".susp" %in% names(df) && any(df$.susp == 1, na.rm = TRUE)
  susp_col     <- which(names(df) == ".susp")     - 1L   # 0-based for JS
  spz_col      <- which(names(df) == "SP Skillz") - 1L   # 0-based
  tr_col       <- which(names(df) == "Team Rater")- 1L   # 0-based
  spz_sort_col <- which(names(df) == ".spz_sort") - 1L   # 0-based hidden
  tr_sort_col  <- which(names(df) == ".tr_sort")  - 1L   # 0-based hidden
  # Second "Pitcher" column — used for row-style JS; index it directly so it
  # doesn't break if we add/remove hidden columns later
  pitcher2_col <- which(names(df) == "Pitcher")[2L] - 1L  # 0-based

  # Suspicious row JS: italic + amber tint when hidden flag col == 1
  created_row_js <- JS(sprintf(
    "function(row, data, index) {
       if (data[%d] === 1) {
         $(row).css({'font-style':'italic','opacity':'0.82'});
         $('td:eq(1)', row).css({'color':'#b07a2a','font-style':'italic'});
         $('td:eq(%d)', row).css({'color':'#b07a2a','font-style':'italic'});
       }
     }",
    susp_col, pitcher2_col
  ))

  datatable(
    df,
    filter    = "none",
    rownames  = FALSE,
    selection = "none",
    class     = "pf-dt display nowrap",
    options   = list(
      pageLength    = 200,
      scrollX       = TRUE,
      scrollY       = "calc(100vh - 420px)",
      scrollCollapse= TRUE,
      dom           = "ftp",
      order         = list(),
      createdRow    = created_row_js,
      columnDefs    = list(
        list(className = "dt-center", targets = c(2,3,4,5,6,7)),
        list(visible = FALSE, targets = susp_col),
        # Hide numeric sort columns; point display columns at them so DT sorts
        # "100*" / "97" etc. numerically rather than lexicographically
        list(visible = FALSE, targets = c(spz_sort_col, tr_sort_col)),
        list(orderData = spz_sort_col, targets = spz_col),
        list(orderData = tr_sort_col,  targets = tr_col)
      )
    )
  ) |>
    formatStyle(
      "Streamer Score",
      fontWeight      = "700",
      backgroundColor = styleInterval(c(95,100,105), c("#f7e4d8","#fff9f5","#eef5ec","#d4edda"))
    ) |>
    formatStyle("Pitcher",   fontWeight = "650", color = "#172733") |>
    formatStyle(c("SP Skillz","Park Factor","Team Rater"),
                color = "#4a5a4f", textAlign = "center") |>
    formatStyle("Date",  color = "#4a5a4f", fontSize = "0.83rem") |>
    formatStyle("Opp",   color = "#4a5a4f", fontSize = "0.83rem")
}

# ── Module UI ──────────────────────────────────────────────────────────────────

spStreamUI <- function(id) {
  ns <- NS(id)

  div(
    class = "sps-page",

    # ── Page header (same pattern as all other pages) ────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "SP Streamonator"),
      p(class = "pf-subtitle",
        "Weekly SP streaming recommendations \u2014 probables schedule \u00d7 SP Skillz \u00d7 Park Factor.",
        tags$br(),
        "Streamer Score blends pitcher skill, park environment, and opponent quality into one sortable index."
      )
    ),

    # ── Top toggles row ───────────────────────────────────────────────────────
    div(
      class = "pf-controls-row",

      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Week"),
        div(class = "pf-toggle",
            radioButtons(ns("which_week"), NULL,
                         choices  = c("Current Week" = "current", "Next Week" = "next"),
                         selected = "current", inline = TRUE)),
        div(class = "sps-week-label", textOutput(ns("week_label"), inline = TRUE))
      ),

      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "SP Skillz"),
        div(class = "pf-toggle",
            radioButtons(ns("spz_year"), NULL,
                         choices  = c("2025" = "2025", "2026" = "2026"),
                         selected = "2026", inline = TRUE))
      )
    ),

    # ── Weights: two-panel layout ─────────────────────────────────────────────
    div(
      class = "sps-weights-wrap",

      # Panel 1 — Component Weights
      div(
        class = "sps-weights-panel",
        div(class = "sps-weights-panel-title", "Streamer Weights"),
        div(class = "sps-weights-panel-subtitle", "Set to 0 to exclude a component"),
        div(
          class = "sps-weight-row",
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "SP Skillz"),
              numericInput(ns("w_sp"), NULL, 6, 0, 10, 0.5, width = "90px")),
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Team Rater"),
              numericInput(ns("w_tr"), NULL, 3, 0, 10, 0.5, width = "90px")),
          div(class = "sps-weight-item",
              tags$span(class = "sps-weight-label", "Park Factor"),
              numericInput(ns("w_pf"), NULL, 1, 0, 10, 0.5, width = "90px"))
        )
      ),

      # Panel 2 — Sub-weights
      div(
        class = "sps-weights-panel",
        div(class = "sps-weights-panel-title", "Factor Weights"),
        div(class = "sps-weights-panel-subtitle", "Controls how each component is blended internally"),
        div(
          class = "sps-weight-row",
          # SP Skillz sub-group
          div(
            class = "sps-weight-group",
            div(class = "sps-weight-group-label", "SP Skillz"),
            div(
              class = "sps-weight-row sps-weight-row-inner",
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Season to Date"),
                  numericInput(ns("w_spz_std"), NULL, 1, 0, 10, 0.5, width = "90px")),
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Last 30"),
                  numericInput(ns("w_spz_l30"), NULL, 1, 0, 10, 0.5, width = "90px"))
            )
          ),
          div(class = "sps-weight-divider"),
          # Team Rater sub-group
          div(
            class = "sps-weight-group",
            div(class = "sps-weight-group-label", "Team Rater"),
            div(
              class = "sps-weight-row sps-weight-row-inner",
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Season to Date"),
                  numericInput(ns("w_tr_std"),   NULL, 1, 0, 10, 0.5, width = "90px")),
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "Last 30"),
                  numericInput(ns("w_tr_l30"),   NULL, 1, 0, 10, 0.5, width = "90px")),
              div(class = "sps-weight-item",
                  tags$span(class = "sps-weight-label", "vs Hand"),
                  numericInput(ns("w_tr_vhand"), NULL, 1, 0, 10, 0.5, width = "90px"))
            )
          )
        )
      )
    ),

    # ── Fetch button + status ─────────────────────────────────────────────────
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch Probables",
                   class = "btn btn-pag-generate",
                   icon  = icon("rotate")),
      div(class = "sps-status status-shell", textOutput(ns("status"), inline = TRUE))
    ),

    # ── Sub-tab navigation (pill style, same as Draft Lab) ───────────────────
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4C5"), "All Starts"),
        value = "all",
        div(class = "sps-tab-body",
          div(class = "sps-day-filter-row",
            checkboxGroupInput(ns("day_filter"), label = NULL,
              choices  = c("Mon"="1","Tue"="2","Wed"="3","Thu"="4",
                           "Fri"="5","Sat"="6","Sun"="7"),
              selected = as.character(1:7),
              inline   = TRUE),
            actionButton(ns("day_all"),  "Select All",   class = "btn-sps-day"),
            actionButton(ns("day_none"), "Deselect All", class = "btn-sps-day")
          ),
          uiOutput(ns("all_ui"))
        )
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F501"), "2-Start"),
        value = "two_start",
        div(class = "sps-tab-body", uiOutput(ns("two_start_ui")))
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F9FE"), "My Pitchers"),
        value = "my_pitchers",
        div(class = "sps-tab-body",
          div(class = "sps-my-wrap2",
            div(class = "sps-my-header",
              numericInput(ns("n_pitchers"), "How many pitchers on your roster?",
                           value = 9, min = 1, max = 30, step = 1, width = "220px"),
              div(class = "sps-my-io",
                downloadButton(ns("export_pitchers"), "Export My Pitchers",
                               class = "btn-outline-secondary", title = "Exports as .csv"),
                actionButton(ns("import_trigger"), "Import My Pitchers",
                             class = "btn-outline-secondary"),
                actionButton(ns("clear_pitchers"), "Clear All",
                             class = "btn-outline-secondary"),
                # Hidden real fileInput; triggered by the button above via JS
                tags$div(id = ns("import_wrap"), style = "display:none;",
                  fileInput(ns("import_file"), label = NULL,
                            accept = c(".csv", ".txt"))
                ),
                tags$script(HTML(sprintf(
                  '$(document).on("click","#%s",function(){
                     $("#%s input[type=\'file\']").click();
                   });',
                  ns("import_trigger"), ns("import_wrap")
                )))
              )
            ),
            uiOutput(ns("my_slots_ui")),
            DTOutput(ns("my_dt"))
          )
        )
      )
    ),

  )
}

# ── Module Server ──────────────────────────────────────────────────────────────

spStreamServer <- function(id, spz_data_ext = NULL, team_rater_data = NULL,
                           spz_fetch_trigger = NULL, tr_fetch_trigger = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      probables    = NULL,
      park_factors = stream_load_pf(),
      status       = "Click \u2018Fetch Probables\u2019 to load this week\u2019s schedule.",
      diag         = ""
    )

    # ── Week ──────────────────────────────────────────────────────────────────
    week <- reactive({
      wk <- stream_week_range(Sys.Date(), input$which_week %||% "current")
      # Clip start/end to the actual probables date range within the calendar week.
      # This corrects for UTC-day offset (start) and for grids that don't extend
      # through the full week (end).
      if (!is.null(rv$probables) && nrow(rv$probables) > 0) {
        prob_dates <- sort(unique(rv$probables$date[!is.na(rv$probables$date)]))
        in_wk <- prob_dates[prob_dates >= wk$start & prob_dates <= wk$end]
        if (length(in_wk) > 0) {
          wk$start <- min(in_wk)
          wk$end   <- max(in_wk)
          wk$label <- sprintf("%s \u2013 %s",
                              format(wk$start, "%b %d"),
                              format(wk$end,   "%b %d, %Y"))
        }
      }
      wk
    })
    output$week_label <- renderText({ week()$label })

    # ── SP Skillz ─────────────────────────────────────────────────────────────
    # Uses live data from the SP Skillz module when available, falls back to
    # static CSVs (pre-season / before a fetch has occurred).
    spz_data <- reactive({
      live <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
      if (!is.null(live$std)) return(live)
      yr <- input$spz_year %||% "2026"
      list(
        std = stream_load_spz(year = yr, period = "std"),
        l30 = stream_load_spz(year = yr, period = "l30")
      )
    })

    # Combined autocomplete pool: probables pitcher names + SP Skillz names
    pitcher_pool <- reactive({
      names_spz  <- character(0)
      names_prob <- character(0)
      dat_spz <- spz_data()$std
      if (!is.null(dat_spz) && "player_name" %in% names(dat_spz))
        names_spz <- dat_spz$player_name[nzchar(dat_spz$player_name)]
      if (!is.null(rv$probables) && "pitcher_name" %in% names(rv$probables))
        names_prob <- rv$probables$pitcher_name[nzchar(rv$probables$pitcher_name)]
      # SP Skillz names first so the FanGraphs/ASCII spelling wins on collision
      all_names <- c(names_spz, names_prob)
      # Deduplicate by normalized key (strips diacritics + punctuation) so
      # "Cris Sanchez" and "Crís Sánchez" don't both appear
      all_names <- all_names[!duplicated(stream_norm_name(all_names))]
      sort(all_names)
    })

    # ── Fetch ─────────────────────────────────────────────────────────────────
    output$status  <- renderText({ rv$status })
    output$raw_diag <- renderText({ rv$diag })

    observeEvent(input$fetch, {
      rv$status    <- "Fetching\u2026"
      rv$probables <- NULL
      rv$diag      <- ""
      tryCatch({
        raw    <- stream_fg_fetch(STREAM_PROBABLES_URL)
        parsed <- stream_parse_probables(raw)
        rv$probables <- parsed$data
        rv$diag      <- parsed$diag

        # Auto-advance to Next Week if current week has no games in the fetched data
        wk_cur   <- stream_week_range(Sys.Date(), "current")
        in_cur   <- !is.na(parsed$data$date) &
                    parsed$data$date >= wk_cur$start &
                    parsed$data$date <= wk_cur$end
        if (!any(in_cur) && (input$which_week %||% "current") == "current") {
          updateRadioButtons(session, "which_week", selected = "next")
        }

        n_prob <- nrow(parsed$data)
        n_pit  <- length(unique(parsed$data$pitcher_name))

        # ── SP Skillz: trigger module fetch if not yet loaded this session ────
        spz_live  <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
        spz_note  <- if (!is.null(spz_live$std)) "SP Skillz: cached" else {
          if (!is.null(spz_fetch_trigger))
            spz_fetch_trigger(spz_fetch_trigger() + 1L)
          "SP Skillz: fetching\u2026"
        }

        # ── Team Rater: trigger module fetch if not yet loaded this session ───
        tr_live  <- if (is.function(team_rater_data)) team_rater_data() else NULL
        tr_note  <- if (!is.null(tr_live$std)) "Team Rater: cached" else {
          if (!is.null(tr_fetch_trigger))
            tr_fetch_trigger(tr_fetch_trigger() + 1L)
          "Team Rater: fetching\u2026"
        }

        rv$status <- sprintf(
          "%d starts / %d pitchers \u2014 fetched %s | %s | %s",
          n_prob, n_pit, format(Sys.time(), "%I:%M %p"),
          spz_note, tr_note
        )
      }, error = function(e) {
        rv$status <- paste0("Error: ", conditionMessage(e))
        rv$diag   <- conditionMessage(e)
      })
    })

    # ── Scored table ──────────────────────────────────────────────────────────
    scored <- reactive({
      req(rv$probables)
      wk    <- week()
      spz   <- spz_data()
      tr    <- if (is.function(team_rater_data)) team_rater_data() else NULL
      stream_build(
        rv$probables,
        spz_std    = spz$std,
        spz_l30    = spz$l30,
        pf         = rv$park_factors,
        tr_data    = tr,
        week_start = wk$start, week_end = wk$end,
        w_sp       = max(0, input$w_sp       %||% 1),
        w_spz_std  = max(0, input$w_spz_std  %||% 1),
        w_spz_l30  = max(0, input$w_spz_l30  %||% 1),
        w_tr       = max(0, input$w_tr       %||% 1),
        w_tr_std   = max(0, input$w_tr_std   %||% 1),
        w_tr_l30   = max(0, input$w_tr_l30   %||% 1),
        w_tr_vhand = max(0, input$w_tr_vhand %||% 1),
        w_pf       = max(0, input$w_pf       %||% 1)
      )
    })

    # ── Day-of-week filter ────────────────────────────────────────────────────
    observeEvent(input$day_all,  {
      updateCheckboxGroupInput(session, "day_filter", selected = as.character(1:7))
    })
    observeEvent(input$day_none, {
      updateCheckboxGroupInput(session, "day_filter", selected = character(0))
    })

    # ── All Starts tab ────────────────────────────────────────────────────────
    susp_note <- tags$p(
      class = "sps-susp-note",
      tags$span(class = "sps-susp-dagger", "\u2020"),
      " Starts listed fewer than 4 days apart \u2014 likely reflects schedule uncertainty,",
      " not a confirmed two-start week. Treat with caution."
    )
    spz_placeholder_note <- tags$p(
      class = "sps-susp-note",
      tags$span(style = "font-weight:700;color:#b07a2a;", "* "),
      "Value set to 100 (league avg) \u2014 data not yet available for this pitcher or team. ",
      "Scores will update once enough games have been played."
    )

    output$all_ui <- renderUI({
      if (is.null(rv$probables))
        return(div(class = "sps-empty", p("Fetch the probables grid to populate this table.")))
      df <- scored()
      has_susp <- !is.null(df) && any(df$suspicious_rest, na.rm = TRUE)
      has_placeholder <- !is.null(df) && (any(df$spz_placeholder, na.rm = TRUE) || any(df$tr_placeholder, na.rm = TRUE))
      tagList(DTOutput(ns("all_dt")),
              if (has_placeholder) spz_placeholder_note,
              if (has_susp) susp_note)
    })
    output$all_dt <- renderDT({
      df   <- scored()
      days <- as.integer(input$day_filter %||% as.character(1:7))
      if (!is.null(df) && nrow(df) > 0 && length(days) > 0)
        df <- df[as.integer(format(df$date, "%u")) %in% days, , drop = FALSE]
      stream_render_dt(stream_format_display(df))
    })

    # ── 2-Start tab ───────────────────────────────────────────────────────────
    output$two_start_ui <- renderUI({
      if (is.null(rv$probables))
        return(div(class = "sps-empty", p("Fetch the probables grid to populate this table.")))
      df <- scored()
      two_df <- if (!is.null(df)) df[df$two_start_flag, , drop = FALSE] else NULL
      has_susp <- !is.null(two_df) && any(two_df$suspicious_rest, na.rm = TRUE)
      has_placeholder <- !is.null(two_df) && any(two_df$spz_placeholder, na.rm = TRUE)
      tagList(DTOutput(ns("two_start_dt")),
              if (has_placeholder) spz_placeholder_note,
              if (has_susp) susp_note)
    })
    output$two_start_dt <- renderDT({
      df <- scored()
      if (!is.null(df) && nrow(df) > 0) {
        df <- df[df$two_start_flag, , drop = FALSE]
        df <- df[order(df$pitcher_name, df$date), , drop = FALSE]
      }
      stream_render_dt(stream_format_display(df))
    })

    # ── My Pitchers tab ───────────────────────────────────────────────────────

    # Helper: build one selectize slot div with choices embedded (client-side mode).
    # Choices are embedded at render time so autocomplete works for both initial
    # renderUI slots and dynamically insertUI'd slots alike — no server=TRUE needed.
    # `selected` is baked in so insertUI'd slots don't need a follow-up update call.
    mk_pitcher_slot <- function(i, pool = character(0), selected = "") {
      # Always include the pre-selected name in choices so it shows even if not in pool
      choices <- if (nzchar(selected)) c("", selected, setdiff(pool, selected)) else c("", pool)
      div(class = "sps-my-slot", id = ns(paste0("slot_wrap_", i)),
        selectizeInput(ns(paste0("my_p_", i)), label = NULL,
                       choices  = choices,
                       selected = selected,
                       multiple = FALSE,
                       options  = list(
                         placeholder = paste0("Pitcher ", i, "\u2026"),
                         maxItems    = 1L,
                         create      = FALSE,
                         onItemAdd   = I("function(v,$i){this.close();}")
                       ))
      )
    }

    # Initial render: isolated from input$n_pitchers so this never re-runs reactively.
    # Pool may be empty on first load — that is fine; observeEvent below pushes choices
    # once probables / SP Skillz data arrive.
    # insertUI / removeUI handle all subsequent slot additions / removals.
    output$my_slots_ui <- renderUI({
      n_init    <- isolate(max(1L, min(30L, as.integer(input$n_pitchers %||% 9L))))
      pool_init <- isolate(pitcher_pool())
      div(class = "sps-my-slots", id = ns("slots_container"),
          tagList(lapply(seq_len(n_init), mk_pitcher_slot, pool = pool_init)))
    })

    # Tracks how many slots are currently in the DOM
    prev_n <- reactiveVal(9L)

    # When n changes: add or remove only the delta slot(s) — existing slots untouched
    observeEvent(input$n_pitchers, {
      n_new <- max(1L, min(30L, as.integer(input$n_pitchers %||% 9L)))
      n_old <- prev_n()
      pool  <- isolate(pitcher_pool())

      if (n_new > n_old) {
        for (i in seq(n_old + 1L, n_new))
          insertUI(
            selector = paste0("#", ns("slots_container")),
            where    = "beforeEnd",
            ui       = mk_pitcher_slot(i, pool)
          )
      } else if (n_new < n_old) {
        for (i in seq(n_old, n_new + 1L))
          removeUI(selector = paste0("#", ns(paste0("slot_wrap_", i))))
      }

      prev_n(n_new)
    }, ignoreInit = TRUE)

    # When pitcher pool refreshes: push updated choices to all live slots (client-side,
    # preserves current selections)
    observeEvent(pitcher_pool(), {
      pool <- pitcher_pool()
      n    <- prev_n()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("my_p_", i), choices = c("", pool))
    }, ignoreInit = FALSE)

    # Collect non-empty pitcher selections
    my_selected <- reactive({
      n   <- prev_n()
      nms <- vapply(seq_len(n), function(i) {
        v <- input[[paste0("my_p_", i)]]
        if (is.null(v) || !nzchar(v)) "" else v
      }, character(1))
      nms[nzchar(nms)]
    })

    # Export: write selected pitcher names as a single-column CSV
    output$export_pitchers <- downloadHandler(
      filename = function() paste0("my_pitchers_", format(Sys.Date(), "%Y%m%d"), ".csv"),
      content  = function(file) {
        write.csv(data.frame(pitcher_name = my_selected()), file, row.names = FALSE)
      }
    )

    # Import: parse CSV/TXT, adjust slot count, and populate selections
    observeEvent(input$import_file, {
      req(input$import_file)
      tryCatch({
        raw <- readLines(input$import_file$datapath, warn = FALSE)
        # Strip surrounding CSV quotes + whitespace first, then drop header and blanks
        names_in <- trimws(gsub('^"|"$', "", raw))
        if (length(names_in) > 0 &&
            tolower(names_in[1]) %in% c("pitcher_name", "name", "pitcher"))
          names_in <- names_in[-1]
        names_in <- names_in[nzchar(names_in)]
        names_in <- head(names_in, 30L)
        if (length(names_in) == 0) return()

        n_new <- length(names_in)
        n_old <- prev_n()
        pool  <- isolate(pitcher_pool())

        # New slots: bake selected value into mk_pitcher_slot so there's no
        # insertUI + updateSelectizeInput race on the client.
        if (n_new > n_old) {
          for (i in seq(n_old + 1L, n_new))
            insertUI(selector = paste0("#", ns("slots_container")),
                     where    = "beforeEnd",
                     ui       = mk_pitcher_slot(i, pool, selected = names_in[i]))
        } else if (n_new < n_old) {
          for (i in seq(n_old, n_new + 1L))
            removeUI(selector = paste0("#", ns(paste0("slot_wrap_", i))))
        }
        prev_n(n_new)
        updateNumericInput(session, "n_pitchers", value = n_new)

        # Update only already-existing slots; new slots have selection baked in above
        for (i in seq_len(min(n_old, n_new))) {
          nm <- names_in[i]
          updateSelectizeInput(session, paste0("my_p_", i),
                               choices  = c("", nm, setdiff(pool, nm)),
                               selected = nm)
        }
      }, error = function(e) NULL)
    })

    observeEvent(input$clear_pitchers, {
      n    <- prev_n()
      pool <- isolate(pitcher_pool())
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("my_p_", i),
                             choices = c("", pool), selected = "")
    })

    # Filtered starts table — always present in the DOM; empty states shown as message rows.
    output$my_dt <- renderDT({
      msg_dt <- function(txt) datatable(
        data.frame(` ` = txt, check.names = FALSE),
        rownames = FALSE, options = list(dom = "t", ordering = FALSE)
      )

      if (is.null(rv$probables))
        return(msg_dt("Fetch probables first, then select your pitchers above."))

      sel <- my_selected()
      if (!length(sel))
        return(msg_dt("Enter pitcher names above to see their starts."))

      df <- scored()
      if (is.null(df) || nrow(df) == 0)
        return(msg_dt("No starts data available."))

      sel_norm <- stream_norm_name(sel)
      df_norm  <- stream_norm_name(df$pitcher_name)
      sub <- df[df_norm %in% sel_norm, , drop = FALSE]
      if (nrow(sub) == 0)
        return(msg_dt("None of the selected pitchers have starts this week."))

      sub <- sub[order(-ifelse(is.na(sub$streamer_score), -Inf, sub$streamer_score)), , drop = FALSE]
      stream_render_dt(stream_format_display(sub))
    })

    # ── Return for downstream modules (FAAB Helper) ──────────────────────────
    # probables = full unfiltered grid (all weeks); scored = current week's scored data
    return(reactive({
      list(
        probables = rv$probables,
        scored    = tryCatch(scored(), error = function(e) NULL)
      )
    }))
  })
}
