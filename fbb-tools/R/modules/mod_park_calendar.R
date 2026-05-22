# ── mod_park_calendar.R ──────────────────────────────────────────────────────
# Park Factor Calendar: Interactive calendar showing MLB series at extreme
# hitter-friendly parks (Coors, GABP, Athletics park).
# Uses FullCalendar.js via custom HTML for multi-day event ribbons.

suppressPackageStartupMessages({
  library(jsonlite)
  library(dplyr)
})

# ── Constants ────────────────────────────────────────────────────────────────

# Target home teams (high park factor venues)
PARK_CAL_VENUES <- list(
  COL = list(team_id = 115, label = "@ COL (Coors Field)",      color = "#33006F"),
  CIN = list(team_id = 113, label = "@ CIN (Great American BP)", color = "#C6011F"),
  ATH = list(team_id = 133, label = "@ ATH (Athletics Park)",   color = "#003831")
)

# MLB API team ID → display abbreviation lookup
# Populated at first schedule fetch
.team_abbrevs <- new.env(parent = emptyenv())

# ── Data fetching ────────────────────────────────────────────────────────────

#' Fetch full-season schedule for target home teams from MLB Stats API
#' @param season integer season year
#' @return data.frame with columns: game_date, home_id, away_id, home_abbr, away_abbr, series_num
fetch_park_calendar_schedule <- function(season = 2026) {

  cache_path <- file.path("data", "processed", sprintf("park_calendar_%d.rds", season))

  if (file.exists(cache_path)) {
    cached <- readRDS(cache_path)
    # Refresh if cache is > 7 days old
    if (difftime(Sys.time(), file.mtime(cache_path), units = "days") < 7) {
      return(cached)
    }
  }

  # Fetch team abbreviations
  teams_url <- sprintf("https://statsapi.mlb.com/api/v1/teams?sportId=1&season=%d", season)
  teams_raw <- tryCatch(jsonlite::fromJSON(teams_url, flatten = TRUE), error = function(e) NULL)
  if (!is.null(teams_raw) && "teams" %in% names(teams_raw)) {
    team_df <- teams_raw$teams
    for (i in seq_len(nrow(team_df))) {
      .team_abbrevs[[as.character(team_df$id[i])]] <- team_df$abbreviation[i]
    }
  }

  # Fetch full season schedule — regular season games only
  # MLB API handles monthly chunks well; fetch March–October

  home_ids <- vapply(PARK_CAL_VENUES, function(x) as.integer(x$team_id), integer(1))
  all_rows <- list()

  # Regular season typically late March – late September
  start_dates <- sprintf("%d-%02d-01", season, 3:10)
  end_dates   <- sprintf("%d-%02d-%02d", season, 3:10,
                         c(31, 30, 31, 30, 31, 31, 30, 31))

  for (i in seq_along(start_dates)) {
    url <- sprintf(
      "https://statsapi.mlb.com/api/v1/schedule?sportId=1&season=%d&startDate=%s&endDate=%s&gameType=R",
      season, start_dates[i], end_dates[i]
    )
    raw <- tryCatch(jsonlite::fromJSON(url, flatten = TRUE), error = function(e) NULL)
    if (is.null(raw) || is.null(raw$dates) || raw$totalGames == 0) next

    for (d in seq_len(nrow(raw$dates))) {
      games <- raw$dates$games[[d]]
      if (is.null(games) || !is.data.frame(games) || nrow(games) == 0) next

      # Safely extract columns
      home_id_col <- "teams.home.team.id"
      away_id_col <- "teams.away.team.id"
      if (!(home_id_col %in% names(games))) next

      keep <- games[[home_id_col]] %in% home_ids
      if (!any(keep)) next

      pg <- games[keep, , drop = FALSE]
      for (g in seq_len(nrow(pg))) {
        all_rows[[length(all_rows) + 1]] <- list(
          game_date = as.character(pg[["officialDate"]][g]),
          home_id   = as.integer(pg[[home_id_col]][g]),
          away_id   = as.integer(pg[[away_id_col]][g])
        )
      }
    }
  }

  if (length(all_rows) == 0) return(data.frame())

  schedule <- do.call(rbind, lapply(all_rows, as.data.frame, stringsAsFactors = FALSE))
  schedule$game_date <- as.Date(schedule$game_date)

  # Map IDs to abbreviations
  schedule$home_abbr <- vapply(as.character(schedule$home_id), function(id) {
    if (exists(id, envir = .team_abbrevs)) .team_abbrevs[[id]] else "???"
  }, character(1))
  schedule$away_abbr <- vapply(as.character(schedule$away_id), function(id) {
    if (exists(id, envir = .team_abbrevs)) .team_abbrevs[[id]] else "???"
  }, character(1))

  # Cache it
  if (!dir.exists(dirname(cache_path))) dir.create(dirname(cache_path), recursive = TRUE)
  saveRDS(schedule, cache_path)

  schedule
}

#' Group individual games into series (consecutive dates, same matchup at same park)
#' @param schedule data.frame from fetch_park_calendar_schedule()
#' @return data.frame with one row per series: start, end, home_id, home_abbr, away_abbr, n_games
build_series <- function(schedule) {
  if (nrow(schedule) == 0) return(data.frame())

  schedule <- schedule %>%
    arrange(home_id, away_id, game_date) %>%
    group_by(home_id, away_id, home_abbr, away_abbr) %>%
    mutate(
      gap = as.integer(game_date - lag(game_date, default = as.Date("1900-01-01"))),
      series_break = gap > 1,
      series_group = cumsum(series_break)
    ) %>%
    group_by(home_id, away_id, home_abbr, away_abbr, series_group) %>%
    summarise(
      start   = min(game_date),
      end     = max(game_date),
      n_games = n(),
      .groups = "drop"
    ) %>%
    select(start, end, home_id, home_abbr, away_abbr, n_games)

  as.data.frame(schedule)
}

#' Convert series data to FullCalendar event JSON
#' @param series data.frame from build_series()
#' @param visible_parks character vector of park keys to show (e.g., c("COL", "CIN"))
#' @return JSON string for FullCalendar events array
series_to_fc_events <- function(series, visible_parks = names(PARK_CAL_VENUES)) {
  if (nrow(series) == 0) return("[]")

  # Map home_id → park key
  id_to_key <- setNames(
    names(PARK_CAL_VENUES),
    vapply(PARK_CAL_VENUES, function(x) as.character(x$team_id), character(1))
  )

  series$park_key <- id_to_key[as.character(series$home_id)]
  series <- series[series$park_key %in% visible_parks, ]

  if (nrow(series) == 0) return("[]")

  events <- lapply(seq_len(nrow(series)), function(i) {
    row <- series[i, ]
    venue_info <- PARK_CAL_VENUES[[row$park_key]]
    list(
      title = sprintf("%s @ %s", row$away_abbr, row$home_abbr),
      start = format(row$start, "%Y-%m-%d"),
      # FullCalendar exclusive end: add 1 day to include last game date
      end   = format(row$end + 1, "%Y-%m-%d"),
      color = venue_info$color,
      textColor = "#FFFFFF",
      extendedProps = list(
        park_key = row$park_key,
        n_games  = row$n_games
      )
    )
  })

  jsonlite::toJSON(events, auto_unbox = TRUE)
}


# ── Module UI ────────────────────────────────────────────────────────────────

parkCalendarUI <- function(id) {
  ns <- NS(id)

  # FullCalendar v6 — single JS bundle, no separate CSS needed
  fc_js <- "https://cdn.jsdelivr.net/npm/fullcalendar@6.1.15/index.global.min.js"

  div(
    class = "dl-page",

    # CDN dependency
    tags$head(
      tags$script(src = fc_js)
    ),

    # ── Page header ────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "Park Factor Calendar"),
      p(
        class = "pf-subtitle",
        "Series at the most extreme hitter-friendly parks in baseball.",
        "Toggle parks and adjust the view to plan your streaming targets."
      )
    ),

    # ── Controls ─────────────────────────────────────────────────────────
    div(
      class = "card mb-3",
      style = "background: var(--card); border: 1px solid var(--line); border-radius: var(--r-md); padding: 16px 20px;",
      div(
        class = "d-flex flex-wrap align-items-center gap-3",
        div(
          checkboxGroupInput(
            ns("parks"),
            label = NULL,
            choices  = setNames(
              names(PARK_CAL_VENUES),
              vapply(PARK_CAL_VENUES, function(x) x$label, character(1))
            ),
            selected = names(PARK_CAL_VENUES),
            inline   = TRUE
          )
        ),
        div(
          radioButtons(
            ns("view_range"),
            label = NULL,
            choices  = c("1 Month" = "1mo", "3 Months" = "3mo", "Full Season" = "full"),
            selected = "1mo",
            inline   = TRUE
          )
        )
      )
    ),

    # ── Calendar container ───────────────────────────────────────────────
    div(
      id = ns("calendar"),
      style = "background: var(--card); border: 1px solid var(--line); border-radius: var(--r-md); padding: 16px; min-height: 500px;"
    ),

    # ── Legend ────────────────────────────────────────────────────────────
    div(
      class = "d-flex flex-wrap gap-3 mt-3",
      style = "font-size: 0.85rem; color: var(--muted);",
      lapply(PARK_CAL_VENUES, function(v) {
        div(
          class = "d-flex align-items-center gap-1",
          tags$span(
            style = sprintf(
              "display:inline-block; width:14px; height:14px; border-radius:3px; background:%s;",
              v$color
            )
          ),
          tags$span(v$label)
        )
      })
    ),

    # ── Calendar initialization JS ───────────────────────────────────────
    tags$script(HTML(sprintf("
      $(document).on('shiny:connected', function() {
        var nsPrefix = '%s';
        // Wait for FullCalendar JS to load
        var waitFC = setInterval(function() {
          if (typeof FullCalendar !== 'undefined') {
            clearInterval(waitFC);
            initParkCalendar(nsPrefix);
          }
        }, 100);
      });

      function initParkCalendar(ns) {
        var calEl = document.getElementById(ns + 'calendar');
        if (!calEl) return;

        var calendar = new FullCalendar.Calendar(calEl, {
          initialView: 'dayGridMonth',
          headerToolbar: {
            left:   'prev,next today',
            center: 'title',
            right:  ''
          },
          views: {
            threeMonth: {
              type: 'multiMonth',
              duration: { months: 3 },
              multiMonthMaxColumns: 3
            },
            fullSeason: {
              type: 'multiMonth',
              duration: { months: 7 },
              multiMonthMaxColumns: 3
            }
          },
          initialDate: new Date().toISOString().slice(0, 10),
          height: 'auto',
          events: [],
          eventDisplay: 'block',
          displayEventTime: false,
          eventBorderColor: 'transparent',
          eventTextColor: '#FFFFFF',
          fixedWeekCount: false,
          eventDidMount: function(info) {
            var nGames = info.event.extendedProps.n_games;
            if (nGames) {
              info.el.title = info.event.title + ' (' + nGames + ' games)';
            }
            info.el.style.fontWeight = '600';
            info.el.style.fontSize = '0.82rem';
            info.el.style.borderRadius = '5px';
            info.el.style.paddingLeft = '6px';
            info.el.style.letterSpacing = '0.3px';
          }
        });

        calendar.render();
        calEl._fcInstance = calendar;

        // Listen for event data from Shiny
        Shiny.addCustomMessageHandler(ns + 'update_events', function(msg) {
          var cal = document.getElementById(ns + 'calendar')._fcInstance;
          if (!cal) return;

          // Remove existing events
          cal.getEvents().forEach(function(e) { e.remove(); });

          // Add new events (Shiny auto-deserializes, so msg.events is already an array)
          var events = (typeof msg.events === 'string') ? JSON.parse(msg.events) : msg.events;
          events.forEach(function(ev) { cal.addEvent(ev); });

          // Handle view range
          if (msg.view_range === '3mo') {
            cal.changeView('threeMonth');
            if (msg.range_start) cal.gotoDate(msg.range_start);
          } else if (msg.view_range === 'full') {
            cal.changeView('fullSeason');
            if (msg.range_start) cal.gotoDate(msg.range_start);
          } else {
            cal.changeView('dayGridMonth');
            if (msg.range_start) cal.gotoDate(msg.range_start);
          }
        });

        // Signal R that calendar is ready
        Shiny.setInputValue(ns + 'cal_ready', true, {priority: 'event'});
      }
    ", ns(""))))
  )
}


# ── Module Server ────────────────────────────────────────────────────────────

parkCalendarServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── Load schedule on init ──────────────────────────────────────────────
    schedule_data <- reactiveVal(NULL)
    series_data   <- reactiveVal(NULL)

    observe({
      withProgress(message = "Fetching MLB schedule...", value = 0.3, {
        sched <- tryCatch(
          fetch_park_calendar_schedule(2026),
          error = function(e) {
            showNotification(
              paste("Failed to fetch schedule:", e$message),
              type = "error", duration = 8
            )
            data.frame()
          }
        )
        setProgress(0.7, detail = "Building series...")
        schedule_data(sched)

        if (nrow(sched) > 0) {
          series_data(build_series(sched))
        } else {
          series_data(data.frame())
        }
        setProgress(1, detail = "Done")
      })
    }) %>% bindEvent(input$cal_ready)

    # ── Update calendar when inputs change ─────────────────────────────────
    observe({
      req(series_data())
      ser <- series_data()
      if (nrow(ser) == 0) return()

      parks <- input$parks
      if (is.null(parks) || length(parks) == 0) parks <- character(0)

      events_json <- series_to_fc_events(ser, parks)

      # Compute date range based on view selection
      today <- Sys.Date()
      view_range <- input$view_range %||% "1mo"

      # Season bounds
      season_start <- min(ser$start)
      season_end   <- max(ser$end)

      if (view_range == "1mo") {
        range_start <- format(today, "%Y-%m-%d")
        range_end   <- NULL
      } else if (view_range == "3mo") {
        range_start <- format(today, "%Y-%m-%d")
        range_end   <- format(today + 90, "%Y-%m-%d")
      } else {
        range_start <- format(season_start, "%Y-%m-%d")
        range_end   <- format(season_end + 1, "%Y-%m-%d")
      }

      session$sendCustomMessage(
        ns("update_events"),
        list(
          events      = events_json,
          view_range  = view_range,
          range_start = range_start,
          range_end   = range_end
        )
      )
    }) %>% bindEvent(input$parks, input$view_range, series_data(), ignoreNULL = FALSE)
  })
}
