suppressPackageStartupMessages({ library(DT); library(jsonlite) })

# ── Constants ──────────────────────────────────────────────────────────────────

SPO_MLB_SCHEDULE_URL <- "https://statsapi.mlb.com/api/v1/schedule"
SPO_FETCH_DAYS       <- 35L   # days of MLB schedule beyond today

# MLB team ID → abbreviation: use shared STREAM_MLB_ID_TO_ABR from mod_sp_streamonator.R

# ── MLB schedule fetch ─────────────────────────────────────────────────────────
# Returns data.frame: date (Date), team (chr), opp (chr), ha ("H"/"A")
# De-duplicated to one row per team per day (handles doubleheaders gracefully).

spo_fetch_schedule <- function(from = Sys.Date(), days = SPO_FETCH_DAYS) {
  to  <- from + as.integer(days)
  url <- sprintf(
    "%s?sportId=1&startDate=%s&endDate=%s&gameType=R",
    SPO_MLB_SCHEDULE_URL, format(from, "%Y-%m-%d"), format(to, "%Y-%m-%d")
  )
  payload <- tryCatch(
    jsonlite::fromJSON(url, simplifyVector = FALSE),
    error = function(e) NULL
  )
  if (is.null(payload) || is.null(payload$dates)) return(NULL)

  rows <- list()
  for (day_obj in payload$dates) {
    d <- as.Date(day_obj$date)
    for (game in day_obj$games) {
      home_id <- as.character(game$teams$home$team$id)
      away_id <- as.character(game$teams$away$team$id)
      ht <- unname(STREAM_MLB_ID_TO_ABR[home_id])
      at <- unname(STREAM_MLB_ID_TO_ABR[away_id])
      if (is.na(ht) || is.na(at)) next
      rows[[length(rows) + 1]] <- c(date = as.character(d), team = ht, opp = at, ha = "H")
      rows[[length(rows) + 1]] <- c(date = as.character(d), team = at, opp = ht, ha = "A")
    }
  }
  if (!length(rows)) return(NULL)

  df      <- as.data.frame(do.call(rbind, rows), stringsAsFactors = FALSE)
  df$date <- as.Date(df$date)
  df[!duplicated(df[, c("date", "team")]), ]   # keep first game per team per day
}

# ── Rotation cadence inference (in games, not days) ───────────────────────────
# Counts how many team games occurred between each pair of consecutive starts,
# then returns the median (rounded to 5 or 6).
# Falls back to day-gap heuristic when schedule data is unavailable.

spo_cadence <- function(start_dates, team_game_dates = NULL) {
  start_dates <- sort(as.Date(start_dates))
  if (length(start_dates) < 2L) return(5L)

  if (!is.null(team_game_dates) && length(team_game_dates) > 0) {
    tgd <- sort(as.Date(team_game_dates))
    counts <- vapply(seq_len(length(start_dates) - 1L), function(i) {
      d1 <- start_dates[i]; d2 <- start_dates[i + 1L]
      sum(tgd > d1 & tgd <= d2)
    }, integer(1))
    valid <- counts[counts >= 3L & counts <= 8L]
    if (length(valid)) return(as.integer(round(median(valid))))
  }

  # Fallback: day-gap heuristic
  gaps  <- as.integer(diff(start_dates))
  valid <- gaps[gaps >= 3L & gaps <= 8L]
  if (!length(valid)) return(5L)
  if (median(valid) >= 5.5) 6L else 5L
}

# ── Project next N starts for one pitcher ─────────────────────────────────────
# probables_rows : rows from parsed probables for this pitcher (any date in window)
# team_sched     : schedule rows for this pitcher's team (from spo_fetch_schedule)
# as_of          : earliest date to include in output
# n              : total starts to return (confirmed + projected combined)
#
# Returns: data.frame(date, opponent, home_away, confirmed)
# confirmed=TRUE  → from FanGraphs probables (more certain)
# confirmed=FALSE → projected by counting N games forward in team schedule

spo_next_starts <- function(probables_rows, team_sched, as_of = Sys.Date(), n = 5L) {
  if (is.null(probables_rows) || nrow(probables_rows) == 0) return(NULL)

  all_dates <- sort(as.Date(probables_rows$date))

  # Build sorted team game date vector for game-counting
  team_game_dates <- if (!is.null(team_sched) && nrow(team_sched) > 0)
                       sort(team_sched$date)
                     else as.Date(character(0))

  cadence   <- spo_cadence(all_dates, team_game_dates)
  last_date <- max(all_dates)

  # ── Confirmed future starts (on/after as_of, from probables) ──────────────
  future_prob <- probables_rows[as.Date(probables_rows$date) >= as_of, , drop = FALSE]
  future_prob <- future_prob[order(future_prob$date), , drop = FALSE]

  confirmed <- if (nrow(future_prob) > 0)
    data.frame(
      date      = as.Date(future_prob$date),
      opponent  = future_prob$opponent_team,
      home_away = future_prob$home_away,
      confirmed = TRUE,
      stringsAsFactors = FALSE
    )
  else NULL

  n_conf <- if (!is.null(confirmed)) nrow(confirmed) else 0L
  n_proj <- max(0L, n - n_conf)

  # ── Project beyond last known start (game-counting) ───────────────────────
  proj <- NULL
  if (n_proj > 0L && length(team_game_dates) > 0L) {
    anchor_pos <- max(which(team_game_dates <= last_date), default = 0L)
    if (anchor_pos == 0L) anchor_pos <- 1L

    proj_list <- list()
    cur_pos   <- anchor_pos

    while (length(proj_list) < n_proj) {
      next_pos <- cur_pos + cadence
      if (next_pos > length(team_game_dates)) break

      game_date <- team_game_dates[next_pos]
      game_row  <- team_sched[team_sched$date == game_date, , drop = FALSE]
      if (nrow(game_row) == 0L) { cur_pos <- next_pos; next }

      if (!is.null(confirmed) && any(confirmed$date == game_date)) {
        cur_pos <- next_pos
        next
      }

      proj_list[[length(proj_list) + 1L]] <- data.frame(
        date      = game_date,
        opponent  = game_row$opp[1L],
        home_away = game_row$ha[1L],
        confirmed = FALSE,
        stringsAsFactors = FALSE
      )
      cur_pos <- next_pos
    }
    if (length(proj_list)) proj <- do.call(rbind, proj_list)
  }

  all_starts <- rbind(confirmed, proj)
  if (is.null(all_starts) || nrow(all_starts) == 0) return(NULL)
  all_starts <- all_starts[order(all_starts$date), , drop = FALSE]
  all_starts <- all_starts[all_starts$date >= as_of, , drop = FALSE]
  head(all_starts, n)
}

# ── PF / TR lookups ────────────────────────────────────────────────────────────

spo_lookup_pf <- function(home_team, pf_data) {
  if (is.null(pf_data) || !all(c("team_norm","overall_pf_idx_100") %in% names(pf_data)))
    return(NA_real_)
  i <- match(stream_norm_team(home_team), pf_data$team_norm)
  if (is.na(i)) NA_real_ else round(pf_data$overall_pf_idx_100[i], 0)
}

spo_lookup_tr <- function(opp_team, tr_data) {
  df <- if (!is.null(tr_data) && !is.null(tr_data$std) && nrow(tr_data$std) > 0)
          tr_data$std else NULL
  if (is.null(df) || !all(c("abbr","team_rater_index") %in% names(df))) return(NA_real_)
  i <- match(stream_norm_team(opp_team), stream_norm_team(df$abbr))
  if (is.na(i)) NA_real_ else round(df$team_rater_index[i], 0)
}

# ── HTML cell renderer ─────────────────────────────────────────────────────────

spo_cell_html <- function(info) {
  if (is.null(info)) return('<span class="spo-na">\u2014</span>')

  ha_label <- if (!is.na(info$home_away) && info$home_away == "H")
                paste0("vs\u00a0", info$opponent)
              else
                paste0("@\u00a0",  info$opponent)

  dt_label <- format(as.Date(info$date), "%a %m/%d")
  pf_s     <- if (!is.na(info$pf)) paste0("PF\u202f", info$pf) else NULL
  tr_s     <- if (!is.na(info$tr)) paste0("TR\u202f", info$tr) else NULL
  meta     <- paste(c(pf_s, tr_s), collapse = "\u00b7")
  cls      <- if (isTRUE(info$confirmed)) "spo-cell" else "spo-cell spo-proj"

  meta_html <- if (nzchar(meta))
    sprintf('<div class="spo-meta">%s</div>', meta)
  else ""

  sprintf(
    '<div class="%s"><div class="spo-ha">%s</div><div class="spo-date">%s</div>%s</div>',
    cls, ha_label, dt_label, meta_html
  )
}

# ── Build the full outlook table ───────────────────────────────────────────────
# Returns a flat data.frame ready for spo_render_dt():
#   Pitcher, Team, SP Skillz, Next Start, 2nd Start, ..., 5th Start
# Start columns contain pre-rendered HTML strings.

spo_build <- function(probables, schedule, spz_data = NULL, pf_data = NULL,
                      tr_data = NULL, n_starts = 5L, as_of = Sys.Date()) {
  if (is.null(probables) || nrow(probables) == 0) return(NULL)

  # Resolve SP Skillz index (prefer std period, fall back to l30)
  spz_lkup <- NULL
  for (period in c("std", "l30")) {
    s <- if (!is.null(spz_data)) spz_data[[period]] else NULL
    if (!is.null(s) && "player_name" %in% names(s)) {
      s <- stream_index_spz(s)
      if ("sp_skillz_index" %in% names(s)) { spz_lkup <- s; break }
    }
  }

  # Unique pitchers
  pitchers <- unique(probables[, c("pitcher_name","pitcher_team"), drop = FALSE])
  pitchers <- pitchers[!is.na(pitchers$pitcher_name) & nzchar(pitchers$pitcher_name), ]

  start_labels <- c("Next Start", "2nd Start", "3rd Start", "4th Start", "5th Start")

  result <- lapply(seq_len(nrow(pitchers)), function(i) {
    nm   <- pitchers$pitcher_name[i]
    team <- pitchers$pitcher_team[i]

    p_starts <- probables[probables$pitcher_name == nm &
                          probables$pitcher_team == team, , drop = FALSE]
    t_sched  <- if (!is.null(schedule))
                  schedule[schedule$team == team, , drop = FALSE]
                else NULL

    starts <- spo_next_starts(p_starts, t_sched, as_of = as_of, n = n_starts)

    # SP Skillz
    spz_val <- NA_real_
    if (!is.null(spz_lkup)) {
      idx <- stream_match_names(nm, spz_lkup$player_name)
      if (!is.na(idx)) spz_val <- spz_lkup$sp_skillz_index[idx]
    }

    # Build one HTML string per start slot
    cells <- vapply(seq_len(n_starts), function(s) {
      if (is.null(starts) || s > nrow(starts))
        return('<span class="spo-na">\u2014</span>')
      row     <- starts[s, , drop = FALSE]
      park_tm <- if (row$home_away == "H") team else row$opponent
      spo_cell_html(list(
        date      = as.Date(row$date),
        opponent  = row$opponent,
        home_away = row$home_away,
        confirmed = row$confirmed,
        pf        = spo_lookup_pf(park_tm, pf_data),
        tr        = spo_lookup_tr(row$opponent, tr_data)
      ))
    }, character(1))

    row_out <- data.frame(
      Pitcher       = nm,
      Team          = team,
      `SP Skillz`   = spz_val,
      check.names   = FALSE,
      stringsAsFactors = FALSE
    )
    for (s in seq_len(n_starts)) row_out[[start_labels[s]]] <- cells[s]
    row_out
  })

  if (!length(result)) return(NULL)
  out <- do.call(rbind, result)
  out[order(out$`SP Skillz`, decreasing = TRUE, na.last = TRUE), , drop = FALSE]
}

# ── DT renderer ───────────────────────────────────────────────────────────────

spo_render_dt <- function(df, scroll_y = "calc(100vh - 400px)") {
  if (is.null(df) || nrow(df) == 0) {
    return(datatable(
      data.frame(` ` = "No data available.", check.names = FALSE),
      rownames = FALSE, options = list(dom = "t", ordering = FALSE)
    ))
  }

  spz_col    <- 2L
  start_cols <- seq.int(3L, 3L + sum(grepl("Start", names(df))) - 1L)

  datatable(
    df,
    escape    = FALSE,
    rownames  = FALSE,
    filter    = "none",
    selection = "none",
    class     = "pf-dt display nowrap",
    options   = list(
      pageLength    = 50,
      scrollX       = TRUE,
      scrollY       = scroll_y,
      scrollCollapse= TRUE,
      dom           = "tp",
      order         = list(list(spz_col, "desc")),
      columnDefs    = list(
        list(className = "dt-center", targets = seq.int(0L, 2L + length(start_cols))),
        list(orderable = FALSE,       targets = as.list(start_cols)),
        list(width = "150px", targets = 0L),
        list(width = "50px",  targets = 1L),
        list(width = "68px",  targets = spz_col),
        list(width = "118px", targets = as.list(start_cols))
      )
    )
  ) |>
    formatRound("SP Skillz", digits = 1) |>
    formatStyle("Pitcher",   fontWeight = "600") |>
    formatStyle("Team",      color = "#4a5a4f") |>
    formatStyle("SP Skillz", color = "#4a5a4f")
}

# ── Module UI ─────────────────────────────────────────────────────────────────

spOutlookUI <- function(id) {
  ns <- NS(id)
  div(
    class = "sps-page",

    # ── Header ──────────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "SP Outlook"),
      p(class = "pf-subtitle",
        "Forward-looking SP start matrix \u2014 probables \u00d7 rotation patterns \u00d7 MLB schedule.",
        tags$br(),
        "Confirmed starts from FanGraphs probables grid; projected starts (faded) estimated from rotation cadence + MLB schedule.",
        tags$br(),
        tags$span(style = "font-size:0.82rem; color:var(--muted);",
          "PF and TR shown when Park Factors / Team Rater have been loaded this session. ",
          "Fetch will auto-load them if not yet available.")
      )
    ),

    # ── Fetch row ────────────────────────────────────────────────────────────
    div(
      class = "sps-fetch-row",
      actionButton(ns("fetch"), "Fetch",
                   class = "btn btn-pag-generate", icon = icon("rotate")),
      div(class = "sps-status status-shell",
          textOutput(ns("status"), inline = TRUE))
    ),

    # ── Legend ──────────────────────────────────────────────────────────────
    div(
      class = "spo-legend",
      tags$span(class = "spo-legend-item",
        tags$span(class = "spo-legend-swatch spo-legend-confirmed", "ABC"),
        tags$span(class = "spo-legend-label", "Confirmed (probables grid)")
      ),
      tags$span(class = "spo-legend-item",
        tags$span(class = "spo-legend-swatch spo-legend-projected", "ABC"),
        tags$span(class = "spo-legend-label", "Projected start")
      )
    ),

    # ── Sub-tab navigation ───────────────────────────────────────────────────
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", "\U0001F4C5"), "All Pitchers"),
        value = "all",
        div(class = "sps-tab-body",
          div(
            class = "pf-controls-row spz-search-row",
            div(
              class = "spz-search-wrap",
              tags$span(class = "spz-search-icon", HTML("&#x2315;")),
              textInput(ns("search"), label = NULL,
                        placeholder = "Search pitcher or team\u2026", width = "100%")
            )
          ),
          uiOutput(ns("all_ui"))
        )
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
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

spOutlookServer <- function(id, spz_data_ext = NULL, team_rater_data = NULL,
                             spz_fetch_trigger = NULL, tr_fetch_trigger = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    rv <- reactiveValues(
      probables = NULL,
      schedule  = NULL,
      status    = "Click \u2018Fetch\u2019 to load the SP start matrix."
    )

    output$status <- renderText({ rv$status })

    # ── Pitcher pool for autocomplete ────────────────────────────────────────
    pitcher_pool <- reactive({
      names_spz  <- character(0)
      names_prob <- character(0)
      spz_live <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
      dat_spz  <- spz_live$std
      if (!is.null(dat_spz) && "player_name" %in% names(dat_spz))
        names_spz <- dat_spz$player_name[nzchar(dat_spz$player_name)]
      if (!is.null(rv$probables) && "pitcher_name" %in% names(rv$probables))
        names_prob <- rv$probables$pitcher_name[nzchar(rv$probables$pitcher_name)]
      all_names <- c(names_spz, names_prob)
      all_names <- all_names[!duplicated(stream_norm_name(all_names))]
      sort(all_names)
    })

    # ── Fetch ────────────────────────────────────────────────────────────────
    observeEvent(input$fetch, {
      withProgress(message = "Fetching probables…", value = 0, {
        rv$status    <- "Fetching…"
        rv$probables <- NULL
        rv$schedule  <- NULL
        tryCatch({
          incProgress(0.1, detail = "Connecting…")
          raw          <- stream_fg_fetch(STREAM_PROBABLES_URL)
          incProgress(0.4, detail = "Parsing probables…")
          parsed       <- stream_parse_probables(raw)
          rv$probables <- parsed$data
          incProgress(0.3, detail = "Fetching schedule…")
          rv$schedule  <- spo_fetch_schedule(from = Sys.Date(), days = SPO_FETCH_DAYS)

          n_p <- length(unique(rv$probables$pitcher_name))

          incProgress(0.1, detail = "Checking supporting data…")
          spz_live <- if (is.function(spz_data_ext)) spz_data_ext() else NULL
          spz_note <- if (!is.null(spz_live$std)) "SPZ cached" else {
            if (!is.null(spz_fetch_trigger)) spz_fetch_trigger(spz_fetch_trigger() + 1L)
            "SPZ loading…"
          }

          tr_live <- if (is.function(team_rater_data)) team_rater_data() else NULL
          tr_note <- if (!is.null(tr_live$std)) "TR cached" else {
            if (!is.null(tr_fetch_trigger)) tr_fetch_trigger(tr_fetch_trigger() + 1L)
            "TR loading…"
          }

          incProgress(0.1)
          rv$status <- sprintf("%d pitchers — %s | %s | %s",
                               n_p, format(Sys.time(), "%I:%M %p"), spz_note, tr_note)
        }, error = function(e) {
          rv$status <- paste0("Error: ", conditionMessage(e))
        })
      })
    })
    # ── Built table (all pitchers) ───────────────────────────────────────────
    built <- reactive({
      req(rv$probables)
      spz <- if (is.function(spz_data_ext))    spz_data_ext()    else NULL
      tr  <- if (is.function(team_rater_data)) team_rater_data() else NULL
      pf  <- stream_load_pf()
      spo_build(rv$probables, rv$schedule,
                spz_data = spz, pf_data = pf, tr_data = tr,
                n_starts = 5L, as_of = Sys.Date())
    })

    # ── All Pitchers tab ─────────────────────────────────────────────────────
    search_d <- debounce(reactive(input$search), 300)

    filtered <- reactive({
      df <- req(built())
      q  <- trimws(search_d() %||% "")
      if (!nzchar(q)) return(df)
      mask <- grepl(q, df[["Pitcher"]], ignore.case = TRUE) |
              grepl(q, df[["Team"]],    ignore.case = TRUE)
      df[mask, , drop = FALSE]
    })

    output$all_ui <- renderUI({
      if (is.null(rv$probables))
        return(div(class = "sps-empty",
                   p("Click \u2018Fetch\u2019 to load the forward-looking SP start matrix.")))
      DTOutput(ns("all_dt"))
    })

    output$all_dt <- renderDT({
      spo_render_dt(req(filtered()))
    })

    # ── My Pitchers tab ───────────────────────────────────────────────────────

    # Build one selectize slot div with choices embedded at render time.
    # `selected` is baked in so insertUI'd slots don't need a follow-up updateSelectizeInput.
    mk_pitcher_slot <- function(i, pool = character(0), selected = "") {
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

    # Initial render: isolated so it doesn't re-run reactively
    output$my_slots_ui <- renderUI({
      n_init    <- isolate(max(1L, min(30L, as.integer(input$n_pitchers %||% 9L))))
      pool_init <- isolate(pitcher_pool())
      div(class = "sps-my-slots", id = ns("slots_container"),
          tagList(lapply(seq_len(n_init), mk_pitcher_slot, pool = pool_init)))
    })

    prev_n <- reactiveVal(9L)

    # Add/remove slots when n_pitchers changes
    observeEvent(input$n_pitchers, {
      n_new <- max(1L, min(30L, as.integer(input$n_pitchers %||% 9L)))
      n_old <- prev_n()
      pool  <- isolate(pitcher_pool())

      if (n_new > n_old) {
        for (i in seq(n_old + 1L, n_new))
          insertUI(selector = paste0("#", ns("slots_container")),
                   where    = "beforeEnd",
                   ui       = mk_pitcher_slot(i, pool))
      } else if (n_new < n_old) {
        for (i in seq(n_old, n_new + 1L))
          removeUI(selector = paste0("#", ns(paste0("slot_wrap_", i))))
      }
      prev_n(n_new)
    }, ignoreInit = TRUE)

    # Push updated choices to all live slots when pool refreshes
    observeEvent(pitcher_pool(), {
      pool <- pitcher_pool()
      n    <- prev_n()
      for (i in seq_len(n))
        updateSelectizeInput(session, paste0("my_p_", i), choices = c("", pool))
    }, ignoreInit = TRUE)

    # Collect non-empty pitcher selections
    my_selected <- reactive({
      n   <- prev_n()
      nms <- vapply(seq_len(n), function(i) {
        v <- input[[paste0("my_p_", i)]]
        if (is.null(v) || !nzchar(v)) "" else v
      }, character(1))
      nms[nzchar(nms)]
    })

    # Export
    output$export_pitchers <- downloadHandler(
      filename = function() paste0("my_pitchers_spo_", format(Sys.Date(), "%Y%m%d"), ".csv"),
      content  = function(file) {
        write.csv(data.frame(pitcher_name = my_selected()), file, row.names = FALSE)
      }
    )

    # Import: parse CSV/TXT, adjust slot count, populate selections
    observeEvent(input$import_file, {
      req(input$import_file)
      tryCatch({
        raw      <- readLines(input$import_file$datapath, warn = FALSE)
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

        for (i in seq_len(min(n_old, n_new))) {
          nm <- names_in[i]
          updateSelectizeInput(session, paste0("my_p_", i),
                               choices  = c("", nm, setdiff(pool, nm)),
                               selected = nm)
        }
      }, error = function(e) NULL)
    })

    # My Pitchers DT: filter built() to selected names
    output$my_dt <- renderDT({
      msg_dt <- function(txt) datatable(
        data.frame(` ` = txt, check.names = FALSE),
        rownames = FALSE, options = list(dom = "t", ordering = FALSE)
      )

      if (is.null(rv$probables))
        return(msg_dt("Fetch probables first, then select your pitchers above."))

      sel <- my_selected()
      if (!length(sel))
        return(msg_dt("Enter pitcher names above to see their schedule."))

      df <- tryCatch(built(), error = function(e) NULL)
      if (is.null(df) || nrow(df) == 0)
        return(msg_dt("No data available."))

      sel_norm <- stream_norm_name(sel)
      df_norm  <- stream_norm_name(df[["Pitcher"]])
      sub      <- df[df_norm %in% sel_norm, , drop = FALSE]
      if (nrow(sub) == 0)
        return(msg_dt("None of the selected pitchers were found in the probables grid."))

      spo_render_dt(sub, scroll_y = "calc(100vh - 520px)")
    })
  })
}
