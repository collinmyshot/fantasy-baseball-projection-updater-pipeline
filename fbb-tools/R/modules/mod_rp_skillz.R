suppressPackageStartupMessages({
  library(DT)
})

# ── Constants ─────────────────────────────────────────────────────────────────

RPZ_FILE_2025 <- "data/processed/2025_rp_skillz_scores.csv"

RPZ_FILES_2026 <- list(
  std = "data/processed/2026_rp_skillz_scores.csv",
  l30 = "data/processed/2026_rp_skillz_scores_l30.csv"
)

RPZ_PERIOD_CHOICES_2025 <- c("Season to Date" = "std")

RPZ_PERIOD_CHOICES_2026 <- c(
  "Season to Date" = "std",
  "Last 30 Days"   = "l30"
)

# FanGraphs month codes for each period key
RPZ_MONTH_CODES <- c(std = 0L, l30 = 3L)

RPZ_DISPLAY_COLS <- c(
  "rp_skillz_rank", "player_name", "team", "p_throws",
  "rp_skillz_score",
  "velo_max", "stuff_plus", "pitching_plus",
  "k_pct", "swstr_pct",
  "sd_md_net", "gmli"
)

RPZ_DISPLAY_NAMES <- c(
  "RK", "Player", "Team", "Throws",
  "Score",
  "Velo", "Stuff+", "Pitching+",
  "K%", "SwStr%",
  "SD-MD", "gmLI"
)

# Empirically derived weights (role_2.5: best mean rho 0.357 across 2021-2024)
RPZ_EMPIRICAL_WEIGHTS <- list(
  w_velo    = 1,
  w_stuff   = 1,
  w_pitch   = 1,
  w_kpct    = 1,
  w_swstr   = 1,
  w_sdmd    = 2.5,
  w_gmli    = 2.5
)

RPZ_EVEN_WEIGHTS <- list(
  w_velo = 1, w_stuff = 1, w_pitch = 1,
  w_kpct = 1, w_swstr = 1, w_sdmd  = 1, w_gmli = 1
)

RPZ_SKILL_HEAVY_WEIGHTS <- list(
  w_velo = 1.5, w_stuff = 1.5, w_pitch = 1.5,
  w_kpct = 1.5, w_swstr = 1.5, w_sdmd  = 1,   w_gmli = 1
)

RPZ_GLOSSARY <- list(
  list(
    term = "Score",
    def  = "RP Skillz composite index. 100 = pool average, \u00b110 pts \u2248 \u00b11 SD. Weighted sum of z-scores across all metrics. Flat weights; no reliability adjustment (samples too small).",
    href = NULL
  ),
  list(
    term = "Velo",
    def  = "Max average fastball-family velocity across 4-seam, 2-seam, sinker, and cutter. Best single-metric signal for raw arm power.",
    href = NULL
  ),
  list(
    term = "Stuff+",
    def  = "Model-based pitch quality score (100 = league avg). Measures velocity, movement, and release characteristics independent of outcomes.",
    href = "https://library.fangraphs.com/pitching/stuff-plus/"
  ),
  list(
    term = "Pitching+",
    def  = "Model-based pitch effectiveness score (100 = league avg). Combines Stuff+ with command and location quality.",
    href = "https://library.fangraphs.com/pitching/stuff-plus/"
  ),
  list(
    term = "K%",
    def  = "Strikeout rate (batters faced). Stabilizes faster than most metrics; reliable signal by ~60 TBF.",
    href = "https://library.fangraphs.com/pitching/rate-stats/"
  ),
  list(
    term = "SwStr%",
    def  = "Swinging Strike Rate. Percentage of total pitches that result in a swing and miss. Faster-stabilizing than CSW% and the better K% predictor across all time horizons.",
    href = NULL
  ),
  list(
    term = "SD-MD",
    def  = "Shutdowns minus Meltdowns. A shutdown is an outing that adds \u22650.06 WPA; a meltdown removes \u22650.06 WPA. Net positive = more wins added than blown. Empirically validated over SD% and SD/(SD+MD) for role prediction.",
    href = "https://library.fangraphs.com/pitching/sd-md/"
  ),
  list(
    term = "gmLI",
    def  = "Average Leverage Index at game entry. 1.0 = average leverage; closers typically enter at 1.5\u20132.0+. Measures manager trust and high-leverage usage. Weighted 2.5x empirically.",
    href = "https://library.fangraphs.com/misc/li/"
  )
)

# ── FanGraphs fetch/parse ─────────────────────────────────────────────────────

RPZ_GEN_FG_BASE  <- "https://www.fangraphs.com/api/leaders/major-league/data"
RPZ_GEN_FG_AGENT <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

# type=3 is the Win Probability / Leverage leaderboard — contains SD, MD,
# gmLI, K%, SwStr%, Stuff+, Pitching+, and per-type fastball velocities.
RPZ_GEN_TYPE <- "3"

rpz_gen_build_url <- function(season = 2026, month = 0) {
  paste0(
    RPZ_GEN_FG_BASE,
    "?pos=all&stats=pit&lg=all&ind=0&qual=0",
    "&type=", RPZ_GEN_TYPE,
    "&season=", season, "&season1=", season,
    "&month=", month,
    "&pageitems=2000&pagenum=1"
  )
}

rpz_gen_fetch <- function(url) {
  fg_fetch_json(url, referer = "https://www.fangraphs.com/leaders/major-league")
}

rpz_gen_parse <- function(result) {
  if (!isTRUE(result$ok) || is.null(result$payload))
    stop("Fetch failed: ", result$error %||% "unknown error")
  p  <- result$payload
  df <- if (is.data.frame(p)) p else if (is.data.frame(p$data)) p$data else NULL
  if (is.null(df) || nrow(df) == 0) stop("No rows in API response")
  df[, colSums(!is.na(df)) > 0, drop = FALSE]
}

# Score raw FanGraphs API data with custom weights
rpz_gen_compute <- function(df, weights = DEFAULT_RP_SKILLZ_WEIGHTS,
                             min_relief_g = 5, relief_share_min = 0.5,
                             num_teams = 15, rp_depth = 3) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  std <- tryCatch(
    standardize_rp_skillz_input(df),
    error = function(e) { message("rpz standardize error: ", conditionMessage(e)); NULL }
  )
  if (is.null(std) || nrow(std) == 0) return(NULL)
  tryCatch(
    compute_rp_skillz_scores(
      skillz_data      = std,
      weights          = weights,
      min_relief_g     = min_relief_g,
      relief_share_min = relief_share_min,
      num_teams        = num_teams,
      rp_depth         = rp_depth
    ),
    error = function(e) { message("rpz compute error: ", conditionMessage(e)); NULL }
  )
}

# Format scored data for the display table
rpz_gen_format <- function(out) {
  if (is.null(out) || nrow(out) == 0) return(NULL)
  raw_scores <- out$rp_skillz_score
  s_mu <- mean(raw_scores, na.rm = TRUE)
  s_sg <- sd(raw_scores,   na.rm = TRUE)
  indexed <- if (is.na(s_sg) || s_sg == 0) rep(100, length(raw_scores)) else
    100 + (raw_scores - s_mu) / s_sg * 10

  data.frame(
    RK          = as.integer(out$rp_skillz_rank),
    Player      = out$player_name,
    Team        = toupper(out$team),
    Throws      = toupper(trimws(as.character(out$p_throws))),
    Score       = round(indexed, 1),
    Velo        = round(out$velo_max, 1),
    `Stuff+`    = as.integer(round(out$stuff_plus)),
    `Pitching+` = as.integer(round(out$pitching_plus)),
    `K%`        = round(out$k_pct * 100, 1),
    `SwStr%`    = round(out$swstr_pct * 100, 1),
    `SD-MD`     = as.integer(out$sd_md_net),
    gmLI        = round(out$gmli, 2),
    check.names      = FALSE,
    stringsAsFactors = FALSE
  )
}

# ── Helpers ───────────────────────────────────────────────────────────────────

load_rpz_data <- function(year, period) {
  path <- if (year == "2025") RPZ_FILE_2025 else RPZ_FILES_2026[[period]]
  if (is.null(path) || !file.exists(path)) return(NULL)

  dat <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)

  # Filter to pool (same concept as SP Skillz starter pool)
  if ("rp_skillz_pool_flag" %in% names(dat)) {
    dat <- dat[dat$rp_skillz_pool_flag == TRUE, , drop = FALSE]
  }

  # Add missing display columns as NA
  for (col in RPZ_DISPLAY_COLS) {
    if (!col %in% names(dat)) dat[[col]] <- NA_real_
  }
  dat <- dat[, RPZ_DISPLAY_COLS, drop = FALSE]
  names(dat) <- RPZ_DISPLAY_NAMES

  # Scale score to 100-index
  raw_scores <- dat[["Score"]]
  s_mu <- mean(raw_scores, na.rm = TRUE)
  s_sg <- sd(raw_scores,   na.rm = TRUE)
  dat[["Score"]] <- round(
    if (is.na(s_sg) || s_sg == 0) rep(100, nrow(dat))
    else 100 + (raw_scores - s_mu) / s_sg * 10,
    1
  )

  # Format numerics
  dat[["Velo"]]      <- round(dat[["Velo"]], 1)
  dat[["Stuff+"]]    <- as.integer(round(dat[["Stuff+"]]))
  dat[["Pitching+"]] <- as.integer(round(dat[["Pitching+"]]))
  dat[["K%"]]        <- round(dat[["K%"]]   * 100, 1)
  dat[["SwStr%"]]    <- round(dat[["SwStr%"]] * 100, 1)
  dat[["SD-MD"]]     <- as.integer(dat[["SD-MD"]])
  dat[["gmLI"]]      <- round(dat[["gmLI"]], 2)

  dat[order(dat$RK), , drop = FALSE]
}

apply_rpz_style <- function(dt) {
  dt |>
    formatStyle("Score",    fontWeight = "700", textAlign = "center") |>
    formatStyle("RK",
      color = "#8a9a8f", fontWeight = "400",
      fontSize = "0.8rem", textAlign = "right"
    ) |>
    formatStyle("Player",   fontWeight = "650", color = "#172733") |>
    formatStyle("Team",     color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle("Throws",   color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center") |>
    formatStyle(c("Velo", "Stuff+", "Pitching+", "K%", "SwStr%", "SD-MD", "gmLI"),
      color = "#4a5a4f", textAlign = "center"
    )
}

rpz_glossary_ui <- function() {
  make_item <- function(entry) {
    div(
      class = "spz-gloss-item",
      tags$span(
        class = "spz-gloss-term",
        if (!is.null(entry$href))
          tags$a(entry$term, href = entry$href, target = "_blank", class = "spz-gloss-link")
        else entry$term
      ),
      tags$span(class = "spz-gloss-def", entry$def)
    )
  }
  div(
    class = "spz-glossary",
    div(class = "spz-glossary-title", "Metric Reference"),
    div(class = "spz-glossary-grid", tagList(lapply(RPZ_GLOSSARY, make_item)))
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

rpSkillzUI <- function(id) {
  ns <- NS(id)

  div(
    class = "spz-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "RP Skillz"),
      p(
        class = "pf-subtitle",
        "Reliever quality index \u2014 fastball velocity, Stuff+, Pitching+, K%, SwStr%, Shutdowns\u2013Meltdowns, and gmLI.",
        tags$br(),
        "Weighted composite score indexed to 100 (pool avg). \u00b110 pts \u2248 \u00b11 SD. ",
        "Role metrics (SD-MD, gmLI) weighted 2.5\u00d7 by default \u2014 empirically optimal for predicting next-season SV+HLD.",
        tags$br(),
        "Pool = top ", tags$b("45"), " relievers (15 teams \u00d7 3 RP slots)."
      )
    ),

    # ── Season + period + fetch ───────────────────────────────────────────────
    div(
      class = "pf-controls-row",
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season"),
        div(
          class = "pf-toggle",
          radioButtons(
            ns("year"),
            label    = NULL,
            choices  = c("2025" = "2025", "2026" = "2026"),
            selected = "2026",
            inline   = TRUE
          )
        )
      ),
      div(
        class = "pf-control-group",
        actionButton(ns("gen_fetch"), "Fetch Data",
                     class = "btn btn-pag-generate", icon = icon("rotate-right")),
        div(class = "sps-status status-shell", textOutput(ns("gen_status"), inline = TRUE))
      )
    ),
    uiOutput(ns("period_ui")),

    # ── Weights card ─────────────────────────────────────────────────────────
    div(
      class = "spz-weights-card",
      div(class = "spz-weights-card-title", "Metric Weights"),
      p(class = "spz-weights-card-desc",
        "Single weight set (no IP paradigm \u2014 all relievers scored together). ",
        "Role metrics (SD-MD, gmLI) have been empirically validated to 2.5\u00d7 skill metrics for predicting ",
        "next-season SV+HLD (Spearman \u03c1 = 0.357 vs 0.292 at equal weights, 2021\u20132024). ",
        "Adjust below to explore skill-first or role-first rankings."),
      div(class = "spz-preset-row",
        tags$span(class = "spz-preset-label", "PRESET"),
        actionButton(ns("preset_empirical"), "Empirical",
                     class = "btn btn-spz-preset btn-spz-preset-active"),
        actionButton(ns("preset_even"),      "Even",
                     class = "btn btn-spz-preset"),
        actionButton(ns("preset_skill"),     "Skill Heavy",
                     class = "btn btn-spz-preset")
      ),
      # Single row of 7 metric weights
      div(class = "spz-weights-table spz-weights-table-rp",
        local({
          metrics <- list(
            list("Velo",      "w_velo",  1),
            list("Stuff+",    "w_stuff", 1),
            list("Pitching+", "w_pitch", 1),
            list("K%",        "w_kpct",  1),
            list("SwStr%",    "w_swstr", 1),
            list("SD-MD",     "w_sdmd",  2.5),
            list("gmLI",      "w_gmli",  2.5)
          )
          hdr <- div(class = "spz-wt-row spz-wt-header rpz-wt-row",
            div(class = "spz-wt-paradigm-label"),
            tagList(lapply(metrics, function(m) div(class = "spz-wt-col-label", m[[1]])))
          )
          wt_row <- div(class = "spz-wt-row rpz-wt-row",
            div(class = "spz-wt-paradigm-label",
              "Weight", tags$br(), tags$small("all IP")),
            tagList(lapply(metrics, function(m)
              div(class = "spz-wt-input",
                numericInput(ns(m[[2]]), NULL, value = m[[3]], step = 0.1, width = "78px"))
            ))
          )
          tagList(hdr, wt_row)
        })
      )
    ),

    # ── Search bar ────────────────────────────────────────────────────────────
    div(
      class = "pf-controls-row spz-search-row",
      div(
        class = "spz-search-wrap",
        tags$span(class = "spz-search-icon", HTML("&#x2315;")),
        textInput(
          ns("search"),
          label       = NULL,
          placeholder = "Search player or team\u2026",
          width       = "100%"
        )
      )
    ),

    # ── Table or empty state ──────────────────────────────────────────────────
    uiOutput(ns("body_ui")),

    # ── API diagnostic ────────────────────────────────────────────────────────
    tags$details(
      style = "margin-top:16px",
      tags$summary(class = "sps-diag-toggle", "API Response Diagnostic"),
      verbatimTextOutput(ns("rpz_diag"))
    ),

    # ── Footer + glossary ─────────────────────────────────────────────────────
    div(
      class = "pf-footer",
      tags$span(
        class = "pf-footer-text",
        "Model: fastball velo (max across FA/FT/SI/FC), Stuff+, Pitching+, K%, SwStr%, SD\u2013MD (net shutdowns), gmLI (entry leverage). ",
        "Eligibility: \u226550% relief IP, \u22655 relief appearances. ",
        "Role weights (SD-MD, gmLI) empirically set to 2.5\u00d7 via Spearman correlation to next-season SV+HLD (2021\u20132024)."
      )
    ),
    rpz_glossary_ui()
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

rpSkillzServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    output$period_ui <- renderUI({
      req(input$year)
      choices <- if (input$year == "2025") RPZ_PERIOD_CHOICES_2025 else RPZ_PERIOD_CHOICES_2026
      div(
        class = "pf-controls-row",
        div(
          class = "pf-control-group",
          tags$span(class = "pf-control-label", "Period"),
          div(
            class = "pf-toggle spz-period-toggle",
            radioButtons(
              ns("period"),
              label    = NULL,
              choices  = choices,
              selected = "std",
              inline   = TRUE
            )
          )
        )
      )
    })

    rv_rpz <- reactiveValues(
      raw_std       = NULL,
      raw_l30       = NULL,
      std           = NULL,
      l30           = NULL,
      status        = "Select season and click \u2018Fetch Data\u2019.",
      diag          = NULL,
      fetch_state   = "none"   # "none" | "ok" | "error"
    )

    output$gen_status <- renderText({ rv_rpz$status })
    output$rpz_diag   <- renderText({ rv_rpz$diag %||% "" })

    # Current weights from inputs (with empirical defaults)
    cur_weights <- function() {
      g <- function(id, default) { v <- suppressWarnings(as.numeric(input[[id]])); if (is.na(v)) default else v }
      c(
        velo_max      = g("w_velo",  1),
        stuff_plus    = g("w_stuff", 1),
        pitching_plus = g("w_pitch", 1),
        k_pct         = g("w_kpct",  1),
        swstr_pct     = g("w_swstr", 1),
        sd_md_net     = g("w_sdmd",  2.5),
        gmli          = g("w_gmli",  2.5)
      )
    }

    # Re-score all periods whenever weights change (no re-fetch needed)
    observe({
      w <- cur_weights()
      score_one <- function(raw) {
        if (is.null(raw)) return(NULL)
        rpz_gen_compute(raw, weights = w)
      }
      rv_rpz$std <- score_one(rv_rpz$raw_std)
      rv_rpz$l30 <- score_one(rv_rpz$raw_l30)
    })

    observeEvent(input$gen_fetch, {
      yr      <- as.integer(input$year %||% "2026")
      periods <- if (yr == 2025L) c("std") else c("std", "l30")
      withProgress(message = "Fetching RP Skillz…", value = 0, {
        rv_rpz$status      <- "Fetching…"
        rv_rpz$diag        <- NULL
        rv_rpz$fetch_state <- "none"
        for (k in c("std", "l30")) rv_rpz[[paste0("raw_", k)]] <- NULL

        fetch_raw <- function(month) {
          tryCatch({
            url <- rpz_gen_build_url(season = yr, month = month)
            res <- rpz_gen_fetch(url)
            rpz_gen_parse(res)
          }, error = function(e) {
            message("rpz fetch error (month=", month, "): ", conditionMessage(e))
            NULL
          })
        }
        for (k in periods) {
          lbl <- names(RPZ_PERIOD_CHOICES_2026)[RPZ_PERIOD_CHOICES_2026 == k]
          incProgress(1 / length(periods),
                      detail = if (length(lbl) > 0) lbl[1] else k)
          rv_rpz[[paste0("raw_", k)]] <- fetch_raw(RPZ_MONTH_CODES[[k]])
        }

        raw_std <- rv_rpz$raw_std
        if (!is.null(raw_std) && nrow(raw_std) > 0) {
          n <- nrow(raw_std)
          rv_rpz$status      <- sprintf("%d relievers — %s", n, format(Sys.time(), "%I:%M %p"))
          rv_rpz$fetch_state <- "ok"
          rv_rpz$diag <- paste0(
            "Shape: ", nrow(raw_std), " rows × ", ncol(raw_std), " cols\n",
            "Columns: ", paste(names(raw_std), collapse = ", "), "\n",
            "Row 1: ", paste(
              mapply(function(nm, v) paste0(nm, "=", substr(as.character(v), 1, 40)),
                     names(raw_std), as.list(raw_std[1, ])),
              collapse = " | ")
          )
        } else {
          rv_rpz$status      <- paste0("Fetch failed — ", format(Sys.time(), "%I:%M %p"))
          rv_rpz$fetch_state <- "error"
        }
      })
    })
    # Weight presets
    apply_weights <- function(wts) {
      for (id in names(wts)) updateNumericInput(session, id, value = wts[[id]])
    }
    observeEvent(input$preset_empirical, { apply_weights(RPZ_EMPIRICAL_WEIGHTS) })
    observeEvent(input$preset_even,      { apply_weights(RPZ_EVEN_WEIGHTS) })
    observeEvent(input$preset_skill,     { apply_weights(RPZ_SKILL_HEAVY_WEIGHTS) })

    # Reactive data: 2025 always loads from CSV (season over); 2026 live-fetch only.
    rpz_data <- reactive({
      period <- input$period %||% "std"
      yr     <- input$year   %||% "2026"
      if (yr == "2025") return(load_rpz_data("2025", "std"))
      rpz_gen_format(rv_rpz[[period]])
    })

    search_d <- debounce(reactive(input$search), 300)

    rpz_filtered <- reactive({
      dat <- rpz_data()
      if (is.null(dat) || nrow(dat) == 0) return(NULL)
      q   <- trimws(search_d() %||% "")
      if (nchar(q) == 0) return(dat)
      mask <- grepl(q, dat[["Player"]],  ignore.case = TRUE) |
              grepl(q, dat[["Team"]],    ignore.case = TRUE) |
              grepl(q, dat[["Throws"]], ignore.case = TRUE)
      dat[mask, , drop = FALSE]
    })

    output$body_ui <- renderUI({
      dat   <- rpz_data()
      state <- rv_rpz$fetch_state
      yr    <- input$year %||% "2026"
      if (!is.null(dat) && nrow(dat) > 0) {
        div(class = "pf-table-wrap", DTOutput(ns("table"), width = "100%"))
      } else if (state == "error") {
        div(
          class = "spz-empty",
          div(
            class = "spz-empty-inner",
            h3(class = "spz-empty-title", "\u26a0\ufe0f Fetch failed"),
            p(class = "spz-empty-desc",
              "Could not retrieve data from FanGraphs. This may be a temporary issue.",
              tags$br(),
              "Please wait a moment and click \u2018Fetch Data\u2019 again."
            )
          )
        )
      } else {
        div(
          class = "spz-empty",
          div(
            class = "spz-empty-inner",
            h3(class = "spz-empty-title", "No data loaded"),
            p(class = "spz-empty-desc",
              if (yr == "2025")
                "No 2025 data file found."
              else
                "Select a season and click \u2018Fetch Data\u2019 to fetch live data from FanGraphs."
            )
          )
        )
      }
    })

    output$table <- renderDT({
      dat <- rpz_filtered()
      if (is.null(dat) || nrow(dat) == 0) return(NULL)

      # Column layout (0-based):
      # 0:RK  1:Player  2:Team  3:Throws  4:Score  5:Velo  6:Stuff+  7:Pitching+
      # 8:K%  9:SwStr%  10:SD-MD  11:gmLI
      score_col <- 4L

      scores <- dat[["Score"]]
      s_min  <- min(scores, na.rm = TRUE)
      s_max  <- max(scores, na.rm = TRUE)
      s_med  <- median(scores, na.rm = TRUE)

      col_defs <- list(
        list(className = "dt-right",  targets = 0L),
        list(className = "dt-left",   targets = 1L),
        list(className = "dt-center", targets = seq.int(2L, 11L)),
        list(width = "36px",  targets = 0L),   # RK
        list(width = "160px", targets = 1L),   # Player
        list(width = "52px",  targets = 2L),   # Team
        list(width = "46px",  targets = 3L),   # Throws
        list(width = "68px",  targets = 4L),   # Score
        list(width = "56px",  targets = 5L),   # Velo
        list(width = "60px",  targets = 6L),   # Stuff+
        list(width = "68px",  targets = 7L),   # Pitching+
        list(width = "56px",  targets = 8L),   # K%
        list(width = "60px",  targets = 9L),   # SwStr%
        list(width = "58px",  targets = 10L),  # SD-MD
        list(width = "58px",  targets = 11L)   # gmLI
      )

      datatable(
        dat,
        rownames  = FALSE,
        filter    = "none",
        selection = "none",
        options   = list(
          dom           = "t",
          ordering      = TRUE,
          pageLength    = nrow(dat),
          scrollX       = TRUE,
          scrollY       = "calc(100vh - 380px)",
          scrollCollapse = FALSE,
          order         = list(list(0L, "asc")),
          createdRow = JS(sprintf(
            "function(row, data, index) {
               var score = data[%d];
               if (score === null || score === undefined || score === '') return;
               var sMin = %s, sMed = %s, sMax = %s;
               var pct;
               if (sMed > sMin && score <= sMed) {
                 pct = 0.5 * (score - sMin) / (sMed - sMin);
               } else if (sMax > sMed) {
                 pct = 0.5 + 0.5 * (score - sMed) / (sMax - sMed);
               } else { pct = 0.5; }
               pct = Math.max(0, Math.min(1, pct));
               var c1=[31,53,86], c2=[238,245,236], c3=[183,115,67], ca, cb, t;
               if (pct <= 0.5) { t = pct*2;       ca = c1; cb = c2; }
               else            { t = (pct-0.5)*2; ca = c2; cb = c3; }
               var r=Math.round(ca[0]+(cb[0]-ca[0])*t);
               var g=Math.round(ca[1]+(cb[1]-ca[1])*t);
               var b=Math.round(ca[2]+(cb[2]-ca[2])*t);
               var lum=0.2126*(r/255)+0.7152*(g/255)+0.0722*(b/255);
               var txt = lum < 0.45 ? '#ffffff' : '#172733';
               $('td:eq(%d)', row).css({'background-color':'rgb('+r+','+g+','+b+')','color':txt});
             }",
            score_col, s_min, s_med, s_max, score_col
          )),
          columnDefs = col_defs
        ),
        class = "pf-dt display nowrap"
      ) |>
        apply_rpz_style()
    })

  })
}
