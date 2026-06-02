# ── mod_player_rater.R ────────────────────────────────────────────────────────
# In-Season Tool: Player Rater
# Fetches YTD stats from FanGraphs, runs the same z-score/points pipeline as
# the AVC, and outputs ranked player values.
# Reuses: auc_fetch_ytd(), compute_roto_h_zscores(), compute_roto_p_zscores(),
#         compute_roto_dollars(), compute_points_h(), compute_points_p(),
#         classify_sp(), classify_role(), fmt_display_h(), fmt_display_p(),
#         make_combined(), make_dt_opts, auc_cat_row_ui(),
#         AUC_H_ROTO_CATS, AUC_P_ROTO_CATS, etc. — all defined in mod_auc_val.R

# ── UI ────────────────────────────────────────────────────────────────────────

playerRaterUI <- function(id) {
  ns <- NS(id)

  div(
    class = "auc-page pag-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pag-page-header",
      div(class = "pag-breadcrumb", "IN-SEASON TOOLS"),
      div(
        h1(class = "pag-page-title", "Player Rater"),
        p(class = "pag-page-desc",
          "Rank players by season-to-date performance. ",
          "Stats are auto-fetched from FanGraphs when you click Calculate.")
      )
    ),

    # ── Scoring mode toggle bar ───────────────────────────────────────────────
    div(
      class = "auc-mode-bar",
      id    = ns("pr_mode_bar"),
      tags$button(
        id    = ns("mode_btn_roto"),
        class = "auc-mode-btn auc-mode-btn--active",
        type  = "button",
        onclick = paste0(
          "document.getElementById('", ns("mode_btn_roto"), "').classList.add('auc-mode-btn--active');",
          "document.getElementById('", ns("mode_btn_points"), "').classList.remove('auc-mode-btn--active');",
          "Shiny.setInputValue('", ns("scoring_mode"), "', 'roto', {priority:'event'});"
        ),
        tags$span(class = "auc-mode-label", "Roto"),
        tags$span(class = "auc-mode-sub",   "Z-Scores & Dollar Values")
      ),
      tags$button(
        id    = ns("mode_btn_points"),
        class = "auc-mode-btn",
        type  = "button",
        onclick = paste0(
          "document.getElementById('", ns("mode_btn_points"), "').classList.add('auc-mode-btn--active');",
          "document.getElementById('", ns("mode_btn_roto"), "').classList.remove('auc-mode-btn--active');",
          "Shiny.setInputValue('", ns("scoring_mode"), "', 'points', {priority:'event'});"
        ),
        tags$span(class = "auc-mode-label", "Points"),
        tags$span(class = "auc-mode-sub",   "Ottoneu / Custom Scoring")
      )
    ),
    # Hidden radio drives conditionalPanel JS conditions
    tags$div(style = "display:none;",
      radioButtons(ns("scoring_mode"), NULL,
                   choices = c("roto", "points"), selected = "roto")),

    # ── League Settings ───────────────────────────────────────────────────────
    div(
      class = "auc-section",
      div(class = "auc-section-title", "League Settings"),
      div(
        class = "auc-league-row",
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "# Teams"),
          tags$input(id = ns("num_teams"), type = "number",
                     value = AUC_DEFAULT_TEAMS, min = "1", step = "1",
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Budget / team ($)"),
          tags$input(id = ns("budget"), type = "text", value = AUC_DEFAULT_BUDGET,
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Starting bats / team"),
          tags$input(id = ns("start_bats"), type = "text",
                     value = AUC_DEFAULT_START_BATS,
                     class = "form-control auc-budget-input")
        ),
        div(
          class = "auc-league-field",
          tags$label(class = "auc-field-label", "Starting SP / team"),
          tags$input(id = ns("start_sp"), type = "text",
                     value = AUC_DEFAULT_START_SP,
                     class = "form-control auc-budget-input")
        ),
        conditionalPanel(
          condition = "input.scoring_mode === 'roto'",
          ns = ns,
          div(
            class = "auc-league-field",
            tags$label(class = "auc-field-label", "Hitting budget %"),
            div(class = "auc-split-row",
                tags$input(id = ns("hit_pct"), type = "text",
                           value = AUC_DEFAULT_HIT_PCT,
                           class = "form-control auc-budget-input auc-split-input"),
                tags$span(class = "auc-split-sep", "/"),
                uiOutput(ns("pit_pct_display"))
            )
          )
        )
      )
    ),

    # ── PA / IP Minimums ─────────────────────────────────────────────────────
    div(
      class = "auc-section",
      div(class = "auc-section-title", "Playing Time Minimums"),
      div(
        class = "auc-league-row",
        div(
          class = "auc-league-field",
          numericInput(ns("pr_pa_min"), "Min PA", value = 50, min = 0, step = 10, width = "90px")
        ),
        div(
          class = "auc-league-field",
          numericInput(ns("pr_ip_min"), "Min IP", value = 10, min = 0, step = 5, width = "90px")
        )
      )
    ),

    # ── Roto Categories & Weights ─────────────────────────────────────────────
    conditionalPanel(
      condition = "input.scoring_mode === 'roto'",
      ns = ns,
      div(
        class = "auc-section",
        div(class = "auc-section-title", "Roto Categories & Weights"),
        div(
          class = "pag-preset-row",
          tags$span(class = "pag-preset-label", "Category Presets:"),
          lapply(names(AUC_ROTO_PRESETS), function(nm) {
            actionButton(ns(paste0("preset_roto_", gsub("[^a-z0-9]", "", tolower(nm)))),
                         nm, class = "btn btn-pag-preset")
          })
        ),
        layout_columns(
          col_widths = c(5, 7), gap = "20px",
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Hitting"),
            div(
              class = "pag-preset-row",
              tags$span(class = "pag-preset-label", "Weight Presets:"),
              actionButton(ns("preset_wt_equal_h"),  "Equal",     class = "btn btn-pag-preset btn-pag-preset--active"),
              actionButton(ns("preset_wt_novol_h"),  "No Volume", class = "btn btn-pag-preset")
            ),
            div(
              class = "auc-vol-weight-row",
              tags$span(class = "auc-vol-weight-label", "PA Volume Weight"),
              numericInput(ns("pr_pa_wt"), label = NULL, value = 1.0, min = 0, step = 0.05, width = "70px")
            ),
            div(class = "pag-panel-subtitle", "Tick to include \u2014 weight adjusts category importance"),
            div(
              class = "auc-cat-grid",
              lapply(seq_along(AUC_H_ROTO_CATS), function(i) {
                cat <- AUC_H_ROTO_CATS[i]
                lab <- AUC_H_ROTO_LABS[i]
                if (cat %in% AUC_H_DISPLAY_ONLY) return(NULL)
                auc_cat_row_ui(ns, cat, lab, "h",
                               default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$h,
                               default_wt = 1)
              })
            )
          ),
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Pitching"),
            div(
              class = "pag-preset-row",
              tags$span(class = "pag-preset-label", "Weight Presets:"),
              actionButton(ns("preset_wt_equal_p"),  "Equal",     class = "btn btn-pag-preset btn-pag-preset--active"),
              actionButton(ns("preset_wt_novol_p"),  "No Volume", class = "btn btn-pag-preset")
            ),
            div(
              class = "auc-vol-weight-row",
              tags$span(class = "auc-vol-weight-label", "IP Volume Weight"),
              numericInput(ns("pr_ip_wt"), label = NULL, value = 1.0, min = 0, step = 0.05, width = "70px")
            ),
            div(class = "pag-panel-subtitle",
                "Tick to include \u2014 weight adjusts importance. SVHD = SV + HD combined."),
            div(
              class = "auc-cat-grid",
              lapply(seq_along(AUC_P_ROTO_CATS), function(i) {
                cat <- AUC_P_ROTO_CATS[i]
                lab <- AUC_P_ROTO_LABS[i]
                if (cat %in% AUC_P_DISPLAY_ONLY) return(NULL)
                auc_cat_row_ui(ns, cat, lab, "p",
                               default_selected = cat %in% AUC_ROTO_PRESETS[["Standard 5x5"]]$p,
                               default_wt = 1)
              })
            )
          )
        )
      )
    ),

    # ── Points Values ─────────────────────────────────────────────────────────
    conditionalPanel(
      condition = "input.scoring_mode === 'points'",
      ns = ns,
      div(
        class = "auc-section",
        div(class = "auc-section-title", "Point Values"),
        div(
          class = "auc-pts-preset-row",
          tags$span(class = "pag-preset-label", "Preset:"),
          selectInput(ns("pts_preset"), label = NULL,
                      choices = AUC_POINTS_PRESET_NAMES,
                      selected = AUC_POINTS_PRESET_NAMES[1],
                      width = "220px")
        ),
        layout_columns(
          col_widths = c(5, 7), gap = "20px",
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Hitting Point Values"),
            div(
              class = "auc-pts-grid",
              lapply(names(AUC_H_PTS_STAT_LABS), function(s) {
                div(class = "auc-pts-cell",
                    numericInput(ns(paste0("pts_h_", s)),
                                 label = AUC_H_PTS_STAT_LABS[[s]],
                                 value = AUC_POINTS_PRESETS[[1]]$h[[s]],
                                 step = 0.1, width = "80px"))
              })
            )
          ),
          div(
            class = "pag-panel",
            div(class = "pag-panel-title", "Pitching Point Values"),
            div(
              class = "auc-pts-grid",
              lapply(names(AUC_P_PTS_STAT_LABS), function(s) {
                div(class = "auc-pts-cell",
                    numericInput(ns(paste0("pts_p_", s)),
                                 label = AUC_P_PTS_STAT_LABS[[s]],
                                 value = AUC_POINTS_PRESETS[[1]]$p[[s]],
                                 step = 0.1, width = "80px"))
              })
            )
          )
        )
      )
    ),

    # ── Action row ────────────────────────────────────────────────────────────
    div(
      class = "auc-action-row",
      actionButton(ns("calculate"), "Calculate",
                   class = "btn btn-pag-generate",
                   icon  = icon("calculator")),
      uiOutput(ns("export_btn"))
    ),

    uiOutput(ns("error_msg")),

    # ── Results ───────────────────────────────────────────────────────────────
    uiOutput(ns("results_ui"))
  )
}

# ── Server ────────────────────────────────────────────────────────────────────

playerRaterServer <- function(id, adp_data = NULL) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── State ─────────────────────────────────────────────────────────────────
    calculated <- reactiveVal(FALSE)
    last_error <- reactiveVal(NULL)
    ytd_h      <- reactiveVal(NULL)
    ytd_p      <- reactiveVal(NULL)

    # ── Pit % display ─────────────────────────────────────────────────────────
    output$pit_pct_display <- renderUI({
      pct <- suppressWarnings(as.numeric(input$hit_pct))
      if (is.na(pct)) pct <- AUC_DEFAULT_HIT_PCT
      tags$span(class = "auc-split-pct",
                paste0(max(0, min(100, round(100 - pct))), "% pitching"))
    })

    # ── Category selection reactives ──────────────────────────────────────────
    roto_cats_h_sel <- reactive({
      cats <- setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY)
      sel  <- Filter(function(cat) isTRUE(input[[paste0("auc_cat_h_", cat)]]), cats)
      unique(c(AUC_H_DISPLAY_ONLY, sel))
    })
    roto_cats_p_sel <- reactive({
      cats <- setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY)
      sel  <- Filter(function(cat) isTRUE(input[[paste0("auc_cat_p_", cat)]]), cats)
      unique(c(AUC_P_DISPLAY_ONLY, sel))
    })

    # ── Cat weight reactives ──────────────────────────────────────────────────
    cat_weights_h <- reactive({
      cats <- setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY)
      setNames(vapply(cats, function(cat) {
        v <- input[[paste0("auc_cat_wt_h_", cat)]]
        if (is.null(v) || is.na(v)) 1.0 else max(0, as.numeric(v))
      }, numeric(1)), cats)
    })
    cat_weights_p <- reactive({
      cats <- setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY)
      setNames(vapply(cats, function(cat) {
        v <- input[[paste0("auc_cat_wt_p_", cat)]]
        if (is.null(v) || is.na(v)) 1.0 else max(0, as.numeric(v))
      }, numeric(1)), cats)
    })

    # ── Roto preset buttons ───────────────────────────────────────────────────
    lapply(names(AUC_ROTO_PRESETS), function(nm) {
      btn_id <- paste0("preset_roto_", gsub("[^a-z0-9]", "", tolower(nm)))
      observeEvent(input[[btn_id]], {
        req(input[[btn_id]] > 0)
        h_sel <- AUC_ROTO_PRESETS[[nm]]$h
        p_sel <- AUC_ROTO_PRESETS[[nm]]$p
        for (cat in setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY))
          updateCheckboxInput(session, paste0("auc_cat_h_", cat), value = cat %in% h_sel)
        for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
          updateCheckboxInput(session, paste0("auc_cat_p_", cat), value = cat %in% p_sel)
      })
    })

    # ── Weight preset buttons ─────────────────────────────────────────────────
    observeEvent(input$preset_wt_equal_h, {
      req(input$preset_wt_equal_h > 0)
      for (cat in setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_h_", cat), value = 1)
      updateNumericInput(session, "pr_pa_wt", value = 1)
    })
    observeEvent(input$preset_wt_novol_h, {
      req(input$preset_wt_novol_h > 0)
      for (cat in setdiff(AUC_H_ROTO_CATS, AUC_H_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_h_", cat), value = 1)
      updateNumericInput(session, "pr_pa_wt", value = 0)
    })
    observeEvent(input$preset_wt_equal_p, {
      req(input$preset_wt_equal_p > 0)
      for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_p_", cat), value = 1)
      updateNumericInput(session, "pr_ip_wt", value = 1)
    })
    observeEvent(input$preset_wt_novol_p, {
      req(input$preset_wt_novol_p > 0)
      for (cat in setdiff(AUC_P_ROTO_CATS, AUC_P_DISPLAY_ONLY))
        updateNumericInput(session, paste0("auc_cat_wt_p_", cat), value = 1)
      updateNumericInput(session, "pr_ip_wt", value = 0)
    })

    # ── SVHD exclusivity ──────────────────────────────────────────────────────
    observeEvent(input$auc_cat_p_svhd, {
      if (isTRUE(input$auc_cat_p_svhd)) {
        updateCheckboxInput(session, "auc_cat_p_sv", value = FALSE)
        updateCheckboxInput(session, "auc_cat_p_hd", value = FALSE)
      }
    }, ignoreInit = TRUE)
    observeEvent(input$auc_cat_p_sv, {
      if (isTRUE(input$auc_cat_p_sv))
        updateCheckboxInput(session, "auc_cat_p_svhd", value = FALSE)
    }, ignoreInit = TRUE)
    observeEvent(input$auc_cat_p_hd, {
      if (isTRUE(input$auc_cat_p_hd))
        updateCheckboxInput(session, "auc_cat_p_svhd", value = FALSE)
    }, ignoreInit = TRUE)

    # ── Points preset loader ──────────────────────────────────────────────────
    observeEvent(input$pts_preset, {
      nm <- input$pts_preset
      if (!nm %in% names(AUC_POINTS_PRESETS)) return()
      spec_h <- AUC_POINTS_PRESETS[[nm]]$h
      spec_p <- AUC_POINTS_PRESETS[[nm]]$p
      for (s in names(spec_h))
        updateNumericInput(session, paste0("pts_h_", s), value = spec_h[[s]])
      for (s in names(spec_p))
        updateNumericInput(session, paste0("pts_p_", s), value = spec_p[[s]])
    })

    # ── Points spec reactives ─────────────────────────────────────────────────
    pts_spec_h <- reactive({
      req(input$scoring_mode == "points")
      setNames(
        vapply(names(AUC_H_PTS_STAT_LABS), function(s) {
          v <- input[[paste0("pts_h_", s)]]; if (is.null(v) || is.na(v)) 0 else as.numeric(v)
        }, numeric(1)),
        names(AUC_H_PTS_STAT_LABS)
      )
    })
    pts_spec_p <- reactive({
      req(input$scoring_mode == "points")
      setNames(
        vapply(names(AUC_P_PTS_STAT_LABS), function(s) {
          v <- input[[paste0("pts_p_", s)]]; if (is.null(v) || is.na(v)) 0 else as.numeric(v)
        }, numeric(1)),
        names(AUC_P_PTS_STAT_LABS)
      )
    })

    # ── Calculate handler ─────────────────────────────────────────────────────
    observeEvent(input$calculate, {
      last_error(NULL)
      ytd_h(NULL)
      ytd_p(NULL)

      withProgress(message = "Fetching Season-to-Date stats\u2026", value = 0, {
        tryCatch({
          incProgress(0.4, detail = "Hitters\u2026")
          h <- auc_fetch_ytd("bat")
          incProgress(0.4, detail = "Pitchers\u2026")
          p <- auc_fetch_ytd("pit")
          if (is.null(h) && is.null(p)) {
            last_error("No YTD data returned from FanGraphs. Try again later.")
            return()
          }
          ytd_h(h)
          ytd_p(p)
          calculated(TRUE)
        }, error = function(e) {
          last_error(paste("Failed to fetch stats:", conditionMessage(e)))
        })
      })
    })

    # ── Aggregated YTD data (with singles derivation) ─────────────────────────
    # YTD data is already in the flat format expected by the z-score pipeline.
    # We just need to add x1b and ensure er is present.
    agg_h <- reactive({
      req(calculated())
      df <- ytd_h()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      # Derive 1B if components present
      if (all(c("h","x2b","x3b","hr") %in% names(df))) {
        df$x1b <- pmax(0, df$h - df$x2b - df$x3b - df$hr, na.rm = FALSE)
        df$x1b[is.na(df$h)|is.na(df$x2b)|is.na(df$x3b)|is.na(df$hr)] <- NA_real_
      }
      df
    })

    agg_p <- reactive({
      req(calculated())
      df <- ytd_p()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      # Derive ER from ERA * IP / 9 if not present
      need_er <- !("er" %in% names(df)) || all(is.na(df$er))
      if (need_er && all(c("era","ip") %in% names(df)))
        df$er <- round(df$era * df$ip / 9, 1)
      df$role <- ifelse(classify_sp(df), "SP", "RP")
      df
    })

    # ── Shared z-score reactives ──────────────────────────────────────────────
    z_result_h <- reactive({
      req(calculated(), agg_h(), input$scoring_mode == "roto")
      cats <- roto_cats_h_sel()
      if (is.null(cats) || length(cats) == 0) return(NULL)
      n_teams <- suppressWarnings(as.integer(input$num_teams))
      if (is.na(n_teams)) n_teams <- AUC_DEFAULT_TEAMS
      n_start <- suppressWarnings(as.integer(input$start_bats))
      if (is.na(n_start)) n_start <- AUC_DEFAULT_START_BATS
      pa_min <- suppressWarnings(as.numeric(input$pr_pa_min))
      if (is.na(pa_min)) pa_min <- 50
      compute_roto_h_zscores(agg_h(), cats, pa_min,
                              starter_count = n_teams * n_start,
                              cat_weights   = cat_weights_h(),
                              pa_weight     = suppressWarnings(as.numeric(input$pr_pa_wt)) %||% 1.0)
    })

    z_result_p <- reactive({
      req(calculated(), agg_p(), input$scoring_mode == "roto")
      cats <- roto_cats_p_sel()
      if (is.null(cats) || length(cats) == 0) return(NULL)
      n_teams <- suppressWarnings(as.integer(input$num_teams))
      if (is.na(n_teams)) n_teams <- AUC_DEFAULT_TEAMS
      n_start <- suppressWarnings(as.integer(input$start_sp))
      if (is.na(n_start)) n_start <- AUC_DEFAULT_START_SP
      ip_min <- suppressWarnings(as.numeric(input$pr_ip_min))
      if (is.na(ip_min)) ip_min <- 10
      compute_roto_p_zscores(agg_p(), cats, ip_min,
                              starter_count = n_teams * n_start,
                              cat_weights   = cat_weights_p(),
                              ip_weight     = suppressWarnings(as.numeric(input$pr_ip_wt)) %||% 1.0)
    })

    # ── Computed results ──────────────────────────────────────────────────────
    result_h <- reactive({
      req(calculated(), agg_h())
      mode <- input$scoring_mode
      if (mode == "roto") {
        z_res <- z_result_h()
        if (is.null(z_res)) return(NULL)
        budget  <- suppressWarnings(as.numeric(input$budget))
        if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
        hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
        if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
        n_teams <- suppressWarnings(as.integer(input$num_teams))
        if (is.na(n_teams)) n_teams <- AUC_DEFAULT_TEAMS
        h_budget <- budget * hit_pct * n_teams
        compute_roto_dollars(z_res, h_budget)
      } else {
        compute_points_h(agg_h(), pts_spec_h())
      }
    })

    result_p <- reactive({
      req(calculated(), agg_p())
      mode <- input$scoring_mode
      if (mode == "roto") {
        z_res <- tryCatch(z_result_p(), error = function(e) NULL)
        if (is.null(z_res)) {
          ap2 <- agg_p()[classify_sp(agg_p()), , drop = FALSE]
          ip_min <- suppressWarnings(as.numeric(input$pr_ip_min)) %||% 10
          ap2 <- ap2[!is.na(ap2$ip) & ap2$ip >= ip_min, , drop = FALSE]
          return(if (nrow(ap2) == 0) NULL else ap2)
        }
        budget  <- suppressWarnings(as.numeric(input$budget))
        if (is.na(budget)) budget <- AUC_DEFAULT_BUDGET
        hit_pct <- suppressWarnings(as.numeric(input$hit_pct)) / 100
        if (is.na(hit_pct)) hit_pct <- AUC_DEFAULT_HIT_PCT / 100
        n_teams <- suppressWarnings(as.integer(input$num_teams))
        if (is.na(n_teams)) n_teams <- AUC_DEFAULT_TEAMS
        pit_budget <- budget * (1 - hit_pct) * n_teams
        compute_roto_dollars(z_res, pit_budget)
      } else {
        compute_points_p(agg_p(), pts_spec_p())
      }
    })

    # ── Error display ─────────────────────────────────────────────────────────
    output$error_msg <- renderUI({
      err <- last_error()
      if (is.null(err)) return(NULL)
      div(class = "auc-error-box",
          tags$strong("Error: "), err)
    })

    # ── Export button ─────────────────────────────────────────────────────────
    output$export_btn <- renderUI({
      if (!calculated()) {
        tags$button("Export", class = "btn btn-pag-export",
                    title = "Exports as .xlsx",
                    disabled = NA, style = "opacity:0.4;cursor:not-allowed;")
      } else {
        downloadButton(ns("download"), "Export", class = "btn btn-pag-export",
                       title = "Exports as .xlsx")
      }
    })

    output$download <- downloadHandler(
      filename = function() sprintf("player_rater_%s_%s.xlsx", input$scoring_mode, Sys.Date()),
      content  = function(file) {
        wb <- createWorkbook()
        rh <- tryCatch(result_h(), error = function(e) NULL)
        rp <- tryCatch(result_p(), error = function(e) NULL)
        if (!is.null(rh)) { addWorksheet(wb, "Hitters");  writeData(wb, "Hitters",  rh) }
        if (!is.null(rp)) { addWorksheet(wb, "Pitchers"); writeData(wb, "Pitchers", rp) }
        comb <- pr_make_combined(rh, rp, input$scoring_mode)
        if (!is.null(comb)) { addWorksheet(wb, "Combined"); writeData(wb, "Combined", comb) }
        saveWorkbook(wb, file, overwrite = TRUE)
      }
    )

    # ── Local combined builder (no ADP join needed) ───────────────────────────
    pr_make_combined <- function(rh, rp, mode) {
      if (is.null(rh) && is.null(rp)) return(NULL)
      if (!is.null(rp)) rp$positions <- classify_role(rp)
      val_col <- if (mode == "roto") "dollar_value" else "total_pts"

      build_row <- function(d, is_p = FALSE) {
        if (is.null(d) || !(val_col %in% names(d))) return(NULL)
        cols <- list(name = d$name, team = d$team,
                     positions = if (is_p && "positions" %in% names(d))
                                   d$positions
                                 else
                                   rep("", nrow(d)))
        if (mode == "roto") {
          cols$z_total      <- d$z_total_s
          cols$dollar_value <- d$dollar_value
        } else {
          cols$total_pts <- d$total_pts
          rate_col <- if (is_p) "pts_per_ip" else "pts_per_g"
          if (rate_col %in% names(d)) cols$pts_rate <- d[[rate_col]]
        }
        as.data.frame(cols, stringsAsFactors = FALSE)
      }

      comb <- rbind(build_row(rh, is_p = FALSE), build_row(rp, is_p = TRUE))
      if (is.null(comb) || nrow(comb) == 0) return(comb)
      comb[order(comb[[val_col]], decreasing = TRUE, na.last = TRUE), ]
    }

    # ── DT helpers ────────────────────────────────────────────────────────────
    pr_make_dt_opts <- list(
      dom         = "<'pag-dt-controls'lf>rtip",
      ordering    = TRUE,
      pageLength  = 30,
      lengthMenu  = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
      searchDelay = 200,
      scrollX     = TRUE,
      autoWidth   = FALSE
    )

    PR_H_LABEL_MAP <- c(
      rank = "#",
      name = "Player", team = "Team", pa = "PA", g = "G",
      z_total_s = "Z-Sum", dollar_value = "$",
      total_pts = "Total Pts", pts_per_g = "P/G",
      setNames(AUC_H_ROTO_LABS, AUC_H_ROTO_CATS),
      setNames(paste0("Z-", AUC_H_ROTO_LABS), paste0("z_", AUC_H_ROTO_CATS, "_s")),
      setNames(unname(AUC_H_PTS_STAT_LABS), names(AUC_H_PTS_STAT_LABS)),
      setNames(paste0(unname(AUC_H_PTS_STAT_LABS), " Pts"), paste0(names(AUC_H_PTS_STAT_LABS), "_pts"))
    )
    PR_P_LABEL_MAP <- c(
      rank = "#",
      name = "Player", team = "Team", positions = "Role", ip = "IP",
      z_total_s = "Z-Sum", dollar_value = "$",
      total_pts = "Total Pts", pts_per_ip = "P/IP",
      setNames(AUC_P_ROTO_LABS, AUC_P_ROTO_CATS),
      setNames(paste0("Z-", AUC_P_ROTO_LABS), paste0("z_", AUC_P_ROTO_CATS, "_s")),
      setNames(unname(AUC_P_PTS_STAT_LABS), names(AUC_P_PTS_STAT_LABS)),
      setNames(paste0(unname(AUC_P_PTS_STAT_LABS), " Pts"), paste0(names(AUC_P_PTS_STAT_LABS), "_pts"))
    )

    pr_datatable <- function(df, hide_last = TRUE, col_labels = NULL) {
      n_cols   <- ncol(df)
      has_rank <- identical(names(df)[1], "rank")
      name_col <- if (has_rank) 1L else 0L
      stat_end <- n_cols - if (hide_last) 3L else 2L
      col_defs <- list(
        list(className = "dt-left",   targets = name_col),
        list(className = "dt-center", targets = seq(name_col + 1L, stat_end))
      )
      if (has_rank) col_defs <- c(col_defs, list(
        list(className = "dt-center auc-rank-col", targets = 0L, width = "28px")
      ))
      if (hide_last) col_defs <- c(col_defs, list(
        list(targets = n_cols - 1L, visible = FALSE, searchable = TRUE)
      ))
      datatable(df, rownames = FALSE, filter = "none", selection = "none",
                colnames = if (!is.null(col_labels)) col_labels else names(df),
                options = c(pr_make_dt_opts, list(columnDefs = col_defs)),
                class = "pf-dt display nowrap")
    }

    add_rank_col_pr <- function(df, val_col) {
      if (is.null(df) || nrow(df) == 0 || !(val_col %in% names(df))) return(df)
      df  <- df[order(df[[val_col]], decreasing = TRUE, na.last = TRUE), , drop = FALSE]
      cbind(rank = seq_len(nrow(df)), df)
    }

    # Simple display prep (no ADP join since this is pure YTD)
    fmt_pr_h <- function(df, mode, selected_cats = NULL) {
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      if (mode == "roto") {
        cats     <- if (!is.null(selected_cats) && length(selected_cats) > 0) selected_cats else AUC_H_ROTO_CATS
        raw_cols <- intersect(cats, names(df))
        z_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
        keep <- intersect(c("name", "team", "pa", raw_cols, z_cols, "z_total_s", "dollar_value", "name_search"), names(df))
        df[order(df$dollar_value, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      } else {
        pts_pairs <- character(0)
        for (s in names(AUC_H_PTS_STAT_LABS)) {
          if (s %in% names(df))               pts_pairs <- c(pts_pairs, s)
          pc <- paste0(s, "_pts")
          if (pc %in% names(df))              pts_pairs <- c(pts_pairs, pc)
        }
        keep <- intersect(c("name", "team", "g", pts_pairs, "total_pts", "pts_per_g", "name_search"), names(df))
        df[order(df$total_pts, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      }
    }

    fmt_pr_p <- function(df, mode, selected_cats = NULL) {
      if (is.null(df) || nrow(df) == 0) return(NULL)
      if (!"role" %in% names(df)) df$role <- classify_role(df)
      df$positions   <- df$role
      df$name_search <- iconv(df$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      if (mode == "roto") {
        cats     <- if (!is.null(selected_cats) && length(selected_cats) > 0) selected_cats else AUC_P_ROTO_CATS
        raw_cols <- intersect(cats, names(df))
        if ("dollar_value" %in% names(df)) {
          keep <- intersect(c("name", "team", "positions", "ip", raw_cols,
                              "z_total_s", "dollar_value", "name_search"), names(df))
          df[order(df$dollar_value, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
        } else {
          keep <- intersect(c("name", "team", "positions", "ip", raw_cols, "name_search"), names(df))
          df[order(-df$ip, na.last = TRUE), keep, drop = FALSE]
        }
      } else {
        pts_pairs <- character(0)
        for (s in names(AUC_P_PTS_STAT_LABS)) {
          if (s %in% names(df))               pts_pairs <- c(pts_pairs, s)
          pc <- paste0(s, "_pts")
          if (pc %in% names(df))              pts_pairs <- c(pts_pairs, pc)
        }
        keep <- intersect(c("name", "team", "positions", "ip", pts_pairs, "total_pts", "pts_per_ip", "name_search"), names(df))
        df[order(df$total_pts, decreasing = TRUE, na.last = TRUE), keep, drop = FALSE]
      }
    }

    # ── Role filter helper ────────────────────────────────────────────────────
    filter_by_role_pr <- function(df, selected) {
      if (is.null(selected)) return(df)
      if (length(selected) == 0) return(df[0L, , drop = FALSE])
      if (!("role" %in% names(df))) return(df)
      df[df$role %in% selected, , drop = FALSE]
    }

    # Role quick-select observers
    observeEvent(input$p_role_all, {
      updateCheckboxGroupInput(session, "role_filter_p", selected = c("SP", "SP/RP", "RP"))
    }, ignoreInit = TRUE)
    observeEvent(input$p_role_deselect, {
      updateCheckboxGroupInput(session, "role_filter_p", selected = character(0))
    }, ignoreInit = TRUE)

    # ── Results UI ────────────────────────────────────────────────────────────
    output$results_ui <- renderUI({
      if (!is.null(last_error()) && nzchar(last_error())) {
        return(div(class = "pf-empty",
          tags$p("⚠️ Fetch failed:", last_error()),
          tags$p("Please wait a moment and click Calculate again.")))
      }
      if (!calculated()) {
        return(div(class = "pf-empty",
                   tags$p("Click Calculate to fetch season-to-date stats and generate rankings.")))
      }
      mode <- input$scoring_mode
      navset_pill(
        id = ns("results_tab"),

        # ── Hitters ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Hitters", value = "res_h",
          if (mode == "points") {
            navset_pill(
              id = ns("h_pts_tabs"),
              nav_panel("Expanded",   value = "h_exp",  DTOutput(ns("tbl_h_exp"),  width = "100%")),
              nav_panel("Simplified", value = "h_simp", DTOutput(ns("tbl_h_simp"), width = "100%"))
            )
          } else {
            DTOutput(ns("tbl_h"), width = "100%")
          }
        ),

        # ── Pitchers ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Pitchers", value = "res_p",
          div(
            class = "adp-pos-section",
            div(
              class = "adp-pos-header",
              tags$span(class = "pf-control-label", "Pitcher Type"),
              div(
                class = "adp-pos-btns",
                actionButton(ns("p_role_all"),      "Select All",   class = "btn btn-adp-pos-quick"),
                actionButton(ns("p_role_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
              )
            ),
            div(
              class = "adp-pos-checks",
              checkboxGroupInput(ns("role_filter_p"), label = NULL, inline = TRUE,
                                 choices  = c("SP", "SP/RP", "RP"),
                                 selected = if (mode == "roto") c("SP", "SP/RP") else c("SP", "SP/RP", "RP"))
            )
          ),
          if (mode == "points") {
            navset_pill(
              id = ns("p_pts_tabs"),
              nav_panel("Expanded",   value = "p_exp",  DTOutput(ns("tbl_p_exp"),  width = "100%")),
              nav_panel("Simplified", value = "p_simp", DTOutput(ns("tbl_p_simp"), width = "100%"))
            )
          } else {
            DTOutput(ns("tbl_p"), width = "100%")
          }
        ),

        # ── Combined ──────────────────────────────────────────────────────────
        nav_panel(
          title = "Combined", value = "res_comb",
          DTOutput(ns("tbl_combined"), width = "100%")
        )
      )
    })

    # ── Roto hitter table ─────────────────────────────────────────────────────
    output$tbl_h <- renderDT(server = TRUE, {
      req(result_h(), input$scoring_mode == "roto")
      cats <- roto_cats_h_sel()
      df   <- fmt_pr_h(result_h(), "roto", cats)
      if (is.null(df)) return(NULL)
      df <- add_rank_col_pr(df, "dollar_value")
      nm <- ifelse(names(df) %in% names(PR_H_LABEL_MAP),
                   PR_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      count_cols <- intersect(setdiff(cats, c(AUC_H_RATE_CATS, AUC_H_DISPLAY_ONLY)), names(df))
      rate_cols  <- intersect(AUC_H_RATE_CATS, names(df))
      z_s_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
      if ("pa"           %in% names(df)) dt <- DT::formatRound(dt, "pa",          digits = 0)
      if (length(count_cols) > 0)        dt <- DT::formatRound(dt, count_cols,    digits = 1)
      if (length(rate_cols)  > 0)        dt <- DT::formatRound(dt, rate_cols,     digits = 4)
      if (length(z_s_cols)   > 0)        dt <- DT::formatRound(dt, z_s_cols,      digits = 2)
      if ("z_total_s"    %in% names(df)) dt <- DT::formatRound(dt,    "z_total_s",    digits = 2)
      if ("dollar_value" %in% names(df)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      dt
    })

    # ── Roto pitcher table ────────────────────────────────────────────────────
    output$tbl_p <- renderDT(server = TRUE, {
      req(result_p(), input$scoring_mode == "roto")
      cats    <- roto_cats_p_sel()
      df_raw  <- result_p()
      df_raw$role <- classify_role(df_raw)
      df_raw  <- filter_by_role_pr(df_raw, input$role_filter_p)
      df <- fmt_pr_p(df_raw, "roto", cats)
      if (is.null(df)) return(NULL)
      df <- add_rank_col_pr(df, "dollar_value")
      nm <- ifelse(names(df) %in% names(PR_P_LABEL_MAP),
                   PR_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      count_cols <- intersect(setdiff(cats, AUC_P_RATE_CATS), names(df))
      count_cols <- intersect(count_cols, c("ip", "w", "k", "sv", "hd", "svhd"))
      z_s_cols   <- intersect(paste0("z_", cats, "_s"), names(df))
      if (length(count_cols) > 0)        dt <- DT::formatRound(dt, count_cols, digits = 1)
      if ("era"          %in% names(df)) dt <- DT::formatRound(dt, "era",       digits = 4)
      if ("whip"         %in% names(df)) dt <- DT::formatRound(dt, "whip",      digits = 3)
      if (length(z_s_cols)   > 0)        dt <- DT::formatRound(dt, z_s_cols,    digits = 2)
      if ("z_total_s"    %in% names(df)) dt <- DT::formatRound(dt, "z_total_s", digits = 2)
      if ("dollar_value" %in% names(df)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      dt
    })

    # ── Combined table ────────────────────────────────────────────────────────
    output$tbl_combined <- renderDT(server = TRUE, {
      rh   <- tryCatch(result_h(), error = function(e) NULL)
      rp   <- tryCatch(result_p(), error = function(e) NULL)
      comb <- pr_make_combined(rh, rp, input$scoring_mode)
      if (is.null(comb) || nrow(comb) == 0) return(NULL)
      val_col_comb <- if (input$scoring_mode == "roto") "dollar_value" else "total_pts"
      comb <- comb[order(comb[[val_col_comb]], decreasing = TRUE, na.last = TRUE), , drop = FALSE]
      comb$rank <- seq_len(nrow(comb))
      comb <- cbind(rank = comb$rank, comb[, setdiff(names(comb), "rank"), drop = FALSE])
      comb$name_search <- iconv(comb$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      n_cols <- ncol(comb)
      col_defs <- list(
        list(className = "dt-center auc-rank-col", targets = 0L, width = "28px"),
        list(className = "dt-left",   targets = 1L),
        list(className = "dt-center", targets = seq(2L, n_cols - 2L)),
        list(targets = n_cols - 1L, visible = FALSE, searchable = TRUE)
      )
      comb_label_map <- c(rank = "#", name = "Player", team = "Team",
                          positions = "Pos",
                          z_total = "Z-Sum", dollar_value = "$",
                          total_pts = "Total Pts", pts_rate = "Pts/G\u00b7IP")
      nm_c <- ifelse(names(comb) %in% names(comb_label_map),
                     comb_label_map[names(comb)], toupper(names(comb)))
      nm_c[length(nm_c)] <- ""
      dt <- datatable(comb, rownames = FALSE, filter = "none", selection = "none",
                      colnames = nm_c,
                      options = c(pr_make_dt_opts, list(columnDefs = col_defs)),
                      class = "pf-dt display nowrap") |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("positions" %in% names(comb))
        dt <- DT::formatStyle(dt, "positions", color = "#4a5a4f", fontSize = "0.82rem")
      if (input$scoring_mode == "roto") {
        if ("z_total"      %in% names(comb)) dt <- DT::formatRound(dt,    "z_total",      digits = 2)
        if ("dollar_value" %in% names(comb)) dt <- DT::formatCurrency(dt, "dollar_value", currency = "$", digits = 2)
      } else {
        if ("total_pts" %in% names(comb)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
        if ("pts_rate"  %in% names(comb)) dt <- DT::formatRound(dt, "pts_rate",  digits = 2)
      }
      dt
    })

    # ── Points hitter tables ──────────────────────────────────────────────────
    output$tbl_h_exp <- renderDT(server = TRUE, {
      req(result_h(), input$scoring_mode == "points")
      df <- fmt_pr_h(result_h(), "points")
      if (is.null(df)) return(NULL)
      df <- add_rank_col_pr(df, "total_pts")
      nm <- ifelse(names(df) %in% names(PR_H_LABEL_MAP),
                   PR_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("g" %in% names(df)) dt <- DT::formatRound(dt, "g", digits = 1)
      raw_cols <- intersect(names(AUC_H_PTS_STAT_LABS), names(df))
      pts_cols <- intersect(paste0(names(AUC_H_PTS_STAT_LABS), "_pts"), names(df))
      if (length(raw_cols) > 0) dt <- DT::formatRound(dt, raw_cols, digits = 1)
      if (length(pts_cols) > 0) dt <- DT::formatRound(dt, pts_cols, digits = 1)
      if ("total_pts" %in% names(df)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
      if ("pts_per_g" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_g", digits = 2)
      dt
    })

    output$tbl_h_simp <- renderDT(server = TRUE, {
      req(result_h(), input$scoring_mode == "points")
      rh <- result_h()
      if (is.null(rh) || nrow(rh) == 0) return(NULL)
      rh <- rh[order(rh$total_pts, decreasing = TRUE, na.last = TRUE), ]
      rh$name_search <- iconv(rh$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", "total_pts", "pts_per_g", "name_search"), names(rh))
      df <- rh[, keep, drop = FALSE]
      df <- add_rank_col_pr(df, "total_pts")
      nm <- ifelse(names(df) %in% names(PR_H_LABEL_MAP),
                   PR_H_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("total_pts" %in% names(df)) dt <- DT::formatRound(dt, "total_pts", digits = 1)
      if ("pts_per_g" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_g", digits = 2)
      dt
    })

    # ── Points pitcher tables ─────────────────────────────────────────────────
    output$tbl_p_exp <- renderDT(server = TRUE, {
      req(result_p(), input$scoring_mode == "points")
      df_raw <- result_p()
      df_raw$role <- classify_role(df_raw)
      df_raw <- filter_by_role_pr(df_raw, input$role_filter_p)
      df <- fmt_pr_p(df_raw, "points")
      if (is.null(df)) return(NULL)
      df <- add_rank_col_pr(df, "total_pts")
      nm <- ifelse(names(df) %in% names(PR_P_LABEL_MAP),
                   PR_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("ip" %in% names(df)) dt <- DT::formatRound(dt, "ip", digits = 1)
      raw_cols <- intersect(names(AUC_P_PTS_STAT_LABS), names(df))
      pts_cols <- intersect(paste0(names(AUC_P_PTS_STAT_LABS), "_pts"), names(df))
      if (length(raw_cols) > 0) dt <- DT::formatRound(dt, raw_cols, digits = 1)
      if (length(pts_cols) > 0) dt <- DT::formatRound(dt, pts_cols, digits = 1)
      if ("total_pts"  %in% names(df)) dt <- DT::formatRound(dt, "total_pts",  digits = 1)
      if ("pts_per_ip" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_ip", digits = 2)
      dt
    })

    output$tbl_p_simp <- renderDT(server = TRUE, {
      req(result_p(), input$scoring_mode == "points")
      rp_raw <- result_p()
      rp_raw$role <- classify_role(rp_raw)
      rp_raw <- filter_by_role_pr(rp_raw, input$role_filter_p)
      if (is.null(rp_raw) || nrow(rp_raw) == 0) return(NULL)
      rp_raw <- rp_raw[order(rp_raw$total_pts, decreasing = TRUE, na.last = TRUE), ]
      rp_raw$name_search <- iconv(rp_raw$name, from = "UTF-8", to = "ASCII//TRANSLIT")
      keep <- intersect(c("name", "team", "total_pts", "pts_per_ip", "name_search"), names(rp_raw))
      df <- rp_raw[, keep, drop = FALSE]
      df <- add_rank_col_pr(df, "total_pts")
      nm <- ifelse(names(df) %in% names(PR_P_LABEL_MAP),
                   PR_P_LABEL_MAP[names(df)], toupper(names(df)))
      nm[length(nm)] <- ""
      dt <- pr_datatable(df, col_labels = nm) |>
        DT::formatStyle("name", fontWeight = "650", color = "#172733")
      if ("total_pts"  %in% names(df)) dt <- DT::formatRound(dt, "total_pts",  digits = 1)
      if ("pts_per_ip" %in% names(df)) dt <- DT::formatRound(dt, "pts_per_ip", digits = 2)
      dt
    })

    invisible(NULL)
  })
}
