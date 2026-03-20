# ── mod_draft_lab.R ───────────────────────────────────────────────────────────
# Draft Lab: self-contained multi-tool draft prep module
# Phase 1: Setup tab (upload + status) + ADP sub-tab
# Phases 2-5: Projections, SP Skillz, Compare, Team (placeholders → to be built)

# ── Helpers ───────────────────────────────────────────────────────────────────

# Parse an ADP CSV upload (exported from NFBC ADP Scraper tab)
parse_adp_csv_upload <- function(path) {
  tryCatch({
    df <- utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
    required <- c("Rank", "Player", "Team", "Pos", "ADP", "Min", "Max")
    if (!all(required %in% names(df))) {
      stop("File is missing required columns: ",
           paste(setdiff(required, names(df)), collapse = ", "))
    }
    # Normalise to the same schema as parse_nfbc_adp_all()
    out <- data.frame(
      nfbc_rank    = suppressWarnings(as.integer(df[["Rank"]])),
      player_name  = trimws(as.character(df[["Player"]])),
      team         = trimws(as.character(df[["Team"]])),
      positions    = trimws(as.character(df[["Pos"]])),
      adp          = suppressWarnings(as.numeric(df[["ADP"]])),
      adp_min_pick = suppressWarnings(as.integer(df[["Min"]])),
      adp_max_pick = suppressWarnings(as.integer(df[["Max"]])),
      adp_picks    = if ("# Drafts" %in% names(df))
                       suppressWarnings(as.integer(df[["# Drafts"]])) else NA_integer_,
      stringsAsFactors = FALSE
    )
    out <- out[!is.na(out$adp) & nzchar(out$player_name), , drop = FALSE]
    out <- out[order(out$nfbc_rank, out$adp, na.last = TRUE), , drop = FALSE]
    rownames(out) <- NULL
    out
  }, error = function(e) e)
}

# ── Placeholder card UI ───────────────────────────────────────────────────────
dl_placeholder_ui <- function(title, icon_html, desc, phase) {
  div(
    class = "dl-placeholder",
    div(
      class = "dl-placeholder-inner",
      tags$span(class = "dl-placeholder-icon", HTML(icon_html)),
      h3(class = "dl-placeholder-title", title),
      p(class = "dl-placeholder-desc", desc),
      tags$span(class = "dl-placeholder-badge", paste("Phase", phase))
    )
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

draftLabUI <- function(id) {
  ns <- NS(id)

  div(
    class = "dl-page",

    # ── Page header ─────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "Draft Lab"),
      p(
        class = "pf-subtitle",
        "End-to-end draft preparation: ADP, projections, SP analysis, player comparison, and team tracking."
      )
    ),

    # ── How it works — always visible above tabs ────────────────────────────
    div(
      class = "dl-howto",
      div(class = "dl-howto-title", "How Draft Lab Works"),
      div(
        class = "dl-howto-steps",
        div(
          class = "dl-howto-step",
          div(class = "dl-howto-step-num", "1"),
          div(
            class = "dl-howto-step-body",
            tags$b("Load data"),
            p("Upload exports from NFBC ADP Scraper and Auction Value Calculator,
              or generate fresh data from the ADP and Projections tabs.")
          )
        ),
        div(
          class = "dl-howto-step",
          div(class = "dl-howto-step-num", "2"),
          div(
            class = "dl-howto-step-body",
            tags$b("Explore"),
            p("Browse ADP, review SP Skillz with draft context, and compare
              players head-to-head with hypothetical custom PA/IP projections.")
          )
        ),
        div(
          class = "dl-howto-step",
          div(class = "dl-howto-step-num", "3"),
          div(
            class = "dl-howto-step-body",
            tags$b("Build your team"),
            p("Import your draft picks into the Team tab and track your roster
              against NFBC 80th-percentile winning targets by category.")
          )
        )
      )
    ),

    # ── Sub-tab navigation ──────────────────────────────────────────────────
    navset_pill(
      id = ns("active_tab"),

      # ═══════════════════════════════════════════════════════════════════════
      # GROUP 1: Data Import / Setup
      # ═══════════════════════════════════════════════════════════════════════
      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x2699;")), "Setup"),
        value = "setup",

        div(
          class = "dl-setup-page",

          # Status row
          div(
            class = "dl-status-row",
            div(
              class = "dl-status-card",
              div(class = "dl-status-card-title", HTML("&#x1F4CB;  ADP Data")),
              uiOutput(ns("adp_status_badge")),
              uiOutput(ns("adp_status_detail"))
            ),
            div(
              class = "dl-status-card",
              div(class = "dl-status-card-title", HTML("&#x1F4CA;  Projections")),
              uiOutput(ns("proj_status_badge")),
              uiOutput(ns("proj_status_detail"))
            )
          ),

          # Upload row
          div(
            class = "dl-upload-row",
            div(
              class = "dl-upload-card",
              div(class = "dl-upload-card-title", "Upload ADP CSV"),
              p(
                class = "dl-upload-instructions",
                "Go to ", tags$b("NFBC ADP Scraper"), " \u2192 configure settings \u2192 click ",
                tags$b("Generate"), " \u2192 click ", tags$b("Export .csv"),
                ". Then upload that file here, or use the ", tags$b("ADP tab"),
                " to generate directly."
              ),
              fileInput(
                ns("adp_upload"),
                label       = NULL,
                accept      = ".csv",
                buttonLabel = "Choose ADP CSV",
                placeholder = "No file chosen"
              ),
              uiOutput(ns("adp_upload_error"))
            ),
            div(
              class = "dl-upload-card",
              div(class = "dl-upload-card-title", "Upload Projections CSV"),
              p(
                class = "dl-upload-instructions",
                "Go to ", tags$b("Auction Value Calculator"), " \u2192 configure projection sources ",
                "and scoring settings \u2192 click ", tags$b("Calculate"), " \u2192 click ",
                tags$b("Export .xlsx"), ". Then upload that file here, or use the ",
                tags$b("Projections tab"), " to build inline."
              ),
              p(class = "dl-upload-instructions",
                tags$em("Note: Projections upload is not yet supported. Use the Projections tab to generate values directly."))
            )
          )
        )
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CB;")), "ADP"),
        value = "adp_tab",
        div(class = "dl-adp-tab", adpUI(ns("adp")))
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CA;")), "Projections"),
        value = "proj_tab",
        div(class = "dl-proj-tab", aucValUI(ns("auc_val")))
      ),

      # ── Visual divider ──────────────────────────────────────────────────
      nav_item(div(class = "dl-tab-divider")),

      # ═══════════════════════════════════════════════════════════════════════
      # GROUP 2: Reference Data
      # ═══════════════════════════════════════════════════════════════════════
      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x26BE;")), "SP Skillz"),
        value = "spz_tab",
        div(class = "dl-spz-tab", spSkillzUI(ns("sp_skillz"), draft_mode = TRUE))
      ),

      # ── Visual divider ──────────────────────────────────────────────────
      nav_item(div(class = "dl-tab-divider")),

      # ═══════════════════════════════════════════════════════════════════════
      # GROUP 3: Draft Tools
      # ═══════════════════════════════════════════════════════════════════════
      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x2194;")), "Compare"),
        value = "compare_tab",
        div(style = "margin-top:16px;",
          dlCompareUI(ns("dl_compare"))
        )
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F3C6;")), "Team"),
        value = "team_tab",
        dl_placeholder_ui(
          title     = "Team Importer",
          icon_html = "&#x1F3C6;",
          desc      = "Import your draft picks, see your roster projections,
                       and compare team totals against NFBC 80th-percentile winning targets.",
          phase     = 5
        )
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

draftLabServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── Shared reactive state ───────────────────────────────────────────────
    dl_rv <- reactiveValues(
      adp_source  = "none",   # "none" | "generated" | "uploaded"
      adp_data    = NULL,     # master ADP data used by all tabs
      adp_label   = "",       # human-readable label (e.g. "Main Event • 12tm • 14d")
      proj_source = "none",
      proj_data   = NULL
    )

    # ── ADP sub-module ─────────────────────────────────────────────────────
    # adpServer returns reactive(rv$adp_data) — capture it
    adp_from_tab <- adpServer("adp")

    # ── Projections sub-module (Auc Val with ADP join) ────────────────────
    # Pass shared ADP data so tables get Pos + ADP columns
    proj_results <- aucValServer("auc_val", adp_data = reactive(dl_rv$adp_data))

    # ── SP Skillz sub-module (with ADP join) ──────────────────────────────
    spSkillzServer("sp_skillz", adp_data = reactive(dl_rv$adp_data), draft_mode = TRUE)

    # ── Compare sub-module (uses proj_results + dl_rv$adp_data) ───────────
    dlCompareServer("dl_compare",
      proj_results = proj_results,
      adp_data     = reactive(dl_rv$adp_data)
    )

    # When ADP tab generates data, sync to Draft Lab state
    observeEvent(adp_from_tab(), {
      dat <- adp_from_tab()
      if (!is.null(dat) && nrow(dat) > 0) {
        dl_rv$adp_data   <- dat
        dl_rv$adp_source <- "generated"
        dl_rv$adp_label  <- paste0(nrow(dat), " players")
      }
    }, ignoreNULL = TRUE)

    # ── ADP CSV upload (Setup tab) ─────────────────────────────────────────
    observeEvent(input$adp_upload, {
      req(input$adp_upload)
      result <- parse_adp_csv_upload(input$adp_upload$datapath)
      if (inherits(result, "error")) {
        dl_rv$adp_source <- "error"
        dl_rv$adp_label  <- conditionMessage(result)
        showNotification(paste("ADP upload error:", conditionMessage(result)),
                         type = "error", duration = 8)
      } else {
        dl_rv$adp_data   <- result
        dl_rv$adp_source <- "uploaded"
        dl_rv$adp_label  <- paste0(input$adp_upload$name, " \u2022 ", nrow(result), " players")
        showNotification(
          sprintf("ADP uploaded: %d players from %s", nrow(result), input$adp_upload$name),
          type = "message", duration = 4
        )
      }
    }, ignoreNULL = TRUE)

    # ── Status badge ────────────────────────────────────────────────────────
    output$adp_status_badge <- renderUI({
      switch(dl_rv$adp_source,
        "none"      = div(class = "dl-status-badge dl-status-badge--none",      "Not loaded"),
        "generated" = div(class = "dl-status-badge dl-status-badge--generated", "Generated"),
        "uploaded"  = div(class = "dl-status-badge dl-status-badge--uploaded",  "Uploaded"),
        "error"     = div(class = "dl-status-badge dl-status-badge--error",     "Error"),
        div(class = "dl-status-badge dl-status-badge--none", "Not loaded")
      )
    })

    output$adp_status_detail <- renderUI({
      if (nzchar(dl_rv$adp_label)) {
        p(class = "dl-status-detail", dl_rv$adp_label)
      } else {
        p(class = "dl-status-detail", "Use the ADP tab or upload a CSV below.")
      }
    })

    output$adp_upload_error <- renderUI({
      if (dl_rv$adp_source == "error") {
        div(class = "dl-upload-error", dl_rv$adp_label)
      }
    })

    # ── Projections status badge ─────────────────────────────────────────
    proj_loaded <- reactive({
      rh <- tryCatch(proj_results$result_h(), error = function(e) NULL)
      rp <- tryCatch(proj_results$result_p(), error = function(e) NULL)
      !is.null(rh) || !is.null(rp)
    })

    output$proj_status_badge <- renderUI({
      if (proj_loaded()) {
        div(class = "dl-status-badge dl-status-badge--generated", "Calculated")
      } else {
        div(class = "dl-status-badge dl-status-badge--none", "Not loaded")
      }
    })

    output$proj_status_detail <- renderUI({
      if (proj_loaded()) {
        rh <- tryCatch(proj_results$result_h(), error = function(e) NULL)
        rp <- tryCatch(proj_results$result_p(), error = function(e) NULL)
        n_h <- if (!is.null(rh)) nrow(rh) else 0L
        n_p <- if (!is.null(rp)) nrow(rp) else 0L
        p(class = "dl-status-detail",
          sprintf("%d hitters \u2022 %d pitchers", n_h, n_p))
      } else {
        p(class = "dl-status-detail", "Use the Projections tab to calculate values.")
      }
    })

    # Return shared data for potential future cross-module use
    list(
      adp_data  = reactive(dl_rv$adp_data),
      result_h  = proj_results$result_h,
      result_p  = proj_results$result_p
    )
  })
}
