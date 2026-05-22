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
      tags$ol(
        class = "dl-howto-list",
        tags$li(tags$b("ADP"), " \u2014 Scrape or upload NFBC draft-pick data"),
        tags$li(tags$b("Projection Aggregator"), " \u2014 Choose systems & weights, click Fetch Projections to build raw stat aggregate"),
        tags$li(tags$b("Auction Value Calculator"), " \u2014 Set roto or points scoring; player rankings auto-update (roto: $ values; points: total pts + Pts/G\u00b7IP)"),
        tags$li(tags$b("SP Ranking Summary"), " \u2014 Skills-based SP tiers for draft context"),
        tags$li(tags$b("Team"), " \u2014 Import your picks; track projected stats vs. 80th-pct benchmarks (\u00b15% color coding)")
      )
    ),

    # ── Sub-tab navigation ──────────────────────────────────────────────────
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CB;")), "ADP"),
        value = "adp_tab",
        div(class = "dl-adp-tab", adpUI(ns("adp")))
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CA;")), "Projection Aggregator"),
        value = "proj_agg_tab",
        div(class = "dl-proj-tab", aucValAggUI(ns("auc_val")))
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4B0;")), "Auction Value Calculator"),
        value = "proj_val_tab",
        div(class = "dl-proj-tab", aucValCalcUI(ns("auc_val")))
      ),

      # ── Visual divider ──────────────────────────────────────────────────
      nav_item(div(class = "dl-tab-divider")),

      # ═══════════════════════════════════════════════════════════════════════
      # GROUP 2: Reference Data
      # ═══════════════════════════════════════════════════════════════════════
      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CA;")), "SP Ranking Summary"),
        value = "spz_tab",
        div(class = "dl-spz-tab", spRankOverviewUI(ns("sp_rank")))
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
        div(style = "margin-top:16px;",
          teamImporterUI(ns("team_importer"))
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
    proj_results <- aucValServer("auc_val", adp_data = reactive(dl_rv$adp_data),
                                 context = "draftlab")

    # ── SP Rank Overview sub-module ────────────────────────────────────────
    spr_rv <- spRankOverviewServer("sp_rank", adp_data = reactive(dl_rv$adp_data))

    # ── Compare sub-module (uses proj_results + dl_rv$adp_data + spr_rv) ──
    dlCompareServer("dl_compare",
      proj_results = proj_results,
      adp_data     = reactive(dl_rv$adp_data),
      spr_data     = spr_rv$display_df
    )

    # ── Team Importer sub-module ───────────────────────────────────────────
    ti_rv <- teamImporterServer("team_importer",
      proj_results = proj_results,
      adp_data     = reactive(dl_rv$adp_data)
    )

    # "Go to Projections" button inside Team tab navigates to the proj_tab
    observeEvent(ti_rv$go_to_proj(), {
      updateNavsetPill(session, "active_tab", selected = "proj_val_tab")
    }, ignoreNULL = TRUE, ignoreInit = TRUE)

    # When ADP tab generates data, sync to Draft Lab state
    observeEvent(adp_from_tab(), {
      dat <- adp_from_tab()
      if (!is.null(dat) && nrow(dat) > 0) {
        dl_rv$adp_data   <- dat
        dl_rv$adp_source <- "generated"
        dl_rv$adp_label  <- paste0(nrow(dat), " players")
      }
    }, ignoreNULL = TRUE)


    # Return shared data for potential future cross-module use
    list(
      adp_data  = reactive(dl_rv$adp_data),
      result_h  = proj_results$result_h,
      result_p  = proj_results$result_p
    )
  })
}
