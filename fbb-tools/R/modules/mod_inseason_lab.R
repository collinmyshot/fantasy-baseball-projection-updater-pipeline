# ── mod_inseason_lab.R ────────────────────────────────────────────────────────
# RoS Projection Values: RoS projection aggregator + value calculator + player compare.
# Mirrors Draft Lab structure minus ADP, SP Ranking Summary, and Team Importer.
# Uses ROS_FG_API_TYPES for all FanGraphs fetches.
# Player Rater is a separate top-level nav panel (see app.R / mod_player_rater.R).

# ── Module UI ─────────────────────────────────────────────────────────────────

inseasonLabUI <- function(id) {
  ns <- NS(id)

  div(
    class = "dl-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "In-Season Tools"),
      p(
        class = "pf-subtitle",
        "Rest-of-season projections, season-to-date player ratings, and side-by-side player comparison."
      )
    ),

    # ── Sub-tab navigation ────────────────────────────────────────────────────
    navset_pill(
      id = ns("active_tab"),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4CA;")), "Projection Aggregator"),
        value = "proj_agg_tab",
        div(class = "dl-proj-tab", aucValAggUI(ns("auc_val"), label_suffix = "(RoS)", allow_upload = FALSE))
      ),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x1F4B0;")), "Value Calculator"),
        value = "proj_val_tab",
        div(class = "dl-proj-tab", aucValCalcUI(ns("auc_val")))
      ),

      nav_item(div(class = "dl-tab-divider")),

      nav_panel(
        title = tagList(tags$span(class = "dl-tab-icon", HTML("&#x2194;")), "Compare"),
        value = "compare_tab",
        div(style = "margin-top:16px;",
          dlCompareUI(ns("dl_compare"))
        )
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

inseasonLabServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    # Projections — RoS API types, no ADP feed
    proj_results <- aucValServer("auc_val",
                                 adp_data      = reactive(NULL),
                                 context       = "draftlab",
                                 api_types_map = ROS_FG_API_TYPES)

    # Compare — no ADP data, no SP rank overview
    dlCompareServer("dl_compare",
                    proj_results = proj_results,
                    adp_data     = reactive(NULL))

    invisible(NULL)
  })
}
