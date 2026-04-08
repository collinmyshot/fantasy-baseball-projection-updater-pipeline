# ── Tool Catalog ──────────────────────────────────────────────────────────────
#
# Single source of truth for every tool card shown on the home page.
# status: "available" = show an Open button; "soon" = show Coming Soon badge.
# nav_value must match the value= argument of the corresponding nav_panel()
# in app.R.

TOOL_CATALOG <- list(

  list(
    category  = "Draft Tools",
    cat_id    = "draft",
    emoji     = "\U0001F3AF",
    cat_desc  = "Projection and valuation tools for pre-draft preparation",
    tools = list(
      list(
        nav_value = "proj_agg",
        name      = "Projection Aggregator",
        desc      = "Weighted multi-system aggregation with z-scores and dollar values",
        status    = "available"
      ),
      list(
        nav_value = "adp",
        name      = "NFBC ADP Scraper",
        desc      = "Live NFBC average draft position with position filters and snake draft pick calculator",
        status    = "available"
      ),
      list(
        nav_value = "auction",
        name      = "Auction Value Calculator",
        desc      = "Roto z-score dollar values or Ottoneu FG points totals from any projection blend",
        status    = "available"
      ),
      list(
        nav_value = "draft_helper",
        name      = "Draft Lab",
        desc      = "ADP + projections + SP Skillz + head-to-head player comparison in one integrated workspace",
        status    = "available"
      )
    )
  ),

  list(
    category  = "In-Season Tools",
    cat_id    = "inseason",
    emoji     = "\U0001F4CA",
    cat_desc  = "Real-time tools for managing and optimizing your roster",
    tools = list(
      list(
        nav_value = "sp_skillz",
        name      = "SP Skillz",
        desc      = "Pitcher evaluation via weighted metrics and reliability scoring",
        status    = "available"
      ),
      list(
        nav_value = "sp_stream",
        name      = "SP Streamonator",
        desc      = "Streaming SP recommendations based on schedule and matchup data",
        status    = "soon"
      ),
      list(
        nav_value = "hit_stream",
        name      = "Hitter Streamonator",
        desc      = "Streaming hitter recommendations using park factors and platoon splits",
        status    = "soon"
      )
    )
  ),

  list(
    category  = "Park Factors",
    cat_id    = "park",
    emoji     = "\U0001F3DF\uFE0F",
    cat_desc  = "Stadium-level analysis to inform fantasy decisions",
    tools = list(
      list(
        nav_value = "park_factors",
        name      = "Park Factor Tool",
        desc      = "Hierarchical park factor model with fantasy-weighted BACON, HR, and XBH components",
        status    = "available"
      )
    )
  )
)

# ── UI Helpers ────────────────────────────────────────────────────────────────

tool_row_ui <- function(ns, tool) {
  is_available <- identical(tool$status, "available")
  btn_id <- paste0("open_", tool$nav_value)

  div(
    class = paste("tool-row", if (!is_available) "tool-row-soon"),
    div(
      class = "tool-row-info",
      div(class = "tool-row-name", tool$name),
      div(class = "tool-row-desc", tool$desc)
    ),
    div(
      class = "tool-row-action",
      if (is_available) {
        actionButton(
          ns(btn_id),
          "Open \u2192",
          class = "btn btn-tool-open"
        )
      } else {
        tags$span(class = "badge-soon", "Coming Soon")
      }
    )
  )
}

category_card_ui <- function(ns, cat) {
  card(
    class = paste0("cat-card cat-card-", cat$cat_id),
    full_screen = FALSE,
    card_header(
      class = "cat-card-header",
      div(
        class = "cat-header-inner",
        tags$span(class = "cat-emoji", cat$emoji),
        div(
          h3(class = "cat-name", cat$category),
          p(class = "cat-desc", cat$cat_desc)
        )
      )
    ),
    card_body(
      class = "cat-card-body",
      tagList(lapply(cat$tools, tool_row_ui, ns = ns))
    )
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

homeUI <- function(id) {
  ns <- NS(id)

  tagList(
    # ── Hero ──────────────────────────────────────────────────────────────────
    div(
      class = "home-hero",
      tags$img(src = "logo_collinmyshot.png", class = "hero-logo", alt = "Collinmyshot Fantasy Baseball")
    ),

    # ── Tool Cards ────────────────────────────────────────────────────────────
    div(
      class = "home-tools-section",
      p(class = "tools-section-label", "Available Tools"),
      do.call(
        layout_columns,
        c(
          list(col_widths = c(4, 4, 4), gap = "20px"),
          lapply(TOOL_CATALOG, category_card_ui, ns = ns)
        )
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

homeServer <- function(id, main_nav_id, root_session) {
  moduleServer(id, function(input, output, session) {

    # Collect nav_values for all tools marked as available
    available_nav_values <- unlist(lapply(TOOL_CATALOG, function(cat) {
      vapply(cat$tools, function(t) {
        if (identical(t$status, "available")) t$nav_value else NA_character_
      }, character(1))
    }))
    available_nav_values <- available_nav_values[!is.na(available_nav_values)]

    # Wire each Open button to nav_select on the root navbar
    lapply(available_nav_values, function(nav_val) {
      btn_id <- paste0("open_", nav_val)
      observeEvent(input[[btn_id]], {
        nav_select(main_nav_id, selected = nav_val, session = root_session)
        root_session$sendCustomMessage("collapse_navbar", list())
      }, ignoreInit = TRUE)
    })
  })
}
