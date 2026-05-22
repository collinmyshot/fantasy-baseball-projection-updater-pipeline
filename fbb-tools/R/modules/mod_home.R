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
    cat_desc  = "Rest-of-season projections, player evaluation, and team rankings",
    tools = list(
      list(
        nav_value = "inseason_lab",
        name      = "RoS Projection Values",
        desc      = "Rest-of-season projections: aggregate across systems, calculate values, and compare players",
        status    = "available"
      ),
      list(
        nav_value = "player_rater",
        name      = "Player Rater",
        desc      = "Season-to-date player performance ratings across counting stats and rate metrics",
        status    = "available"
      ),
      list(
        nav_value = "team_rater",
        name      = "Team Rater",
        desc      = "MLB team offense ranked by composite z-score across HR, R, BB%, K%, and wOBA",
        status    = "available"
      )
    )
  ),

  list(
    category  = "Leaderboards",
    cat_id    = "leaderboards",
    emoji     = "\U0001F3C6",
    cat_desc  = "Pitcher and hitter leaderboards with park context",
    tools = list(
      list(
        nav_value = "sp_skillz",
        name      = "SP Skillz",
        desc      = "Starting pitcher quality index via weighted metrics, IP paradigms, and reliability scoring",
        status    = "available"
      ),
      list(
        nav_value = "rp_skillz",
        name      = "RP Skillz",
        desc      = "Reliever quality index \u2014 velo, Stuff+, K%, CSW%, Shutdowns\u2013Meltdowns, and gmLI",
        status    = "available"
      ),
      list(
        nav_value = "gsm",
        name      = "Good Start Metric",
        desc      = "Per-start quality score (0\u20134): IP, K rate, ER (sliding), and WHIP. GSM% = starts scoring 3+.",
        status    = "available"
      ),
      list(
        nav_value = "hit_dashboard",
        name      = "Hitter Dashboard",
        desc      = "Statcast-powered hitter leaderboard: Luck, Power, and Plate Skills with xBA, Barrel%, and more",
        status    = "available"
      ),
      list(
        nav_value = "pit_dashboard",
        name      = "Pitcher Dashboard",
        desc      = "Statcast + FanGraphs + PitchingBot pitcher leaderboard: K%, command, ERA estimators, and Stuff+",
        status    = "available"
      ),
      list(
        nav_value = "park_factors",
        name      = "Park Factors",
        desc      = "Hierarchical park factor model with fantasy-weighted BACON, HR, and XBH components",
        status    = "available"
      ),
      list(
        nav_value = "abrl_leaderboard",
        name      = "Adjusted Barrels (aBrl)",
        desc      = "Season-adjusted barrel rates with player trends and % lost to park/environment",
        status    = "available"
      )
    )
  ),

  list(
    category  = "Streamonators",
    cat_id    = "streamonators",
    emoji     = "\U0001F30A",
    cat_desc  = "Weekly streaming recommendations powered by schedule, skill, and matchup data",
    tools = list(
      list(
        nav_value = "sp_stream",
        name      = "SP Streamonator",
        desc      = "Streaming SP recommendations based on schedule, SP Skillz, park factors, and opponent quality",
        status    = "available"
      ),
      list(
        nav_value = "hit_stream",
        name      = "Hitter Streamonator",
        desc      = "Streaming hitter recommendations using park factors and platoon splits",
        status    = "available"
      ),
      list(
        nav_value = "sp_outlook",
        name      = "SP Outlook",
        desc      = "Forward-looking SP start matrix \u2014 confirmed probables and projected starts ranked by SP Skillz",
        status    = "available"
      )
    )
  ),

  list(
    category  = "Methodology",
    cat_id    = "methodology",
    emoji     = "\U0001F4D6",
    cat_desc  = "How the models work: detailed write-ups on scoring, weights, and validation",
    tools = list(
      list(
        nav_value = "methodology_streamonator_appendix",
        name      = "Streamonator: Weights & Thresholds",
        desc      = "Weight derivation and threshold validation for the SP and Hitter Streamonators",
        status    = "available"
      ),
      list(
        nav_value = "methodology_sp_skillz",
        name      = "SP Skillz",
        desc      = "SP Skillz scoring methodology, metric weights, and reliability framework",
        status    = "available"
      ),
      list(
        nav_value = "methodology_park_factors",
        name      = "Park Factors",
        desc      = "Hierarchical Bayesian park factor model: estimation, priors, and fantasy weighting",
        status    = "available"
      ),
      list(
        nav_value = "methodology_team_rater",
        name      = "Team Rater",
        desc      = "Team offensive strength composite: z-score methodology and component weights",
        status    = "available"
      ),
      list(
        nav_value = "methodology_hitter",
        name      = "Hitter Valuation",
        desc      = "Projection aggregation, z-score normalization, and dollar value conversion",
        status    = "available"
      )
    )
  ),

  list(
    category  = "Research",
    cat_id    = "research",
    emoji     = "\U0001F52C",
    cat_desc  = "Interactive explorations of Statcast data, pitch modeling, and park effects",
    tools = list(
      list(
        nav_value = "research_hr_ev",
        name      = "HR\u2013EV Relationship",
        desc      = "How exit velocity relates to home run probability across launch angles",
        status    = "available"
      ),
      list(
        nav_value = "research_bat_speed",
        name      = "Bat Speed & EV",
        desc      = "Bat speed as a predictor of exit velocity and power output",
        status    = "available"
      ),
      list(
        nav_value = "research_csw",
        name      = "Pitcher K% Prediction",
        desc      = "CSW%, SwStr%, and other correlates of pitcher strikeout rate",
        status    = "available"
      ),
      list(
        nav_value = "research_hitter_whiff",
        name      = "Hitter K% Prediction",
        desc      = "Swing-and-miss drivers of hitter strikeout rate from a batter\u2019s perspective",
        status    = "available"
      ),
      list(
        nav_value = "research_park_hr_barrel",
        name      = "Park HR/Barrel",
        desc      = "How park dimensions and environment affect home run rate per barrel",
        status    = "available"
      ),
      list(
        nav_value = "research_adj_barrel",
        name      = "Adjusted Barrels (aBrl)",
        desc      = "The aBrl research article: why standard barrels mislead and how the adjustment works",
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

category_accordion_panel <- function(ns, cat) {
  accordion_panel(
    title = div(
      class = "cat-header-inner",
      tags$span(class = "cat-emoji", cat$emoji),
      div(
        h3(class = "cat-name", cat$category),
        p(class = "cat-desc", cat$cat_desc)
      )
    ),
    value = cat$cat_id,
    div(
      class = paste0("cat-card-body cat-card-body-", cat$cat_id),
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
      div(
        class = "hero-logo-wrap",
        tags$img(src = "logo_collinmyshot.png", class = "hero-logo", alt = "Collinmyshot Fantasy Baseball"),
        tags$span(class = "hero-est", "Est. 2026")
      ),
      p(
        class = "hero-tagline",
        "Player Valuation",
        tags$span(class = "hero-dot", "\u00b7"),
        "Pitcher Skillz",
        tags$span(class = "hero-dot", "\u00b7"),
        "Leaderboards",
        tags$span(class = "hero-dot", "\u00b7"),
        "Streamonators"
      )
    ),

    # ── Tool Accordions ──────────────────────────────────────────────────────
    div(
      class = "home-tools-section",
      p(class = "tools-section-label", "Available Tools"),
      layout_columns(
        col_widths = c(6, 6),
        gap = "20px",

        # Left column: Draft, Leaderboards, Methodology
        div(
          class = "home-accordion-col",
          do.call(
            accordion,
            c(
              list(id = ns("acc_left"), open = FALSE, multiple = TRUE,
                   class = "home-accordion"),
              lapply(
                TOOL_CATALOG[vapply(TOOL_CATALOG, function(x)
                  x$cat_id %in% c("draft", "leaderboards", "methodology"), logical(1))],
                category_accordion_panel, ns = ns
              )
            )
          )
        ),

        # Right column: In-Season, Streamonators, Research
        div(
          class = "home-accordion-col",
          do.call(
            accordion,
            c(
              list(id = ns("acc_right"), open = FALSE, multiple = TRUE,
                   class = "home-accordion"),
              lapply(
                TOOL_CATALOG[vapply(TOOL_CATALOG, function(x)
                  x$cat_id %in% c("inseason", "streamonators", "research"), logical(1))],
                category_accordion_panel, ns = ns
              )
            )
          )
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
