suppressPackageStartupMessages({
  library(DT)
  library(dplyr)
})

# ── Constants ─────────────────────────────────────────────────────────────────

PF_FILES <- list(
  overall = "data/park_factors/park_factors_savant_style_clean_2026.csv",
  `1H`    = "data/park_factors/park_factors_savant_style_clean_2026_1H.csv",
  `2H`    = "data/park_factors/park_factors_savant_style_clean_2026_2H.csv"
)

MIN_SAMPLE_CHOICES <- c(
  "1-5 seasons  (< 25k BBE)"  = "0",
  "5+ seasons   (>= 25k BBE)" = "25000",
  "10+ seasons  (>= 50k BBE)" = "50000"
)

# PF color gradient: navy (pitcher-friendly) → white (neutral 100) → orange (hitter-friendly)
# Uses createdRow JS callback (same pattern as SP Skillz) for true per-cell continuous coloring.
# Three anchor colors: navy [31,53,86] at PF≤85, white [255,255,255] at PF=100, orange [183,115,67] at PF≥120.

# ── Helpers ───────────────────────────────────────────────────────────────────

load_pf_data <- function(half, min_bbe) {
  path <- PF_FILES[[half]]
  if (!file.exists(path)) return(NULL)

  dat <- read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)

  if (min_bbe > 0) {
    dat <- dat[dat[["Total BBE"]] >= min_bbe, ]
  }

  # Rename for compact display
  names(dat)[names(dat) == "Overall Park Factor"] <- "Overall"
  names(dat)[names(dat) == "BACON Park Factor"]   <- "BACON"
  names(dat)[names(dat) == "HR Park Factor"]      <- "HR"
  names(dat)[names(dat) == "Total BBE"]           <- "BBE"
  names(dat)[names(dat) == "Rank"]                <- "RK"

  # Re-rank after filtering
  dat$RK <- seq_len(nrow(dat))

  dat
}

# JS for createdRow: colors each PF cell (cols 4, 5, 6) continuously.
# Mirrors SP Skillz pattern exactly: two-segment linear interpolation, luminance-based text flip.
PF_CREATED_ROW_JS <- "
function(row, data, index) {
  var cols = [4, 5, 6];
  var cMin = 85, cMid = 100, cMax = 125;  // cMax capped at 125 for color purposes; Coors displays true value
  var c1 = [31,53,86], c2 = [255,255,255], c3 = [183,115,67];
  for (var j = 0; j < cols.length; j++) {
    var val = parseFloat(data[cols[j]]);
    if (isNaN(val)) continue;
    var pct;
    if (val <= cMin)       pct = 0;
    else if (val >= cMax)  pct = 1;
    else if (val <= cMid)  pct = 0.5 * (val - cMin) / (cMid - cMin);
    else                   pct = 0.5 + 0.5 * (val - cMid) / (cMax - cMid);
    var ca, cb, t;
    if (pct <= 0.5) { t = pct * 2;        ca = c1; cb = c2; }
    else            { t = (pct - 0.5)*2;  ca = c2; cb = c3; }
    var r = Math.round(ca[0] + (cb[0]-ca[0])*t);
    var g = Math.round(ca[1] + (cb[1]-ca[1])*t);
    var b = Math.round(ca[2] + (cb[2]-ca[2])*t);
    var lum = 0.2126*(r/255) + 0.7152*(g/255) + 0.0722*(b/255);
    var txt = lum < 0.45 ? '#ffffff' : '#172733';
    $('td:eq(' + cols[j] + ')', row).css({
      'background-color': 'rgb('+r+','+g+','+b+')',
      'color': txt,
      'font-weight': '700'
    });
  }
}
"

apply_pf_style <- function(dt) {
  dt |>
    formatRound(c("Overall", "BACON", "HR"), digits = 1) |>
    formatStyle(
      "RK",
      color      = "#8a9a8f",
      fontWeight = "400",
      fontSize   = "0.8rem",
      textAlign  = "right"
    ) |>
    formatStyle(
      "Team",
      fontWeight = "650",
      color      = "#172733"
    ) |>
    formatStyle(
      "Park",
      color    = "#4a5a4f",
      fontSize = "0.875rem"
    ) |>
    formatStyle(
      "Years",
      color     = "#8a9a8f",
      fontSize  = "0.78rem",
      textAlign = "center"
    ) |>
    formatStyle(
      "BBE",
      color     = "#8a9a8f",
      fontSize  = "0.82rem",
      textAlign = "right"
    ) |>
    formatCurrency("BBE", currency = "", digits = 0, mark = ",")
}

pf_legend_html <- function() {
  tags$div(
    class = "pf-legend",
    tags$span(class = "pf-legend-label pf-legend-left",  "\u2190 Pitcher-friendly"),
    tags$div(
      class = "pf-legend-bar",
      style = paste0(
        "background: linear-gradient(to right, ",
        "rgb(31,53,86), rgb(255,255,255), rgb(183,115,67)",
        ");"
      )
    ),
    tags$span(class = "pf-legend-label pf-legend-right", "Hitter-friendly \u2192")
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

parkFactorsUI <- function(id) {
  ns <- NS(id)

  div(
    class = "pf-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(
        class = "pf-header-left",
        div(class = "pf-header-eyebrow", "Collinmyshot"),
        h1(class = "pf-title", "\U0001F3DF\uFE0F  Park Factors"),
        p(
          class = "pf-subtitle",
          "Fantasy-weighted hierarchical park factor model \u2014 BACON, HR, and XBH components.",
          tags$br(),
          "Values indexed to 100 (neutral). Above 100 \u2192 hitter-friendly. Below 100 \u2192 pitcher-friendly."
        )
      )
    ),

    # ── Controls + legend ─────────────────────────────────────────────────────
    div(
      class = "pf-controls-row",

      # Half toggle
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Season half"),
        div(
          class = "pf-toggle",
          radioButtons(
            ns("half"),
            label    = NULL,
            choices  = c("Overall" = "overall", "1st Half" = "1H", "2nd Half" = "2H"),
            selected = "overall",
            inline   = TRUE
          )
        )
      ),

      # Min sample
      div(
        class = "pf-control-group",
        tags$span(class = "pf-control-label", "Min. sample"),
        selectInput(
          ns("min_bbe"),
          label   = NULL,
          choices = MIN_SAMPLE_CHOICES,
          selected = "0",
          width   = "210px"
        )
      ),

      # Spacer + legend
      div(class = "pf-controls-spacer"),
      pf_legend_html()
    ),

    # ── Table ─────────────────────────────────────────────────────────────────
    div(
      class = "pf-table-wrap",
      DTOutput(ns("table"), width = "100%")
    ),

    # ── Footer note ───────────────────────────────────────────────────────────
    div(
      class = "pf-footer",
      tags$span(class = "pf-footer-text",
        tags$strong("Last updated: May 14, 2026."), " ",
        "Model: hierarchical random effects on Statcast batted-ball events (BBE), 2015\u20132025 (2026 refresh, incl. new park eras). ",
        "Weighted blend: BACON 45%, HR 35%, XBH 20%. ",
        "Park eras adjusted for relocations, dimension changes, and structural changes."
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

parkFactorsServer <- function(id) {
  moduleServer(id, function(input, output, session) {

    pf_data <- reactive({
      req(input$half, input$min_bbe)
      load_pf_data(
        half    = input$half,
        min_bbe = as.integer(input$min_bbe)
      )
    })

    output$table <- renderDT({
      dat <- req(pf_data())

      datatable(
        dat,
        rownames  = FALSE,
        filter    = "none",
        selection = "none",
        options   = list(
          dom        = "t",
          ordering   = TRUE,
          pageLength = nrow(dat),
          scrollX    = FALSE,
          order      = list(list(4, "desc")),  # default sort: Overall desc (col index 4)
          createdRow = JS(PF_CREATED_ROW_JS),
          columnDefs = list(
            list(className = "dt-right",  targets = 0),        # RK
            list(className = "dt-left",   targets = c(1, 2)),  # Team, Park
            list(className = "dt-center", targets = 3),        # Years
            list(className = "dt-center", targets = c(4,5,6)), # PF cols
            list(className = "dt-right",  targets = 7),        # BBE
            list(width = "36px",  targets = 0),   # RK narrow
            list(width = "130px", targets = 1),   # Team
            list(width = "220px", targets = 2),   # Park
            list(width = "75px",  targets = 3),   # Years
            list(width = "90px",  targets = c(4,5,6)),  # PF cols
            list(width = "85px",  targets = 7)    # BBE
          )
        ),
        class = "pf-dt display nowrap"
      ) |>
        apply_pf_style()
    })
  })
}
