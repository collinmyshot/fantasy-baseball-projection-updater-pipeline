# ── mod_sp_rank_overview.R ────────────────────────────────────────────────────
# Draft Lab "SP Rank Overview" tab
# Combines: ADP (from Draft Lab shared state), SP Skillz rank, Eno Sarris rank,
# PitcherList rank — all joined by player name.

# ── Constants ─────────────────────────────────────────────────────────────────

SPR_ENO_URL <- paste0(
  "https://docs.google.com/spreadsheets/d/",
  "1daR9RNic3GcfDb6FLsm2OZRBS8VkqucOqHSnIS7ru5c",
  "/export?format=csv&gid=2070453570"
)
SPR_PL_URL  <- "https://pitcherlist.com/top-100-starting-pitchers-for-2026-fantasy-baseball-3-13-update/"
SPR_SPZ_FILE <- "data/processed/2025_sp_skillz_scores_2026_plus_model.csv"

SPR_ENO_LINK <- "https://www.nytimes.com/athletic/7010771/2026/02/03/mlb-starting-pitcher-rankings-2026-fantasy-baseball/"
SPR_PL_LINK  <- "https://pitcherlist.com/top-100-starting-pitchers-for-2026-fantasy-baseball-3-13-update/"
SPR_ADP_LINK <- "https://nfc.shgn.com/adp/baseball"
SPR_APP_LINK <- "https://collinmyshot.shinyapps.io/fbb-tools/"

# ── Helpers ───────────────────────────────────────────────────────────────────

# Normalize names for joining — delegates to shared player_nk() (utils_names.R),
# which folds diacritics, strips punctuation, and applies override aliases.
spr_nk <- player_nk

# Load SP Skillz ranks from local file (static for this session)
spr_load_spz <- function() {
  tryCatch({
    df <- read.csv(SPR_SPZ_FILE, stringsAsFactors = FALSE)
    if ("sp_skillz_starter_pool_flag" %in% names(df))
      df <- df[!is.na(df$sp_skillz_starter_pool_flag) & df$sp_skillz_starter_pool_flag == 1, ]
    keep <- intersect(c("player_name", "sp_skillz_rank_stabilized"), names(df))
    df <- df[, keep, drop = FALSE]
    df$nk <- spr_nk(df$player_name)
    df
  }, error = function(e) NULL)
}

# Fetch Eno Sarris ranks from Google Sheet CSV export
spr_fetch_eno <- function() {
  tryCatch({
    con <- url(SPR_ENO_URL)
    on.exit(try(close(con), silent = TRUE))
    df  <- read.csv(con, stringsAsFactors = FALSE, check.names = FALSE)

    # Detect name column: character column whose values look like names
    name_col <- NA_character_
    for (cn in names(df)) {
      vals <- df[[cn]]
      if (is.character(vals) && mean(grepl(" ", trimws(vals)), na.rm = TRUE) > 0.4) {
        name_col <- cn; break
      }
    }
    # Detect rank column: numeric/integer, sequential from 1
    rank_col <- NA_character_
    for (cn in names(df)) {
      vals <- suppressWarnings(as.integer(df[[cn]]))
      if (!is.na(vals[1]) && vals[1] == 1 && !any(is.na(vals[1:10]))) {
        rank_col <- cn; break
      }
    }
    # Hard-coded fallback for known column names
    if (is.na(name_col)) name_col <- intersect(c("player_name","Player","Name","Pitcher"), names(df))[1]
    if (is.na(rank_col)) rank_col <- intersect(c("eno_rank","Rank","rank","RK"), names(df))[1]

    if (is.na(name_col) || is.na(rank_col)) return(NULL)

    out <- data.frame(
      player_name = trimws(as.character(df[[name_col]])),
      eno_rank    = suppressWarnings(as.integer(df[[rank_col]])),
      stringsAsFactors = FALSE
    )
    out <- out[!is.na(out$eno_rank) & nchar(out$player_name) > 1, ]
    out$nk <- spr_nk(out$player_name)
    out
  }, error = function(e) NULL)
}

# Fetch PitcherList ranks via HTML scrape
spr_fetch_pl <- function() {
  tryCatch({
    page   <- rvest::read_html(SPR_PL_URL)
    tbls   <- rvest::html_table(rvest::html_nodes(page, "table"), fill = TRUE)
    if (length(tbls) == 0) return(NULL)

    # Find table containing Rank and Pitcher/Name columns
    tbl <- NULL
    for (t in tbls) {
      has_rank   <- any(grepl("^rank$|^rk$|^#$", tolower(names(t))))
      has_player <- any(grepl("pitcher|name|player", tolower(names(t))))
      if (has_rank && has_player) { tbl <- t; break }
    }
    if (is.null(tbl)) tbl <- tbls[[1]]

    rank_col <- names(tbl)[grepl("^rank$|^rk$|^#$", tolower(names(tbl)))][1]
    name_col <- names(tbl)[grepl("pitcher|name|player", tolower(names(tbl)))][1]
    if (is.na(rank_col) || is.na(name_col)) {
      rank_col <- names(tbl)[1]
      name_col <- names(tbl)[2]
    }

    raw_names <- trimws(as.character(tbl[[name_col]]))
    # Strip trailing tier tokens appended by the HTML parser (e.g. "Garrett CrochetT1")
    raw_names <- gsub("\\s*T\\d+$", "", raw_names)
    out <- data.frame(
      player_name = raw_names,
      pl_rank     = suppressWarnings(as.integer(tbl[[rank_col]])),
      stringsAsFactors = FALSE
    )
    out <- out[!is.na(out$pl_rank) & nchar(out$player_name) > 1, ]
    out$nk <- spr_nk(out$player_name)
    out
  }, error = function(e) NULL)
}

# Weight presets
SPR_WT_EVEN <- c(adp = 1, eno = 1, pl = 1, spz = 1)
SPR_WT_CK   <- c(adp = 3, eno = 4, pl = 1, spz = 2)

# Fallback list lengths if data is empty (overridden dynamically at runtime)
SPR_N_ENO_DEFAULT <- 207L
SPR_N_PL_DEFAULT  <- 100L
SPR_N_SPZ_DEFAULT <- 173L

# ── UI ────────────────────────────────────────────────────────────────────────

spRankOverviewUI <- function(id) {
  ns <- NS(id)
  div(
    class = "spr-panel",

    # Action bar
    div(
      class = "spr-actions",
      actionButton(ns("refresh"), "Fetch Rankings",
                   class = "btn btn-pag-generate",
                   icon  = icon("rotate-right")),
      uiOutput(ns("fetch_status"))
    ),

    # ── Weighted Rank settings ──────────────────────────────────────────────
    div(
      class = "pag-panel spr-weights",
      div(class = "pag-panel-title", "Rank Weights"),
      div(
        class = "pag-preset-row",
        tags$span(class = "pag-preset-label", "Presets:"),
        actionButton(ns("wt_preset_even"), "Even",       class = "btn btn-pag-preset"),
        actionButton(ns("wt_preset_ck"),   "CK Weights", class = "btn btn-pag-preset btn-pag-preset--active")
      ),
      div(
        class = "spr-wt-inputs",
        div(class = "spr-wt-field",
            tags$label(class = "spr-wt-label", "ADP Rank"),
            numericInput(ns("wt_adp"), label = NULL,
                         value = SPR_WT_CK[["adp"]], min = 0, step = 0.5, width = "80px")),
        div(class = "spr-wt-field",
            tags$label(class = "spr-wt-label", "Eno Rank"),
            numericInput(ns("wt_eno"), label = NULL,
                         value = SPR_WT_CK[["eno"]], min = 0, step = 0.5, width = "80px")),
        div(class = "spr-wt-field",
            tags$label(class = "spr-wt-label", "PitcherList Rank"),
            numericInput(ns("wt_pl"),  label = NULL,
                         value = SPR_WT_CK[["pl"]],  min = 0, step = 0.5, width = "80px")),
        div(class = "spr-wt-field",
            tags$label(class = "spr-wt-label", "SP Skillz Rank"),
            numericInput(ns("wt_spz"), label = NULL,
                         value = SPR_WT_CK[["spz"]], min = 0, step = 0.5, width = "80px"))
      ),
      tags$p(class = "spr-wt-hint",
             "Weights are relative. List lengths are set dynamically from actual fetched data. ",
             "Pitchers absent from a list are treated as last-place for that source.")
    ),

    # Main table
    DTOutput(ns("tbl"), width = "100%"),

    # Source links
    div(
      class = "spr-links",
      tags$strong("Sources"),
      tags$ul(
        tags$li(tags$a("Eno Sarris SP Rankings (The Athletic)",
                       href = SPR_ENO_LINK, target = "_blank")),
        tags$li(tags$a("PitcherList Top 100 SP (3/13 Update)",
                       href = SPR_PL_LINK, target = "_blank")),
        tags$li(tags$a("NFBC ADP",
                       href = SPR_ADP_LINK, target = "_blank")),
        tags$li(tags$a("SP Skillz (navigate to In-Season Tools \u2192 SP Skillz)",
                       href = SPR_APP_LINK, target = "_blank"))
      )
    )
  )
}

# ── Server ────────────────────────────────────────────────────────────────────

spRankOverviewServer <- function(id, adp_data = reactive(NULL)) {
  moduleServer(id, function(input, output, session) {

    # Static SP Skillz load
    spz_df <- spr_load_spz()

    # Reactive stores for external fetches
    eno_rv <- reactiveVal(NULL)
    pl_rv  <- reactiveVal(NULL)

    # Fetch on first load
    observe({
      isolate({
        eno_rv(spr_fetch_eno())
        pl_rv(spr_fetch_pl())
      })
    })

    # Re-fetch on Refresh button
    observeEvent(input$refresh, {
      eno_rv(spr_fetch_eno())
      pl_rv(spr_fetch_pl())
    })

    # Build combined table
    combined <- reactive({
      adp <- adp_data()
      eno <- eno_rv()
      pl  <- pl_rv()
      spz <- spz_df

      # Union of all name keys
      all_nk <- unique(c(
        if (!is.null(spz)) spz$nk,
        if (!is.null(eno)) eno$nk,
        if (!is.null(pl))  pl$nk
      ))
      if (length(all_nk) == 0) return(NULL)

      df <- data.frame(nk = all_nk, player_name = NA_character_,
                       stringsAsFactors = FALSE)

      # Resolve display name (prefer SP Skillz → Eno → PL)
      for (src in list(pl, eno, spz)) {
        if (is.null(src)) next
        m <- match(df$nk, src$nk)
        df$player_name <- ifelse(
          is.na(df$player_name) & !is.na(m),
          src$player_name[m], df$player_name
        )
      }

      # Join ADP
      df$adp <- NA_real_
      if (!is.null(adp) && nrow(adp) > 0 && "player_name" %in% names(adp)) {
        adp$nk <- spr_nk(adp$player_name)
        m <- match(df$nk, adp$nk)
        df$adp <- adp$adp[m]
      }

      # Join SP Skillz rank
      df$spz_rank <- NA_integer_
      if (!is.null(spz)) {
        m <- match(df$nk, spz$nk)
        df$spz_rank <- as.integer(spz$sp_skillz_rank_stabilized[m])
      }

      # Join Eno rank
      df$eno_rank <- NA_integer_
      if (!is.null(eno)) {
        m <- match(df$nk, eno$nk)
        df$eno_rank <- eno$eno_rank[m]
      }

      # Join PitcherList rank
      df$pl_rank <- NA_integer_
      if (!is.null(pl)) {
        m <- match(df$nk, pl$nk)
        df$pl_rank <- pl$pl_rank[m]
      }

      # ADP Rank (rank by ADP ascending; NA gets NA)
      df$adp_rank <- NA_integer_
      has_adp <- !is.na(df$adp)
      df$adp_rank[has_adp] <- rank(df$adp[has_adp], ties.method = "min")

      # Sort: ADP ascending (NA last), then avg of available ranks
      avg_rank <- rowMeans(cbind(df$spz_rank, df$eno_rank, df$pl_rank), na.rm = TRUE)
      df <- df[order(is.na(df$adp), df$adp, avg_rank, na.last = TRUE), ]

      # Final column selection (no Weighted Rank yet — added in display_df)
      df <- df[, c("player_name", "adp", "adp_rank",
                   "eno_rank", "pl_rank", "spz_rank"), drop = FALSE]
      names(df) <- c("Player", "ADP", "ADP Rank",
                     "Eno Rank", "PitcherList Rank", "SP Skillz Rank")
      df
    })

    # ── Preset weight handlers ─────────────────────────────────────────────────
    observeEvent(input$wt_preset_even, {
      updateNumericInput(session, "wt_adp", value = SPR_WT_EVEN[["adp"]])
      updateNumericInput(session, "wt_eno", value = SPR_WT_EVEN[["eno"]])
      updateNumericInput(session, "wt_pl",  value = SPR_WT_EVEN[["pl"]])
      updateNumericInput(session, "wt_spz", value = SPR_WT_EVEN[["spz"]])
    })
    observeEvent(input$wt_preset_ck, {
      updateNumericInput(session, "wt_adp", value = SPR_WT_CK[["adp"]])
      updateNumericInput(session, "wt_eno", value = SPR_WT_CK[["eno"]])
      updateNumericInput(session, "wt_pl",  value = SPR_WT_CK[["pl"]])
      updateNumericInput(session, "wt_spz", value = SPR_WT_CK[["spz"]])
    })

    # Debounce weight inputs so rapid typing doesn't thrash the table
    wt_adp_d <- debounce(reactive(input$wt_adp), 200)
    wt_eno_d <- debounce(reactive(input$wt_eno), 200)
    wt_pl_d  <- debounce(reactive(input$wt_pl),  200)
    wt_spz_d <- debounce(reactive(input$wt_spz), 200)

    # ── Weighted rank computation ──────────────────────────────────────────────
    # Each source rank is normalised to [0,1] by its list length.
    # Players absent from a list get 1.0 (last place). Weighted average of the
    # four normalised scores is then ranked to produce Weighted Rank.
    display_df <- reactive({
      df <- combined()
      req(!is.null(df) && nrow(df) > 0)

      w_adp <- max(0, suppressWarnings(as.numeric(wt_adp_d())) %||% 0)
      w_eno <- max(0, suppressWarnings(as.numeric(wt_eno_d())) %||% 0)
      w_pl  <- max(0, suppressWarnings(as.numeric(wt_pl_d()))  %||% 0)
      w_spz <- max(0, suppressWarnings(as.numeric(wt_spz_d())) %||% 0)
      w_tot <- w_adp + w_eno + w_pl + w_spz

      # Dynamic list lengths from actual fetched data
      dyn_n <- function(col, fallback) {
        v <- df[[col]]
        if (any(!is.na(v))) max(v, na.rm = TRUE) else fallback
      }
      n_adp <- dyn_n("ADP Rank",        175L)
      n_eno <- dyn_n("Eno Rank",        SPR_N_ENO_DEFAULT)
      n_pl  <- dyn_n("PitcherList Rank",SPR_N_PL_DEFAULT)
      n_spz <- dyn_n("SP Skillz Rank",  SPR_N_SPZ_DEFAULT)

      if (w_tot > 0) {
        norm <- function(x, n) ifelse(is.na(x), 1.0, x / n)
        score <- (w_adp * norm(df$`ADP Rank`,        n_adp) +
                  w_eno * norm(df$`Eno Rank`,         n_eno) +
                  w_pl  * norm(df$`PitcherList Rank`, n_pl)  +
                  w_spz * norm(df$`SP Skillz Rank`,   n_spz)) / w_tot
        df$`Weighted Rank` <- rank(score, ties.method = "min")
      } else {
        df$`Weighted Rank` <- NA_integer_
      }

      # Re-sort: ADP ascending, players with no ADP pushed to the bottom
      df <- df[order(is.na(df$ADP), df$ADP, na.last = TRUE), ]

      # Replace NA with sentinel so DataTables sorts missing values last on any column click
      # (9999 is safely above all real rank/ADP values; rendered as "—" in the DT callback)
      spr_num_cols <- c("ADP", "ADP Rank", "Eno Rank", "PitcherList Rank",
                        "SP Skillz Rank", "Weighted Rank")
      for (col in spr_num_cols) {
        if (col %in% names(df))
          df[[col]] <- ifelse(is.na(df[[col]]), 9999L, df[[col]])
      }
      df
    })

    # ── DT output ─────────────────────────────────────────────────────────────
    output$tbl <- renderDT(server = FALSE, {
      df <- display_df()
      req(!is.null(df) && nrow(df) > 0)

      n <- ncol(df)
      # JS render: show "—" for sentinel (>=9999); format ADP to 1 decimal
      sentinel_render <- DT::JS(
        "function(data, type, row, meta) {",
        "  if (type !== 'display') return data;",
        "  if (data === null || data === undefined || data >= 9999) return '\u2014';",
        "  if (meta.col === 1) return parseFloat(data).toFixed(1);",
        "  return data;",
        "}"
      )
      datatable(
        df, rownames = FALSE, filter = "none", selection = "none",
        options = list(
          pageLength = 100,
          order      = list(list(1L, "asc")),
          columnDefs = list(
            list(className = "dt-left",   targets = 0L),
            list(className = "dt-center", targets = seq_len(n - 1L)),
            list(targets = seq_len(n - 1L), render = sentinel_render)
          )
        ),
        class = "pf-dt display nowrap"
      ) |>
        DT::formatStyle("Player", fontWeight = "650", color = "#172733") |>
        DT::formatStyle("ADP", fontWeight = "700", color = "var(--primary)")
    })

    # ── Status indicator ──────────────────────────────────────────────────────
    output$fetch_status <- renderUI({
      eno_ok <- !is.null(eno_rv())
      pl_ok  <- !is.null(pl_rv())
      msg <- if (eno_ok && pl_ok)
        tags$span(class = "status-ok", "\u2713 All external rankings loaded")
      else if (!eno_ok && !pl_ok)
        tags$span(class = "status-error", "\u26A0 Could not load Eno or PitcherList rankings")
      else if (!eno_ok)
        tags$span(class = "status-warn", "\u26A0 Could not load Eno rankings")
      else
        tags$span(class = "status-warn", "\u26A0 Could not load PitcherList rankings")
      div(class = "status-shell", style = "margin-left: 10px;", msg)
    })

    # Return display_df so Compare tab can access rank data
    list(display_df = display_df)
  })
}
