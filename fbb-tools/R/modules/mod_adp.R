suppressPackageStartupMessages({
  library(DT)
})

# ── Constants ─────────────────────────────────────────────────────────────────

NFBC_ADP_ENDPOINT <- "https://nfc.shgn.com/adp.data.php"

ADP_DRAFT_TYPE_CHOICES <- c(
  "All"                = "",
  "NFBC 50"            = "897",
  "Draft Champions"    = "893",
  "Main Event"         = "889",
  "Online Championship" = "890"
)

ADP_NUM_TEAMS_CHOICES <- c("All" = "", "10" = "10", "12" = "12", "15" = "15")

ADP_POS_CHOICES <- c("C", "1B", "2B", "3B", "SS", "OF", "UT", "CI", "MI", "P")

ADP_HITTER_SET <- c("C", "1B", "2B", "3B", "SS", "OF", "UT", "CI", "MI")

# ── Helpers ───────────────────────────────────────────────────────────────────

adp_name_last_first <- function(x) {
  out <- trimws(as.character(x))
  has_comma <- grepl(",", out, fixed = TRUE)
  if (any(has_comma)) {
    out[has_comma] <- vapply(
      strsplit(out[has_comma], ",", fixed = TRUE),
      FUN = function(parts) {
        parts <- trimws(parts)
        if (length(parts) < 2 || !nzchar(parts[2])) return(parts[1])
        paste(parts[2], parts[1])
      },
      FUN.VALUE = character(1)
    )
  }
  out
}

parse_nfbc_adp_all <- function(tsv_path) {
  # Parses NFBC ADP TSV and returns ALL players (hitters + pitchers)
  if (!nzchar(tsv_path) || !file.exists(tsv_path)) return(NULL)

  raw <- tryCatch(
    utils::read.delim(tsv_path, sep = "\t", stringsAsFactors = FALSE,
                      check.names = FALSE, fileEncoding = "UTF-8"),
    error = function(e) NULL
  )
  if (is.null(raw) || nrow(raw) == 0) return(NULL)

  col <- function(i) {
    if (ncol(raw) >= i) raw[[i]] else rep(NA_character_, nrow(raw))
  }

  out <- data.frame(
    nfbc_rank    = suppressWarnings(as.integer(col(1))),
    player_name  = adp_name_last_first(col(3)),
    team         = trimws(as.character(col(4))),
    positions    = trimws(as.character(col(5))),
    adp          = suppressWarnings(as.numeric(col(6))),
    adp_min_pick = suppressWarnings(as.integer(col(7))),
    adp_max_pick = suppressWarnings(as.integer(col(8))),
    adp_picks    = suppressWarnings(as.integer(col(10))),
    stringsAsFactors = FALSE
  )

  out <- out[!is.na(out$adp) & nzchar(out$player_name), , drop = FALSE]
  out <- out[order(out$nfbc_rank, out$adp, na.last = TRUE), , drop = FALSE]
  rownames(out) <- NULL
  out
}

download_nfbc_adp <- function(lookback_days = 14, draft_type = "", num_teams = "") {
  curl_bin <- Sys.which("curl")
  if (!nzchar(curl_bin)) stop("curl is required to download NFBC ADP data.")

  to_date   <- Sys.Date()
  from_date <- to_date - as.integer(lookback_days)
  out_path  <- tempfile(fileext = ".tsv")

  fmt <- function(d) format(as.Date(d), "%Y-%m-%d")

  curl_args <- c(
    "-sS", "-L", "--fail",
    "--retry", "2",
    "--connect-timeout", "15",
    "--max-time", "60",
    NFBC_ADP_ENDPOINT,
    "--data-urlencode", "team_id=0",
    "--data-urlencode", paste0("from_date=",  fmt(from_date)),
    "--data-urlencode", paste0("to_date=",    fmt(to_date)),
    "--data-urlencode", paste0("num_teams=",  as.character(num_teams)),
    "--data-urlencode", paste0("draft_type=", as.character(draft_type)),
    "--data-urlencode", "position=",
    "--data-urlencode", "league_teams=",
    "--data-urlencode", "sport=mlb",
    "--data-urlencode", "download=Download",
    "-o", out_path
  )

  res <- tryCatch(
    suppressWarnings(system2(curl_bin, args = curl_args, stdout = TRUE, stderr = TRUE)),
    error = function(e) e
  )

  if (inherits(res, "error")) stop("curl error: ", conditionMessage(res))

  status <- attr(res, "status")
  if (!is.null(status) && as.integer(status) != 0L) {
    stop(sprintf("NFBC ADP download failed (curl exit %s).", status))
  }

  if (!file.exists(out_path) || file.info(out_path)$size <= 0) {
    stop("NFBC ADP download produced an empty file.")
  }

  out_path
}

row_has_position <- function(pos_col, selected) {
  if (length(selected) == 0) return(rep(FALSE, length(pos_col)))
  pattern <- paste0("\\b(", paste(selected, collapse = "|"), ")\\b")
  grepl(pattern, pos_col, ignore.case = FALSE)
}

# ── Module UI ─────────────────────────────────────────────────────────────────

adpUI <- function(id) {
  ns <- NS(id)

  div(
    class = "adp-page",

    # ── Page header ───────────────────────────────────────────────────────────
    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "NFBC ADP Scraper"),
      p(
        class = "pf-subtitle",
        "Live NFBC average draft position. Configure pull settings and click Generate to fetch data."
      )
    ),

    # ── ADP Pull Settings ─────────────────────────────────────────────────────
    div(
      class = "adp-settings-card",
      div(class = "adp-settings-title", "ADP Pull Settings"),
      div(
        class = "adp-settings-row",
        div(
          class = "adp-settings-group",
          tags$span(class = "adp-context-label", "Draft Type"),
          selectInput(
            ns("draft_type"),
            label   = NULL,
            choices = ADP_DRAFT_TYPE_CHOICES,
            selected = "889",
            width   = "210px"
          )
        ),
        div(
          class = "adp-settings-group",
          tags$span(class = "adp-context-label", "# of Teams"),
          selectInput(
            ns("num_teams"),
            label    = NULL,
            choices  = ADP_NUM_TEAMS_CHOICES,
            selected = "",
            width    = "130px"
          )
        ),
        div(
          class = "adp-settings-group",
          tags$span(class = "adp-context-label", "Lookback Days"),
          numericInput(
            ns("lookback_days"),
            label = NULL,
            value = 14, min = 1, step = 1,
            width = "90px"
          )
        ),
        div(
          class = "adp-settings-btns",
          actionButton(
            ns("generate"), "Fetch ADP",
            class = "btn btn-pag-generate",
            icon  = icon("rotate-right")
          ),
          uiOutput(ns("export_btn"))
        )
      ),
      # ── CSV Upload ──────────────────────────────────────────────────────────
      div(
        class = "adp-upload-row",
        tags$span(class = "adp-context-label", "Or upload a CSV export:"),
        fileInput(
          ns("adp_upload_csv"),
          label       = NULL,
          accept      = ".csv",
          buttonLabel = "Upload ADP CSV",
          placeholder = "No file chosen",
          width       = "100%"
        ),
        uiOutput(ns("adp_upload_status"))
      )
    ),

    # ── Position filter ───────────────────────────────────────────────────────
    div(
      class = "adp-pos-section",
      div(
        class = "adp-pos-header",
        tags$span(class = "pf-control-label", "Positions"),
        div(
          class = "adp-pos-btns",
          actionButton(ns("pos_all"),      "All",          class = "btn btn-adp-pos-quick"),
          actionButton(ns("pos_hitters"),  "Hitters Only", class = "btn btn-adp-pos-quick"),
          actionButton(ns("pos_pitchers"), "Pitchers",     class = "btn btn-adp-pos-quick"),
          actionButton(ns("pos_deselect"), "Deselect All", class = "btn btn-adp-pos-quick")
        )
      ),
      div(
        class = "adp-pos-checks",
        checkboxGroupInput(
          ns("pos_filter"),
          label    = NULL,
          choices  = ADP_POS_CHOICES,
          selected = ADP_POS_CHOICES,
          inline   = TRUE
        )
      )
    ),

    # ── Search ────────────────────────────────────────────────────────────────
    div(
      class = "pf-controls-row adp-search-row",
      div(
        class = "spz-search-wrap adp-search-wrap",
        tags$span(class = "spz-search-icon", icon("magnifying-glass")),
        textInput(
          ns("search"),
          label       = NULL,
          placeholder = "Search player\u2026",
          width       = "100%"
        )
      )
    ),

    # ── Table or empty state ──────────────────────────────────────────────────
    uiOutput(ns("body_ui")),

    # ── Footer ────────────────────────────────────────────────────────────────
    div(
      class = "pf-footer",
      tags$span(
        class = "pf-footer-text",
        "Source: NFBC (National Fantasy Baseball Championship). ",
        "ADP based on non-auction drafts. Min/Max show pick range across sampled drafts."
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

adpServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    ADP_DRAFT_TYPE_SLUGS <- c(
      "897" = "nfbc50",
      "893" = "draft_champs",
      "889" = "main_event",
      "890" = "online_champ"
    )

    rv <- reactiveValues(
      adp_data        = NULL,
      generated       = FALSE,
      pull_draft_type = "",
      pull_num_teams  = "",
      pull_lookback   = 14L,
      upload_error    = NULL
    )

    # ── Generate ──────────────────────────────────────────────────────────────
    observeEvent(input$generate, {
      draft_type <- as.character(input$draft_type %||% "")
      num_teams  <- as.character(input$num_teams  %||% "")
      lookback   <- suppressWarnings(as.integer(input$lookback_days))
      if (is.na(lookback) || lookback < 1L) lookback <- 14L

      withProgress(message = "Fetching NFBC ADP data\u2026", value = 0.3, {
        result <- tryCatch({
          tsv_path <- download_nfbc_adp(
            lookback_days = lookback,
            draft_type    = draft_type,
            num_teams     = num_teams
          )
          setProgress(0.75, message = "Parsing\u2026")
          dat <- parse_nfbc_adp_all(tsv_path)
          if (is.null(dat) || nrow(dat) == 0) stop("No data returned from NFBC.")
          dat
        }, error = function(e) e)
      })

      if (inherits(result, "error")) {
        showNotification(
          paste("Error:", conditionMessage(result)),
          type = "error", duration = 8
        )
      } else {
        rv$adp_data       <- result
        rv$generated      <- TRUE
        rv$pull_draft_type <- draft_type
        rv$pull_num_teams  <- num_teams
        rv$pull_lookback   <- lookback
        showNotification(
          sprintf("ADP loaded: %d players", nrow(result)),
          type = "message", duration = 4
        )
      }
    }, ignoreInit = TRUE)

    # ── CSV upload ────────────────────────────────────────────────────────────
    observeEvent(input$adp_upload_csv, {
      req(input$adp_upload_csv)
      result <- tryCatch(
        parse_adp_csv_upload(input$adp_upload_csv$datapath),
        error = function(e) e
      )
      if (inherits(result, "error")) {
        rv$upload_error <- conditionMessage(result)
        showNotification(paste("Upload error:", conditionMessage(result)),
                         type = "error", duration = 8)
      } else {
        rv$adp_data    <- result
        rv$generated   <- TRUE
        rv$upload_error <- NULL
        showNotification(
          sprintf("ADP uploaded: %d players from %s",
                  nrow(result), input$adp_upload_csv$name),
          type = "message", duration = 4
        )
      }
    }, ignoreNULL = TRUE)

    output$adp_upload_status <- renderUI({
      err <- rv$upload_error %||% NULL
      if (!is.null(err))
        div(class = "status-shell status-error", style = "margin-top:4px;", err)
    })

    # ── Position quick-select buttons ─────────────────────────────────────────
    observeEvent(input$pos_all, {
      updateCheckboxGroupInput(session, "pos_filter", selected = ADP_POS_CHOICES)
    }, ignoreInit = TRUE)

    observeEvent(input$pos_hitters, {
      updateCheckboxGroupInput(session, "pos_filter", selected = ADP_HITTER_SET)
    }, ignoreInit = TRUE)

    observeEvent(input$pos_pitchers, {
      updateCheckboxGroupInput(session, "pos_filter", selected = "P")
    }, ignoreInit = TRUE)

    observeEvent(input$pos_deselect, {
      updateCheckboxGroupInput(session, "pos_filter", selected = character(0))
    }, ignoreInit = TRUE)

    # ── Debounced search ──────────────────────────────────────────────────────
    search_d <- debounce(reactive(input$search), 300)

    # ── Filtered data ─────────────────────────────────────────────────────────
    adp_filtered <- reactive({
      dat <- rv$adp_data
      if (is.null(dat)) return(NULL)

      sel <- input$pos_filter %||% character(0)
      if (length(sel) == 0) return(dat[0, , drop = FALSE])
      dat <- dat[row_has_position(dat[["positions"]], sel), , drop = FALSE]

      q <- trimws(search_d() %||% "")
      if (nchar(q) > 0)
        dat <- dat[grepl(q, dat[["player_name"]], ignore.case = TRUE), , drop = FALSE]

      dat
    })

    # ── Body UI ───────────────────────────────────────────────────────────────
    output$body_ui <- renderUI({
      if (!rv$generated) {
        div(
          class = "adp-empty",
          div(
            class = "adp-empty-inner",
            tags$span(class = "adp-empty-icon", HTML("&#x1F4CB;")),
            h3(class = "spz-empty-title", "No data loaded"),
            p(class = "spz-empty-desc",
              "Configure ADP Pull Settings above and click Generate to fetch live NFBC data.")
          )
        )
      } else {
        div(
          class = "pf-table-wrap",
          DTOutput(ns("table"), width = "100%")
        )
      }
    })

    # ── Table ─────────────────────────────────────────────────────────────────
    output$table <- renderDT({
      dat <- req(adp_filtered())

      display <- data.frame(
        Rank       = as.integer(dat$nfbc_rank),
        Player     = dat$player_name,
        Team       = dat$team,
        Pos        = dat$positions,
        ADP        = round(dat$adp, 1),
        Min        = as.integer(dat$adp_min_pick),
        Max        = as.integer(dat$adp_max_pick),
        `# Drafts` = as.integer(dat$adp_picks),
        check.names = FALSE,
        stringsAsFactors = FALSE
      )

      dt <- datatable(
        display,
        rownames  = FALSE,
        filter    = "none",
        selection = "none",
        options   = list(
          dom           = "t",
          ordering      = TRUE,
          pageLength    = nrow(display),
          scrollX       = TRUE,
          scrollY       = "calc(100vh - 420px)",
          scrollCollapse = FALSE,
          order         = list(list(4, "asc")),
          columnDefs    = list(
            list(className = "dt-right",  targets = 0),
            list(className = "dt-left",   targets = 1),
            list(className = "dt-center", targets = c(2, 3, 4, 5, 6, 7)),
            list(width = "44px",  targets = 0),
            list(width = "165px", targets = 1),
            list(width = "52px",  targets = 2),
            list(width = "90px",  targets = 3),
            list(width = "60px",  targets = 4),
            list(width = "50px",  targets = 5),
            list(width = "50px",  targets = 6),
            list(width = "78px",  targets = 7)
          )
        ),
        class = "pf-dt display nowrap"
      ) |>
        formatStyle("Rank",      color = "#8a9a8f", fontWeight = "400", fontSize = "0.8rem") |>
        formatStyle("Player",    fontWeight = "650", color = "#172733") |>
        formatStyle("Team",      color = "#4a5a4f", fontSize = "0.82rem") |>
        formatStyle("Pos",       color = "#4a5a4f", fontSize = "0.82rem") |>
        formatStyle("ADP",       fontWeight = "700", color = "var(--primary)") |>
        formatStyle(c("Min", "Max", "# Drafts"), color = "#8a9a8f", fontSize = "0.82rem")

      dt
    })

    # ── Export button ─────────────────────────────────────────────────────────
    output$export_btn <- renderUI({
      if (rv$generated) {
        downloadButton(ns("export_csv"), "Export ADP", class = "btn btn-pag-export", title = "Exports as .csv")
      } else {
        tags$button("Export ADP", class = "btn btn-pag-export", disabled = NA, title = "Exports as .csv")
      }
    })

    output$export_csv <- downloadHandler(
      filename = function() {
        dt_slug <- if (nzchar(rv$pull_draft_type))
          ADP_DRAFT_TYPE_SLUGS[rv$pull_draft_type] else ""
        tm_slug <- if (nzchar(rv$pull_num_teams))
          paste0(rv$pull_num_teams, "tm") else ""
        lookback_slug <- paste0(rv$pull_lookback, "d")

        parts <- c("nfbc_adp",
                   dt_slug[!is.na(dt_slug) & nzchar(dt_slug)],
                   tm_slug[nzchar(tm_slug)],
                   lookback_slug,
                   format(Sys.Date(), "%Y%m%d"))
        paste0(paste(parts, collapse = "_"), ".csv")
      },
      content = function(file) {
        dat <- adp_filtered()
        if (is.null(dat) || nrow(dat) == 0) {
          utils::write.csv(data.frame(), file, row.names = FALSE)
          return()
        }
        display <- data.frame(
          Rank       = as.integer(dat$nfbc_rank),
          Player     = dat$player_name,
          Team       = dat$team,
          Pos        = dat$positions,
          ADP        = round(dat$adp, 1),
          Min        = as.integer(dat$adp_min_pick),
          Max        = as.integer(dat$adp_max_pick),
          `# Drafts` = as.integer(dat$adp_picks),
          check.names = FALSE,
          stringsAsFactors = FALSE
        )
        utils::write.csv(display, file, row.names = FALSE)
      }
    )

    # Return ADP data as a reactive so parent modules (Draft Lab) can access it
    reactive(rv$adp_data)
  })
}
