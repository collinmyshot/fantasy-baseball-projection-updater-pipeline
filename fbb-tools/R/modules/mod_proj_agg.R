suppressPackageStartupMessages({
  library(DT)
  library(openxlsx)
  library(jsonlite)
})

# ── Constants ─────────────────────────────────────────────────────────────────

PROJ_SYSTEMS <- list(
  list(key = "steamer",  label = "Steamer",   collin_wt_h = 2, collin_wt_p = 1),
  list(key = "zips",     label = "ZiPS",      collin_wt_h = 0, collin_wt_p = 0),
  list(key = "atc",      label = "ATC",       collin_wt_h = 1, collin_wt_p = 0),
  list(key = "thebat",   label = "THE BAT",   collin_wt_h = 0, collin_wt_p = 0),
  list(key = "thebatx",  label = "THE BAT X", collin_wt_h = 3, collin_wt_p = 0),
  list(key = "oopsy",    label = "OOPSY",     collin_wt_h = 3, collin_wt_p = 1)
)

PROJ_KEYS        <- vapply(PROJ_SYSTEMS, `[[`, character(1), "key")
PROJ_LABELS      <- setNames(vapply(PROJ_SYSTEMS, `[[`, character(1), "label"),      PROJ_KEYS)
PROJ_COLLIN_WT_H <- setNames(vapply(PROJ_SYSTEMS, `[[`, numeric(1),   "collin_wt_h"), PROJ_KEYS)
PROJ_COLLIN_WT_P <- setNames(vapply(PROJ_SYSTEMS, `[[`, numeric(1),   "collin_wt_p"), PROJ_KEYS)
PROJ_EVEN_WT     <- setNames(rep(1, length(PROJ_KEYS)), PROJ_KEYS)

# Some systems use alternate API type strings (try in order)
FG_API_TYPES <- list(
  steamer  = c("steamer"),
  zips     = c("zips"),
  atc      = c("atc"),
  thebat   = c("thebat"),
  thebatx  = c("thebatx", "batx"),
  oopsy    = c("oopsy")
)

# Rest-of-season API type strings (same system keys, different FG type codes)
ROS_FG_API_TYPES <- list(
  steamer  = c("steamerr"),
  zips     = c("rzips"),
  atc      = c("ratcdc"),
  thebat   = c("rthebat"),
  thebatx  = c("rthebatx"),
  oopsy    = c("roopsydc")
)

# Hitter stats — H and BB are computed but not exposed as checkboxes
# (they are auto-included when AVG or OBP is selected)
H_STAT_COLS    <- c("pa", "h", "bb", "r", "hr", "rbi", "sb", "avg", "obp")
H_STAT_LABELS  <- c("PA", "H", "BB",  "R", "HR",  "RBI", "SB", "AVG", "OBP")
H_ROUND        <- c(pa = 1, h = 1, bb = 1, r = 1, hr = 1, rbi = 1, sb = 1, avg = 3, obp = 3)
H_CAT_CHOICES  <- c("pa", "r", "hr", "rbi", "sb", "avg", "obp")  # checkbox options
H_CAT_LABELS   <- c("PA", "R",  "HR",  "RBI", "SB", "AVG", "OBP")
H_DEFAULT_CATS <- c("pa", "hr", "r", "rbi", "sb", "avg")
H_COUNT_STATS  <- c("h", "bb", "r", "hr", "rbi", "sb")

# Pitcher stats
P_STAT_COLS    <- c("ip", "w", "sv", "hd", "svhd", "k", "era", "whip")
P_STAT_LABELS  <- c("IP", "W", "SV", "HD", "SVHD", "K", "ERA", "WHIP")
P_ROUND        <- c(ip = 1, w = 1, sv = 1, hd = 1, svhd = 1, k = 1, era = 2, whip = 2)
P_DEFAULT_CATS <- c("ip", "w", "sv", "k", "era", "whip")  # HD/SVHD off by default
P_COUNT_STATS  <- c("w", "sv", "hd", "svhd", "k")         # scaled per IP

FG_API_BASE            <- "https://www.fangraphs.com/api/projections"
FG_REQUEST_RETRIES     <- 3L
FG_RETRY_SLEEP         <- 1.5
FG_USER_AGENT          <- paste0(
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
FG_CURRENT_SEASON      <- 2026L

# Column name candidates (normalized lowercase) for hitters
FG_H_COL_CANDIDATES <- list(
  playerid = c("playerid", "playeridfg", "fgplayerid", "idfg", "id"),
  name     = c("playername", "name", "shortname"),
  team     = c("team", "tm"),
  g        = c("g", "games", "gp"),
  pa       = c("pa", "plateappearances"),
  ab       = c("ab", "atbats"),
  h        = c("h", "hits"),
  x2b      = c("2b", "doubles", "x2b"),
  x3b      = c("3b", "triples", "x3b"),
  bb       = c("bb", "walks"),
  hbp      = c("hbp", "hitbypitch"),
  r        = c("r", "runs"),
  hr       = c("hr", "homeruns"),
  rbi      = c("rbi", "runsbattedin"),
  sb       = c("sb", "stolenbases"),
  cs       = c("cs", "caughtstealing"),
  avg      = c("avg", "battingaverage")
)

# Column name candidates for pitchers
FG_P_COL_CANDIDATES <- list(
  playerid = c("playerid", "playeridfg", "fgplayerid", "idfg", "id"),
  name     = c("playername", "name", "shortname"),
  team     = c("team", "tm"),
  g        = c("g", "games", "gp"),
  gs       = c("gs", "gamesstarted"),
  ip       = c("ip", "inningspitched"),
  w        = c("w", "wins"),
  sv       = c("sv", "saves"),
  hd       = c("hld", "hd", "holds", "hold"),
  k        = c("so", "k", "strikeouts", "ks"),
  h_allow  = c("h", "hits"),
  bb_allow = c("bb", "walks"),
  hbp_bat  = c("hbp", "hitbypitch"),
  hr_allow = c("hr", "homeruns"),
  era      = c("era", "earnedruntaverage"),
  whip     = c("whip")
)

# ── FanGraphs scraping ────────────────────────────────────────────────────────

normalize_col <- function(x) gsub("[^a-z0-9]", "", tolower(x))

fg_select_col <- function(dat, candidates) {
  norm_names      <- normalize_col(names(dat))
  norm_candidates <- normalize_col(candidates)
  idx <- match(norm_candidates, norm_names, nomatch = 0L)
  idx <- idx[idx > 0L]
  if (length(idx) == 0L) return(rep(NA_character_, nrow(dat)))
  dat[[names(dat)[idx[1L]]]]
}

build_fg_url <- function(api_type, stats_type) {
  params <- list(
    pos     = "all",
    stats   = stats_type,
    lg      = "all",
    qual    = "0",
    type    = api_type,
    season  = as.character(FG_CURRENT_SEASON),
    month   = "0",
    season1 = as.character(FG_CURRENT_SEASON),
    ind     = "0",
    team    = "0",
    rost    = "0",
    age     = "0",
    filter  = "",
    players = "0",
    z       = as.character(as.integer(Sys.time())),
    sort    = "0,1"
  )
  query <- paste(
    paste0(names(params), "=", vapply(params, utils::URLencode, character(1L), reserved = TRUE)),
    collapse = "&"
  )
  paste0(FG_API_BASE, "?", query)
}

proj_fg_fetch <- function(url) {
  result <- fg_fetch_json(url, referer = "https://www.fangraphs.com/projections")
  if (isTRUE(result$ok)) result$payload else NULL
}

as_df <- function(raw) {
  if (is.null(raw)) return(NULL)
  if (is.list(raw) && "data" %in% names(raw)) raw <- raw$data
  if (is.data.frame(raw)) return(raw)
  tryCatch(as.data.frame(raw, stringsAsFactors = FALSE), error = function(e) NULL)
}

fetch_fg_projections <- function(key, stats_type) {
  # stats_type: "bat" or "pit"
  col_map <- if (stats_type == "bat") FG_H_COL_CANDIDATES else FG_P_COL_CANDIDATES
  api_types <- FG_API_TYPES[[key]]
  if (is.null(api_types)) api_types <- key

  for (api_type in api_types) {
    url     <- build_fg_url(api_type, stats_type)
    payload <- proj_fg_fetch(url)
    raw     <- as_df(payload)
    if (is.null(raw) || nrow(raw) == 0) next

    # Build normalized output
    out <- data.frame(
      playerid = as.character(fg_select_col(raw, col_map$playerid)),
      name     = as.character(fg_select_col(raw, col_map$name)),
      team     = as.character(fg_select_col(raw, col_map$team)),
      stringsAsFactors = FALSE
    )

    stat_cols <- setdiff(names(col_map), c("playerid", "name", "team"))
    for (s in stat_cols) {
      out[[s]] <- suppressWarnings(as.numeric(fg_select_col(raw, col_map[[s]])))
    }

    if (stats_type == "bat") {
      # Compute OBP for hitters if not directly provided
      if (!"obp" %in% names(out) || all(is.na(out[["obp"]]))) {
        h_vals  <- suppressWarnings(as.numeric(fg_select_col(raw, c("h", "hits"))))
        bb_vals <- suppressWarnings(as.numeric(fg_select_col(raw, c("bb", "walks"))))
        pa_vals <- suppressWarnings(as.numeric(fg_select_col(raw, c("pa"))))
        if (!all(is.na(h_vals)) && !all(is.na(bb_vals)) && !all(is.na(pa_vals))) {
          out$obp <- ifelse(
            !is.na(pa_vals) & pa_vals > 0,
            (h_vals + bb_vals) / pa_vals,
            NA_real_
          )
        }
      }
    }

    if (stats_type == "pit") {
      # Compute SVHD = SV + HD
      sv_vals <- if ("sv" %in% names(out)) out$sv else NA_real_
      hd_vals <- if ("hd" %in% names(out)) out$hd else NA_real_
      out$svhd <- ifelse(
        is.na(sv_vals) & is.na(hd_vals), NA_real_,
        rowSums(cbind(
          ifelse(is.na(sv_vals), 0, sv_vals),
          ifelse(is.na(hd_vals), 0, hd_vals)
        ))
      )
    }

    # Drop rows with no playerid
    out <- out[!is.na(out$playerid) & nzchar(out$playerid), ]
    if (nrow(out) > 0) return(out)
  }

  NULL
}

# ── Aggregate computation ─────────────────────────────────────────────────────

compute_aggregate <- function(dat_list, has_data, weights, stat_cols,
                               pt_col, pt_source = "aggregate",
                               count_stats = character(0),
                               pt_min = 0) {
  active <- PROJ_KEYS[vapply(PROJ_KEYS, function(k) {
    isTRUE(has_data[[k]]) && !is.na(weights[k]) && weights[k] > 0
  }, logical(1))]
  if (length(active) == 0) return(NULL)

  # Apply PT minimum per-system before aggregation
  if (!is.na(pt_min) && pt_min > 0) {
    dat_list <- lapply(dat_list, function(d) {
      if (is.null(d) || !pt_col %in% names(d)) return(d)
      d[!is.na(d[[pt_col]]) & d[[pt_col]] >= pt_min, , drop = FALSE]
    })
  }

  all_ids <- unique(unlist(lapply(dat_list[active], function(d) as.character(d$playerid))))

  lookup <- do.call(rbind, lapply(dat_list[active], function(d) {
    d[, intersect(c("playerid", "name", "team"), names(d)), drop = FALSE]
  }))
  lookup$playerid <- as.character(lookup$playerid)
  lookup <- lookup[!duplicated(lookup$playerid), ]
  result <- lookup[match(all_ids, lookup$playerid), , drop = FALSE]

  use_pt_adjust <- (
    pt_source != "aggregate" &&
    pt_source %in% active &&
    pt_col %in% names(dat_list[[pt_source]])
  )
  if (use_pt_adjust) {
    src_d <- dat_list[[pt_source]]
    src_d$playerid <- as.character(src_d$playerid)
    pt_src_vals <- as.numeric(src_d[[pt_col]][match(result$playerid, src_d$playerid)])
  }

  for (stat in stat_cols) {
    if (use_pt_adjust && stat == pt_col) {
      result[[stat]] <- pt_src_vals

    } else if (use_pt_adjust && stat %in% count_stats) {
      rate_num <- rep(0, nrow(result))
      rate_den <- rep(0, nrow(result))
      for (k in active) {
        d <- dat_list[[k]]
        if (is.null(d) || !stat %in% names(d) || !pt_col %in% names(d)) next
        d$playerid <- as.character(d$playerid)
        m  <- match(result$playerid, d$playerid)
        ok <- which(!is.na(m))
        if (length(ok) == 0) next
        w    <- weights[k]
        pt_k <- as.numeric(d[[pt_col]][m[ok]])
        st_k <- as.numeric(d[[stat]][m[ok]])
        valid <- !is.na(pt_k) & pt_k > 0 & !is.na(st_k)
        rate_num[ok[valid]] <- rate_num[ok[valid]] + w * (st_k[valid] / pt_k[valid])
        rate_den[ok[valid]] <- rate_den[ok[valid]] + w
      }
      rate <- ifelse(rate_den > 0, rate_num / rate_den, NA_real_)
      result[[stat]] <- ifelse(
        !is.na(pt_src_vals) & pt_src_vals > 0,
        rate * pt_src_vals,
        NA_real_
      )

    } else {
      num <- rep(0, nrow(result))
      den <- rep(0, nrow(result))
      for (k in active) {
        d <- dat_list[[k]]
        if (is.null(d) || !stat %in% names(d)) next
        d$playerid <- as.character(d$playerid)
        m  <- match(result$playerid, d$playerid)
        ok <- !is.na(m)
        w  <- weights[k]
        num[ok] <- num[ok] + w * as.numeric(d[[stat]][m[ok]])
        den[ok] <- den[ok] + w
      }
      result[[stat]] <- ifelse(den > 0, num / den, NA_real_)
    }
  }
  result
}

# ── Rounding ──────────────────────────────────────────────────────────────────

apply_rounding <- function(dat, round_spec) {
  for (s in names(round_spec)) {
    if (s %in% names(dat)) dat[[s]] <- round(dat[[s]], round_spec[[s]])
  }
  dat
}

# ── System row UI ─────────────────────────────────────────────────────────────

system_row_ui <- function(ns, key, label, has_data, collin_wt, suffix) {
  wt_id <- ns(paste0("wt_", suffix, "_", key))
  # has_data = NULL → not yet fetched (no status); TRUE → ok; FALSE → no data
  is_missing <- isFALSE(has_data)
  div(
    class = paste("pag-sys-row", if (is_missing) "pag-sys-unavail"),
    div(
      class = "pag-sys-label",
      tags$span(class = "pag-sys-name", label),
      if (is_missing) tags$span(class = "pag-sys-status", "no data")
    ),
    div(
      class = "pag-sys-input",
      numericInput(wt_id, label = NULL, value = collin_wt, min = 0, step = 1, width = "70px")
    )
  )
}

# ── Module UI ─────────────────────────────────────────────────────────────────

projAggUI <- function(id) {
  ns <- NS(id)

  pt_choices <- c("Weighted Average" = "aggregate", setNames(PROJ_KEYS, PROJ_LABELS))

  div(
    class = "pag-page",

    div(
      class = "pf-header",
      div(class = "pf-header-eyebrow", "Collinmyshot"),
      h1(class = "pf-title", "Projection Aggregator"),
      p(
        class = "pf-subtitle",
        "Weighted multi-system aggregation across Steamer, ZiPS, ATC, THE BAT, THE BAT X, and OOPSY.",
        tags$br(),
        "Configure weights, select a playing time source, then click Generate to fetch from FanGraphs."
      )
    ),

    navset_pill(
      id = ns("proj_type"),

      # ── Hitters ─────────────────────────────────────────────────────────────
      nav_panel(
        "Hitters", value = "hitters",
        div(
          class = "pag-tab-body",

          layout_columns(
            col_widths = c(5, 7), gap = "20px",

            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Projection Systems"),
              div(
                class = "pag-preset-row",
                tags$span(class = "pag-preset-label", "Weight Presets:"),
                actionButton(ns("preset_h_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
                actionButton(ns("preset_h_even"),   "Even",          class = "btn btn-pag-preset")
              ),
              div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude a system"),
              tagList(lapply(PROJ_SYSTEMS, function(sys) {
                system_row_ui(ns, sys$key, sys$label, NULL, PROJ_COLLIN_WT_H[sys$key], "h")
              }))
            ),

            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Categories"),
              div(class = "pag-panel-subtitle", "Select stats to display"),
              checkboxGroupInput(
                ns("cats_h"),
                label    = NULL,
                choices  = setNames(H_CAT_CHOICES, H_CAT_LABELS),
                selected = H_DEFAULT_CATS,
                inline   = TRUE
              )
            )
          ),

          div(
            class = "pag-controls-row",
            div(
              class = "pag-pt-source pag-pt-source--highlighted",
              div(class = "pag-pt-source-label",
                tags$span("\u23F1 Playing Time Source")
              ),
              selectInput(
                ns("pt_source_h"),
                label    = NULL,
                choices  = pt_choices,
                selected = "atc",
                width    = "220px"
              ),
              tags$p(class = "pag-pt-hint",
                "Counting stats (R, HR, RBI, SB) are scaled to this system\u2019s projected PA"
              )
            ),
            div(
              class = "pag-pa-min",
              numericInput(
                ns("pa_min"),
                label = "Min PA",
                value = 200,
                min   = 0,
                step  = 10,
                width = "90px"
              )
            ),
            div(
              class = "pag-generate-wrap",
              actionButton(
                ns("generate_h"), "Fetch Projections",
                class = "btn btn-pag-generate",
                icon  = icon("rotate-right")
              )
            )
          ),

          uiOutput(ns("export_btn_h")),

          uiOutput(ns("body_h"))
        )
      ),

      # ── Pitchers ────────────────────────────────────────────────────────────
      nav_panel(
        "Pitchers", value = "pitchers",
        div(
          class = "pag-tab-body",

          layout_columns(
            col_widths = c(5, 7), gap = "20px",

            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Projection Systems"),
              div(
                class = "pag-preset-row",
                tags$span(class = "pag-preset-label", "Weight Presets:"),
                actionButton(ns("preset_p_collin"), "Collin\u2019s", class = "btn btn-pag-preset"),
                actionButton(ns("preset_p_even"),   "Even",          class = "btn btn-pag-preset")
              ),
              div(class = "pag-panel-subtitle", "Weight \u2014 set to 0 to exclude a system"),
              tagList(lapply(PROJ_SYSTEMS, function(sys) {
                system_row_ui(ns, sys$key, sys$label, NULL, PROJ_COLLIN_WT_P[sys$key], "p")
              }))
            ),

            div(
              class = "pag-panel",
              div(class = "pag-panel-title", "Categories"),
              div(class = "pag-panel-subtitle", "Select stats to display"),
              checkboxGroupInput(
                ns("cats_p"),
                label    = NULL,
                choices  = setNames(P_STAT_COLS, P_STAT_LABELS),
                selected = P_DEFAULT_CATS,
                inline   = TRUE
              )
            )
          ),

          div(
            class = "pag-controls-row",
            div(
              class = "pag-pt-source pag-pt-source--highlighted",
              div(class = "pag-pt-source-label",
                tags$span("\u23F1 Playing Time Source")
              ),
              selectInput(
                ns("pt_source_p"),
                label    = NULL,
                choices  = pt_choices,
                selected = "oopsy",
                width    = "220px"
              ),
              tags$p(class = "pag-pt-hint",
                "Counting stats (W, SV, K) are scaled to this system\u2019s projected IP"
              )
            ),
            div(
              class = "pag-ip-min",
              numericInput(
                ns("ip_min"),
                label = "Min IP",
                value = 50,
                min   = 0,
                step  = 5,
                width = "90px"
              )
            ),
            div(
              class = "pag-generate-wrap",
              actionButton(
                ns("generate_p"), "Fetch Projections",
                class = "btn btn-pag-generate",
                icon  = icon("rotate-right")
              )
            )
          ),

          uiOutput(ns("export_btn_p")),

          uiOutput(ns("body_p"))
        )
      )
    )
  )
}

# ── Module Server ─────────────────────────────────────────────────────────────

projAggServer <- function(id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns

    # ── Data caches — populated on Generate ───────────────────────────────────

    empty_cache <- function() setNames(vector("list", length(PROJ_KEYS)), PROJ_KEYS)

    fetched_h <- reactiveVal(empty_cache())
    fetched_p <- reactiveVal(empty_cache())

    # TRUE = has data, FALSE = fetched but missing, NULL = not yet attempted
    any_fetched_h <- reactiveVal(FALSE)
    any_fetched_p <- reactiveVal(FALSE)

    has_data_h <- reactive({
      d <- fetched_h()
      setNames(vapply(PROJ_KEYS, function(k) !is.null(d[[k]]), logical(1)), PROJ_KEYS)
    })
    has_data_p <- reactive({
      d <- fetched_p()
      setNames(vapply(PROJ_KEYS, function(k) !is.null(d[[k]]), logical(1)), PROJ_KEYS)
    })

    # ── Generate handlers ─────────────────────────────────────────────────────

    observeEvent(input$generate_h, {
      withProgress(message = "Fetching hitter projections\u2026", value = 0, {
        result <- empty_cache()
        n <- length(PROJ_KEYS)
        for (i in seq_along(PROJ_KEYS)) {
          k <- PROJ_KEYS[i]
          incProgress(1 / n, detail = PROJ_LABELS[[k]])
          result[[k]] <- fetch_fg_projections(k, "bat")
        }
        fetched_h(result)
        any_fetched_h(TRUE)
      })
    })

    observeEvent(input$generate_p, {
      withProgress(message = "Fetching pitcher projections\u2026", value = 0, {
        result <- empty_cache()
        n <- length(PROJ_KEYS)
        for (i in seq_along(PROJ_KEYS)) {
          k <- PROJ_KEYS[i]
          incProgress(1 / n, detail = PROJ_LABELS[[k]])
          result[[k]] <- fetch_fg_projections(k, "pit")
        }
        fetched_p(result)
        any_fetched_p(TRUE)
      })
    })

    # ── Preset handlers ───────────────────────────────────────────────────────

    apply_preset <- function(suffix, weights) {
      for (k in PROJ_KEYS) {
        updateNumericInput(session, paste0("wt_", suffix, "_", k), value = unname(weights[k]))
      }
    }

    observeEvent(input$preset_h_collin, {
      req(input$preset_h_collin > 0)
      apply_preset("h", PROJ_COLLIN_WT_H)
    })

    observeEvent(input$preset_h_even, {
      req(input$preset_h_even > 0)
      apply_preset("h", PROJ_EVEN_WT)
    })

    observeEvent(input$preset_p_collin, {
      req(input$preset_p_collin > 0)
      apply_preset("p", PROJ_COLLIN_WT_P)
    })

    observeEvent(input$preset_p_even, {
      req(input$preset_p_even > 0)
      apply_preset("p", PROJ_EVEN_WT)
    })

    # ── Reactive weights ──────────────────────────────────────────────────────

    read_weights <- function(suffix) {
      reactive({
        setNames(
          vapply(PROJ_KEYS, function(k) {
            v <- input[[paste0("wt_", suffix, "_", k)]]
            if (is.null(v) || is.na(v)) 0 else as.numeric(v)
          }, numeric(1)),
          PROJ_KEYS
        )
      })
    }

    weights_h <- read_weights("h")
    weights_p <- read_weights("p")

    # ── Aggregate reactives ───────────────────────────────────────────────────

    agg_h <- reactive({
      d      <- fetched_h()
      hd     <- has_data_h()
      if (!any(hd)) return(NULL)
      pt     <- if (is.null(input$pt_source_h)) "aggregate" else input$pt_source_h
      pt_min <- if (is.null(input$pa_min) || is.na(input$pa_min)) 0 else as.numeric(input$pa_min)
      dat <- compute_aggregate(d, hd, weights_h(), H_STAT_COLS,
                               pt_col = "pa", pt_source = pt,
                               count_stats = H_COUNT_STATS,
                               pt_min = pt_min)
      if (!is.null(dat)) apply_rounding(dat, H_ROUND) else NULL
    })

    agg_p <- reactive({
      d   <- fetched_p()
      pd  <- has_data_p()
      if (!any(pd)) return(NULL)
      pt  <- if (is.null(input$pt_source_p)) "aggregate" else input$pt_source_p
      pt_min <- if (is.null(input$ip_min) || is.na(input$ip_min)) 0 else as.numeric(input$ip_min)
      dat <- compute_aggregate(d, pd, weights_p(), P_STAT_COLS,
                               pt_col = "ip", pt_source = pt,
                               count_stats = P_COUNT_STATS,
                               pt_min = pt_min)
      if (!is.null(dat)) apply_rounding(dat, P_ROUND) else NULL
    })

    # ── Body UI ───────────────────────────────────────────────────────────────

    make_body_ui <- function(has_data_rv, generated_flag, agg_rv, tbl_id) {
      renderUI({
        if (!generated_flag()) {
          div(
            class = "spz-empty",
            div(
              class = "spz-empty-inner",
              h3(class = "spz-empty-title", "No projections loaded"),
              p(class  = "spz-empty-desc",
                "Configure weights and click Generate to fetch projections from FanGraphs.")
            )
          )
        } else {
          hd <- has_data_rv()
          dat <- agg_rv()
          if (is.null(dat) || nrow(dat) == 0) {
            n_failed <- sum(!hd)
            msg <- if (n_failed == length(PROJ_KEYS)) {
              "All systems failed to fetch. Check your connection and try again."
            } else {
              "Set at least one system weight above 0."
            }
            div(
              class = "spz-empty",
              div(
                class = "spz-empty-inner",
                h3(class = "spz-empty-title", "No data to display"),
                p(class  = "spz-empty-desc", msg)
              )
            )
          } else {
            div(
              class = "pf-table-wrap",
              div(class = "pag-tbl-section-title", "Aggregate Projections"),
              DTOutput(ns(tbl_id), width = "100%")
            )
          }
        }
      })
    }

    output$body_h <- make_body_ui(has_data_h, any_fetched_h, agg_h, "table_h")
    output$body_p <- make_body_ui(has_data_p, any_fetched_p, agg_p, "table_p")

    # ── Tables ────────────────────────────────────────────────────────────────

    make_dt <- function(agg_rv, cats_input_id, stat_cols, stat_labels, sort_stat) {
      renderDT(server = TRUE, {
        dat <- req(agg_rv())
        vis <- input[[cats_input_id]]
        if (is.null(vis) || length(vis) == 0) vis <- stat_cols

        # For hitters: auto-include H when AVG selected; H+BB when OBP selected
        if (identical(cats_input_id, "cats_h")) {
          if ("avg" %in% vis || "obp" %in% vis) vis <- union(vis, "h")
          if ("obp" %in% vis)                   vis <- union(vis, "bb")
        }
        show_stats <- intersect(stat_cols, vis)
        avail_cols <- intersect(c("name", "team", show_stats), names(dat))
        display    <- dat[, avail_cols, drop = FALSE]

        # Hidden column with diacritics stripped for accent-insensitive search
        display$name_search <- stringi::stri_trans_general(display$name, "Latin-ASCII")

        if (sort_stat %in% names(display)) {
          display <- display[order(display[[sort_stat]], decreasing = TRUE, na.last = TRUE), ]
        }

        stat_name_map <- setNames(stat_labels, stat_cols)
        shown_stats   <- intersect(show_stats, avail_cols)
        # col_names: one entry per column including hidden name_search
        col_names   <- c("Player", "Team", unname(stat_name_map[shown_stats]), "")
        n_stat_cols <- length(shown_stats)
        search_col  <- ncol(display) - 1L  # 0-based index of name_search

        datatable(
          display,
          rownames  = FALSE,
          colnames  = col_names,
          filter    = "none",
          selection = "none",
          options   = list(
            dom         = "<'pag-dt-controls'lf>rtip",
            ordering    = TRUE,
            pageLength  = 30,
            lengthMenu  = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
            searchDelay = 200,
            scrollX     = FALSE,
            columnDefs  = list(
              list(className = "dt-left",   targets = 0),
              list(className = "dt-center", targets = seq_len(n_stat_cols + 1)),
              list(targets = search_col, visible = FALSE, searchable = TRUE)
            )
          ),
          class = "pf-dt display nowrap"
        ) |>
          formatStyle("name", fontWeight = "650", color = "#172733") |>
          formatStyle("team", color = "#4a5a4f", fontSize = "0.82rem", textAlign = "center")
      })
    }

    output$table_h <- make_dt(agg_h, "cats_h", H_STAT_COLS, H_STAT_LABELS, "pa")
    output$table_p <- make_dt(agg_p, "cats_p", P_STAT_COLS, P_STAT_LABELS, "ip")

    # ── Export ────────────────────────────────────────────────────────────────

    make_export <- function(fetched_rv, has_data_rv, agg_rv, round_spec, prefix) {
      downloadHandler(
        filename = function() sprintf("aggregate_proj_%s_%s.xlsx", prefix, Sys.Date()),
        content  = function(file) {
          wb  <- createWorkbook()
          d   <- fetched_rv()
          hd  <- has_data_rv()
          for (k in PROJ_KEYS) {
            if (!isTRUE(hd[[k]])) next
            addWorksheet(wb, PROJ_LABELS[k])
            writeData(wb, PROJ_LABELS[k], apply_rounding(d[[k]], round_spec))
          }
          agg <- agg_rv()
          if (!is.null(agg)) {
            addWorksheet(wb, "Aggregate")
            writeData(wb, "Aggregate", agg)
          }
          saveWorkbook(wb, file, overwrite = TRUE)
        }
      )
    }

    export_btn_label <- tags$span(
      tags$span(class = "glyphicon glyphicon-download-alt", style = "margin-right:5px;"),
      "Export .xlsx"
    )

    make_export_btn <- function(generated_flag, download_id, label = "Export .xlsx", fmt = ".xlsx") {
      renderUI({
        div(class = "pag-export-row",
          if (generated_flag()) {
            downloadButton(ns(download_id), label, class = "btn btn-pag-export", title = paste("Exports as", fmt))
          } else {
            tags$button(
              label,
              class    = "btn btn-pag-export",
              disabled = NA,
              style    = "opacity: 0.4; cursor: not-allowed;",
              title    = paste("Exports as", fmt)
            )
          }
        )
      })
    }

    output$export_btn_h <- make_export_btn(any_fetched_h, "export_h", "Export Hitters")
    output$export_btn_p <- make_export_btn(any_fetched_p, "export_p", "Export Pitchers")

    output$export_h <- make_export(fetched_h, has_data_h, agg_h, H_ROUND, "hitters")
    output$export_p <- make_export(fetched_p, has_data_p, agg_p, P_ROUND, "pitchers")
  })
}
