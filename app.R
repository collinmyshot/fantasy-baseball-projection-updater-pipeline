#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(shiny)
  library(DT)
  library(bslib)
})

source(file.path("R", "pipeline_config.R"))
source(file.path("R", "fangraphs_projections.R"))

adp_draft_type_choices <- c(
  "All Drafts (Non Auction)" = "",
  "NFBC 50" = "897",
  "Draft Champions" = "896",
  "Main Event" = "895",
  "Online Championship" = "899"
)

adp_position_filter_choices <- c(
  "C" = "C",
  "1B" = "1B",
  "2B" = "2B",
  "3B" = "3B",
  "SS" = "SS",
  "OF" = "OF",
  "UT" = "UT",
  "CI" = "CI",
  "MI" = "MI",
  "P" = "P"
)

adp_hitter_filter_set <- c("C", "1B", "2B", "3B", "SS", "OF", "UT", "CI", "MI")

coalesce_chr <- function(x, fallback = "") {
  if (is.null(x) || length(x) == 0 || is.na(x) || !nzchar(as.character(x))) {
    fallback
  } else {
    as.character(x)
  }
}

read_cached_pitcher_output <- function(cfg, season) {
  out_path <- file.path(cfg$paths$processed_dir, sprintf("%s_pitchers_integrated_table.csv", season))
  if (!file.exists(out_path)) {
    return(NULL)
  }
  tryCatch(utils::read.csv(out_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
}

read_cached_pitcher_sp_skillz <- function(cfg) {
  lookback <- as.integer(cfg$pitcher$sp_skillz$lookback_season)
  primary <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_scores_2026_plus_model.csv", lookback))
  fallback <- file.path(cfg$paths$processed_dir, sprintf("%s_sp_skillz_scores.csv", lookback))
  use_path <- if (file.exists(primary)) primary else fallback
  if (!file.exists(use_path)) {
    return(NULL)
  }
  tryCatch(utils::read.csv(use_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
}

normalize_adp_player_name <- function(x) {
  x <- as.character(x)
  has_comma <- grepl(",", x, fixed = TRUE)
  out <- x
  out[has_comma] <- vapply(
    strsplit(x[has_comma], ",", fixed = TRUE),
    FUN.VALUE = character(1),
    FUN = function(parts) {
      parts <- trimws(parts)
      if (length(parts) < 2) return(parts[1])
      paste(parts[2], parts[1])
    }
  )
  trimws(out)
}

prepare_adp_table <- function(dat) {
  if (is.null(dat) || !is.data.frame(dat) || nrow(dat) == 0) {
    return(NULL)
  }

  col_or_na <- function(name_options) {
    hit <- intersect(name_options, names(dat))
    if (length(hit) == 0) return(rep(NA, nrow(dat)))
    dat[[hit[1]]]
  }

  out <- data.frame(
    player_name = normalize_adp_player_name(col_or_na(c("player_name", "Player"))),
    positions = as.character(col_or_na(c("positions", "Position(s)"))),
    team = as.character(col_or_na(c("team", "Team"))),
    adp = suppressWarnings(as.numeric(col_or_na(c("adp", "ADP")))),
    nfbc_rank = suppressWarnings(as.numeric(col_or_na(c("nfbc_rank", "Rank")))),
    adp_min_pick = suppressWarnings(as.numeric(col_or_na(c("adp_min_pick", "Min Pick")))),
    adp_max_pick = suppressWarnings(as.numeric(col_or_na(c("adp_max_pick", "Max Pick")))),
    adp_picks = suppressWarnings(as.numeric(col_or_na(c("adp_picks", "# Picks")))),
    stringsAsFactors = FALSE
  )

  out <- out[!is.na(out$adp) & nzchar(out$player_name), , drop = FALSE]
  out <- out[order(out$adp, na.last = TRUE), , drop = FALSE]
  rownames(out) <- NULL
  out
}

read_cached_adp_output <- function(cfg, season) {
  season_tsv <- file.path(cfg$paths$raw_dir, sprintf("%s_nfbc_adp.tsv", season))
  if (file.exists(season_tsv)) {
    dat <- tryCatch(utils::read.delim(season_tsv, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    return(prepare_adp_table(dat))
  }

  local_tsv <- as.character(cfg$adp$local_tsv %||% "")
  if (nzchar(local_tsv) && file.exists(local_tsv)) {
    dat <- tryCatch(utils::read.delim(local_tsv, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    return(prepare_adp_table(dat))
  }

  processed_path <- file.path(cfg$paths$processed_dir, sprintf("%s_nfbc_adp_clean.csv", season))
  if (file.exists(processed_path)) {
    dat <- tryCatch(utils::read.csv(processed_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    return(prepare_adp_table(dat))
  }

  NULL
}

read_cached_run_data <- function(cfg, season) {
  snapshot_path <- file.path(cfg$paths$processed_dir, sprintf("%s_run_data_snapshot.csv", season))
  if (file.exists(snapshot_path)) {
    dat <- tryCatch(utils::read.csv(snapshot_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
    if (!is.null(dat) && is.data.frame(dat) && nrow(dat) > 0) {
      return(dat)
    }
  }

  meta_path <- file.path(cfg$paths$processed_dir, sprintf("%s_refresh_metadata.csv", season))
  meta <- tryCatch(utils::read.csv(meta_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
  if (!is.null(meta) && nrow(meta) > 0) {
    m <- meta[1, , drop = FALSE]
    systems <- coalesce_chr(m$systems, fallback = "")
    return(data.frame(
      Section = c("Run", "Run", "Projection"),
      Metric = c("Season", "Refreshed UTC", "Systems Used"),
      Value = c(as.character(season), coalesce_chr(m$refreshed_at_utc, fallback = ""), systems),
      stringsAsFactors = FALSE
    ))
  }

  NULL
}

extract_position_tokens <- function(x) {
  x <- toupper(gsub("\\s+", "", as.character(x)))
  x[is.na(x)] <- ""
  lapply(strsplit(x, "[,/]"), function(parts) {
    parts <- trimws(parts)
    unique(parts[nzchar(parts)])
  })
}

row_has_any_position <- function(position_strings, selected_positions) {
  if (length(selected_positions) == 0) {
    return(rep(TRUE, length(position_strings)))
  }
  selected_positions <- unique(toupper(as.character(selected_positions)))
  tokens <- extract_position_tokens(position_strings)

  is_hitter_only_ut <- function(tok) {
    hitter_base <- c("C", "1B", "2B", "3B", "SS", "OF", "UT")
    hitter_tok <- tok[tok %in% hitter_base]
    "UT" %in% hitter_tok && all(hitter_tok %in% "UT")
  }

  row_matches <- function(tok) {
    checks <- logical(0)
    if ("ALL_HITTERS" %in% selected_positions) {
      checks <- c(checks, any(tok %in% c("C", "1B", "2B", "3B", "SS", "OF", "UT")))
    }
    if ("CI" %in% selected_positions) {
      checks <- c(checks, any(tok %in% c("1B", "3B")))
    }
    if ("MI" %in% selected_positions) {
      checks <- c(checks, any(tok %in% c("2B", "SS")))
    }
    if ("P" %in% selected_positions) {
      checks <- c(checks, any(tok %in% c("P", "SP", "RP")))
    }

    base_selected <- setdiff(selected_positions, c("ALL_HITTERS", "CI", "MI", "P"))
    if (length(base_selected) > 0) {
      base_hits <- vapply(base_selected, function(sel) {
        if (identical(sel, "UT")) {
          return(is_hitter_only_ut(tok))
        }
        sel %in% tok
      }, logical(1))
      checks <- c(checks, any(base_hits))
    }

    any(checks)
  }

  vapply(tokens, row_matches, logical(1))
}

add_pitcher_role_integrated <- function(dat) {
  if (is.null(dat) || !is.data.frame(dat) || nrow(dat) == 0) {
    return(dat)
  }
  sv <- suppressWarnings(as.numeric(dat$proj_sv))
  dat$pitcher_role <- ifelse(!is.na(sv) & sv >= 5, "RP", "SP")
  dat
}

add_pitcher_role_sp_skillz <- function(dat) {
  if (is.null(dat) || !is.data.frame(dat) || nrow(dat) == 0) {
    return(dat)
  }
  gs <- suppressWarnings(as.numeric(dat$gs))
  start_share <- suppressWarnings(as.numeric(dat$start_share))
  is_sp <- (!is.na(gs) & gs >= 5) | (!is.na(start_share) & start_share >= 0.5)
  dat$pitcher_role <- ifelse(is_sp, "SP", "RP")
  dat
}

build_run_data_rows <- function(result, inputs) {
  systems_txt <- paste(inputs$systems, collapse = ",")

  adp_draft_type_chr <- coalesce_chr(inputs$adp_draft_type, fallback = "")
  adp_num_teams_chr <- coalesce_chr(inputs$adp_num_teams, fallback = "")

  draft_label <- if (adp_draft_type_chr %in% unname(adp_draft_type_choices)) {
    names(adp_draft_type_choices)[match(adp_draft_type_chr, unname(adp_draft_type_choices))]
  } else {
    if (nzchar(adp_draft_type_chr)) sprintf("Draft Type %s", adp_draft_type_chr) else "All Drafts (Non Auction)"
  }

  adp_from <- coalesce_chr(result$nfbc_adp_from_date, fallback = "")
  adp_to <- coalesce_chr(result$nfbc_adp_to_date, fallback = "")

  if (!nzchar(adp_to)) {
    adp_to <- as.character(Sys.Date())
  }
  if (!nzchar(adp_from)) {
    adp_from <- as.character(as.Date(adp_to) - as.integer(inputs$adp_lookback_days))
  }

  p_base_names <- names(inputs$pitcher_weights)
  p_low_names <- names(inputs$pitcher_low_ip_weights)
  p_high_names <- names(inputs$pitcher_high_ip_weights)
  p_stab_names <- names(inputs$pitcher_stabilization_points)

  data.frame(
    Section = c(
      "Run",
      "Hitter Projection",
      "Pitcher Projection",
      rep("Hitter Projection Weights", length(inputs$projection_weights)),
      rep("Hitter Stat Weights", length(inputs$category_weights)),
      "Pitcher SP Skillz",
      "Pitcher SP Skillz",
      "Pitcher SP Skillz",
      "Pitcher SP Skillz",
      "Pitcher SP Skillz",
      rep("Pitcher SP Skillz Base Weights", length(inputs$pitcher_weights)),
      rep("Pitcher SP Skillz Low-IP Weights", length(inputs$pitcher_low_ip_weights)),
      rep("Pitcher SP Skillz High-IP Weights", length(inputs$pitcher_high_ip_weights)),
      rep("Pitcher SP Skillz Stabilization Points", length(inputs$pitcher_stabilization_points)),
      "League",
      "League",
      "League",
      "ADP Run",
      "ADP Run",
      "ADP Run",
      "ADP Run"
    ),
    Metric = c(
      "Season",
      "Systems Used",
      "Projection System (Stats Display)",
      toupper(names(inputs$projection_weights)),
      toupper(names(inputs$category_weights)),
      "Use IP Paradigms",
      "Low-IP Max Start-IP",
      "High-IP Min Start-IP",
      "GS Fallback Threshold",
      "Reliability Method",
      toupper(p_base_names),
      toupper(p_low_names),
      toupper(p_high_names),
      toupper(p_stab_names),
      "Teams",
      "PA Floor",
      "Starter Count",
      "Draft Type",
      "Number of Teams",
      "Date From",
      "Date To"
    ),
    Value = c(
      as.character(inputs$season),
      systems_txt,
      as.character(inputs$pitcher_projection_system),
      as.character(as.numeric(inputs$projection_weights)),
      as.character(as.numeric(inputs$category_weights)),
      ifelse(isTRUE(inputs$pitcher_use_ip_paradigms), "TRUE", "FALSE"),
      as.character(inputs$pitcher_low_ip_max_start_ip),
      as.character(inputs$pitcher_high_ip_min_start_ip),
      as.character(inputs$pitcher_gs_fallback_threshold),
      as.character(inputs$pitcher_reliability_method),
      as.character(as.numeric(inputs$pitcher_weights)),
      as.character(as.numeric(inputs$pitcher_low_ip_weights)),
      as.character(as.numeric(inputs$pitcher_high_ip_weights)),
      as.character(as.numeric(inputs$pitcher_stabilization_points)),
      as.character(inputs$num_teams),
      as.character(inputs$pa_floor),
      as.character(inputs$starter_count),
      draft_label,
      ifelse(nzchar(adp_num_teams_chr), adp_num_teams_chr, "All Teams"),
      adp_from,
      adp_to
    ),
    stringsAsFactors = FALSE
  )
}

read_cached_output <- function(cfg, season) {
  out_path <- file.path(cfg$paths$processed_dir, sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", season))
  meta_path <- file.path(cfg$paths$processed_dir, sprintf("%s_refresh_metadata.csv", season))

  if (!file.exists(out_path)) {
    return(NULL)
  }

  dat <- tryCatch(utils::read.csv(out_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)
  meta <- tryCatch(utils::read.csv(meta_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL)

  list(data = dat, meta = meta)
}

swap_adp_team_columns <- function(dat) {
  if (is.null(dat) || !is.data.frame(dat)) {
    return(dat)
  }
  if (!all(c("adp", "team") %in% names(dat))) {
    return(dat)
  }

  cols <- names(dat)
  adp_idx <- match("adp", cols)
  team_idx <- match("team", cols)
  cols[c(adp_idx, team_idx)] <- cols[c(team_idx, adp_idx)]
  dat[, cols, drop = FALSE]
}

drop_match_flag_columns <- function(dat) {
  if (is.null(dat) || !is.data.frame(dat)) {
    return(dat)
  }
  dat[, setdiff(names(dat), c("adp_match_quality", "name_team_flag")), drop = FALSE]
}

summary_strip_ui <- function(suffix) {
  layout_columns(
    col_widths = c(4, 4, 4),
    card(class = "metric-card", card_header("Last Completed Run"), htmlOutput(paste0("summary_snapshot_", suffix))),
    card(class = "metric-card", card_header("Dataset Counts"), htmlOutput(paste0("summary_counts_", suffix))),
    card(class = "metric-card", card_header("Run Parameters"), htmlOutput(paste0("summary_filters_", suffix)))
  )
}

ui <- page_sidebar(
  title = div(
    class = "app-title-wrap",
    actionButton("toggle_sidebar_btn", "Controls", class = "sidebar-toggle-btn"),
    tags$span("Fantasy Baseball Draft Projections")
  ),
  theme = bs_theme(
    version = 5,
    bg = "#f6f3eb",
    fg = "#16222b",
    primary = "#0f7a5f",
    secondary = "#c57a1f",
    base_font = font_google("Manrope"),
    heading_font = font_google("Sora")
  ),
  tags$head(
    tags$style(HTML("
    .app-shell {
      background:
        radial-gradient(1200px 480px at 95% -5%, rgba(15,122,95,0.12), transparent 65%),
        radial-gradient(900px 420px at -5% 100%, rgba(197,122,31,0.12), transparent 70%),
        linear-gradient(160deg, #f6f3eb 0%, #efe7d8 50%, #f6f3eb 100%);
    }
    .app-title-wrap {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .app-title-wrap .sidebar-toggle-btn {
      border: 1px solid #d9d1bf;
      border-radius: 9px;
      background: #fbf8f1;
      padding: 4px 10px;
      line-height: 1.2;
      font-weight: 650;
      color: #23313a;
    }
    .sidebar-shell {
      background: rgba(255,255,255,0.84);
      border: 1px solid #d9d1bf;
      border-radius: 14px;
      padding: 12px;
      box-shadow: 0 12px 24px rgba(0,0,0,0.06);
      backdrop-filter: blur(3px);
    }
    .sidebar-shell .accordion-button {
      font-weight: 700;
      padding: 0.65rem 0.9rem;
    }
    .sidebar-shell .accordion-body {
      padding: 0.8rem 0.9rem;
    }
    .sidebar-shell .shiny-input-container {
      margin-bottom: 0.65rem;
    }
    .sidebar-shell .control-label {
      font-weight: 650;
      margin-bottom: 0.3rem;
      line-height: 1.2;
      color: #22313a;
    }
    .sidebar-shell .form-control,
    .sidebar-shell .form-select {
      height: 40px;
      font-size: 1rem;
      padding: 6px 10px;
      border-radius: 8px;
    }
    .sidebar-shell input[type=number] {
      font-variant-numeric: tabular-nums;
      letter-spacing: 0.1px;
    }
    .sidebar-shell input[type=number]::-webkit-outer-spin-button,
    .sidebar-shell input[type=number]::-webkit-inner-spin-button {
      -webkit-appearance: none;
      margin: 0;
    }
    .sidebar-shell input[type=number] {
      -moz-appearance: textfield;
    }
    .input-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      column-gap: 12px;
      row-gap: 8px;
      align-items: end;
    }
    .input-grid .shiny-input-container {
      margin-bottom: 0.2rem;
    }
    .weight-section-title {
      font-weight: 750;
      margin-top: 0.2rem;
      margin-bottom: 0.35rem;
      color: #1f2c34;
    }
    @media (max-width: 1250px) {
      .input-grid {
        grid-template-columns: 1fr;
      }
    }
    .sidebar-title {
      margin-bottom: 10px;
      padding: 10px 12px;
      border-radius: 12px;
      background: linear-gradient(120deg, rgba(15,122,95,0.15), rgba(197,122,31,0.15));
    }
    .sidebar-title h5 {
      margin: 0;
      letter-spacing: .2px;
      font-weight: 700;
    }
    .sidebar-title p {
      margin: 4px 0 0;
      font-size: 0.85rem;
      color: #2f3d46;
    }
    .run-actions .btn {
      width: 100%;
      margin-bottom: 8px;
      border-radius: 10px;
      font-weight: 650;
    }
    .run-actions .btn-primary {
      background: #0f7a5f;
      border-color: #0f7a5f;
    }
    .run-actions .btn-default {
      background: #f8f6ef;
      border-color: #d9d1bf;
      color: #22313a;
    }
    .metric-card {
      background: rgba(255,255,255,0.88);
      border: 1px solid #dcd5c4;
      border-radius: 14px;
      box-shadow: 0 8px 20px rgba(18,28,35,0.05);
    }
    .table-card {
      margin-top: 12px;
      border-radius: 14px;
      border: 1px solid #dcd5c4;
      box-shadow: 0 8px 20px rgba(18,28,35,0.05);
      background: rgba(255,255,255,0.88);
    }
    .table-card .card-header {
      font-weight: 700;
      color: #1f2c34;
      background: rgba(255,255,255,0.6);
      border-bottom: 1px solid #e3decf;
    }
    .table-tools {
      margin-top: 10px;
      margin-bottom: 8px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .table-tools .btn {
      border-radius: 9px;
      font-weight: 650;
    }
    .status-shell {
      margin-top: 8px;
      font-size: 0.9rem;
      color: #2c3a43;
      background: #f8f6ef;
      border: 1px solid #d9d1bf;
      border-radius: 10px;
      padding: 8px 10px;
    }
    .nav-tabs .nav-link {
      font-weight: 650;
      color: #2f414b;
      border-radius: 10px 10px 0 0;
    }
    .nav-tabs .nav-link.active {
      color: #0f7a5f;
      border-color: #d9d1bf #d9d1bf #fff;
    }
    table.dataTable thead th {
      background: #f4efe3;
      color: #22313a;
      border-bottom: 1px solid #d9d1bf !important;
      vertical-align: middle !important;
      white-space: nowrap;
    }
    table.dataTable thead > tr:first-child > th {
      padding-top: 10px !important;
      padding-bottom: 10px !important;
    }
    table.dataTable thead > tr.dt-custom-filter > th {
      background: #f9f6ee !important;
      padding: 6px 8px !important;
      border-top: 1px solid #e3ddce !important;
      border-bottom: 1px solid #d9d1bf !important;
    }
    table.dataTable thead > tr.dt-custom-filter > th::before,
    table.dataTable thead > tr.dt-custom-filter > th::after {
      display: none !important;
    }
    table.dataTable thead > tr.dt-custom-filter input {
      width: 100% !important;
      min-width: 68px;
      height: 34px;
      border: 1px solid #a9b0b6;
      border-radius: 7px;
      padding: 5px 8px;
      font-size: 0.98rem;
      background: #fff;
    }
    table.dataTable thead > tr:first-child > th.sorting,
    table.dataTable thead > tr:first-child > th.sorting_asc,
    table.dataTable thead > tr:first-child > th.sorting_desc {
      padding-right: 24px !important;
    }
    table.dataTable thead > tr:first-child > th.sorting::before,
    table.dataTable thead > tr:first-child > th.sorting_asc::before,
    table.dataTable thead > tr:first-child > th.sorting_desc::before {
      top: 52% !important;
      right: 8px !important;
      transform: translateY(-70%);
    }
    table.dataTable thead > tr:first-child > th.sorting::after,
    table.dataTable thead > tr:first-child > th.sorting_asc::after,
    table.dataTable thead > tr:first-child > th.sorting_desc::after {
      top: 52% !important;
      right: 8px !important;
      transform: translateY(-30%);
    }
    .shiny-notification {
      font-weight: 650;
      border-radius: 10px;
    }
  ")),
    tags$script(HTML("
      (function() {
        if (window.__dtRangeFilterInstalled) return;
        window.__dtRangeFilterInstalled = true;
        window.__dtRangeFilterTables = {};

        function normalizeCell(cell) {
          if (cell === null || cell === undefined) return '';
          return String(cell).replace(/<[^>]*>/g, '').trim();
        }

        function parseNumber(txt) {
          if (txt === null || txt === undefined) return NaN;
          var cleaned = String(txt)
            .replace(/,/g, '')
            .replace(/[%$]/g, '')
            .trim();
          return parseFloat(cleaned);
        }

        function inferNumericColumn(api, colIdx) {
          var values = api.column(colIdx, { search: 'none' }).data().toArray();
          var checked = 0;
          for (var i = 0; i < values.length && checked < 40; i++) {
            var cell = normalizeCell(values[i]);
            if (!cell) continue;
            checked += 1;
            if (isNaN(parseNumber(cell))) return false;
          }
          return checked > 0;
        }

        function tableConfig(settings) {
          var tableId = settings.sTableId;
          if (!tableId) return null;
          return window.__dtRangeFilterTables[tableId] || null;
        }

        $.fn.dataTable.ext.search.push(function(settings, data) {
          var cfg = tableConfig(settings);
          if (!cfg) return true;

          var api = new $.fn.dataTable.Api(settings);
          var $container = $(api.table().container());
          var $inputs = $container.find('table.dataTable thead tr.dt-custom-filter input');
          if ($inputs.length === 0) return true;

          for (var i = 0; i < $inputs.length; i++) {
            var raw = (($inputs[i].value || '') + '').trim();
            if (!raw) continue;

            var cellText = normalizeCell(data[i]);
            var isNumericCol = !!cfg.numericCols[i];
            var rangeMatch = raw.match(/^(-?\\d+)\\s*-\\s*(-?\\d+)$/);
            var intMatch = raw.match(/^-?\\d+$/);

            if (isNumericCol && (rangeMatch || intMatch)) {
              var num = parseNumber(cellText);
              if (isNaN(num)) return false;

              if (rangeMatch) {
                var left = parseInt(rangeMatch[1], 10);
                var right = parseInt(rangeMatch[2], 10);
                var minv = Math.min(left, right);
                var maxv = Math.max(left, right);
                if (num < minv || num > maxv) return false;
              } else {
                var expected = parseInt(intMatch[1], 10);
                if (Math.round(num) !== expected) return false;
              }
            } else {
              if (cellText.toLowerCase().indexOf(raw.toLowerCase()) === -1) return false;
            }
          }
          return true;
        });

        window.enableDtRangeFilter = function(api) {
          var tableId = api.table().node().id;
          if (!tableId) return;

          var numericCols = [];
          var colCount = api.columns().count();
          for (var i = 0; i < colCount; i++) {
            numericCols.push(inferNumericColumn(api, i));
          }
          window.__dtRangeFilterTables[tableId] = { numericCols: numericCols };

          var $table = $(api.table().node());
          var $thead = $table.find('thead');
          $thead.find('tr.dt-custom-filter').remove();

          var $row = $('<tr class=\"dt-custom-filter\"></tr>');
          for (var c = 0; c < colCount; c++) {
            var $th = $('<th></th>');
            var $input = $('<input type=\"search\" placeholder=\"All\" style=\"width:100%;\" />');
            $input.attr('data-col', c);
            if (numericCols[c]) {
              $input.attr('inputmode', 'numeric');
              $input.attr('title', 'Use whole number or range (example: 30-140)');
              $input.attr('placeholder', 'e.g. 30-140');
            } else {
              $input.attr('title', 'Text contains match');
            }
            $th.append($input);
            $th.removeClass('sorting sorting_asc sorting_desc sorting_disabled');
            $th.attr('tabindex', '-1');
            $row.append($th);
          }
          $thead.append($row);

          var $container = $(api.table().container());
          var $inputs = $container.find('table.dataTable thead tr.dt-custom-filter input');
          $inputs.off();
          $inputs.on('keyup change', function() {
            api.draw();
          });
        };

        Shiny.addCustomMessageHandler('dt-range-filter-clear', function(payload) {
          var tableId = payload && payload.id;
          if (!tableId) return;
          var $tbl = $('#' + tableId);
          if ($tbl.length === 0 || !$.fn.DataTable.isDataTable($tbl)) return;
          var api = $tbl.DataTable();
          var $container = $(api.table().container());
          $container.find('table.dataTable thead tr.dt-custom-filter input').val('');
          api.search('');
          api.columns().search('');
          api.draw();
        });
      })();
    "))
  ),
  class = "app-shell",
  sidebar = sidebar(
    id = "controls_sidebar",
    width = "420px",
    open = "desktop",
    class = "sidebar-shell",
    div(
      class = "sidebar-title",
      tags$h5("Control Center"),
      tags$p("Tune settings, run refresh, and inspect outputs across hitters, pitchers, and ADP.")
    ),
    div(
      class = "run-actions",
      actionButton("run_model", "Run Full Refresh", class = "btn-primary"),
      actionButton("load_cached", "Load Latest Cached"),
      actionButton("reset_table_filters", "Reset Table Filters"),
      downloadButton("download_output", "Download Hitters CSV"),
      downloadButton("download_pitchers", "Download Pitchers CSV")
    ),
    accordion(
      id = "control_sections",
      open = c("league_budget", "hitter_proj", "adp_pull"),
      accordion_panel(
        value = "league_budget",
        title = "League & Budget",
        numericInput("season", "Season", value = as.integer(format(Sys.Date(), "%Y")), min = 2020, step = 1),
        selectInput("num_teams", "League Teams", choices = c(10, 12, 15), selected = 15),
        numericInput("pa_floor", "PA Floor", value = 200, min = 0, step = 10),
        numericInput("total_budget", "Total Budget", value = 260, min = 1, step = 1),
        numericInput("hitter_budget_share", "Hitter Budget Share", value = 0.70, min = 0.01, max = 0.99, step = 0.01),
        numericInput("min_bid", "Minimum Bid", value = 1, min = 0, step = 1)
      ),
      accordion_panel(
        value = "hitter_proj",
        title = "Hitter Projection Inputs",
        checkboxGroupInput(
          "systems",
          "Projection Systems",
          choices = c("BATX" = "batx", "Steamer" = "steamer", "OOPSY" = "oopsy", "ATC" = "atc"),
          selected = c("batx", "steamer", "oopsy", "atc")
        ),
        div(class = "weight-section-title", "Projection Weights"),
        div(
          class = "input-grid",
          numericInput("w_batx", "BATX", value = 3, min = 0, step = 0.1),
          numericInput("w_steamer", "Steamer", value = 2, min = 0, step = 0.1),
          numericInput("w_oopsy", "OOPSY", value = 3, min = 0, step = 0.1),
          numericInput("w_atc", "ATC", value = 1, min = 0, step = 0.1)
        ),
        div(class = "weight-section-title", "Stat Category Weights"),
        div(
          class = "input-grid",
          numericInput("sw_pa", "PA", value = 2.3, min = 0, step = 0.05),
          numericInput("sw_hr", "HR", value = 1.35, min = 0, step = 0.05),
          numericInput("sw_sb", "SB", value = 1.0, min = 0, step = 0.05),
          numericInput("sw_r", "R", value = 0.6, min = 0, step = 0.05),
          numericInput("sw_rbi", "RBI", value = 0.6, min = 0, step = 0.05),
          numericInput("sw_h", "H", value = 1.0, min = 0, step = 0.05)
        )
      ),
      accordion_panel(
        value = "pitcher_model",
        title = "Pitcher Model Inputs",
        selectInput(
          "pitcher_projection_system",
          "Pitcher Proj System (Stats Display)",
          choices = c("OOPSY" = "oopsy", "ATC" = "atc", "THEBAT" = "thebat", "Steamer" = "steamer", "ZiPS" = "zips"),
          selected = "oopsy"
        )
      ),
      accordion_panel(
        value = "adp_pull",
        title = "ADP Pull Settings",
        selectInput("adp_draft_type", "ADP Draft Type", choices = adp_draft_type_choices, selected = ""),
        selectInput(
          "adp_num_teams",
          "ADP Number of Teams",
          choices = c("All Teams" = "", "10" = "10", "12" = "12", "15" = "15"),
          selected = ""
        ),
        numericInput("adp_lookback_days", "ADP Lookback Days", value = 14, min = 1, step = 1),
        checkboxInput("use_local_adp", "Use local ADP.tsv if present", value = TRUE)
      )
    ),
    div(class = "status-shell", textOutput("status"))
  ),
  navset_tab(
    nav_panel(
      "Hitter Projection",
      summary_strip_ui("hit"),
      card(
        class = "table-card",
        card_header("Hitter Output"),
        navset_card_tab(
          nav_panel("Full Output", DTOutput("full_table")),
          nav_panel("Proj Stats Only", DTOutput("proj_table")),
          nav_panel("Z-Score View", DTOutput("z_table"))
        )
      )
    ),
    nav_panel(
      "SP Skillz",
      summary_strip_ui("sp"),
      card(
        class = "table-card",
        card_header("Starting Pitcher Skill Model"),
        DTOutput("pitcher_sp_skillz_table")
      )
    ),
    nav_panel(
      "SP Rank Overview",
      summary_strip_ui("overview"),
      card(
        class = "table-card",
        card_header("Integrated SP Rankings"),
        DTOutput("pitcher_table")
      )
    ),
    nav_panel(
      "ADP",
      summary_strip_ui("adp"),
      card(
        class = "table-card",
        card_header("ADP Board"),
        div(
          class = "table-tools",
          actionButton("adp_all_hitters", "All Hitters"),
          actionButton("adp_select_all", "Select All"),
          actionButton("adp_deselect_all", "Deselect All")
        ),
        checkboxGroupInput(
          "adp_pos_filter",
          "Position Filter",
          choices = adp_position_filter_choices,
          selected = c("C", "1B", "2B", "3B", "SS", "OF", "UT", "CI", "MI", "P"),
          inline = TRUE
        ),
        DTOutput("adp_table")
      ),
    ),
    nav_panel(
      "Run Data",
      summary_strip_ui("run"),
      card(
        class = "table-card",
        card_header("Run Metadata"),
        DTOutput("run_data_table")
      )
    )
  )
)

server <- function(input, output, session) {
  cfg <- reactiveVal(load_pipeline_config(file.path("config", "pipeline.yml")))

  rv <- reactiveValues(
    output_data = NULL,
    pitcher_sp_skillz = NULL,
    pitcher_data = NULL,
    adp_data = NULL,
    run_data = NULL,
    status = "Ready."
  )

  observeEvent(input$toggle_sidebar_btn, {
    bslib::sidebar_toggle("controls_sidebar", session = session)
  })

  observeEvent(input$load_cached, {
    cached <- read_cached_output(cfg(), season = input$season)
    if (is.null(cached) || is.null(cached$data)) {
      rv$status <- sprintf("No cached output found for season %s.", input$season)
      return()
    }

    rv$output_data <- drop_match_flag_columns(swap_adp_team_columns(cached$data))
    rv$pitcher_sp_skillz <- add_pitcher_role_sp_skillz(read_cached_pitcher_sp_skillz(cfg()))
    rv$pitcher_data <- add_pitcher_role_integrated(read_cached_pitcher_output(cfg(), season = input$season))
    rv$adp_data <- read_cached_adp_output(cfg(), season = input$season)
    rv$run_data <- read_cached_run_data(cfg(), season = input$season)

    rv$status <- sprintf("Loaded cached output for season %s (%s rows).", input$season, nrow(rv$output_data))
  })

  observeEvent(input$run_model, {
    if (length(input$systems) == 0) {
      showNotification("Select at least one projection system.", type = "error")
      rv$status <- "No systems selected."
      return()
    }

    projection_weights <- c(
      batx = as.numeric(input$w_batx),
      steamer = as.numeric(input$w_steamer),
      oopsy = as.numeric(input$w_oopsy),
      atc = as.numeric(input$w_atc)
    )

    category_weights <- c(
      pa = as.numeric(input$sw_pa),
      hr = as.numeric(input$sw_hr),
      sb = as.numeric(input$sw_sb),
      r = as.numeric(input$sw_r),
      rbi = as.numeric(input$sw_rbi),
      h = as.numeric(input$sw_h)
    )

    starter_count <- as.integer(as.numeric(input$num_teams) * sum(cfg()$projection$league$hitter_slots, na.rm = TRUE))

    adp_local <- ""
    configured_local_adp <- cfg()$adp$local_tsv
    if (isTRUE(input$use_local_adp) && nzchar(configured_local_adp) && file.exists(configured_local_adp)) {
      adp_local <- configured_local_adp
    }

      inputs_used <- list(
        season = as.integer(input$season),
        systems = input$systems,
        pitcher_projection_system = as.character(input$pitcher_projection_system),
        projection_weights = projection_weights[input$systems],
        category_weights = category_weights,
        pitcher_use_ip_paradigms = isTRUE(cfg()$pitcher$sp_skillz$use_ip_paradigms),
      pitcher_low_ip_max_start_ip = as.numeric(cfg()$pitcher$sp_skillz$low_ip_max_start_ip),
      pitcher_high_ip_min_start_ip = as.numeric(cfg()$pitcher$sp_skillz$high_ip_min_start_ip),
      pitcher_gs_fallback_threshold = as.numeric(cfg()$pitcher$sp_skillz$gs_fallback_threshold),
      pitcher_reliability_method = as.character(cfg()$pitcher$sp_skillz$reliability_method),
      pitcher_weights = cfg()$pitcher$sp_skillz$weights,
      pitcher_low_ip_weights = cfg()$pitcher$sp_skillz$low_ip_weights,
      pitcher_high_ip_weights = cfg()$pitcher$sp_skillz$high_ip_weights,
      pitcher_stabilization_points = cfg()$pitcher$sp_skillz$stabilization_points,
      pa_floor = as.numeric(input$pa_floor),
      num_teams = as.integer(input$num_teams),
      starter_count = starter_count,
      total_budget = as.numeric(input$total_budget),
      hitter_budget_share = as.numeric(input$hitter_budget_share),
      min_bid = as.numeric(input$min_bid),
      adp_draft_type = suppressWarnings(as.integer(input$adp_draft_type)),
      adp_num_teams = suppressWarnings(as.integer(input$adp_num_teams)),
      adp_lookback_days = as.integer(input$adp_lookback_days)
    )

    rv$status <- "Running model..."
    dir.create(cfg()$paths$raw_dir, recursive = TRUE, showWarnings = FALSE)
    dir.create(cfg()$paths$processed_dir, recursive = TRUE, showWarnings = FALSE)
    adp_raw_path <- file.path(cfg()$paths$raw_dir, sprintf("%s_nfbc_adp.tsv", as.integer(input$season)))

    withProgress(message = "Refreshing projections", value = 0.1, {
      result <- tryCatch(
        fetch_all_systems(
          systems = input$systems,
          season = as.integer(input$season),
          weights = projection_weights,
          pa_floor = as.numeric(input$pa_floor),
          category_weights = category_weights,
          starter_count = starter_count,
          starter_rank_metric = cfg()$projection$starter_rank_metric,
          num_teams = as.integer(input$num_teams),
          total_budget = as.numeric(input$total_budget),
          hitter_budget_share = as.numeric(input$hitter_budget_share),
          min_bid = as.numeric(input$min_bid),
          adp_local_tsv_path = adp_local,
          adp_fill_missing_with_max = isTRUE(cfg()$adp$fill_missing_with_max),
          adp_lookback_days = as.integer(input$adp_lookback_days),
          adp_draft_type = suppressWarnings(as.integer(input$adp_draft_type)),
          adp_num_teams = suppressWarnings(as.integer(input$adp_num_teams)),
          adp_raw_output_path = adp_raw_path,
          player_match_overrides_path = cfg()$paths$match_overrides_csv,
          quality = cfg()$quality
        ),
        error = function(e) e
      )

      incProgress(0.9)

      if (inherits(result, "error")) {
        rv$status <- sprintf("Run failed: %s", conditionMessage(result))
        showNotification(conditionMessage(result), type = "error", duration = NULL)
        return()
      }

      rv$output_data <- drop_match_flag_columns(swap_adp_team_columns(result$z_scored_aggregate_projection_output))
      rv$adp_data <- if (file.exists(adp_raw_path)) {
        prepare_adp_table(tryCatch(utils::read.delim(adp_raw_path, stringsAsFactors = FALSE, check.names = FALSE), error = function(e) NULL))
      } else {
        prepare_adp_table(result$nfbc_adp)
      }
      rv$run_data <- build_run_data_rows(result, inputs_used)
      rv$status <- sprintf(
        "Run complete: %s hitters ranked | ADP source: %s",
        nrow(rv$output_data),
        coalesce_chr(result$nfbc_adp_source, fallback = "unknown")
      )

      out_path <- file.path(cfg()$paths$processed_dir, sprintf("%s_hitters_z_scored_aggregate_projection_output.csv", as.integer(input$season)))
      dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
      utils::write.csv(rv$output_data, out_path, row.names = FALSE, na = "")
      run_data_snapshot_path <- file.path(cfg()$paths$processed_dir, sprintf("%s_run_data_snapshot.csv", as.integer(input$season)))
      utils::write.csv(rv$run_data, run_data_snapshot_path, row.names = FALSE, na = "")

      pitcher_args <- c(
        "scripts/build_pitcher_integration_table.R",
        "--config", file.path("config", "pipeline.yml"),
        "--season", as.character(as.integer(input$season)),
        "--pitcher-system", as.character(input$pitcher_projection_system)
      )
      pitcher_cmd <- tryCatch(
        system2("Rscript", pitcher_args, stdout = TRUE, stderr = TRUE),
        error = function(e) e
      )

      pitcher_ok <- TRUE
      if (inherits(pitcher_cmd, "error")) {
        pitcher_ok <- FALSE
      } else {
        cmd_status <- attr(pitcher_cmd, "status")
        if (!is.null(cmd_status) && as.integer(cmd_status) != 0) {
          pitcher_ok <- FALSE
        }
      }

      if (pitcher_ok) {
        rv$pitcher_sp_skillz <- add_pitcher_role_sp_skillz(read_cached_pitcher_sp_skillz(cfg()))
        rv$pitcher_data <- add_pitcher_role_integrated(read_cached_pitcher_output(cfg(), season = input$season))
      } else {
        rv$pitcher_sp_skillz <- NULL
        rv$pitcher_data <- NULL
        showNotification("Pitcher table refresh failed. Hitter output was still updated.", type = "warning", duration = 8)
      }

      pitcher_rows <- if (is.null(rv$pitcher_data)) 0L else nrow(rv$pitcher_data)
      adp_rows <- if (is.null(rv$adp_data)) 0L else nrow(rv$adp_data)
      rv$status <- sprintf(
        "Run complete: %s hitters ranked | %s pitchers integrated | %s ADP rows | ADP source: %s",
        nrow(rv$output_data),
        pitcher_rows,
        adp_rows,
        coalesce_chr(result$nfbc_adp_source, fallback = "unknown")
      )

      })
  })

  dt_opts <- list(
    pageLength = 30,
    lengthMenu = list(
      c(30, 50, 100, 200, -1),
      c("30", "50", "100", "200", "Infinity")
    ),
    scrollX = TRUE,
    orderCellsTop = TRUE,
    autoWidth = TRUE,
    deferRender = TRUE
  )
  dt_callback <- JS("if (window.enableDtRangeFilter) { window.enableDtRangeFilter(table); }")

  filtered_adp <- reactive({
    dat <- rv$adp_data
    req(dat)
    selected <- input$adp_pos_filter
    if (is.null(selected) || length(selected) == 0) {
      return(dat[0, , drop = FALSE])
    }
    if (!"positions" %in% names(dat)) {
      return(dat)
    }
    keep <- row_has_any_position(dat$positions, toupper(selected))
    dat[keep, , drop = FALSE]
  })

  observeEvent(input$adp_select_all, {
    updateCheckboxGroupInput(
      session,
      "adp_pos_filter",
      selected = unname(adp_position_filter_choices)
    )
  })

  observeEvent(input$adp_all_hitters, {
    updateCheckboxGroupInput(
      session,
      "adp_pos_filter",
      selected = adp_hitter_filter_set
    )
  })

  observeEvent(input$adp_deselect_all, {
    updateCheckboxGroupInput(
      session,
      "adp_pos_filter",
      selected = character(0)
    )
  })

  clear_dt_filters <- function(output_id, dat) {
    if (is.null(dat) || !is.data.frame(dat)) {
      return(invisible(NULL))
    }
    proxy <- dataTableProxy(outputId = output_id)
    updateSearch(proxy, keywords = list(global = "", columns = rep("", ncol(dat))))
    session$sendCustomMessage("dt-range-filter-clear", list(id = output_id))
    invisible(NULL)
  }

  observeEvent(input$reset_table_filters, {
    clear_dt_filters("full_table", rv$output_data)

    proj_dat <- if (is.null(rv$output_data)) NULL else {
      keep <- !grepl("_z$", names(rv$output_data)) & !(names(rv$output_data) %in% c("z_total_raw", "z_total_weights"))
      rv$output_data[, keep, drop = FALSE]
    }
    clear_dt_filters("proj_table", proj_dat)

    z_dat <- if (is.null(rv$output_data)) NULL else {
      id_cols <- intersect(c("player_name", "position", "adp", "team"), names(rv$output_data))
      z_cols <- names(rv$output_data)[grepl("_z$", names(rv$output_data)) | names(rv$output_data) %in% c("z_total_raw", "z_total_weights")]
      rv$output_data[, unique(c(id_cols, z_cols)), drop = FALSE]
    }
    clear_dt_filters("z_table", z_dat)

    clear_dt_filters("pitcher_sp_skillz_table", rv$pitcher_sp_skillz)
    clear_dt_filters("pitcher_table", rv$pitcher_data)
    clear_dt_filters("adp_table", filtered_adp())

    showNotification("Table filters reset.", type = "message", duration = 3)
  })

  summary_counts_ui <- reactive({
    hitters_n <- if (is.null(rv$output_data)) 0L else nrow(rv$output_data)
    sp_skillz_n <- if (is.null(rv$pitcher_sp_skillz)) 0L else nrow(rv$pitcher_sp_skillz)
    integrated_n <- if (is.null(rv$pitcher_data)) 0L else nrow(rv$pitcher_data)
    adp_n <- if (is.null(rv$adp_data)) 0L else nrow(rv$adp_data)
    tags$div(
      tags$div(tags$strong("Hitters: "), format(hitters_n, big.mark = ",")),
      tags$div(tags$strong("Pitcher SP Skillz: "), format(sp_skillz_n, big.mark = ",")),
      tags$div(tags$strong("Integrated SP Rankings: "), format(integrated_n, big.mark = ",")),
      tags$div(tags$strong("ADP Players: "), format(adp_n, big.mark = ","))
    )
  })

  run_metric <- reactive({
    dat <- rv$run_data
    function(metric, default = "") {
      if (is.null(dat) || !all(c("Metric", "Value") %in% names(dat))) {
        return(default)
      }
      idx <- which(as.character(dat$Metric) == metric)
      if (length(idx) == 0) return(default)
      value <- as.character(dat$Value[idx[1]])
      if (!nzchar(value) || identical(value, "NA")) return(default)
      value
    }
  })

  summary_snapshot_ui <- reactive({
    m <- run_metric()
    tags$div(
      tags$div(tags$strong("Season: "), m("Season", default = "")),
      tags$div(tags$strong("Systems: "), m("Systems Used", default = "")),
      tags$div(tags$strong("Pitcher System: "), m("Projection System (Stats Display)", default = "")),
      tags$div(tags$strong("ADP Draft Type: "), m("Draft Type", default = ""))
    )
  })

  summary_filters_ui <- reactive({
    m <- run_metric()
    adp_from <- m("Date From", default = "")
    adp_to <- m("Date To", default = "")
    run_dat <- rv$run_data
    has_run <- !is.null(run_dat) && nrow(run_dat) > 0
    adp_window <- if (nzchar(adp_from) && nzchar(adp_to)) sprintf("%s to %s", adp_from, adp_to) else ""
    tags$div(
      tags$div(tags$strong("League Teams: "), m("Teams", default = "")),
      tags$div(tags$strong("PA Floor: "), m("PA Floor", default = "")),
      tags$div(tags$strong("Starter Count: "), m("Starter Count", default = "")),
      tags$div(tags$strong("ADP Team Filter: "), m("Number of Teams", default = "")),
      tags$div(tags$strong("ADP Window: "), ifelse(has_run, adp_window, ""))
    )
  })

  bind_summary_outputs <- function(suffix) {
    output[[paste0("summary_snapshot_", suffix)]] <- renderUI(summary_snapshot_ui())
    output[[paste0("summary_counts_", suffix)]] <- renderUI(summary_counts_ui())
    output[[paste0("summary_filters_", suffix)]] <- renderUI(summary_filters_ui())
  }
  for (suffix in c("hit", "sp", "overview", "adp", "run")) {
    bind_summary_outputs(suffix)
  }

  output$full_table <- renderDT({
    req(rv$output_data)
    datatable(
      rv$output_data,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = dt_opts,
      callback = dt_callback
    )
  })

  output$proj_table <- renderDT({
    req(rv$output_data)
    dat <- rv$output_data
    keep <- !grepl("_z$", names(dat)) & !(names(dat) %in% c("z_total_raw", "z_total_weights"))
    dat <- dat[, keep, drop = FALSE]
    datatable(
      dat,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = dt_opts,
      callback = dt_callback
    )
  })

  output$z_table <- renderDT({
    req(rv$output_data)
    dat <- rv$output_data
    id_cols <- intersect(c("player_name", "position", "adp", "team"), names(dat))
    z_cols <- names(dat)[grepl("_z$", names(dat)) | names(dat) %in% c("z_total_raw", "z_total_weights")]
    dat <- dat[, unique(c(id_cols, z_cols)), drop = FALSE]
    datatable(
      dat,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = dt_opts,
      callback = dt_callback
    )
  })

  output$pitcher_table <- renderDT({
    req(rv$pitcher_data)
    dat <- rv$pitcher_data
    dat <- dat[, setdiff(names(dat), "pitcher_role"), drop = FALSE]
    for (nm in c("proj_ip", "proj_w", "proj_k")) {
      if (nm %in% names(dat)) {
        dat[[nm]] <- round(suppressWarnings(as.numeric(dat[[nm]])), 1)
      }
    }
    col_map <- c(
      player_name = "Name",
      team = "Team",
      adp = "ADP",
      adp_rank = "ADP Rk",
      proj_ip = "Proj IP",
      proj_w = "Proj W",
      proj_sv = "Proj SV",
      proj_k = "Proj K",
      proj_era = "Proj ERA",
      proj_whip = "Proj WHIP",
      sp_skillz_2025_score_stabilized = "SP25 Score",
      sp_skillz_2025_rank_stabilized = "SP25 Rank",
      sp_skillz_2026_score_stabilized = "SP26 Score",
      sp_skillz_2026_rank_stabilized = "SP26 Rank",
      eno_rank = "Eno",
      ck_rank = "CK",
      ds_rank = "DS"
    )
    keep <- intersect(names(col_map), names(dat))
    dat <- dat[, keep, drop = FALSE]
    names(dat) <- unname(col_map[keep])
    table_opts <- dt_opts
    skillz_idx <- which(names(dat) %in% c("SP25 Score", "SP25 Rank", "SP26 Score", "SP26 Rank")) - 1L
    metric_idx <- which(names(dat) %in% c("ADP Rk", "Proj IP", "Proj W", "Proj SV", "Proj K", "Proj ERA", "Proj WHIP")) - 1L
    width_defs <- list()
    if (length(skillz_idx) > 0) {
      width_defs <- c(width_defs, list(list(width = "86px", targets = skillz_idx)))
    }
    if (length(metric_idx) > 0) {
      width_defs <- c(width_defs, list(list(width = "74px", targets = metric_idx)))
    }
    if (length(width_defs) > 0) {
      table_opts$columnDefs <- width_defs
    }
    datatable(
      dat,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = table_opts,
      callback = dt_callback
    )
  })

  output$pitcher_sp_skillz_table <- renderDT({
    req(rv$pitcher_sp_skillz)
    dat <- rv$pitcher_sp_skillz
    dat <- dat[, setdiff(names(dat), "pitcher_role"), drop = FALSE]
    col_map <- c(
      player_name = "Name",
      team = "Team",
      gs = "GS",
      ip = "IP",
      siera = "SIERA",
      xfip = "xFIP",
      k_minus_bb_pct = "K-BB%",
      contact_pct = "Contact%",
      csw_pct = "CSW%",
      ball_pct = "Ball%",
      stuff_plus = "Stuff+",
      pitching_plus = "Pitching+",
      sp_skillz_score = "SP Skillz Score",
      sp_skillz_rank = "SP Skillz Rank",
      sp_skillz_score_stabilized = "SP Skillz Score Stabilized",
      sp_skillz_rank_stabilized = "SP Skillz Rank Stabilized",
      sp_skillz_reliability = "Reliability",
      sp_skillz_weight_profile = "Weight Profile"
    )
    keep <- intersect(names(col_map), names(dat))
    dat <- dat[, keep, drop = FALSE]
    numeric_round_map <- c(
      ip = 1L,
      siera = 3L,
      xfip = 3L,
      k_minus_bb_pct = 2L,
      contact_pct = 2L,
      csw_pct = 2L,
      ball_pct = 2L,
      stuff_plus = 2L,
      pitching_plus = 2L,
      sp_skillz_score = 2L,
      sp_skillz_rank = 2L,
      sp_skillz_score_stabilized = 2L,
      sp_skillz_rank_stabilized = 2L,
      sp_skillz_reliability = 2L
    )
    for (nm in names(numeric_round_map)) {
      if (nm %in% names(dat)) {
        dat[[nm]] <- round(suppressWarnings(as.numeric(dat[[nm]])), digits = numeric_round_map[[nm]])
      }
    }
    names(dat) <- unname(col_map[keep])
    datatable(
      dat,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = dt_opts,
      callback = dt_callback
    )
  })

  output$adp_table <- renderDT({
    dat <- filtered_adp()
    datatable(
      dat,
      filter = "none",
      rownames = FALSE,
      selection = "none",
      options = dt_opts,
      callback = dt_callback
    )
  })

  output$run_data_table <- renderDT({
    req(rv$run_data)
    datatable(
      rv$run_data,
      rownames = FALSE,
      selection = "none",
      options = list(dom = "t", ordering = FALSE, paging = FALSE),
      class = "compact"
    )
  })

  output$status <- renderText({
    rv$status
  })

  output$download_output <- downloadHandler(
    filename = function() {
      sprintf("%s_hitters_web_output.csv", as.integer(input$season))
    },
    content = function(file) {
      req(rv$output_data)
      utils::write.csv(rv$output_data, file, row.names = FALSE, na = "")
    }
  )

  output$download_pitchers <- downloadHandler(
    filename = function() {
      sprintf("%s_pitchers_web_output.csv", as.integer(input$season))
    },
    content = function(file) {
      req(rv$pitcher_data)
      utils::write.csv(rv$pitcher_data, file, row.names = FALSE, na = "")
    }
  )

  observe({
    if (is.null(rv$output_data)) {
      cached <- read_cached_output(cfg(), season = input$season)
      if (!is.null(cached) && !is.null(cached$data)) {
        rv$output_data <- drop_match_flag_columns(swap_adp_team_columns(cached$data))
        rv$status <- sprintf("Loaded cached output for season %s on startup.", input$season)
      }
    }
    if (is.null(rv$pitcher_data)) {
      rv$pitcher_data <- add_pitcher_role_integrated(read_cached_pitcher_output(cfg(), season = input$season))
    }
    if (is.null(rv$pitcher_sp_skillz)) {
      rv$pitcher_sp_skillz <- add_pitcher_role_sp_skillz(read_cached_pitcher_sp_skillz(cfg()))
    }
    if (is.null(rv$adp_data)) {
      rv$adp_data <- read_cached_adp_output(cfg(), season = input$season)
    }
    if (is.null(rv$run_data)) {
      rv$run_data <- read_cached_run_data(cfg(), season = input$season)
    }
  })
}

shinyApp(ui, server)
