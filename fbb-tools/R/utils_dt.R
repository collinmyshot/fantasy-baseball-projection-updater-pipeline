# Shared DataTables utilities — SP Skillz-style table template
#
# All tables using this template share:
#   - class "spz-dt" (no global width:100%!important — FixedHeader clones align)
#   - No scrollX / scrollY (single-table layout, page scrolls horizontally)
#   - FixedHeader extension for frozen header row
#   - Pagination with top + bottom controls
#   - "Page Size: N" length selector inline with page buttons

# Standard paginated dom: nav+sizer left, info right, above and below table
SPZ_DT_DOM <- paste0(
  "<'spz-ctrl-top'<'spz-ctrl-nav'<'spz-ctrl-pager'p><'spz-ctrl-sizer'l>>",
  "<'spz-ctrl-info'i>>",
  "t",
  "<'spz-ctrl-bot'<'spz-ctrl-nav'<'spz-ctrl-pager'p><'spz-ctrl-sizer'l>>",
  "<'spz-ctrl-info'i>>"
)

# Park Factors variant: no pagination chrome, just the table
SPZ_DT_DOM_SIMPLE <- "t"

# Base options shared by all SPZ-template tables.
# Pass extra = list(...) to override or add options.
spz_dt_options <- function(col_defs, order = list(list(0L, "asc")),
                            paginate = TRUE, extra = list()) {
  base <- list(
    dom           = if (paginate) SPZ_DT_DOM else SPZ_DT_DOM_SIMPLE,
    pagingType    = "full_numbers",
    pageLength    = 30L,
    lengthMenu    = list(c(30, 50, 100, -1), c("30", "50", "100", "All")),
    language      = list(lengthMenu = "Page Size: _MENU_"),
    autoWidth     = FALSE,
    fixedHeader   = TRUE,
    ordering      = TRUE,
    order         = order,
    columnDefs    = col_defs
  )
  modifyList(base, extra)
}

# Wrapper div for SPZ-template tables
spz_table_wrap <- function(...) {
  div(class = "spz-table-wrap", ...)
}
