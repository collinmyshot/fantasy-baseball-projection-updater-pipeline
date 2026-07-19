#!/usr/bin/env Rscript
# Generate the "Tracked Park Changes" appendix table for the methodology page
# from data/manual/park_era_events.csv (every row there carries primary
# sources) plus observed season ranges from the current build's era audit.
#
# Output: an HTML fragment (table only) to splice into
# www/methodology_park_factors.html in fbb-tools.

source(file.path("R", "utils.R"))

parsed <- parse_cli_args(list(
  events_csv = list(flag = "--events",  default = file.path("data", "manual", "park_era_events.csv")),
  pf_dir     = list(flag = "--pf-dir",  default = file.path("data", "processed", "park_factors")),
  out_html   = list(flag = "--out",     default = file.path("data", "processed", "park_factors", "park_era_appendix.html"))
))

events <- utils::read.csv(parsed$events_csv, stringsAsFactors = FALSE, check.names = FALSE)
audit <- utils::read.csv(file.path(parsed$pf_dir, "team_park_era_audit.csv"), stringsAsFactors = FALSE, check.names = FALSE)

# Observed season range per era suffix (team + suffix uniquely identify events here).
audit$suffix <- sub("^.*__", "", audit$park_era_id)
obs <- do.call(rbind, lapply(split(audit, paste(audit$home_team, audit$suffix)), function(d) {
  data.frame(
    team = d$home_team[1],
    suffix = d$suffix[1],
    obs_years = sprintf("%s-%s", min(d$season, na.rm = TRUE), max(d$season, na.rm = TRUE)),
    n_bbe = sum(d$n_bbe, na.rm = TRUE),
    stringsAsFactors = FALSE
  )
}))

norm_team <- function(x) {
  x <- toupper(trimws(x))
  map <- c(ARI = "AZ", KC = "KCR", TB = "TBR", OAK = "ATH", WAS = "WSH")
  hit <- x %in% names(map)
  x[hit] <- map[x[hit]]
  x
}
events$team_n <- norm_team(events$team)
events$suffix_n <- tolower(trimws(events$era_suffix))
events <- merge(
  events,
  obs,
  by.x = c("team_n", "suffix_n"),
  by.y = c("team", "suffix"),
  all.x = TRUE
)

pretty_type <- function(x) {
  x <- gsub("_", " ", as.character(x))
  paste0(toupper(substring(x, 1, 1)), substring(x, 2))
}

fmt_effective <- function(start_date, end_date) {
  sy <- substr(as.character(start_date), 1, 4)
  ey <- substr(as.character(end_date), 1, 4)
  out <- ifelse(nzchar(ey) & !is.na(end_date) & end_date != "", paste0(sy, " to ", ey), paste0(sy, " onward"))
  ifelse(sy == ey & nzchar(ey), sy, out)
}

# Notes come from the internal events file: strip editor annotations ("NOTE:"
# onward) and convert dash punctuation to comply with site style (no dashes).
sanitize_notes <- function(x) {
  x <- sub("\\s*NOTE:.*$", "", x)
  x <- gsub("—", "; ", x)                 # em dash
  x <- gsub("(\\d)–(\\d)", "\\1 to \\2", x) # en dash between numbers
  x <- gsub("–", "; ", x)                 # remaining en dashes
  x <- gsub("\\s+;", ";", x)
  x <- gsub(";\\s*;", ";", x)
  trimws(x)
}

esc <- function(x) {
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  gsub(">", "&gt;", x, fixed = TRUE)
}

events <- events[order(events$team_n, events$start_date), ]

rows <- vapply(seq_len(nrow(events)), function(i) {
  e <- events[i, ]
  src <- sprintf('<a href="%s">source</a>', e$source_primary)
  if (!is.na(e$source_secondary) && nzchar(e$source_secondary)) {
    src <- paste0(src, sprintf(', <a href="%s">source 2</a>', e$source_secondary))
  }
  obs_txt <- ifelse(is.na(e$obs_years), "not yet observed", e$obs_years)
  sprintf(
    "    <tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
    esc(e$team_n),
    esc(pretty_type(e$event_type)),
    fmt_effective(e$start_date, e$end_date),
    obs_txt,
    esc(sanitize_notes(e$notes)),
    src
  )
}, character(1))

html <- c(
  "<table>",
  "  <thead>",
  "    <tr><th>Team</th><th>Change type</th><th>Effective</th><th>Observed seasons</th><th>What changed</th><th>Sources</th></tr>",
  "  </thead>",
  "  <tbody>",
  rows,
  "  </tbody>",
  "</table>"
)

writeLines(html, parsed$out_html)
message(sprintf("Wrote appendix fragment with %s tracked changes: %s", nrow(events), parsed$out_html))
