#!/usr/bin/env bash
# dehashed_domain_search.sh (enhanced)
# DeHashed domain search helper with:
# - fixtures/dry-run and live mode
# - canonical column selection & header validation
# - CSV writing via jq @csv
# - pagination with --max-pages guard
# - retry with exponential backoff on 429/5xx
# - configurable timeouts
# - robust excerpt & breach count via Python helper (handles CSV quoting)
# - deterministic, ranked excerpt (prefer corporate domain if provided)
#
# Usage:
#   ./dehashed_domain_search.sh -d example.com -c out.csv [-C "col1,col2,..."] [-R]
#                               [-F fixtures_dir --dry-run]
#                               [--max-pages 1000] [--timeout 20] [--sleep 1]
#                               [--prefer-domain example.com]
#                               [-v]
#
# Env:
#   DEHASHED_USER, DEHASHED_KEY required in live mode. Optionally, a .env file in the same dir
#   with lines DEHASHED_USER=... and DEHASHED_KEY=... will be sourced automatically.
#
set -euo pipefail

# Auto-source .env if present
if [[ -f ".env" ]]; then
  # shellcheck disable=SC1091
  source ".env"
fi

domain=""
out=""
columns=""
resume=false
fixtures=""
dry=false
max_pages=100000
curl_timeout=30
sleep_s=1
prefer_domain=""
verbose=false

log() {
  if $verbose; then
    printf '%s\n' "$*" >&2
  fi
}

usage() {
  cat >&2 <<'USG'
Usage:
  dehashed_domain_search.sh -d <domain> -c <out.csv> [options]

Options:
  -d, --domain DOMAIN          Domain to search (e.g., example.com) [required]
  -c PATH                      Output CSV path [required]
  -C "c1,c2,..."               Explicit CSV columns (defaults to canonical set)
  -R                           Resume: do not rewrite header if out.csv exists
  -F, --fixtures DIR           Fixtures dir containing *.json (for --dry-run)
  --dry-run                    Use fixtures instead of live API
  --max-pages N                Max pages to fetch (default: 100000)
  --timeout SECONDS            curl timeout per request (default: 30)
  --sleep SECONDS              base sleep between pages/retries (default: 1)
  --prefer-domain DOMAIN       Prefer this email domain in the excerpt ranking
  -v                           Verbose logging to stderr
  -h, --help                   Show help
USG
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--domain) domain="${2:-}"; shift 2 ;;
    -c) out="${2:-}"; shift 2 ;;
    -C) columns="${2:-}"; shift 2 ;;
    -R) resume=true; shift ;;
    -F|--fixtures) fixtures="${2:-}"; shift 2 ;;
    --dry-run) dry=true; shift ;;
    --max-pages) max_pages="${2:-}"; shift 2 ;;
    --timeout) curl_timeout="${2:-}"; shift 2 ;;
    --sleep) sleep_s="${2:-}"; shift 2 ;;
    --prefer-domain) prefer_domain="${2:-}"; shift 2 ;;
    -v) verbose=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) log "Ignoring unknown arg: $1"; shift ;;
  esac
done

if [[ -z "$domain" || -z "$out" ]]; then
  usage; exit 2
fi

mkdir -p "$(dirname "$out")"

# Canonical columns (stable order). Matches previous script but curated.
canonical_cols="domain,email,username,first_name,last_name,name,password,hashed_password,hash,password_hash,ip,address,breach,source,created_at,updated_at"

# Validate/choose columns
if [[ -n "$columns" ]]; then
  cols="$columns"
else
  cols="$canonical_cols"
fi

# Header handling
hdr_written=false
if [[ -f "$out" && "$resume" == true ]]; then
  hdr_written=true
fi

write_header() {
  if ! $hdr_written; then
    IFS=, read -ra a <<<"$cols"
    (IFS=,; echo "${a[*]}") > "$out"
    hdr_written=true
    log "Wrote header to $out"
  fi
}

# jq program to serialize a row in requested column order safely as CSV
rowjq='
def row($cols):
  ($cols | split(",")) as $cs
  | [$cs[] as $c | (.[ $c ] // "")] | @csv;
'

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# ---- Fixtures mode ----
if [[ "$dry" == true ]]; then
  [[ -d "$fixtures" ]] || { echo "fixtures dir required with --dry-run" >&2; exit 3; }
  write_header
  shopt -s nullglob
  count_files=0
  for f in "$fixtures"/*.json; do
    count_files=$((count_files+1))
    log "Reading fixture: $f"
    jq -r --arg cols "$cols" "$rowjq [.entries[]] | .[] | row(\$cols)" "$f" >> "$out"
  done
  shopt -u nullglob
  log "Processed $count_files fixture file(s)."

  # Robust excerpt + breach count via Python helper
  python3 "$(dirname "$0")/dehashed_excerpt.py" --csv "$out" --limit 10 ${prefer_domain:+--prefer-domain "$prefer_domain"}
  # Row count (excluding header)
  rows=$(( $(wc -l < "$out") - 1 ))
  log "Wrote $rows rows to $out"
  exit 0
fi

# ---- Live mode ----
: "${DEHASHED_USER:?set DEHASHED_USER}"
: "${DEHASHED_KEY:?set DEHASHED_KEY}"

write_header

page=1
pages_fetched=0

while (( page <= max_pages )); do
  log "Fetching page $page"

  http_code=0
  attempt=0
  max_attempts=6
  backoff="$sleep_s"

  while : ; do
    attempt=$((attempt+1))
    # Use curl to capture HTTP code and write body to tmp
    http_code=$(curl -sS -u "$DEHASHED_USER:$DEHASHED_KEY" -G "https://api.dehashed.com/search" \
      --data-urlencode "query=domain:$domain" \
      --data-urlencode "page=$page" \
      --data-urlencode "size=500" \
      --data-urlencode "expand=true" \
      --max-time "$curl_timeout" \
      -w "%{http_code}" -o "$tmp" || echo "000")

    if [[ "$http_code" =~ ^2 ]]; then
      break
    fi

    if [[ "$http_code" == "404" ]]; then
      echo "ERROR: 404 Not Found (check domain or credentials)" >&2
      exit 4
    fi

    if [[ "$http_code" == "401" || "$http_code" == "403" ]]; then
      echo "ERROR: Auth failed (check DEHASHED_USER/DEHASHED_KEY)" >&2
      exit 5
    fi

    if (( attempt >= max_attempts )); then
      echo "ERROR: HTTP $http_code after $attempt attempts. Aborting." >&2
      exit 6
    fi

    # 429/5xx backoff
    log "HTTP $http_code – retry $attempt/$max_attempts after ${backoff}s"
    sleep "$backoff"
    # exponential backoff with jitter (0-250ms)
    jitter_ms=$(( RANDOM % 250 ))
    backoff=$(awk -v b="$backoff" -v j="$jitter_ms" 'BEGIN { printf "%.3f", b*2 + (j/1000.0) }')
  done

  # Parse count
  count=$(jq '.entries | length' "$tmp" 2>/dev/null || echo 0)
  if [[ "$count" -le 0 ]]; then
    log "No entries on page $page. Stopping."
    break
  fi

  jq -r --arg cols "$cols" "$rowjq [.entries[]] | .[] | row(\$cols)" "$tmp" >> "$out"
  pages_fetched=$((pages_fetched+1))
  page=$((page+1))
  sleep "$sleep_s"
done

log "Fetched $pages_fetched page(s)."

# Robust excerpt + breach count via Python helper (text to stdout)
python3 "$(dirname "$0")/dehashed_excerpt.py" --csv "$out" --limit 10 ${prefer_domain:+--prefer-domain "$prefer_domain"}

# Row count (excluding header) to stderr for logging
rows=$(( $(wc -l < "$out") - 1 ))
log "Wrote $rows rows to $out"
