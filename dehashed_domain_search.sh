#!/usr/bin/env bash
# dehashed_domain_search.sh
# Minimal DeHashed domain search helper with fixtures, resume, selectable columns,
# CSV header handling, pagination, and a "Breached Databases: N" summary.
#
# Usage:
#   dehashed_domain_search.sh -d <domain> -c <out.csv> [-C "c1,c2,..."] [-R] [-F fixtures_dir --dry-run]
#
# Env (live mode):
#   DEHASHED_USER, DEHASHED_KEY

set -euo pipefail

domain=""
out=""
columns=""
resume=false
fixtures=""
dry=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -d|--domain) domain="$2"; shift 2 ;;
    -c) out="$2"; shift 2 ;;
    -C) columns="$2"; shift 2 ;;
    -R) resume=true; shift ;;
    -F|--fixtures) fixtures="$2"; shift 2 ;;
    --dry-run) dry=true; shift ;;
    -h|--help)
      echo "Usage: $0 -d <domain> -c <out.csv> [-C \"c1,c2,...\"] [-R] [-F fixtures_dir --dry-run]"
      exit 0
      ;;
    *) shift ;; # ignore unknown flags (runner passes extras to module sometimes)
  esac
done

[[ -n "$domain" && -n "$out" ]] || { echo "usage: -d domain -c out.csv [-C cols] [-R] [-F dir --dry-run]"; exit 2; }
mkdir -p "$(dirname "$out")"

# Default columns (align with runner allowlist)
cols="${columns:-domain,email,username,password,hashed_password,hash,password_hash,first_name,last_name,name,ip,address,breach,source,created_at,updated_at}"

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
  fi
}

# jq program to output rows as CSV in the requested column order.
# It pulls fields by name from each entry, substitutes empty string when absent.
rowjq='
def row($cols):
  ($cols | split(",")) as $cs
  | [$cs[] as $c | (.[ $c ] // "")] | @csv;
'

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

# ----- Fixtures (dry run) -----
if [[ "$dry" == true ]]; then
  [[ -d "$fixtures" ]] || { echo "fixtures dir required with --dry-run"; exit 3; }
  write_header
  shopt -s nullglob
  for f in "$fixtures"/*.json; do
    jq -r --arg cols "$cols" "$rowjq [.entries[]] | .[] | row(\$cols)" "$f" >> "$out"
  done
  shopt -u nullglob

  # Summary: Breached Databases (unique values in "breach" column)
  idx=""
  IFS=, read -ra a <<<"$cols"
  for i in "${!a[@]}"; do [[ "${a[$i]}" == "breach" ]] && idx=$((i+1)); done
  if [[ -n "$idx" && -s "$out" ]]; then
    bd=$(tail -n +2 "$out" | cut -d, -f"$idx" | sed 's/^"//;s/"$//' | sed '/^$/d' | sort -u | wc -l)
  else
    bd=0
  fi
  echo "Breached Databases: $bd"
  echo "Wrote $(($(wc -l < "$out")-1)) rows to $out"
  exit 0
fi

# ----- Live mode -----
: "${DEHASHED_USER:?set DEHASHED_USER}"
: "${DEHASHED_KEY:?set DEHASHED_KEY}"

write_header

page=1
while :; do
  curl -sS -u "$DEHASHED_USER:$DEHASHED_KEY" -G "https://api.dehashed.com/search" \
    --data-urlencode "query=domain:$domain" \
    --data-urlencode "page=$page" \
    --data-urlencode "size=500" \
    --data-urlencode "expand=true" > "$tmp"

  count=$(jq '.entries | length' "$tmp")
  [[ "$count" -gt 0 ]] || break

  jq -r --arg cols "$cols" "$rowjq [.entries[]] | .[] | row(\$cols)" "$tmp" >> "$out"
  (( page++ ))
done

# Summary: Breached Databases (unique values in "breach" column)
idx=""
IFS=, read -ra a <<<"$cols"
for i in "${!a[@]}"; do [[ "${a[$i]}" == "breach" ]] && idx=$((i+1)); done
if [[ -n "$idx" && -s "$out" ]]; then
  bd=$(tail -n +2 "$out" | cut -d, -f"$idx" | sed 's/^"//;s/"$//' | sed '/^$/d' | sort -u | wc -l)
else
  bd=0
fi
echo "Breached Databases: $bd"

# Row count (excluding header)
echo "Wrote $(($(wc -l < "$out")-1)) rows to $out"
