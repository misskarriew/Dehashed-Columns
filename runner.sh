#!/usr/bin/env bash
# Dehashed Columns v1.1 — single-domain runner (YAML-aware)
# Purpose: Safely orchestrate DeHashed lookups for one domain per run with strong defaults
#
# New in v1.1:
# - Optional YAML config loading via --config or $DEHASHED_CONFIG (defaults to ./config.yaml if present)
# - Precedence: CLI > ENV > YAML > built-in defaults
# - YAML fields: runner.log_dir, runner.out_dir, runner.case_root, runner.default_columns,
#                runner.retries, runner.retry_wait, runner.evidence, runner.dry_run,
#                runner.fixtures_dir, runner.allowed_columns[]
#
# Security & hardening:
# - Strict shell: set -euo pipefail, safe IFS, umask 077, globbing off
# - Secrets never logged; env presence enforced in live mode
# - Column allowlist (merge/override via YAML)
# - Domain validation with IDN punycode support if idn2 is installed
# - Per-run case folder with manifest + SHA256 for artifacts; optional evidence ZIP
# - Retry/backoff on transient failures
# - CSV injection caution file included in run folder
#
# Expected env (live mode):
#   DEHASHED_USER, DEHASHED_KEY
# Optional env:
#   DEHASHED_CONFIG (path to YAML)
# Required tool:
#   /usr/local/bin/dehashed_domain_search.sh
# Optional tools:
#   idn2 (IDN support), yq (YAML parser), sha256sum/shasum, zip
#
# Usage examples:
#   ./runner.sh --config ./config.yaml dehashed:domain -d example.com --resume --evidence
#   ./runner.sh dehashed:domain -d onegi.com --dry-run --fixtures fixtures/onegi --columns "domain,email,username,password,hashed_password"

set -euo pipefail
IFS=$'\n\t'
umask 077
set -f

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${LOG_DIR:-${SCRIPT_DIR}/logs}"
OUT_DIR_DEFAULT="${OUT_DIR_DEFAULT:-${SCRIPT_DIR}/out}"
CASES_DIR="${CASES_DIR:-${SCRIPT_DIR}/cases}"
CONFIG_PATH="${DEHASHED_CONFIG:-}"

mkdir -p "$LOG_DIR" "$OUT_DIR_DEFAULT" "$CASES_DIR"

# Timestamp helper
ts() { date +"%Y-%m-%d_%H-%M-%S"; }

fatal() { echo "[fatal] $*" >&2; exit 1; }
info()  { echo "[info]  $*" >&2; }
warn()  { echo "[warn]  $*" >&2; }

require_bin() {
  command -v "$1" >/dev/null 2>&1 || fatal "Missing dependency: $1"
}

# YAML helpers (non-fatal if yq is absent; we attempt minimal parsing)
have_yq() { command -v yq >/dev/null 2>&1; }

yaml_get_scalar() {
  local key="$1" file="$2"
  if have_yq; then
    yq -oy ".$key" "$file" 2>/dev/null | sed 's/^null$//'
  else
    # naive grep fallback (handles simple 'key: value')
    grep -E "^[[:space:]]*$key:[[:space:]]*" "$file" 2>/dev/null | head -n1 | sed -E "s/^[[:space:]]*$key:[[:space:]]*//" | tr -d '"' | tr -d "'"
  fi
}

yaml_get_array() {
  local key="$1" file="$2"
  if have_yq; then
    yq -oy ".$key[]" "$file" 2>/dev/null | sed '/^null$/d'
  else
    # naive parse for arrays like:
    # key:\n  #   - a\n  #   - b
    awk -v k="$key" '
      $0 ~ k":" {inlist=1; next}
      inlist==1 {
        if ($0 ~ /^\s*-/) { gsub(/^\s*-\s*/, "", $0); gsub(/["'\r]/, "", $0); print $0; next }
        if ($0 ~ /^\S/) { inlist=0 }
      }
    ' "$file"
  fi
}

# Allowlisted CSV columns (defaults; can be overridden by YAML)
ALLOWED_COLUMNS=(
  domain email username password hashed_password hash password_hash
  first_name last_name name ip address breach source created_at updated_at
)

is_allowed_column() {
  local needle="$1"
  for c in "${ALLOWED_COLUMNS[@]}"; do
    [[ "$c" == "$needle" ]] && return 0
  done
  return 1
}

sanitize_columns() {
  local raw="$1" out=() bad=()
  IFS=',' read -r -a parts <<< "$raw"
  for p in "${parts[@]}"; do
    local col="${p// /}"
    if is_allowed_column "$col"; then
      out+=("$col")
    else
      bad+=("$col")
    fi
  done
  if (( ${#bad[@]} > 0 )); then
    warn "Dropping unknown column(s): ${bad[*]}"
  fi
  local IFS=','
  echo "${out[*]}"
}

validate_domain() {
  local d="$1"
  if [[ ! "$d" =~ ^([A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$ ]]; then
    if command -v idn2 >/dev/null 2>&1; then
      local puny
      if puny=$(idn2 --quiet "$d" 2>/dev/null); then
        echo "$puny"; return 0
      fi
    fi
    fatal "Invalid domain format: $d"
  fi
  echo "$d"
}

usage() {
  cat <<'USAGE'
Dehashed Columns v1.1 — single-domain (YAML-aware)

Global Flags
  --config PATH      Path to YAML config (or set $DEHASHED_CONFIG). If omitted, ./config.yaml is used when present.
  -h, --help         Show this help

Subcommands
  dehashed:domain    Query DeHashed for a domain (live or fixtures/dry-run)

Subcommand: dehashed:domain
  -d, --domain DOMAIN              Target domain (required)
  -o, --out PATH                   Output CSV path (default: cases/DOMAIN/YYYY-MM-DD/run-TIMESTAMP/dehashed-DOMAIN.csv)
      --columns "c1,c2,..."        CSV columns (default from YAML runner.default_columns or builtin)
      --resume                     Resume from last checkpoint if present; if CSV exists, append
      --fixtures DIR               Use fixtures directory instead of live API
      --dry-run                    Alias for using fixtures mode
      --evidence                   Create evidence zip in the run folder (adds manifest + SHA256)
      --retries N                  Retries on transient failure
      --retry-wait SECONDS         Wait between retries

Env Vars (for live mode)
  DEHASHED_USER, DEHASHED_KEY (never logged)
USAGE
}

apply_yaml_defaults() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  info "Loading YAML config: $file"

  # Directories
  local v
  v=$(yaml_get_scalar 'runner.log_dir' "$file");          [[ -n "$v" ]] && LOG_DIR="$v"
  v=$(yaml_get_scalar 'runner.out_dir' "$file");          [[ -n "$v" ]] && OUT_DIR_DEFAULT="$v"
  v=$(yaml_get_scalar 'runner.case_root' "$file");        [[ -n "$v" ]] && CASES_DIR="$v"
  mkdir -p "$LOG_DIR" "$OUT_DIR_DEFAULT" "$CASES_DIR"

  # Behavior
  v=$(yaml_get_scalar 'runner.retries' "$file");          [[ -n "$v" ]] && YAML_RETRIES="$v" || YAML_RETRIES=""
  v=$(yaml_get_scalar 'runner.retry_wait' "$file");       [[ -n "$v" ]] && YAML_RETRY_WAIT="$v" || YAML_RETRY_WAIT=""
  v=$(yaml_get_scalar 'runner.evidence' "$file");         [[ -n "$v" ]] && YAML_EVIDENCE="$v" || YAML_EVIDENCE=""
  v=$(yaml_get_scalar 'runner.dry_run' "$file");          [[ -n "$v" ]] && YAML_DRY_RUN="$v" || YAML_DRY_RUN=""
  v=$(yaml_get_scalar 'runner.fixtures_dir' "$file");     [[ -n "$v" ]] && YAML_FIXTURES_DIR="$v" || YAML_FIXTURES_DIR=""

  # Default columns array
  mapfile -t YAML_DEFAULT_COLUMNS < <(yaml_get_array 'runner.default_columns' "$file") || true

  # Allowed columns array (override)
  local yaml_allowed=()
  mapfile -t yaml_allowed < <(yaml_get_array 'runner.allowed_columns' "$file") || true
  if (( ${#yaml_allowed[@]} > 0 )); then
    ALLOWED_COLUMNS=("${yaml_allowed[@]}")
  fi
}

make_manifest() {
  local path="$1"; shift
  {
    echo "Application: Dehashed Columns v1.1"
    echo "Subcommand: $*"
    echo "Started: ${START_TIME}"
    echo "Finished: $(date -Iseconds)"
    echo "ExitCode: ${1:-0}"
    echo "Runner: $(basename "$0")"
    [[ -n "$CONFIG_USED" ]] && echo "Config: $CONFIG_USED"
  } > "$path"
}

sha256sum_file() {
  local target="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$target" | awk '{print $1}' > "$target.sha256"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$target" | awk '{print $1}' > "$target.sha256"
  fi
}

trap_handler() {
  local rc=$?
  [[ -n "${MANIFEST_PATH:-}" ]] && make_manifest "$MANIFEST_PATH" "$rc" "$INVOCATION"
  exit $rc
}
trap trap_handler EXIT INT TERM

run_dehashed_domain() {
  local domain="" out_path="" resume=false fixtures_dir="" dry_run=false evidence=false retries="" retry_wait="" columns=""

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -d|--domain) domain="$2"; shift 2 ;;
      -o|--out) out_path="$2"; shift 2 ;;
      --columns) columns="$2"; shift 2 ;;
      --resume) resume=true; shift ;;
      --fixtures) fixtures_dir="$2"; shift 2 ;;
      --dry-run) dry_run=true; shift ;;
      --evidence) evidence=true; shift ;;
      --retries) retries="$2"; shift 2 ;;
      --retry-wait) retry_wait="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) warn "Unknown arg for dehashed:domain: $1"; shift ;;
    esac
  done

  [[ -n "$domain" ]] || fatal "--domain is required"
  require_bin "/usr/local/bin/dehashed_domain_search.sh"

  local validated_domain
  validated_domain="$(validate_domain "$domain")"

  # Resolve defaults from YAML if unset by CLI/env
  local columns_default="domain,email,username,password,hashed_password"
  if (( ${#YAML_DEFAULT_COLUMNS[@]:-0} > 0 )); then
    local IFS=","; columns_default=$(printf "%s," "${YAML_DEFAULT_COLUMNS[@]}" | sed 's/,$//')
  fi

  # Determine mode
  local mode="live"
  # CLI overrides
  if [[ -n "$fixtures_dir" || "$dry_run" == true ]]; then
    mode="fixtures"
  else
    # YAML/env defaults
    if [[ -z "$fixtures_dir" && -n "${YAML_FIXTURES_DIR:-}" ]]; then
      fixtures_dir="$YAML_FIXTURES_DIR"
    fi
    if [[ -z "$dry_run" && -n "${YAML_DRY_RUN:-}" ]]; then
      [[ "$YAML_DRY_RUN" == "true" ]] && mode="fixtures"
    fi
  fi

  # Case folder structure
  START_TIME="$(date -Iseconds)"
  local today="$(date +%F)"
  local run_id="run-$(ts)"
  local case_root="${CASES_DIR}/${validated_domain}/${today}/${run_id}"
  mkdir -p "$case_root"

  # Output path
  if [[ -z "$out_path" ]]; then
    out_path="${case_root}/dehashed-${validated_domain}.csv"
  fi
  mkdir -p "$(dirname "$out_path")"

  # Log path and meta files
  local log_file="${case_root}/dehashed-domain-${validated_domain}.log"
  local note_file="${case_root}/READ_ME_CSV_RISKS.txt"
  MANIFEST_PATH="${case_root}/manifest.txt"
  INVOCATION="dehashed:domain -d ${domain} ${columns:+--columns \"$columns\"} ${resume:+--resume} ${fixtures_dir:+--fixtures \"$fixtures_dir\"} ${dry_run:+--dry-run} ${evidence:+--evidence} ${retries:+--retries $retries} ${retry_wait:+--retry-wait $retry_wait}"

  # CSV/Excel risk note
  cat > "$note_file" <<'NOTE'
Opening CSVs in spreadsheet apps can execute formulas if any cell begins with =, +, -, or @ (CSV injection). Treat this file as untrusted data. Prefer importing as text-only or reviewing in a plain-text viewer.
NOTE

  # Columns
  local final_columns
  if [[ -n "$columns" ]]; then
    final_columns="$(sanitize_columns "$columns")"
    [[ -z "$final_columns" ]] && fatal "After validation, no columns remained. Check --columns."
  else
    final_columns="$columns_default"
  fi

  # Retries (CLI > ENV > YAML > defaults 1/5)
  local eff_retries eff_wait
  eff_retries="${retries:-${RETRIES:-${YAML_RETRIES:-1}}}"
  eff_wait="${retry_wait:-${RETRY_WAIT:-${YAML_RETRY_WAIT:-5}}}"

  # Build command
  local cmd=("/usr/local/bin/dehashed_domain_search.sh" -d "$validated_domain" -c "$out_path" -C "$final_columns")
  if [[ "$resume" == true ]]; then
    cmd+=( -R )
  fi
  if [[ "$mode" == "fixtures" ]]; then
    if [[ -z "$fixtures_dir" ]]; then
      fatal "--dry-run requires --fixtures DIR"
    fi
    cmd+=( -F "$fixtures_dir" --dry-run )
  else
    : "${DEHASHED_USER:?Set DEHASHED_USER for live mode}"
    : "${DEHASHED_KEY:?Set DEHASHED_KEY for live mode}"
  fi

  info "Starting DeHashed lookup for ${validated_domain}";
  info "Logs: $log_file"
  info "Output (CSV): $out_path"
  [[ -n "$CONFIG_USED" ]] && info "Config: $CONFIG_USED"

  # Execute with retry/backoff
  local attempt=0 rc=1
  set +e
  while (( attempt <= eff_retries )); do
    (( attempt > 0 )) && { warn "Retry attempt $attempt of $eff_retries after ${eff_wait}s"; sleep "$eff_wait"; }
    "${cmd[@]}" 2>&1 | tee -a "$log_file"
    rc=${PIPESTATUS[0]}
    if [[ $rc -eq 0 ]]; then
      break
    fi
    (( attempt++ ))
  done
  set -e

  if [[ $rc -ne 0 ]]; then
    warn "dehashed:domain exited with code $rc (see $log_file)"
    make_manifest "$MANIFEST_PATH" "$rc" "$INVOCATION"
    return $rc
  fi

  # Checksums
  [[ -f "$out_path" ]] && sha256sum_file "$out_path"
  [[ -f "$log_file" ]] && sha256sum_file "$log_file"

  # Evidence packaging (CLI > ENV > YAML)
  local eff_evidence
  eff_evidence=false
  if [[ "$evidence" == true ]]; then eff_evidence=true; fi
  if [[ "$eff_evidence" == false && "${EVIDENCE:-}" == "true" ]]; then eff_evidence=true; fi
  if [[ "$eff_evidence" == false && "${YAML_EVIDENCE:-}" == "true" ]]; then eff_evidence=true; fi

  if [[ "$eff_evidence" == true ]]; then
    local zip_path="${case_root}/evidence-${validated_domain}.zip"
    (
      cd "$case_root"
      zip -q -r -X "$(basename "$zip_path")" .
    )
    sha256sum_file "$zip_path"
    info "Evidence: $zip_path"
  fi

  make_manifest "$MANIFEST_PATH" 0 "$INVOCATION"

  info "Done."
  echo "CSV: $out_path"
  echo "Log: $log_file"
  [[ "$eff_evidence" == true ]] && echo "Evidence ZIP: ${case_root}/evidence-${validated_domain}.zip"
  echo "Manifest: $MANIFEST_PATH"
}

# Parse global flags (currently only --config)
parse_global_flags() {
  local args=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --config)
        CONFIG_PATH="$2"; shift 2 ;;
      -h|--help)
        usage; exit 0 ;;
      dehashed:domain)
        args+=("dehashed:domain"); shift; break ;;
      *)
        args+=("$1"); shift ;;
    esac
  done
  # If CONFIG_PATH unset, prefer ./config.yaml when present
  if [[ -z "${CONFIG_PATH}" && -f "${SCRIPT_DIR}/config.yaml" ]]; then
    CONFIG_PATH="${SCRIPT_DIR}/config.yaml"
  fi
  if [[ -n "$CONFIG_PATH" ]]; then
    CONFIG_USED="$CONFIG_PATH"
    apply_yaml_defaults "$CONFIG_PATH"
  fi
  echo "${args[@]}" "$@"
}

main() {
  # Pre-parse global flags and maybe load YAML
  # shellcheck disable=SC2206
  local argv=( $(parse_global_flags "$@") )
  local subcmd="${argv[0]:-}"
  if [[ -z "$subcmd" || "$subcmd" == "-h" || "$subcmd" == "--help" ]]; then
    usage; exit 0
  fi
  case "$subcmd" in
    dehashed:domain) shift || true; run_dehashed_domain "${argv[@]:1}" ;;
    *) warn "Unknown subcommand: $subcmd"; usage; exit 1 ;;
  esac
}

main "$@"