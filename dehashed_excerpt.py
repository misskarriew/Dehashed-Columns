#!/usr/bin/env python3
"""
dehashed_excerpt.py — robust CSV excerpt + breach count helper (enhanced)

Features:
- Streams CSV, handles quoting/newlines via csv.reader
- Auto-detects delimiter with fallback heuristic; manual override supported
- Encoding-tolerant (default utf-8 with replacement; overrideable)
- Header aliasing for email/name/first/last/breach
- Deterministic, ranked excerpt (prefer specified domain; prefer entries with a name)
- JSON sidecar output via --json-file (stdout remains human format by default)
- Verbose logging to stderr; quiet mode supported

Default stdout format (kept for compatibility):
  Excerpt (name <email>)
  Name <email>
  ...
  
  Breached Databases: N
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import OrderedDict

ALIASES = {
    "email": {"email", "email_address", "e-mail", "mail", "addr", "address_email"},
    "name": {"name", "full_name", "fullname", "display_name"},
    "first": {"first_name", "firstname", "given_name", "givenname", "first"},
    "last": {"last_name", "lastname", "surname", "family_name", "familyname", "last"},
    "breach": {"breach", "source", "database", "breach_name"},
}

def log(msg: str, *, verbose: bool):
    if verbose:
        sys.stderr.write(str(msg) + "\n")

def normalize(s: str) -> str:
    return (s or "").strip()

def choose_col(header_lower, prefer: str, explicit: str | None) -> int | None:
    if not header_lower:
        return None
    if explicit:
        preferred = explicit.lower()
        for i, h in enumerate(header_lower):
            if h == preferred:
                return i
        for i, h in enumerate(header_lower):
            if preferred in h:
                return i
    aliases = ALIASES.get(prefer, set())
    for i, h in enumerate(header_lower):
        if h in aliases:
            return i
    return None

def detect_dialect(sample: str, fallback="excel"):
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(sample, delimiters=[",", ";", "\t", "|", ":"])
        return dialect
    except Exception:
        counts = {",": sample.count(","), ";": sample.count(";"), "\t": sample.count("\t"), "|": sample.count("|"), ":": sample.count(":")}
        delim = max(counts, key=counts.get) if sample else ","
        class F(csv.Dialect):
            delimiter = delim
            doublequote = True
            escapechar = None
            lineterminator = "\n"
            quotechar = '"'
            quoting = csv.QUOTE_MINIMAL
            skipinitialspace = True
        F.__name__ = f"Detected_{delim.encode('unicode_escape').decode()}"
        return F

def build_name(row, idx_name, idx_first, idx_last):
    name = ""
    if idx_name is not None and idx_name < len(row):
        name = normalize(row[idx_name])
    if not name:
        fn = normalize(row[idx_first]) if idx_first is not None and idx_first < len(row) else ""
        ln = normalize(row[idx_last]) if idx_last is not None and idx_last < len(row) else ""
        name = (fn + (" " if fn and ln else "") + ln).strip()
    return name

def email_domain(email: str) -> str:
    m = re.search(r"@([^>]+)$", email)
    return m.group(1).lower() if m else ""

def main():
    ap = argparse.ArgumentParser(description="Generate an excerpt of names & emails and count unique breached databases.")
    ap.add_argument("--csv", required=True, help="Path to the CSV file produced by dehashed_domain_search.sh")
    ap.add_argument("--limit", type=int, default=10, help="Max number of unique excerpt lines to show (default: 10)")
    ap.add_argument("--email-col", default=None, help="Email column name (case-insensitive). If omitted, auto-detect.")
    ap.add_argument("--name-col", default=None, help="Name column name (case-insensitive). If omitted, auto-detect.")
    ap.add_argument("--first-col", default=None, help="First name column (case-insensitive).")
    ap.add_argument("--last-col", default=None, help="Last name column (case-insensitive).")
    ap.add_argument("--breach-col", default=None, help="Breach column (case-insensitive). If omitted, auto-detect.")
    ap.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8). Use 'latin-1' if needed.")
    ap.add_argument("--delimiter", default="auto", help="Delimiter: auto, ',', ';', '\\t', '|', ':'")
    ap.add_argument("--prefer-domain", default=None, help="Prefer emails with this domain in the excerpt ranking.")
    ap.add_argument("--json-file", default=None, help="Write a JSON summary to this file (stdout remains text).")
    ap.add_argument("-v", "--verbose", action="store_true", help="Verbose logging to stderr.")
    ap.add_argument("-q", "--quiet", action="store_true", help="Suppress non-error logging.")
    args = ap.parse_args()

    verbose = args.verbose and not args.quiet

    if not os.path.exists(args.csv):
        print(f"ERROR: File not found: {args.csv}", file=sys.stderr)
        sys.exit(2)

    with open(args.csv, "r", encoding=args.encoding, errors="replace", newline="") as fh:
        sample = fh.read(65536)
        fh.seek(0)
        if args.delimiter == "auto":
            dialect = detect_dialect(sample)
            log(f"Detected delimiter: {dialect.delimiter!r}", verbose=verbose)
        else:
            class Manual(csv.Dialect):
                delimiter = {"\\t":"\t"}.get(args.delimiter, args.delimiter)
                doublequote = True
                escapechar = None
                lineterminator = "\n"
                quotechar = '"'
                quoting = csv.QUOTE_MINIMAL
                skipinitialspace = True
            dialect = Manual
            log(f"Using manual delimiter: {dialect.delimiter!r}", verbose=verbose)

        reader = csv.reader(fh, dialect=dialect)
        try:
            header = next(reader)
        except StopIteration:
            print("Excerpt (name <email>)")
            print("(empty file)")
            print()
            print("Breached Databases: 0")
            if args.json_file:
                with open(args.json_file, "w", encoding="utf-8") as jf:
                    json.dump({"excerpt": [], "breached_databases": 0}, jf, ensure_ascii=False, indent=2)
            return
        except csv.Error as e:
            print(f"ERROR: CSV header parse failed: {e}", file=sys.stderr)
            sys.exit(3)

        header_lower = [normalize(h).lower() for h in header]

        idx_email = choose_col(header_lower, "email", args.email_col)
        idx_name  = choose_col(header_lower, "name", args.name_col)
        idx_first = choose_col(header_lower, "first", args.first_col)
        idx_last  = choose_col(header_lower, "last", args.last_col)
        idx_breach= choose_col(header_lower, "breach", args.breach_col)

        if idx_email is None:
            # Still proceed to count breaches, but excerpt will be empty.
            log("Warning: no email column found; excerpt may be empty.", verbose=verbose)

        # Accumulators
        items = []  # collect tuples for ranking: (rank_tuple, display)
        breaches = set()

        # Scan rows
        for row in reader:
            # breaches
            if idx_breach is not None and idx_breach < len(row):
                b = normalize(row[idx_breach])
                if b:
                    breaches.add(b)

            # emails + names
            if idx_email is None or idx_email >= len(row):
                continue
            email = normalize(row[idx_email])
            if not email:
                continue
            name = build_name(row, idx_name, idx_first, idx_last)
            display = f"{name} <{email}>" if name else email

            # Ranking:
            # 0) prefer preferred-domain emails (if provided)
            # 1) prefer entries with a non-empty name
            # 2) preserve original order via incremental counter
            dom = email_domain(email)
            prefer_hit = 0 if (args.prefer_domain and dom == args.prefer_domain.lower()) else 1
            has_name = 0 if name else 1
            items.append(((prefer_hit, has_name, len(items)), display))

        # Deduplicate preserving best rank then order
        items.sort(key=lambda x: x[0])
        seen = set()
        excerpt_list = []
        for _, disp in items:
            if disp in seen:
                continue
            seen.add(disp)
            excerpt_list.append(disp)
            if len(excerpt_list) >= args.limit:
                break

        # Output text
        print("Excerpt (name <email>)")
        if excerpt_list:
            for line in excerpt_list:
                print(line)
        else:
            if idx_email is None:
                print("(no email column found)")
            else:
                print("(no rows with emails found)")
        print()
        print(f"Breached Databases: {len(breaches)}")

        # Optional JSON sidecar
        if args.json_file:
            try:
                with open(args.json_file, "w", encoding="utf-8") as jf:
                    json.dump({"excerpt": excerpt_list, "breached_databases": len(breaches)}, jf, ensure_ascii=False, indent=2)
                log(f"Wrote JSON summary to {args.json_file}", verbose=verbose)
            except Exception as e:
                print(f"ERROR: failed to write JSON file: {e}", file=sys.stderr)
                # don't fail the whole run
                pass

if __name__ == "__main__":
    main()
