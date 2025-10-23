#!/usr/bin/env python3
"""
generate_fixtures.py — produce mock DeHashed-like JSON fixtures for testing

Example:
  python3 generate_fixtures.py --domain example.com --out ./fixtures --pages 2 --per-page 500 --seed 42 --prefer-corporate 0.8

What it creates:
  ./fixtures/example.com_page1.json
  ./fixtures/example.com_page2.json
Each file shape matches the API-ish structure:
  { "entries": [ { "email": "...", "first_name": "...", "last_name": "...", "breach": "...", ... }, ... ] }

Flags:
  --domain DOMAIN          corporate domain used for most emails (default: example.com)
  --out DIR               directory to write fixtures (default: ./fixtures)
  --pages N               number of fixture files to generate (default: 2)
  --per-page N            entries per page (default: 200)
  --seed N                RNG seed for determinism (default: 1234)
  --prefer-corporate F    probability [0..1] an email uses the corporate domain (default: 0.7)
  --freemail-domains      comma list for freemail pool (default: gmail.com,outlook.com,yahoo.com)
  --breaches              comma list of breach names to sample (default: LinkedIn,Dropbox,Adobe,Canva,Twitter,Collection#1)
  --include-passwords     include plaintext/hashed password-ish fields (default: off)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import string
from datetime import datetime, timedelta

FREEMAIL_DEFAULT = ["gmail.com", "outlook.com", "yahoo.com"]
BREACHES_DEFAULT = ["LinkedIn", "Dropbox", "Adobe", "Canva", "Twitter", "Collection#1"]

FNAMES = ["Alice","Bob","Carol","Dave","Eve","Frank","Grace","Heidi","Ivan","Judy","Mallory","Niaj","Olivia","Peggy","Rupert","Sybil","Trent","Uma","Victor","Wendy"]
LNAMES = ["Smith","Johnson","Brown","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin","Thompson","Garcia","Martinez","Robinson","Clark"]

def rand_user(first, last):
    parts = [
        first.lower(),
        last.lower(),
        f"{first[0].lower()}{last.lower()}",
        f"{first.lower()}.{last.lower()}",
    ]
    suffix = random.choice(["", "", "", str(random.randint(1, 9999))])
    return random.choice(parts) + suffix

def rand_password():
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(random.choice(chars) for _ in range(random.randint(8, 14)))

def rand_hash():
    return "".join(random.choice("0123456789abcdef") for _ in range(64))

def rand_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def rand_addr():
    return f"{random.randint(10,9999)} {random.choice(['Main','Oak','Pine','Cedar','Maple','Elm'])} St"

def rand_datetime_past_year():
    days_back = random.randint(0, 365)
    dt = datetime.utcnow() - timedelta(days=days_back, hours=random.randint(0,23), minutes=random.randint(0,59))
    return dt.replace(microsecond=0).isoformat() + "Z"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domain", default="example.com")
    ap.add_argument("--out", default="./fixtures")
    ap.add_argument("--pages", type=int, default=2)
    ap.add_argument("--per-page", type=int, default=200)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--prefer-corporate", type=float, default=0.7)
    ap.add_argument("--freemail-domains", default=",".join(FREEMAIL_DEFAULT))
    ap.add_argument("--breaches", default=",".join(BREACHES_DEFAULT))
    ap.add_argument("--include-passwords", action="store_true")
    args = ap.parse_args()

    random.seed(args.seed)
    os.makedirs(args.out, exist_ok=True)

    freemails = [d.strip() for d in args.freemail_domains.split(",") if d.strip()]
    breaches = [b.strip() for b in args.breaches.split(",") if b.strip()]

    total = 0
    for page in range(1, args.pages + 1):
        entries = []
        for _ in range(args.per_page):
            first = random.choice(FNAMES)
            last = random.choice(LNAMES)
            use_corp = random.random() < args.prefer_corporate
            domain = args.domain if use_corp else random.choice(freemails)
            username = rand_user(first, last)
            email = f"{username}@{domain}"

            item = {
                "email": email,
                "first_name": first,
                "last_name": last,
                "name": f"{first} {last}",
                "username": username,
                "breach": random.choice(breaches),
                "source": "fixture",
                "ip": rand_ip(),
                "address": rand_addr(),
                "created_at": rand_datetime_past_year(),
                "updated_at": rand_datetime_past_year(),
                "domain": domain,
            }
            if args.include_passwords:
                item["password"] = rand_password()
                item["hashed_password"] = rand_hash()
                item["hash"] = rand_hash()
                item["password_hash"] = rand_hash()

            entries.append(item)
            total += 1

        path = os.path.join(args.out, f"{args.domain}_page{page}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"entries": entries}, f, ensure_ascii=False, indent=2)
        print(f"Wrote {path} ({len(entries)} entries)")

    print(f"Done. Generated {total} entries across {args.pages} page(s) into {args.out}")

if __name__ == "__main__":
    main()
