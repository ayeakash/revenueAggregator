#!/usr/bin/env python3
"""
YouTube AdSense KPI Poster — Revenue ONLY (No Sheets, Global Projection, Improved Latest-Date Detection)

- Pulls per-channel revenue from YouTube Analytics.
- Computes per-channel AND total projections using a single global finalized date.
- Posts one Slack message with totals and per-channel lines.
- Auto-hydrates YT_CLIENT_ID/SECRET from YT_OAUTH_CLIENT_JSON or YT_OAUTH_CLIENT.

Required env:
  - SLACK_WEBHOOK_URL
  - YT_TOKENS_FILE  (e.g., yt_refresh_tokens.json)
  AND EITHER:
  - YT_CLIENT_ID + YT_CLIENT_SECRET
  OR
  - YT_OAUTH_CLIENT_JSON (path) / YT_OAUTH_CLIENT (json blob string)

Optional:
  - YT_CURRENCY (USD|INR; default USD)
"""

import os, json, sys, re, pathlib, traceback
import requests
from datetime import date, timedelta

# -------------------- CONFIG --------------------
CLIENT_ID     = os.getenv("YT_CLIENT_ID",     "YOUR_WEB_CLIENT_ID.apps.googleusercontent.com")
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "YOUR_WEB_CLIENT_SECRET")

TOKENS_FILE   = os.getenv("YT_TOKENS_FILE",   "yt_refresh_tokens.json")

CURRENCY           = os.getenv("YT_CURRENCY", "USD")
SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXXXX/XXXXX/XXXXXXXX")

# -------------------- CONSTANTS --------------------
TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS = "https://youtubeanalytics.googleapis.com/v2/reports"
DATA_API  = "https://www.googleapis.com/youtube/v3/channels"

EM_DASH = "—"
SPACER_BLOCK = {"type": "section", "text": {"type": "mrkdwn", "text": "\u200b"}}

# -------------------- STARTUP INFO --------------------
def info_banner():
    print("="*60)
    print("YouTube AdSense KPI Poster — Revenue ONLY (No Sheets, Global Projection)")
    print("="*60)
    print("Python:", sys.version)
    print("requests:", requests.__version__)
    print("Currency:", CURRENCY)
    print()

# -------------------- UTILS --------------------
def _brief(s, n=500):
    s = str(s)
    return s if len(s) <= n else s[:n] + "…"

def _print_http_error(resp, label):
    try:
        print(f"[{label}] HTTP {resp.status_code}")
        print(_brief(resp.text, 800))
    except Exception:
        print(f"[{label}] HTTP error")

def load_tokens_file(path: str) -> dict:
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tokens file not found: {path}")
    raw = p.read_text(encoding="utf-8")
    # allow //, # comments and trailing commas
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.S)
    raw = re.sub(r"^\s*//.*?$", "", raw, flags=re.M)
    raw = re.sub(r"^\s*#.*?$",  "", raw, flags=re.M)
    raw = re.sub(r",\s*([}\]])", r"\1", raw)
    data = json.loads(raw)
    if not isinstance(data, dict) or not data:
        raise ValueError("Tokens JSON empty or not an object.")
    return data

def require_env(name, placeholder_fragments=()):
    val = os.getenv(name, "")
    if not val or any(frag in val for frag in placeholder_fragments):
        raise RuntimeError(f"Missing or placeholder env: {name}")
    return val

def fmt_day(d):
    # Example: 16-Oct-2025
    return d.strftime("%d-%b-%Y") if d else ""

# -------------------- OPTIONAL: AUTO-HYDRATE OAUTH CLIENT --------------------
def hydrate_oauth_client_from_json():
    """
    If YT_CLIENT_ID/SECRET aren’t set, try to populate them from:
      - YT_OAUTH_CLIENT_JSON: path to Google OAuth client json (with "installed" or "web")
      - YT_OAUTH_CLIENT: JSON string
    """
    cid = os.getenv("YT_CLIENT_ID", "")
    csec = os.getenv("YT_CLIENT_SECRET", "")
    if cid and csec:
        return

    path = os.getenv("YT_OAUTH_CLIENT_JSON", "").strip()
    blob = os.getenv("YT_OAUTH_CLIENT", "").strip()
    data = None
    try:
        if path and pathlib.Path(path).exists():
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        elif blob:
            data = json.loads(blob)
    except Exception:
        data = None

    if not data:
        return

    client = data.get("installed") or data.get("web") or data
    cid = (client or {}).get("client_id")
    csec = (client or {}).get("client_secret")
    if cid and csec:
        os.environ.setdefault("YT_CLIENT_ID", cid)
        os.environ.setdefault("YT_CLIENT_SECRET", csec)

# -------------------- AUTH HELPERS --------------------
def get_access_token(refresh_token: str) -> str:
    r = requests.post(TOKEN_URL, data={
        "client_id": os.getenv("YT_CLIENT_ID"),
        "client_secret": os.getenv("YT_CLIENT_SECRET"),
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }, timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        _print_http_error(r, "Token exchange error")
        raise
    data = r.json()
    if "access_token" not in data:
        raise RuntimeError(f"No access_token in response: {data}")
    return data["access_token"]

def channel_identity(access_token: str):
    r = requests.get(
        DATA_API,
        params={"part": "id,snippet", "mine": "true"},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30
    )
    try:
        r.raise_for_status()
    except requests.HTTPError:
        _print_http_error(r, "YouTube Data API (channel_identity)")
        raise
    items = r.json().get("items", [])
    if not items:
        return None, None
    ch = items[0]
    return ch["id"], ch["snippet"]["title"]

# -------------------- YT ANALYTICS HELPERS --------------------
def yt_query(access_token, start_date, end_date, dims="day", metrics="estimatedRevenue", currency=CURRENCY):
    r = requests.get(
        ANALYTICS,
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "ids": "channel==MINE",
            "startDate": start_date,
            "endDate": end_date,
            "metrics": metrics,
            "dimensions": dims,
            **({"currency": currency} if metrics == "estimatedRevenue" else {})
        },
        timeout=60
    )
    try:
        r.raise_for_status()
    except requests.HTTPError:
        _print_http_error(r, "YouTube Analytics API (yt_query)")
        raise
    return r.json()

# >>> Improved latest-day detection (asks through TODAY and picks max date returned)
def detect_latest_day(access_token, metric: str) -> date:
    end = date.today()                 # allow API to include newest finalized day
    start = end - timedelta(days=30)   # wider window helps during lag
    try:
        data = yt_query(access_token, start.isoformat(), end.isoformat(),
                        dims="day", metrics=metric)
    except requests.HTTPError:
        # try an even wider fallback if needed
        start = end - timedelta(days=60)
        data = yt_query(access_token, start.isoformat(), end.isoformat(),
                        dims="day", metrics=metric)
    rows = data.get("rows") or []
    if not rows:
        return None
    latest_str = max(r[0] for r in rows)
    y, m, d = map(int, latest_str.split("-"))
    return date(y, m, d)

def sum_metric(access_token, start, end, metric, currency=CURRENCY) -> float:
    data = yt_query(access_token, start, end, dims="day", metrics=metric, currency=currency)
    return float(sum(r[1] for r in (data.get("rows") or [])))

# -------------------- SLACK FORMAT & POSTING --------------------
def fmt_money(x):
    return f"${x:,.0f}" if CURRENCY.upper() == "USD" else f"{CURRENCY} {x:,.0f}"

def fmt_or_dash_money(x):
    return fmt_money(x) if isinstance(x, (int, float)) else EM_DASH

def month_projection_for(latest_day: date, mtd_total: float) -> float:
    if not latest_day or not isinstance(mtd_total, (int, float)):
        return 0.0
    # exact month length (31/30/28/29)
    next_month_first = (latest_day.replace(day=28) + timedelta(days=4)).replace(day=1)
    days_in_month = (next_month_first - latest_day.replace(day=1)).days
    elapsed = latest_day.day
    daily_avg = (mtd_total / elapsed) if elapsed else 0.0
    return max(daily_avg * days_in_month, 0.0)

def pct_change(curr: float, base: float):
    if not base:
        return ""
    delta = (curr - base) / base * 100.0
    sign = "+" if delta >= 0 else ""
    return f" ({sign}{delta:.0f}%)"

def build_revenue_sections(labels, per_chan, global_latest):
    y_lines = []
    proj_lines = []
    tot_y = 0.0
    tot_mtd_global = 0.0
    tot_last = 0.0

    for label in labels:
        d = per_chan.get(label, {})
        # ---------- Yesterday ----------
        latest = d.get("rev_latest")
        y_val  = d.get("y_rev")
        if latest is None or y_val is None:
            y_lines.append(f"• {label}: {EM_DASH}")
        else:
            y_lines.append(f"• {label}: {fmt_money(y_val)}")
            tot_y += y_val

        # ---------- Projection (global) ----------
        ch_mtd_glob = d.get("mtd_rev_global", None)
        ch_last     = d.get("last_month_rev", None)

        if global_latest is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        elif ch_mtd_glob is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        else:
            ch_proj = month_projection_for(global_latest, ch_mtd_glob)
            proj_lines.append(f"• {label}: {fmt_money(ch_proj)}{pct_change(ch_proj, ch_last)}")
            tot_mtd_global += ch_mtd_glob
            if isinstance(ch_last, (int, float)):
                tot_last += ch_last

    # Totals & header date
    tot_proj = month_projection_for(global_latest, tot_mtd_global) if global_latest else None
    if global_latest is None:
        tot_y = None
        tot_last = None

    header_title = f"*AdSense KPI* ({fmt_day(global_latest)})" if global_latest else "*AdSense KPI*"
    header_y     = f":spiral_calendar_pad: *Yesterday:* {fmt_or_dash_money(tot_y)}"
    header_proj  = f":calendar: *This Month Projection:* {fmt_or_dash_money(tot_proj)}" + \
                   (pct_change(tot_proj or 0.0, tot_last or 0.0) if (tot_proj is not None and tot_last) else "")

    blocks = [
        {"type":"section","text":{"type":"mrkdwn","text":header_title}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":header_y}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(y_lines) if y_lines else EM_DASH}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":header_proj}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(proj_lines) if proj_lines else EM_DASH}},
    ]
    return blocks

def post_to_slack(payload, label="payload"):
    if not SLACK_WEBHOOK_URL or "hooks.slack.com/services/" not in SLACK_WEBHOOK_URL:
        print("[Slack] Skipped: SLACK_WEBHOOK_URL not set."); return False
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=15)
        ok = 200 <= r.status_code < 300
        print(f"[Slack] {label} status:", r.status_code, (r.text[:300] or "(empty)"))
        if not ok:
            print("[Slack] Payload preview (truncated):", (json.dumps(payload)[:1200] + "…"))
        return ok
    except Exception as e:
        print(f"[Slack] {label} exception:", e)
        print("[Slack] Payload preview (truncated):", (json.dumps(payload)[:1200] + "…"))
        return False

# -------------------- MAIN --------------------
def main():
    info_banner()
    hydrate_oauth_client_from_json()

    require_env("YT_CLIENT_ID", ("YOUR_WEB_CLIENT_ID",))
    require_env("YT_CLIENT_SECRET", ("YOUR_WEB_CLIENT_SECRET",))
    require_env("SLACK_WEBHOOK_URL", ("hooks.slack.com/services/XXXXX",))

    tokens = load_tokens_file(TOKENS_FILE)
    labels_order = list(tokens.keys())

    per_chan = {}

    # ---------- First pass: identify channels, latest day, Y/MTD/Last (per-channel latest) ----------
    for label, obj in tokens.items():
        print(f"— Channel label: {label}")
        rf = obj.get("refresh_token")
        if not rf:
            print("  ! Missing refresh_token, skipping\n")
            per_chan[label] = {"rev_latest": None}
            continue

        try:
            at = get_access_token(rf)
        except Exception:
            print("  ! Token exchange failed; skipping.\n")
            per_chan[label] = {"rev_latest": None}
            continue

        try:
            ch_id, ch_title = channel_identity(at)
            print(f"  id={ch_id}  title={ch_title}")
        except Exception:
            ch_id, ch_title = None, None
            print("  ! channel_identity failed; continuing…")

        rev_latest = None
        y_rev = mtd_rev = last_rev = None
        try:
            rev_latest = detect_latest_day(at, "estimatedRevenue")
            if rev_latest:
                month_first = rev_latest.replace(day=1)
                prev_month_last = month_first - timedelta(days=1)
                prev_month_first = prev_month_last.replace(day=1)

                y_rev   = sum_metric(at, rev_latest.isoformat(), rev_latest.isoformat(), "estimatedRevenue")
                mtd_rev = sum_metric(at, month_first.isoformat(), rev_latest.isoformat(), "estimatedRevenue")
                last_rev= sum_metric(at, prev_month_first.isoformat(), prev_month_last.isoformat(), "estimatedRevenue")
                print(f"  Rev latest: {rev_latest}  Y={fmt_or_dash_money(y_rev)}  MTD={fmt_or_dash_money(mtd_rev)}  LastM={fmt_or_dash_money(last_rev)}")
            else:
                print("  ! No finalized revenue rows.")
        except requests.HTTPError as e:
            status = getattr(e.response, "status_code", None)
            if status == 403:
                print("  ! Revenue 403 (likely not monetized / no Analytics access).")
            else:
                print("  ! Revenue query failed:", e)

        # store access token for second pass
        per_chan[label] = {
            "access_token": at,
            "channel_id": ch_id, "channel_title": ch_title,
            "rev_latest": rev_latest,
            "y_rev": y_rev, "mtd_rev": mtd_rev, "last_month_rev": last_rev,
            "mtd_rev_global": None,
            "reason": None,
        }
        print()

    # ---------- Determine global_latest (across channels that had any finalized day) ----------
    latest_candidates = [d["rev_latest"] for d in per_chan.values() if d.get("rev_latest")]
    global_latest = max(latest_candidates) if latest_candidates else None
    print("Global latest finalized day:", global_latest if global_latest else "(none)")

    # ---------- Second pass: recompute MTD up to global_latest for every channel ----------
    if global_latest:
        month_first_global = global_latest.replace(day=1)
        for label in labels_order:
            d = per_chan.setdefault(label, {})
            d.setdefault("reason", None)

            at = d.get("access_token")
            if not at:
                rf = tokens[label].get("refresh_token")
                if rf:
                    try:
                        at = get_access_token(rf)
                        d["access_token"] = at
                    except Exception:
                        at = None
                        d["reason"] = d.get("reason") or "no_access_token"

            if not at:
                print(f"  ! {label}: cannot recompute MTD to global (no access token).")
                d["mtd_rev_global"] = None
                d["reason"] = d.get("reason") or "no_access_token"
                continue

            try:
                mtd_to_global = sum_metric(
                    at,
                    month_first_global.isoformat(),
                    global_latest.isoformat(),
                    "estimatedRevenue"
                )
                d["mtd_rev_global"] = float(mtd_to_global)  # 0.0 is valid (shows $0)
                print(f"  {label}: MTD to {global_latest} = {fmt_or_dash_money(mtd_to_global)}")
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 403:
                    print(f"  ! {label}: 403 when recomputing MTD to global (not monetized / no Analytics access).")
                    d["mtd_rev_global"] = None
                    d["reason"] = "403_forbidden"
                else:
                    print(f"  ! {label}: error recomputing MTD to global:", e)
                    d["mtd_rev_global"] = None
                    d["reason"] = "error"

    # ---------- Slack ----------
    blocks = build_revenue_sections(labels_order, per_chan, global_latest)
    post_to_slack({"blocks": blocks}, label="Revenue")

    # ---------- Console compact summary ----------
    print("\nSummary (compact)")
    for label in labels_order:
        d = per_chan.get(label, {})
        rY = fmt_or_dash_money(d.get("y_rev")) if d.get("rev_latest") else EM_DASH
        rM = fmt_or_dash_money(d.get("mtd_rev")) if d.get("rev_latest") else EM_DASH
        rG = fmt_or_dash_money(d.get("mtd_rev_global")) if global_latest and d.get("mtd_rev_global") is not None else EM_DASH
        rL = fmt_or_dash_money(d.get("last_month_rev")) if d.get("rev_latest") else EM_DASH
        print(f"{label}: Rev(Y/MTD(ch)/MTD(global)/LM)={rY}/{rM}/{rG}/{rL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nFATAL:", e)
        traceback.print_exc()
        sys.exit(1)
