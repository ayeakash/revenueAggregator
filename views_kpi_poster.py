#!/usr/bin/env python3
"""
YouTube KPI Poster — Views ONLY
(No Sheets, Global Projection, Improved Latest-Date Detection, Tail Debug)

- Pulls per-channel VIEWS from YouTube Analytics.
- Uses a single global latest finalized day for consistent MTD + projection across all channels.
- Posts one Slack message with totals and per-channel lines.
- Auto-hydrates YT_CLIENT_ID/SECRET from YT_OAUTH_CLIENT_JSON or YT_OAUTH_CLIENT.

Env (required):
  - SLACK_WEBHOOK_URL
  - YT_TOKENS_FILE  (e.g., yt_refresh_tokens.json)
  AND EITHER:
  - YT_CLIENT_ID + YT_CLIENT_SECRET
  OR
  - YT_OAUTH_CLIENT_JSON (path) / YT_OAUTH_CLIENT (json blob string)

Optional:
  - YT_LATEST_PAD_DAYS (default 2)  -> pad end of window when detecting latest day
  - YT_DEBUG_TAIL=true|false (default false) -> print last 7 API rows per channel (views)
"""

import os, json, sys, re, pathlib, traceback
import requests
from datetime import date, timedelta

# -------------------- CONFIG --------------------
CLIENT_ID     = os.getenv("YT_CLIENT_ID",     "YOUR_WEB_CLIENT_ID.apps.googleusercontent.com")
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "YOUR_WEB_CLIENT_SECRET")

TOKENS_FILE   = os.getenv("YT_TOKENS_FILE",   "yt_refresh_tokens.json")

SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXXXX/XXXXX/XXXXXXXX")

LATEST_PAD_DAYS    = int(os.getenv("YT_LATEST_PAD_DAYS", "2"))
DEBUG_TAIL         = os.getenv("YT_DEBUG_TAIL", "false").lower() == "true"

# -------------------- CONSTANTS --------------------
TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS = "https://youtubeanalytics.googleapis.com/v2/reports"
DATA_API  = "https://www.googleapis.com/youtube/v3/channels"

EM_DASH = "—"
SPACER_BLOCK = {"type": "section", "text": {"type": "mrkdwn", "text": "\u200b"}}

# -------------------- STARTUP INFO --------------------
def info_banner():
    print("="*60)
    print("YouTube KPI Poster — Views ONLY (No Sheets, Global Projection)")
    print("="*60)
    print("Python:", sys.version)
    print("requests:", requests.__version__)
    print("Latest pad days:", LATEST_PAD_DAYS)
    print("Debug tail:", DEBUG_TAIL)
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
    # allow /* ... */, //..., #... comments, and trailing commas
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
    # Example: 19-Oct-2025
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
def yt_query(access_token, start_date, end_date, dims="day", metrics="views"):
    r = requests.get(
        ANALYTICS,
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "ids": "channel==MINE",
            "startDate": start_date,
            "endDate": end_date,
            "metrics": metrics,
            "dimensions": dims
        },
        timeout=60
    )
    try:
        r.raise_for_status()
    except requests.HTTPError:
        _print_http_error(r, "YouTube Analytics API (yt_query)")
        raise
    return r.json()

# Latest-day detection with padding to cover PT finalize lag & local day boundaries
def detect_latest_day(access_token, metric: str) -> date:
    """
    Ask YT Analytics for a wide window ending 'today + pad' and pick the latest date
    the API actually returns. Padding helps across time zones (IST vs PT).
    """
    pad_days = max(0, LATEST_PAD_DAYS)
    end = date.today() + timedelta(days=pad_days)
    start = end - timedelta(days=45)
    try:
        data = yt_query(access_token, start.isoformat(), end.isoformat(),
                        dims="day", metrics=metric)
    except requests.HTTPError:
        start = end - timedelta(days=75)
        data  = yt_query(access_token, start.isoformat(), end.isoformat(),
                         dims="day", metrics=metric)
    rows = data.get("rows") or []
    if not rows:
        return None
    latest_str = max(r[0] for r in rows)
    y, m, d = map(int, latest_str.split("-"))
    return date(y, m, d)

def sum_metric(access_token, start, end, metric) -> float:
    data = yt_query(access_token, start, end, dims="day", metrics=metric)
    return float(sum(r[1] for r in (data.get("rows") or [])))

# --------- DEBUG HELPERS: print last 7 API rows per channel (toggle via YT_DEBUG_TAIL) ----------
def last_n_days_debug(access_token, metric="views", days=14):
    """Return the last N day-rows (as list of (date_str, value)) that the API actually returns."""
    e = date.today() + timedelta(days=max(0, LATEST_PAD_DAYS))
    s = e - timedelta(days=60)
    data = yt_query(access_token, s.isoformat(), e.isoformat(), dims="day", metrics=metric)
    rows = data.get("rows") or []
    rows.sort(key=lambda r: r[0])  # chronological
    return rows[-days:]

def print_api_tail_for_views(per_chan, labels_order):
    print("\n[DEBUG] API tail for views (last ~7 rows)")
    for label in labels_order:
        at = per_chan.get(label, {}).get("access_token")
        if not at:
            print(f"  - {label}: (no access token)")
            continue
        try:
            tail = last_n_days_debug(at, "views", days=7)
            if not tail:
                print(f"  - {label}: (no rows)")
                continue
            pretty = ", ".join([f"{d}:{int(v):,}" for d, v in tail])
            print(f"  - {label}: {pretty}")
        except Exception as e:
            print(f"  - {label}: error fetching tail: {e}")

# -------------------- SLACK FORMAT & POSTING --------------------
def fmt_number(x):
    return f"{int(round(x)):,.0f}"

def fmt_or_dash_number(x):
    return fmt_number(x) if isinstance(x, (int, float)) else EM_DASH

def month_projection_for(latest_day: date, mtd_total: float) -> float:
    if not latest_day or not isinstance(mtd_total, (int, float)):
        return 0.0
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

def build_views_sections(labels, per_chan, global_latest):
    y_lines = []
    proj_lines = []
    tot_y = 0.0
    tot_mtd_global = 0.0
    tot_last = 0.0

    for label in labels:
        d = per_chan.get(label, {})
        # ---------- Yesterday ----------
        latest = d.get("views_latest")
        y_val  = d.get("y_views")
        if latest is None or y_val is None:
            y_lines.append(f"• {label}: {EM_DASH}")
        else:
            y_lines.append(f"• {label}: {fmt_number(y_val)}")
            tot_y += y_val

        # ---------- Projection (global) ----------
        ch_mtd_glob = d.get("mtd_views_global", None)
        ch_last     = d.get("last_month_views", None)

        if global_latest is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        elif ch_mtd_glob is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        else:
            ch_proj = month_projection_for(global_latest, ch_mtd_glob)
            proj_lines.append(f"• {label}: {fmt_number(ch_proj)}{pct_change(ch_proj, ch_last)}")
            tot_mtd_global += ch_mtd_glob
            if isinstance(ch_last, (int, float)):
                tot_last += ch_last

    # Totals & header date
    tot_proj = month_projection_for(global_latest, tot_mtd_global) if global_latest else None
    if global_latest is None:
        tot_y = None
        tot_last = None

    header_title = f"*Views KPI* ({fmt_day(global_latest)})" if global_latest else "*Views KPI*"
    header_y     = f":spiral_calendar_pad: *Yesterday:* {fmt_or_dash_number(tot_y)}"
    header_proj  = f":calendar: *This Month Projection:* {fmt_or_dash_number(tot_proj)}" + \
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

    # ---------- First pass: identity + latest day + Y/MTD/Last (per-channel latest) ----------
    for label, obj in tokens.items():
        print(f"— Channel label: {label}")
        rf = obj.get("refresh_token")
        if not rf:
            print("  ! Missing refresh_token, skipping\n")
            per_chan[label] = {"views_latest": None}
            continue

        try:
            at = get_access_token(rf)
        except Exception:
            print("  ! Token exchange failed; skipping.\n")
            per_chan[label] = {"views_latest": None}
            continue

        try:
            ch_id, ch_title = channel_identity(at)
            print(f"  id={ch_id}  title={ch_title}")
        except Exception:
            ch_id, ch_title = None, None
            print("  ! channel_identity failed; continuing…")

        views_latest = None
        y_views = mtd_views = last_views = None
        try:
            views_latest = detect_latest_day(at, "views")
            if views_latest:
                month_first = views_latest.replace(day=1)
                prev_month_last = month_first - timedelta(days=1)
                prev_month_first = prev_month_last.replace(day=1)

                y_views    = sum_metric(at, views_latest.isoformat(), views_latest.isoformat(), "views")
                mtd_views  = sum_metric(at, month_first.isoformat(), views_latest.isoformat(), "views")
                last_views = sum_metric(at, prev_month_first.isoformat(), prev_month_last.isoformat(), "views")
                print(f"  Views latest: {views_latest}  Y={fmt_or_dash_number(y_views)}  MTD={fmt_or_dash_number(mtd_views)}  LastM={fmt_or_dash_number(last_views)}")
            else:
                print("  ! No views rows returned.")
        except requests.HTTPError as e:
            print("  ! Views query failed:", e)

        per_chan[label] = {
            "access_token": at,
            "channel_id": ch_id, "channel_title": ch_title,
            "views_latest": views_latest,
            "y_views": y_views, "mtd_views": mtd_views, "last_month_views": last_views,
            "mtd_views_global": None,
            "reason": None,
        }
        print()

    # --------- Optional debug: show last 7 API rows per channel ----------
    if DEBUG_TAIL:
        print_api_tail_for_views(per_chan, labels_order)

    # ---------- Determine global_latest (across channels that had any finalized day) ----------
    latest_candidates = [d["views_latest"] for d in per_chan.values() if d.get("views_latest")]
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
                d["mtd_views_global"] = None
                d["reason"] = d.get("reason") or "no_access_token"
                continue

            try:
                mtd_to_global = sum_metric(
                    at,
                    month_first_global.isoformat(),
                    global_latest.isoformat(),
                    "views"
                )
                d["mtd_views_global"] = float(mtd_to_global)  # 0.0 is valid (shows 0)
                print(f"  {label}: MTD to {global_latest} = {fmt_or_dash_number(mtd_to_global)}")
            except requests.HTTPError as e:
                status = getattr(e.response, "status_code", None)
                if status == 403:
                    print(f"  ! {label}: 403 when recomputing MTD to global (no Analytics access).")
                    d["mtd_views_global"] = None
                    d["reason"] = "403_forbidden"
                else:
                    print(f"  ! {label}: error recomputing MTD to global:", e)
                    d["mtd_views_global"] = None
                    d["reason"] = "error"

    # ---------- Slack ----------
    blocks = build_views_sections(labels_order, per_chan, global_latest)
    post_to_slack({"blocks": blocks}, label="Views")

    # ---------- Console compact summary ----------
    print("\nSummary (compact)")
    for label in labels_order:
        d = per_chan.get(label, {})
        vY = fmt_or_dash_number(d.get("y_views")) if d.get("views_latest") else EM_DASH
        vM = fmt_or_dash_number(d.get("mtd_views")) if d.get("views_latest") else EM_DASH
        vG = fmt_or_dash_number(d.get("mtd_views_global")) if global_latest and d.get("mtd_views_global") is not None else EM_DASH
        vL = fmt_or_dash_number(d.get("last_month_views")) if d.get("views_latest") else EM_DASH
        print(f"{label}: Views(Y/MTD(ch)/MTD(global)/LM)={vY}/{vM}/{vG}/{vL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nFATAL:", e)
        traceback.print_exc()
        sys.exit(1)
