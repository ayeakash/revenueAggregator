#!/usr/bin/env python3
"""
YouTube AdSense KPI Poster — Revenue + Views (GitHub Actions Ready)

- Pulls per-channel revenue (if monetized) and views from YouTube Analytics.
- Posts two Slack messages (Revenue, Views) with totals and per-channel lines.
- Optionally appends a row per channel to a Google Sheet (via Service Account).
- Safe for CI: reads secrets from env vars, reads JSON blobs from files.

Environment (set in GitHub Actions):
  YT_CLIENT_ID, YT_CLIENT_SECRET, SLACK_WEBHOOK_URL, GOOGLE_SHEET_URL (optional)
  SHEETS_AUTH_MODE=service_account (recommended)
  SERVICE_ACCOUNT_JSON=service_account.json
  YT_TOKENS_FILE=yt_refresh_tokens.json
  YT_CURRENCY=USD|INR

Files (written at runtime from GitHub Secrets by the workflow):
  yt_refresh_tokens.json  (from secret: YT_TOKENS_JSON)
  service_account.json    (from secret: SERVICE_ACCOUNT_JSON)
"""

import os, json, sys, re, pathlib, traceback
import requests
import pandas as pd
from datetime import date, timedelta

# Optional Sheets
import gspread
from gspread_dataframe import set_with_dataframe

# -------------------- CONFIG (env-first with safe defaults) --------------------
CLIENT_ID     = os.getenv("YT_CLIENT_ID",     "YOUR_WEB_CLIENT_ID.apps.googleusercontent.com")
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "YOUR_WEB_CLIENT_SECRET")

TOKENS_FILE   = os.getenv("YT_TOKENS_FILE",   "yt_refresh_tokens.json")

CURRENCY           = os.getenv("YT_CURRENCY", "USD")
SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXXXX/XXXXX/XXXXXXXX")

GOOGLE_SHEET_URL   = os.getenv("GOOGLE_SHEET_URL", "")  # leave empty to skip Sheets
SHEET_TAB          = os.getenv("SHEET_TAB", "facts")
APPEND_TO_SHEET    = os.getenv("APPEND_TO_SHEET", "true").lower() == "true"

SHEETS_AUTH_MODE   = os.getenv("SHEETS_AUTH_MODE", "service_account").strip()
SERVICE_ACCOUNT_JSON   = os.getenv("SERVICE_ACCOUNT_JSON", "service_account.json")
GSPREAD_CLIENT_SECRET  = os.getenv("GSPREAD_CLIENT_SECRET", "client_secret_XXXX.json")  # only if oauth mode
GSPREAD_AUTHORIZED_USER = os.getenv("GSPREAD_AUTHORIZED_USER", "gspread_authorized_user.json")

# -------------------- CONSTANTS --------------------
TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS = "https://youtubeanalytics.googleapis.com/v2/reports"
DATA_API  = "https://www.googleapis.com/youtube/v3/channels"

EM_DASH = "—"
SPACER_BLOCK = {"type": "section", "text": {"type": "mrkdwn", "text": "\u200b"}}  # zero-width space

# -------------------- STARTUP INFO --------------------
def info_banner():
    print("="*60)
    print("YouTube AdSense KPI Poster — Revenue + Views (GitHub Actions)")
    print("="*60)
    print("Python:", sys.version)
    print("requests:", requests.__version__)
    print("pandas:", pd.__version__)
    print("Sheets mode:", SHEETS_AUTH_MODE)
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
    """Load JSON with comment/trailing-comma tolerance."""
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tokens file not found: {path}")
    raw = p.read_text(encoding="utf-8")
    raw = re.sub(r"/\*.*?\*/", "", raw, flags=re.S)        # /* ... */
    raw = re.sub(r"^\s*//.*?$", "", raw, flags=re.M)       # // ...
    raw = re.sub(r"^\s*#.*?$",  "", raw, flags=re.M)       # # ...
    raw = re.sub(r",\s*([}\]])", r"\1", raw)               # trailing commas
    data = json.loads(raw)
    if not isinstance(data, dict) or not data:
        raise ValueError("Tokens JSON empty or not an object.")
    return data

def require_env(name, placeholder_fragments=()):
    """Fail fast if a required env var is missing or still a placeholder."""
    val = os.getenv(name, "")
    if not val or any(frag in val for frag in placeholder_fragments):
        raise RuntimeError(f"Missing or placeholder env: {name}")
    return val

# -------------------- AUTH HELPERS --------------------
def get_access_token(refresh_token: str) -> str:
    r = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    })
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
        headers={"Authorization": f"Bearer {access_token}"}
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
            "currency": currency
        }
    )
    try:
        r.raise_for_status()
    except requests.HTTPError:
        _print_http_error(r, "YouTube Analytics API (yt_query)")
        raise
    return r.json()

def detect_latest_day(access_token, metric: str) -> date:
    """Find the latest day with data for the given metric."""
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=14)
    data = yt_query(access_token, start.isoformat(), end.isoformat(), dims="day", metrics=metric)
    rows = data.get("rows") or []
    if not rows:
        start = end - timedelta(days=30)
        data = yt_query(access_token, start.isoformat(), end.isoformat(), dims="day", metrics=metric)
        rows = data.get("rows") or []
        if not rows:
            return None
    latest_str = max(r[0] for r in rows)   # ["YYYY-MM-DD", value]
    y, m, d = map(int, latest_str.split("-"))
    return date(y, m, d)

def sum_metric(access_token, start, end, metric, currency=CURRENCY) -> float:
    data = yt_query(access_token, start, end, dims="day", metrics=metric, currency=currency)
    return float(sum(r[1] for r in (data.get("rows") or [])))

# -------------------- SHEETS (OPTIONAL) --------------------
def get_gspread_client():
    if not GOOGLE_SHEET_URL:
        return None
    mode = SHEETS_AUTH_MODE.lower().strip()
    if mode == "service_account":
        if not os.path.exists(SERVICE_ACCOUNT_JSON):
            raise FileNotFoundError(f"[Sheets] SERVICE_ACCOUNT_JSON not found: {SERVICE_ACCOUNT_JSON}")
        return gspread.service_account(filename=SERVICE_ACCOUNT_JSON)
    if mode == "oauth":
        # Console (no browser) — only if you really want OAuth user flow in CI
        if not os.path.exists(GSPREAD_CLIENT_SECRET):
            raise FileNotFoundError(
                f"[Sheets] GSPREAD_CLIENT_SECRET not found: {GSPREAD_CLIENT_SECRET}\n"
                "Use a Desktop (installed) client JSON (top-level key 'installed')."
            )
        from google_auth_oauthlib.flow import InstalledAppFlow
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        flow = InstalledAppFlow.from_client_secrets_file(GSPREAD_CLIENT_SECRET, SCOPES)
        creds = flow.run_console()
        with open(GSPREAD_AUTHORIZED_USER, "w", encoding="utf-8") as f:
            f.write(json.dumps({
                "client_id": creds.client_id,
                "client_secret": creds.client_secret,
                "refresh_token": creds.refresh_token,
                "token": creds.token,
                "scopes": creds.scopes,
                "type": "authorized_user"
            }))
        return gspread.authorize(creds)
    raise ValueError(f"[Sheets] Unknown SHEETS_AUTH_MODE: {SHEETS_AUTH_MODE}")

def write_facts_to_sheets(per_chan, currency, sheet_url, sheet_tab):
    if not sheet_url:
        print("[Sheets] Skipped (GOOGLE_SHEET_URL empty)")
        return

    gc = get_gspread_client()
    sh = gc.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(sheet_tab)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_tab, rows="1000", cols="20")

    facts = []
    for label, d in per_chan.items():
        latest_any = d.get("rev_latest") or d.get("views_latest")
        if latest_any is None:
            continue
        facts.append({
            "as_of_day": (d.get("rev_latest") or d.get("views_latest")).isoformat(),
            "channel": label,
            # revenue fields (may be None)
            "y_rev": d.get("y_rev"),
            "mtd_rev": d.get("mtd_rev"),
            "last_month_rev": d.get("last_month_rev"),
            "currency": currency,
            # views fields
            "y_views": int(d["y_views"]) if isinstance(d.get("y_views"), (int, float)) else None,
            "mtd_views": int(d["mtd_views"]) if isinstance(d.get("mtd_views"), (int, float)) else None,
            "last_month_views": int(d["last_month_views"]) if isinstance(d.get("last_month_views"), (int, float)) else None,
        })

    df = pd.DataFrame(facts)
    if df.empty:
        print("[Sheets] No rows to write (df empty).")
        return

    cols = ["as_of_day", "channel", "y_rev", "mtd_rev", "last_month_rev", "currency",
            "y_views", "mtd_views", "last_month_views"]
    for c in ["y_rev","mtd_rev","last_month_rev"]:
        if c in df.columns:
            df[c] = df[c].map(lambda v: round(v,2) if isinstance(v,(int,float)) else None)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    if APPEND_TO_SHEET:
        values = ws.get_all_values()
        if not values:
            ws.append_row(cols)
            print("[Sheets] Header created.")
        header = values[0] if values else cols
        if header != cols:
            ws.clear()
            ws.append_row(cols)
            print("[Sheets] Header reset.")
        ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")
        print(f"[Sheets] Appended {len(df)} row(s) to '{sheet_tab}'.")
    else:
        set_with_dataframe(ws, df)
        print(f"[Sheets] Overwrote '{sheet_tab}' with {len(df)} row(s).")

# -------------------- SLACK FORMAT & POSTING --------------------
def fmt_money(x):
    return f"${x:,.0f}" if CURRENCY.upper() == "USD" else f"{CURRENCY} {x:,.0f}"

def fmt_number(x):
    return f"{int(round(x)):,.0f}"

def fmt_or_dash_money(x):
    return fmt_money(x) if isinstance(x, (int, float)) else EM_DASH

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

def build_section_lists(labels, per_chan, metric_key_prefix: str, value_formatter, latest_key: str):
    """
    metric_key_prefix: "rev" or "views"
    latest_key: "rev_latest" or "views_latest"
    Returns (yesterday_lines, projection_lines, total_y, total_mtd, total_last, total_proj)
    """
    y_key = f"y_{metric_key_prefix}"
    mtd_key = f"mtd_{metric_key_prefix}"
    last_key = f"last_month_{metric_key_prefix}"

    total_y = 0.0
    total_mtd = 0.0
    total_last = 0.0
    latest_candidates = []

    y_lines = []
    proj_lines = []

    for label in labels:
        d = per_chan.get(label, {})
        latest = d.get(latest_key)

        # Yesterday
        y_val = d.get(y_key)
        if latest is None or y_val is None:
            y_lines.append(f"• {label}: {EM_DASH}")
        else:
            y_lines.append(f"• {label}: {value_formatter(y_val)}")
            total_y += y_val

        # Projection
        mtd_val = d.get(mtd_key)
        last_val = d.get(last_key)
        if latest is None or mtd_val is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        else:
            ch_proj = month_projection_for(latest, mtd_val)
            ch_pct = pct_change(ch_proj, last_val)
            proj_lines.append(f"• {label}: {value_formatter(ch_proj)}{ch_pct}")
            total_mtd += mtd_val
            if isinstance(last_val, (int, float)):
                total_last += last_val
            latest_candidates.append(latest)

    global_latest = max(latest_candidates) if latest_candidates else None
    total_proj = month_projection_for(global_latest, total_mtd) if global_latest else 0.0

    return y_lines, proj_lines, total_y, total_mtd, total_last, total_proj, global_latest

def slack_blocks_full(per_chan: dict, labels_order: list):
    # Revenue section
    y_lines_rev, proj_lines_rev, tot_y_rev, tot_mtd_rev, tot_last_rev, tot_proj_rev, _ = \
        build_section_lists(labels_order, per_chan, "rev",  fmt_money, "rev_latest")

    # Views section
    y_lines_views, proj_lines_views, tot_y_views, tot_mtd_views, tot_last_views, tot_proj_views, _ = \
        build_section_lists(labels_order, per_chan, "views", fmt_number, "views_latest")

    rev_header = f":spiral_calendar_pad: *Yesterday:* {fmt_or_dash_money(tot_y_rev)}"
    rev_proj_header = f":calendar: *This Month Projection:* {fmt_or_dash_money(tot_proj_rev)}{pct_change(tot_proj_rev, tot_last_rev)}"

    views_header = f":spiral_calendar_pad: *Yesterday (Views):* {fmt_or_dash_number(tot_y_views)}"
    views_proj_header = f":calendar: *This Month Projection (Views):* {fmt_or_dash_number(tot_proj_views)}{pct_change(tot_proj_views, tot_last_views)}"

    return [
        {"type":"section","text":{"type":"mrkdwn","text":"*AdSense KPI*"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":rev_header}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(y_lines_rev)}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":rev_proj_header}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(proj_lines_rev)}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":"*Views KPI*"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":views_header}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(y_lines_views)}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":views_proj_header}},
        SPACER_BLOCK,
        {"type":"section","text":{"type":"mrkdwn","text":"\n\n".join(proj_lines_views)}},
    ]

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

def post_blocks_in_two_parts(blocks):
    # Split on the Views header to avoid size/format issues
    split_idx = next((i for i,b in enumerate(blocks)
                      if b.get("type")=="section"
                      and "*Views KPI*" in b.get("text",{}).get("text","")),
                     None)
    if split_idx is None:
        return post_to_slack({"blocks": blocks}, label="single")

    part_a = blocks[:split_idx]   # Revenue part
    part_b = blocks[split_idx:]   # Views part

    ok1 = post_to_slack({"blocks": part_a}, label="part A (Revenue)")
    ok2 = post_to_slack({"blocks": part_b}, label="part B (Views)") if ok1 else False
    return ok1 and ok2

def slack_self_test():
    if not SLACK_WEBHOOK_URL or "hooks.slack.com/services/" not in SLACK_WEBHOOK_URL:
        print("[Slack] Webhook missing or placeholder. Set SLACK_WEBHOOK_URL."); return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": "✅ Slack webhook test: Hello from YouTube KPI bot."}, timeout=10)
        print("Slack test status:", r.status_code, r.text[:200] or "(empty)")
    except Exception as e:
        print("Slack test failed:", e)

# -------------------- MAIN --------------------
def main():
    info_banner()

    # Fail fast if these are placeholders
    require_env("YT_CLIENT_ID", ("YOUR_WEB_CLIENT_ID",))
    require_env("YT_CLIENT_SECRET", ("YOUR_WEB_CLIENT_SECRET",))
    require_env("SLACK_WEBHOOK_URL", ("hooks.slack.com/services/XXXXX",))

    tokens = load_tokens_file(TOKENS_FILE)
    labels_order = list(tokens.keys())  # preserve channel order for Slack

    per_chan = {}

    for label, obj in tokens.items():
        print(f"— Channel label: {label}")
        rf = obj.get("refresh_token")
        if not rf:
            print("  ! Missing refresh_token, skipping\n")
            per_chan[label] = {"rev_latest": None, "views_latest": None}
            continue

        try:
            at = get_access_token(rf)
        except Exception:
            print("  ! Token exchange failed; skipping.\n")
            per_chan[label] = {"rev_latest": None, "views_latest": None}
            continue

        # Identity (for logs)
        try:
            ch_id, ch_title = channel_identity(at)
            print(f"  id={ch_id}  title={ch_title}")
        except Exception:
            ch_id, ch_title = None, None
            print("  ! channel_identity failed; continuing…")

        # ---------- Revenue ----------
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
                print("  ! Revenue 403 (likely not monetized or no revenue data).")
            else:
                print("  ! Revenue query failed:", e)

        # ---------- Views ----------
        views_latest = None
        y_views = mtd_views = last_views = None
        try:
            views_latest = detect_latest_day(at, "views")
            if views_latest:
                v_month_first = views_latest.replace(day=1)
                v_prev_month_last = v_month_first - timedelta(days=1)
                v_prev_month_first = v_prev_month_last.replace(day=1)

                y_views    = sum_metric(at, views_latest.isoformat(), views_latest.isoformat(), "views")
                mtd_views  = sum_metric(at, v_month_first.isoformat(), views_latest.isoformat(), "views")
                last_views = sum_metric(at, v_prev_month_first.isoformat(), v_prev_month_last.isoformat(), "views")
                print(f"  Views latest: {views_latest}  Y={fmt_or_dash_number(y_views)}  MTD={fmt_or_dash_number(mtd_views)}  LastM={fmt_or_dash_number(last_views)}")
            else:
                print("  ! No views rows returned.")
        except requests.HTTPError as e:
            print("  ! Views query failed:", e)

        per_chan[label] = {
            "channel_id": ch_id, "channel_title": ch_title,

            # revenue fields
            "rev_latest": rev_latest,
            "y_rev": y_rev, "mtd_rev": mtd_rev, "last_month_rev": last_rev,

            # views fields
            "views_latest": views_latest,
            "y_views": y_views, "mtd_views": mtd_views, "last_month_views": last_views,
        }
        print()

    # Sheets (optional)
    try:
        if GOOGLE_SHEET_URL:
            write_facts_to_sheets(per_chan, CURRENCY, GOOGLE_SHEET_URL, SHEET_TAB)
        else:
            print("[Sheets] Skipped (GOOGLE_SHEET_URL empty)")
    except Exception as e:
        print("[Sheets] Skipping Sheets due to error:", e)

    # Slack: test webhook, build blocks, and post in two parts
    slack_self_test()
    blocks = slack_blocks_full(per_chan, labels_order)
    post_blocks_in_two_parts(blocks)

    # Console compact summary
    print("\nSummary (compact)")
    for label in labels_order:
        d = per_chan.get(label, {})
        rY = fmt_or_dash_money(d.get("y_rev")) if d.get("rev_latest") else EM_DASH
        rM = fmt_or_dash_money(d.get("mtd_rev")) if d.get("rev_latest") else EM_DASH
        rL = fmt_or_dash_money(d.get("last_month_rev")) if d.get("rev_latest") else EM_DASH
        vY = fmt_or_dash_number(d.get("y_views")) if d.get("views_latest") else EM_DASH
        vM = fmt_or_dash_number(d.get("mtd_views")) if d.get("views_latest") else EM_DASH
        vL = fmt_or_dash_number(d.get("last_month_views")) if d.get("views_latest") else EM_DASH
        print(f"{label}: Rev(Y/MTD/LM)={rY}/{rM}/{rL} | Views(Y/MTD/LM)={vY}/{vM}/{vL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nFATAL:", e)
        traceback.print_exc()
        sys.exit(1)
