#!/usr/bin/env python3
"""
YouTube AdSense KPI Poster — Revenue ONLY (Colab & CI Ready)

- Pulls per-channel revenue from YouTube Analytics.
- Posts one Slack message (Revenue) with totals and per-channel lines.
- Optional Google Sheets append/overwrite.
- Auto-hydrates YT_CLIENT_ID/SECRET from YT_OAUTH_CLIENT_JSON or YT_OAUTH_CLIENT.

Env you can set (Colab or CI):
  Required (one of):
    - YT_CLIENT_ID + YT_CLIENT_SECRET
    - YT_OAUTH_CLIENT_JSON (path) or YT_OAUTH_CLIENT (json blob string)
  Also required:
    - SLACK_WEBHOOK_URL
    - YT_TOKENS_FILE  (e.g., yt_refresh_tokens.json)

Optional (Sheets):
  - GOOGLE_SHEET_URL (empty to skip)
  - SHEETS_AUTH_MODE=service_account|oauth (default: service_account)
  - SERVICE_ACCOUNT_JSON (file path)
  - SHEET_TAB (default: facts_revenue)
  - APPEND_TO_SHEET=true|false
"""

import os, json, sys, re, pathlib, traceback
import requests
import pandas as pd
from datetime import date, timedelta

# Optional Sheets
import gspread
from gspread_dataframe import set_with_dataframe

# -------------------- CONFIG --------------------
CLIENT_ID     = os.getenv("YT_CLIENT_ID",     "YOUR_WEB_CLIENT_ID.apps.googleusercontent.com")
CLIENT_SECRET = os.getenv("YT_CLIENT_SECRET", "YOUR_WEB_CLIENT_SECRET")

TOKENS_FILE   = os.getenv("YT_TOKENS_FILE",   "yt_refresh_tokens.json")

CURRENCY           = os.getenv("YT_CURRENCY", "USD")
SLACK_WEBHOOK_URL  = os.getenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/XXXXX/XXXXX/XXXXXXXX")

GOOGLE_SHEET_URL   = os.getenv("GOOGLE_SHEET_URL", "")
SHEET_TAB          = os.getenv("SHEET_TAB", "facts_revenue")
APPEND_TO_SHEET    = os.getenv("APPEND_TO_SHEET", "true").lower() == "true"

SHEETS_AUTH_MODE   = os.getenv("SHEETS_AUTH_MODE", "service_account").strip()
SERVICE_ACCOUNT_JSON   = os.getenv("SERVICE_ACCOUNT_JSON", "service_account.json")
GSPREAD_CLIENT_SECRET  = os.getenv("GSPREAD_CLIENT_SECRET", "client_secret_XXXX.json")
GSPREAD_AUTHORIZED_USER = os.getenv("GSPREAD_AUTHORIZED_USER", "gspread_authorized_user.json")

# -------------------- CONSTANTS --------------------
TOKEN_URL = "https://oauth2.googleapis.com/token"
ANALYTICS = "https://youtubeanalytics.googleapis.com/v2/reports"
DATA_API  = "https://www.googleapis.com/youtube/v3/channels"

EM_DASH = "—"
SPACER_BLOCK = {"type": "section", "text": {"type": "mrkdwn", "text": "\u200b"}}

# -------------------- STARTUP INFO --------------------
def info_banner():
    print("="*60)
    print("YouTube AdSense KPI Poster — Revenue ONLY")
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
    p = pathlib.Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tokens file not found: {path}")
    raw = p.read_text(encoding="utf-8")
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

def detect_latest_day(access_token, metric: str) -> date:
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
    latest_str = max(r[0] for r in rows)
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
        from google_auth_oauthlib.flow import InstalledAppFlow
        SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        if not os.path.exists(GSPREAD_CLIENT_SECRET):
            raise FileNotFoundError(
                f"[Sheets] GSPREAD_CLIENT_SECRET not found: {GSPREAD_CLIENT_SECRET}\n"
                "Use a Desktop (installed) client JSON (top-level key 'installed')."
            )
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
        latest_any = d.get("rev_latest")
        if latest_any is None:
            continue
        facts.append({
            "as_of_day": d["rev_latest"].isoformat(),
            "channel": label,
            "y_rev": d.get("y_rev"),
            "mtd_rev": d.get("mtd_rev"),
            "last_month_rev": d.get("last_month_rev"),
            "currency": currency,
        })

    df = pd.DataFrame(facts)
    if df.empty:
        print("[Sheets] No rows to write (df empty).")
        return

    cols = ["as_of_day", "channel", "y_rev", "mtd_rev", "last_month_rev", "currency"]
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

def fmt_or_dash_money(x):
    return fmt_money(x) if isinstance(x, (int, float)) else EM_DASH

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

def build_revenue_sections(labels, per_chan):
    y_lines = []
    proj_lines = []
    tot_y = 0.0
    tot_mtd = 0.0
    tot_last = 0.0
    latest_candidates = []
    y_date_candidates = []  # track date used for “yesterday”

    for label in labels:
        d = per_chan.get(label, {})
        latest = d.get("rev_latest")
        y_val = d.get("y_rev")
        mtd_val = d.get("mtd_rev")
        last_val = d.get("last_month_rev")

        # Yesterday
        if latest is None or y_val is None:
            y_lines.append(f"• {label}: {EM_DASH}")
        else:
            y_lines.append(f"• {label}: {fmt_money(y_val)}")
            tot_y += y_val
            y_date_candidates.append(latest)

        # Projection
        if latest is None or mtd_val is None:
            proj_lines.append(f"• {label}: {EM_DASH}")
        else:
            ch_proj = month_projection_for(latest, mtd_val)
            proj_lines.append(f"• {label}: {fmt_money(ch_proj)}{pct_change(ch_proj, last_val)}")
            tot_mtd += mtd_val
            if isinstance(last_val, (int, float)):
                tot_last += last_val
            latest_candidates.append(latest)

    # Totals & dates
    has_any = bool(latest_candidates)
    global_latest = max(latest_candidates) if has_any else None
    tot_proj = month_projection_for(global_latest, tot_mtd) if has_any else None
    if not has_any:
        tot_y = None
        tot_mtd = None
        tot_last = None
    y_date = max(y_date_candidates).isoformat() if y_date_candidates else EM_DASH

    header_y = f":spiral_calendar_pad: *Yesterday:* {fmt_or_dash_money(tot_y)}"
    # Changed: “Projection” -> “[P]”
    header_proj = f":calendar: *This Month [P]:* {fmt_or_dash_money(tot_proj)}" + \
                  (pct_change(tot_proj or 0.0, tot_last or 0.0) if (tot_proj is not None and tot_last) else "")

    blocks = [
        {"type":"section","text":{"type":"mrkdwn","text":"*AdSense KPI (Revenue)*"}},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":header_y}},
        SPACER_BLOCK,
        {"type":"context","elements":[{"type":"mrkdwn","text":f"_Yesterday date:_ *{y_date}*"}]},
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

def slack_self_test():
    if not SLACK_WEBHOOK_URL or "hooks.slack.com/services/" not in SLACK_WEBHOOK_URL:
        print("[Slack] Webhook missing or placeholder. Set SLACK_WEBHOOK_URL."); return
    try:
        r = requests.post(SLACK_WEBHOOK_URL, json={"text": "✅ Slack webhook test: Hello from YouTube Revenue bot."}, timeout=10)
        print("Slack test status:", r.status_code, r.text[:200] or "(empty)")
    except Exception as e:
        print("Slack test failed:", e)

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

        # Revenue
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

        per_chan[label] = {
            "channel_id": ch_id, "channel_title": ch_title,
            "rev_latest": rev_latest,
            "y_rev": y_rev, "mtd_rev": mtd_rev, "last_month_rev": last_rev,
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

    # Slack
    slack_self_test()
    blocks = build_revenue_sections(labels_order, per_chan)
    post_to_slack({"blocks": blocks}, label="Revenue")

    # Console compact summary
    print("\nSummary (compact)")
    for label in labels_order:
        d = per_chan.get(label, {})
        rY = fmt_or_dash_money(d.get("y_rev")) if d.get("rev_latest") else EM_DASH
        rM = fmt_or_dash_money(d.get("mtd_rev")) if d.get("rev_latest") else EM_DASH
        rL = fmt_or_dash_money(d.get("last_month_rev")) if d.get("rev_latest") else EM_DASH
        print(f"{label}: Rev(Y/MTD/LM)={rY}/{rM}/{rL}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\nFATAL:", e)
        traceback.print_exc()
        sys.exit(1)
