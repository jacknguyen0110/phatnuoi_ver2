import os
import re
import json
import sqlite3
from datetime import datetime

import requests
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials

APP_NAME = "Tra cuu phat nguoi coordinator"
PORT = int(os.getenv("PORT", "8080"))
DB_PATH = os.getenv("DB_PATH", "jobs.db")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()

GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Phat Nguoi").strip()

WORKER_SHARED_KEY = os.getenv("WORKER_SHARED_KEY", "").strip()

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Thiếu TELEGRAM_BOT_TOKEN")
if not WORKER_SHARED_KEY:
    raise RuntimeError("Thiếu WORKER_SHARED_KEY")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

HEADERS = [
    "created_at",
    "job_id",
    "telegram_update_id",
    "telegram_chat_id",
    "telegram_user_id",
    "telegram_username",
    "telegram_full_name",
    "plate_input",
    "plate_normalized",
    "plate_display",
    "job_status",
    "result_status",
    "vehicle_type",
    "plate_color",
    "violation_code",
    "violation_text",
    "violation_time",
    "violation_location",
    "detected_by",
    "resolved_by",
    "screenshot_path",
    "debug_note",
]


app = Flask(__name__)
_gspread_client = None
_worksheet = None


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_plate(text: str) -> str:
    text = (text or "").upper().strip()
    text = re.sub(r"[^A-Z0-9]", "", text)
    return text


def display_plate(plate: str) -> str:
    p = normalize_plate(plate)
    if len(p) == 8 and re.match(r"^[0-9]{2}[A-Z][0-9]{5}$", p):
        return f"{p[:3]}-{p[3:6]}.{p[6:]}"
    if len(p) == 9 and re.match(r"^[0-9]{2}[A-Z]{2}[0-9]{5}$", p):
        return f"{p[:4]}-{p[4:7]}.{p[7:]}"
    return p


def parse_plate_lines(text: str):
    text = (text or "").strip()
    if not text:
        return []
    raw_items = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r"[;,]+", line)
        for part in parts:
            v = normalize_plate(part)
            if v:
                raw_items.append(v)

    seen = set()
    result = []
    for p in raw_items:
        if p not in seen:
            seen.add(p)
            result.append(p)
    return result


def tg_send_message(chat_id, text):
    requests.post(
        f"{TELEGRAM_API}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=30,
    )


def tg_set_webhook():
    if not WEBHOOK_BASE_URL:
        return
    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/telegram/webhook"
    r = requests.post(f"{TELEGRAM_API}/setWebhook", json={"url": webhook_url}, timeout=30)
    return r.json()


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_updates (
        update_id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        telegram_update_id TEXT,
        chat_id TEXT,
        user_id TEXT,
        username TEXT,
        full_name TEXT,
        plate_input TEXT,
        plate_normalized TEXT,
        plate_display TEXT,
        status TEXT NOT NULL,
        result_json TEXT,
        screenshot_path TEXT,
        debug_note TEXT,
        picked_by TEXT,
        picked_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def get_service_account_info():
    return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)


def get_credentials():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    info = get_service_account_info()
    return Credentials.from_service_account_info(info, scopes=scopes)


def get_gspread_client():
    global _gspread_client
    if _gspread_client is None:
        creds = get_credentials()
        _gspread_client = gspread.authorize(creds)
    return _gspread_client


def get_worksheet():
    global _worksheet
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GOOGLE_SHEET_ID:
        return None

    if _worksheet is not None:
        return _worksheet

    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = sh.worksheet(GOOGLE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GOOGLE_SHEET_NAME, rows=1000, cols=len(HEADERS) + 5)

    values = ws.get_all_values()
    if not values:
        ws.append_row(HEADERS, value_input_option="RAW")

    _worksheet = ws
    return _worksheet


def append_sheet_row(job_row, result):
    ws = get_worksheet()
    if not ws:
        return

    row = [
        job_row["created_at"],
        str(job_row["id"]),
        job_row["telegram_update_id"] or "",
        job_row["chat_id"] or "",
        job_row["user_id"] or "",
        job_row["username"] or "",
        job_row["full_name"] or "",
        job_row["plate_input"] or "",
        job_row["plate_normalized"] or "",
        job_row["plate_display"] or "",
        job_row["status"] or "",
        result.get("result_status", ""),
        result.get("vehicle_type", ""),
        result.get("plate_color", ""),
        result.get("violation_code", ""),
        result.get("violation_text", ""),
        result.get("violation_time", ""),
        result.get("violation_location", ""),
        result.get("detected_by", ""),
        result.get("resolved_by", ""),
        job_row["screenshot_path"] or "",
        job_row["debug_note"] or "",
    ]
    ws.append_row(row, value_input_option="RAW")


def format_result_message(plate_display, result):
    status = result.get("result_status", "")

    if status == "KHONG_CO_VI_PHAM":
        return f"Biển số: {plate_display}\nKết quả: Không có vi phạm."

    if status == "KHONG_CO_DU_LIEU_VI_PHAM":
        return f"Biển số: {plate_display}\nKết quả: Không tìm thấy dữ liệu vi phạm."

    if status.startswith("LOI_TRA_CUU"):
        return f"Biển số: {plate_display}\nKết quả: {status}"

    lines = [
        f"Biển số: {plate_display}",
        f"Trạng thái: {status or 'CO_VI_PHAM'}"
    ]

    mapping = [
        ("vehicle_type", "Loại xe"),
        ("plate_color", "Màu biển"),
        ("violation_code", "Mã lỗi"),
        ("violation_text", "Lỗi vi phạm"),
        ("violation_time", "Thời gian"),
        ("violation_location", "Địa điểm"),
        ("detected_by", "Đơn vị phát hiện"),
        ("resolved_by", "Đơn vị giải quyết"),
    ]
    for key, label in mapping:
        val = result.get(key, "")
        if val:
            lines.append(f"{label}: {val}")

    return "\n".join(lines)


@app.route("/", methods=["GET"])
def home():
    return jsonify({"ok": True, "app": APP_NAME, "time": now_str()})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/set-webhook", methods=["GET"])
def set_webhook_route():
    try:
        data = tg_set_webhook()
        return jsonify({"ok": True, "data": data})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(force=True, silent=True) or {}
    update_id = str(update.get("update_id", ""))

    message = update.get("message") or update.get("edited_message") or {}
    text = (message.get("text") or "").strip()

    chat = message.get("chat") or {}
    from_user = message.get("from") or {}

    chat_id = str(chat.get("id", ""))
    user_id = str(from_user.get("id", ""))
    username = from_user.get("username", "")
    full_name = " ".join(filter(None, [from_user.get("first_name", ""), from_user.get("last_name", "")])).strip()

    if not chat_id:
        return jsonify({"ok": True})

    if text.lower() in ["/start", "start"]:
        tg_send_message(chat_id, "Gửi biển số cần tra cứu, mỗi dòng 1 biển số.\nVí dụ:\n50H12345\n60F00906")
        return jsonify({"ok": True})

    plates = parse_plate_lines(text)
    if not plates:
        tg_send_message(chat_id, "Không đọc được biển số. Vui lòng gửi mỗi dòng 1 biển số.")
        return jsonify({"ok": True})

    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute("SELECT update_id FROM processed_updates WHERE update_id = ?", (update_id,)).fetchone()
    if row:
        conn.close()
        return jsonify({"ok": True, "duplicate": True})

    cur.execute(
        "INSERT INTO processed_updates (update_id, created_at) VALUES (?, ?)",
        (update_id, now_str())
    )

    for plate in plates:
        cur.execute("""
            INSERT INTO jobs (
                created_at, updated_at, telegram_update_id, chat_id, user_id, username, full_name,
                plate_input, plate_normalized, plate_display, status, result_json, screenshot_path,
                debug_note, picked_by, picked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            now_str(), now_str(), update_id, chat_id, user_id, username, full_name,
            plate, normalize_plate(plate), display_plate(plate), "pending", "",
            "", "", "", ""
        ))

    conn.commit()
    conn.close()

    tg_send_message(
        chat_id,
        f"Đã nhận {len(plates)} biển số.\nHệ thống sẽ mở phiên tra cứu trên máy worker.\nKhi đến bước captcha, bạn vui lòng xác minh thủ công trên trình duyệt."
    )
    return jsonify({"ok": True})


@app.route("/worker/poll", methods=["POST"])
def worker_poll():
    data = request.get_json(force=True, silent=True) or {}
    worker_key = (data.get("worker_key") or "").strip()
    worker_name = (data.get("worker_name") or "worker-1").strip()

    if worker_key != WORKER_SHARED_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute("""
        SELECT * FROM jobs
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """).fetchone()

    if not row:
        conn.close()
        return jsonify({"ok": True, "job": None})

    cur.execute("""
        UPDATE jobs
        SET status = 'picked',
            updated_at = ?,
            picked_by = ?,
            picked_at = ?
        WHERE id = ?
    """, (now_str(), worker_name, now_str(), row["id"]))
    conn.commit()

    updated = cur.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone()
    conn.close()

    return jsonify({
        "ok": True,
        "job": {
            "id": updated["id"],
            "plate_input": updated["plate_input"],
            "plate_normalized": updated["plate_normalized"],
            "plate_display": updated["plate_display"],
            "chat_id": updated["chat_id"],
        }
    })


@app.route("/worker/status", methods=["POST"])
def worker_status():
    data = request.get_json(force=True, silent=True) or {}
    worker_key = (data.get("worker_key") or "").strip()
    if worker_key != WORKER_SHARED_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    job_id = int(data["job_id"])
    status = (data.get("status") or "").strip()
    note = (data.get("debug_note") or "").strip()

    conn = get_conn()
    cur = conn.cursor()

    row = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404

    cur.execute("""
        UPDATE jobs
        SET status = ?, debug_note = ?, updated_at = ?
        WHERE id = ?
    """, (status, note, now_str(), job_id))
    conn.commit()

    if status == "waiting_captcha" and row["chat_id"]:
        tg_send_message(
            row["chat_id"],
            f"Biển số {row['plate_display']}: đang chờ bạn xác minh captcha trên trình duyệt worker."
        )

    conn.close()
    return jsonify({"ok": True})


@app.route("/worker/result", methods=["POST"])
def worker_result():
    data = request.get_json(force=True, silent=True) or {}
    worker_key = (data.get("worker_key") or "").strip()
    if worker_key != WORKER_SHARED_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    job_id = int(data["job_id"])
    result = data.get("result") or {}
    screenshot_path = (data.get("screenshot_path") or "").strip()
    debug_note = (data.get("debug_note") or "").strip()

    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404

    cur.execute("""
        UPDATE jobs
        SET status = ?, result_json = ?, screenshot_path = ?, debug_note = ?, updated_at = ?
        WHERE id = ?
    """, (
        "done",
        json.dumps(result, ensure_ascii=False),
        screenshot_path,
        debug_note,
        now_str(),
        job_id
    ))
    conn.commit()

    updated = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()

    if updated["chat_id"]:
        tg_send_message(updated["chat_id"], format_result_message(updated["plate_display"], result))

    try:
        append_sheet_row(updated, result)
    except Exception:
        pass

    return jsonify({"ok": True})


@app.route("/worker/fail", methods=["POST"])
def worker_fail():
    data = request.get_json(force=True, silent=True) or {}
    worker_key = (data.get("worker_key") or "").strip()
    if worker_key != WORKER_SHARED_KEY:
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    job_id = int(data["job_id"])
    error_text = (data.get("error") or "Lỗi không xác định").strip()

    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Job not found"}), 404

    result = {"result_status": f"LOI_TRA_CUU: {error_text[:250]}"}

    cur.execute("""
        UPDATE jobs
        SET status = ?, result_json = ?, debug_note = ?, updated_at = ?
        WHERE id = ?
    """, (
        "failed",
        json.dumps(result, ensure_ascii=False),
        error_text[:500],
        now_str(),
        job_id
    ))
    conn.commit()
    updated = cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    conn.close()

    if updated["chat_id"]:
        tg_send_message(updated["chat_id"], f"Biển số {updated['plate_display']}\nKết quả: {result['result_status']}")

    return jsonify({"ok": True})


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=PORT)
else:
    init_db()
