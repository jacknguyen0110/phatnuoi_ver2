import os
import re
import json
import time
import queue
import logging
import threading
from datetime import datetime

import requests
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials


APP_NAME = "Tra cứu phạt nguội bot"
SOURCE_URL = "https://csgt.vn/tra-cuu-phat-nguoi"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Phạt Nguội").strip()
SCRAPER_API_URL = os.getenv("SCRAPER_API_URL", "").strip()
PORT = int(os.getenv("PORT", "8080"))

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Thiếu TELEGRAM_BOT_TOKEN")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("Thiếu GOOGLE_SHEET_ID")
if not SCRAPER_API_URL:
    raise RuntimeError("Thiếu SCRAPER_API_URL")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

HEADERS = [
    "timestamp",
    "telegram_update_id",
    "telegram_chat_id",
    "telegram_user_id",
    "telegram_username",
    "telegram_full_name",
    "plate_input",
    "plate_normalized",
    "plate_display",
    "result_status",
    "vehicle_type",
    "plate_color",
    "violation_code",
    "violation_text",
    "violation_time",
    "violation_location",
    "detected_by",
    "detected_address",
    "detected_phone",
    "resolved_by",
    "resolved_address",
    "resolved_phone",
    "screenshot_drive_url",
    "source_url",
    "debug_note",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)

job_queue = queue.Queue()
processed_update_ids = set()
processed_update_ids_lock = threading.Lock()
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


def chunk_text(text: str, size: int = 3500):
    text = text or ""
    chunks = []
    while len(text) > size:
        cut = text.rfind("\n", 0, size)
        if cut == -1:
            cut = size
        chunks.append(text[:cut])
        text = text[cut:].lstrip()
    if text:
        chunks.append(text)
    return chunks


def tg_send_message(chat_id, text):
    for part in chunk_text(text):
        try:
            requests.post(
                f"{TELEGRAM_API}/sendMessage",
                json={"chat_id": chat_id, "text": part},
                timeout=30,
            )
        except Exception as e:
            logging.exception("Lỗi gửi Telegram: %s", e)


def tg_set_webhook():
    if not WEBHOOK_BASE_URL:
        return

    webhook_url = WEBHOOK_BASE_URL.rstrip("/") + "/telegram/webhook"
    r = requests.post(
        f"{TELEGRAM_API}/setWebhook",
        json={"url": webhook_url},
        timeout=30,
    )
    logging.info("setWebhook response: %s", r.text)


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
    else:
        if values[0] != HEADERS:
            ws.clear()
            ws.append_row(HEADERS, value_input_option="RAW")

    _worksheet = ws
    return _worksheet


def append_sheet_rows(rows):
    if not rows:
        return
    ws = get_worksheet()
    ws.append_rows(rows, value_input_option="RAW")


def call_local_scraper(plate: str):
    url = SCRAPER_API_URL.rstrip("/") + "/scrape"
    r = requests.post(
        url,
        json={"plate": plate},
        timeout=180,
    )
    r.raise_for_status()
    return r.json()


def build_error_record(plate, err_text):
    return {
        "plate_display": display_plate(plate),
        "result_status": f"LOI_TRA_CUU: {err_text[:250]}",
        "vehicle_type": "",
        "plate_color": "",
        "violation_code": "",
        "violation_text": "",
        "violation_time": "",
        "violation_location": "",
        "detected_by": "",
        "detected_address": "",
        "detected_phone": "",
        "resolved_by": "",
        "resolved_address": "",
        "resolved_phone": "",
        "screenshot_url": "",
        "debug_note": err_text[:500],
    }


def normalize_records_from_scraper(plate: str, scraper_result: dict):
    records = scraper_result.get("records", []) or []
    error = scraper_result.get("error", "") or ""

    if not records and error:
        return [build_error_record(plate, error)]

    normalized = []
    for r in records:
        item = {
            "plate_display": r.get("plate_display", display_plate(plate)),
            "result_status": r.get("result_status", ""),
            "vehicle_type": r.get("vehicle_type", ""),
            "plate_color": r.get("plate_color", ""),
            "violation_code": r.get("violation_code", ""),
            "violation_text": r.get("violation_text", ""),
            "violation_time": r.get("violation_time", ""),
            "violation_location": r.get("violation_location", ""),
            "detected_by": r.get("detected_by", ""),
            "detected_address": r.get("detected_address", ""),
            "detected_phone": r.get("detected_phone", ""),
            "resolved_by": r.get("resolved_by", ""),
            "resolved_address": r.get("resolved_address", ""),
            "resolved_phone": r.get("resolved_phone", ""),
            "screenshot_url": "",
            "debug_note": error[:500] if error else "",
        }
        normalized.append(item)

    return normalized


def record_to_sheet_row(record, tg_meta, update_id, plate_input, plate_normalized):
    return [
        now_str(),
        str(update_id),
        str(tg_meta.get("chat_id", "")),
        str(tg_meta.get("user_id", "")),
        tg_meta.get("username", ""),
        tg_meta.get("full_name", ""),
        plate_input,
        plate_normalized,
        record.get("plate_display", ""),
        record.get("result_status", ""),
        record.get("vehicle_type", ""),
        record.get("plate_color", ""),
        record.get("violation_code", ""),
        record.get("violation_text", ""),
        record.get("violation_time", ""),
        record.get("violation_location", ""),
        record.get("detected_by", ""),
        record.get("detected_address", ""),
        record.get("detected_phone", ""),
        record.get("resolved_by", ""),
        record.get("resolved_address", ""),
        record.get("resolved_phone", ""),
        record.get("screenshot_url", ""),
        SOURCE_URL,
        record.get("debug_note", ""),
    ]


def format_plate_message(plate, records):
    title = f"Biển số: {display_plate(plate)}"

    if len(records) == 1 and records[0]["result_status"] == "KHONG_CO_VI_PHAM":
        return f"{title}\nKết quả: Không có vi phạm."

    if len(records) == 1 and records[0]["result_status"] == "KHONG_CO_DU_LIEU_VI_PHAM":
        return f"{title}\nKết quả: Không tìm thấy dữ liệu vi phạm. Vui lòng thử lại sau."

    if len(records) == 1 and str(records[0]["result_status"]).startswith("LOI_TRA_CUU"):
        return f"{title}\nKết quả: {records[0]['result_status']}"

    lines = [title, f"Số vi phạm: {len(records)}"]
    for i, r in enumerate(records, 1):
        lines.append("")
        lines.append(f"[{i}] Trạng thái: {r.get('result_status', '')}")
        if r.get("vehicle_type"):
            lines.append(f"Loại xe: {r['vehicle_type']}")
        if r.get("plate_color"):
            lines.append(f"Màu biển: {r['plate_color']}")
        if r.get("violation_code"):
            lines.append(f"Mã lỗi: {r['violation_code']}")
        if r.get("violation_text"):
            lines.append(f"Lỗi vi phạm: {r['violation_text']}")
        if r.get("violation_time"):
            lines.append(f"Thời gian: {r['violation_time']}")
        if r.get("violation_location"):
            lines.append(f"Địa điểm: {r['violation_location']}")
        if r.get("detected_by"):
            lines.append(f"Đơn vị phát hiện: {r['detected_by']}")
        if r.get("resolved_by"):
            lines.append(f"Đơn vị giải quyết: {r['resolved_by']}")
    return "\n".join(lines)


def handle_update(update: dict):
    update_id = update.get("update_id")
    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat", {})
    from_user = message.get("from", {})
    text = message.get("text", "") or ""

    chat_id = chat.get("id")
    user_id = from_user.get("id")
    username = from_user.get("username", "")
    full_name = " ".join(filter(None, [
        from_user.get("first_name", ""),
        from_user.get("last_name", "")
    ])).strip()

    if not chat_id:
        return

    cmd = text.strip().lower()
    if cmd in ("/start", "start"):
        tg_send_message(
            chat_id,
            "Gửi biển số cần tra cứu, mỗi dòng 1 biển số.\n\nVí dụ:\n50H12314\n51L91000\n60H12916"
        )
        return

    plates = parse_plate_lines(text)
    if not plates:
        tg_send_message(
            chat_id,
            "Không đọc được biển số.\nVui lòng gửi mỗi dòng 1 biển số."
        )
        return

    tg_meta = {
        "chat_id": chat_id,
        "user_id": user_id,
        "username": username,
        "full_name": full_name,
    }

    tg_send_message(chat_id, f"Đã nhận {len(plates)} biển số. Bắt đầu tra cứu...")

    all_rows = []
    for idx, plate in enumerate(plates, 1):
        logging.info("Tra cứu %s/%s: %s", idx, len(plates), plate)

        try:
            scraper_result = call_local_scraper(plate)
            records = normalize_records_from_scraper(plate, scraper_result)
        except Exception as e:
            logging.exception("Call local scraper failed")
            records = [build_error_record(
                plate,
                f"Không gọi được scraper local: {str(e)}"
            )]

        for r in records:
            all_rows.append(
                record_to_sheet_row(r, tg_meta, update_id, plate, normalize_plate(plate))
            )

        tg_send_message(chat_id, format_plate_message(plate, records))
        time.sleep(1)

    append_sheet_rows(all_rows)
    tg_send_message(chat_id, f"Hoàn tất. Đã xử lý {len(plates)} biển số và ghi Google Sheet.")


def worker_loop():
    while True:
        update = job_queue.get()
        try:
            handle_update(update)
        except Exception as e:
            logging.exception("Worker error: %s", e)
            try:
                message = update.get("message") or {}
                chat_id = message.get("chat", {}).get("id")
                if chat_id:
                    tg_send_message(chat_id, f"Lỗi xử lý: {str(e)[:300]}")
            except Exception:
                pass
        finally:
            job_queue.task_done()


worker_thread = threading.Thread(target=worker_loop, daemon=True)
worker_thread.start()


@app.route("/", methods=["GET"])
def home():
    return jsonify({"ok": True, "app": APP_NAME, "time": now_str()})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


@app.route("/set-webhook", methods=["GET"])
def set_webhook_route():
    try:
        tg_set_webhook()
        return jsonify({"ok": True, "message": "Webhook đã được set"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/telegram/webhook", methods=["POST"])
def telegram_webhook():
    update = request.get_json(force=True, silent=True) or {}
    logging.info("Telegram update: %s", update)

    update_id = update.get("update_id")
    if update_id is None:
        return jsonify({"ok": True})

    with processed_update_ids_lock:
        if update_id in processed_update_ids:
            logging.info("Duplicate update skipped: %s", update_id)
            return jsonify({"ok": True})

        processed_update_ids.add(update_id)
        if len(processed_update_ids) > 5000:
            processed_update_ids.clear()
            processed_update_ids.add(update_id)

    job_queue.put(update)
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
