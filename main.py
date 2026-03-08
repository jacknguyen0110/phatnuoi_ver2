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
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from playwright.sync_api import sync_playwright


APP_NAME = "Tra cứu phạt nguội bot"
SOURCE_URL = "https://csgt.vn/tra-cuu-phat-nguoi"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Phạt Nguội").strip()
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Thiếu TELEGRAM_BOT_TOKEN")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("Thiếu GOOGLE_SHEET_ID")
if not GOOGLE_DRIVE_FOLDER_ID:
    raise RuntimeError("Thiếu GOOGLE_DRIVE_FOLDER_ID")

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
_drive_service = None
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


def safe_filename(text: str):
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text or "")
    return text[:150].strip("_") or "file"


def normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").strip())


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
        logging.warning("Chưa có WEBHOOK_BASE_URL, bỏ qua setWebhook")
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
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = get_service_account_info()
    return Credentials.from_service_account_info(info, scopes=scopes)


def get_gspread_client():
    global _gspread_client
    if _gspread_client is None:
        creds = get_credentials()
        _gspread_client = gspread.authorize(creds)
    return _gspread_client


def get_drive_service():
    global _drive_service
    if _drive_service is None:
        creds = get_credentials()
        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


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


def upload_file_to_drive(local_path: str, filename: str):
    service = get_drive_service()

    file_metadata = {
        "name": filename,
        "parents": [GOOGLE_DRIVE_FOLDER_ID],
    }
    media = MediaFileUpload(local_path, mimetype="image/png", resumable=False)

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id,name,webViewLink"
    ).execute()

    file_id = created["id"]

    service.permissions().create(
        fileId=file_id,
        body={"role": "reader", "type": "anyone"},
    ).execute()

    info = service.files().get(fileId=file_id, fields="id,name,webViewLink").execute()
    return info.get("webViewLink", "")


def extract_blocks_from_text(page_text: str):
    text = page_text or ""
    start = text.find("Biển số:")
    if start == -1:
        return []

    text = text[start:]
    parts = re.split(r"(?=Biển số:\s*)", text)
    blocks = []

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if (
            "Loại xe:" in part
            or "Chi tiết vi phạm" in part
            or "Thông tin xử lý" in part
            or "Lỗi vi phạm:" in part
        ):
            blocks.append(part)

    return blocks


def parse_block(block_text: str):
    lines = [x.strip() for x in block_text.splitlines() if x.strip()]
    data = {
        "plate_display": "",
        "result_status": "",
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
        "debug_note": "",
    }

    current = None
    side = None

    for line in lines:
        raw = normalize_spaces(line)

        if raw.startswith("Biển số:"):
            data["plate_display"] = raw.replace("Biển số:", "", 1).strip()
            current = "plate_display"
            continue

        if raw in ("Đã xử phạt", "Chưa xử phạt"):
            data["result_status"] = raw
            current = "result_status"
            continue

        if raw.startswith("Loại xe:"):
            data["vehicle_type"] = raw.replace("Loại xe:", "", 1).strip()
            current = "vehicle_type"
            continue

        if raw.startswith("Màu biển:"):
            data["plate_color"] = raw.replace("Màu biển:", "", 1).strip()
            current = "plate_color"
            continue

        if raw.startswith("Lỗi vi phạm:"):
            detail = raw.replace("Lỗi vi phạm:", "", 1).strip()
            m = re.match(r"^([A-Za-z0-9\.\-\/]+)\s*(.*)$", detail)
            if m:
                data["violation_code"] = m.group(1).strip()
                data["violation_text"] = m.group(2).strip()
            else:
                data["violation_text"] = detail
            current = "violation_text"
            continue

        if raw.startswith("Thời gian:"):
            data["violation_time"] = raw.replace("Thời gian:", "", 1).strip()
            current = "violation_time"
            continue

        if raw.startswith("Địa điểm:"):
            data["violation_location"] = raw.replace("Địa điểm:", "", 1).strip()
            current = "violation_location"
            continue

        if raw.startswith("Đơn vị phát hiện:"):
            side = "detected"
            data["detected_by"] = raw.replace("Đơn vị phát hiện:", "", 1).strip()
            current = "detected_by"
            continue

        if raw.startswith("Đơn vị giải quyết:"):
            side = "resolved"
            data["resolved_by"] = raw.replace("Đơn vị giải quyết:", "", 1).strip()
            current = "resolved_by"
            continue

        if raw.startswith("Địa chỉ:"):
            v = raw.replace("Địa chỉ:", "", 1).strip()
            if side == "detected":
                data["detected_address"] = v
                current = "detected_address"
            elif side == "resolved":
                data["resolved_address"] = v
                current = "resolved_address"
            continue

        if raw.startswith("Điện thoại:"):
            v = raw.replace("Điện thoại:", "", 1).strip()
            if side == "detected":
                data["detected_phone"] = v
                current = "detected_phone"
            elif side == "resolved":
                data["resolved_phone"] = v
                current = "resolved_phone"
            continue

        if current and data.get(current):
            data[current] += " " + raw

    if not data["result_status"]:
        data["result_status"] = "CO_VI_PHAM"

    return data


def build_no_violation_record(plate, screenshot_url="", debug_note=""):
    return {
        "plate_display": display_plate(plate),
        "result_status": "KHONG_CO_VI_PHAM",
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
        "screenshot_url": screenshot_url,
        "debug_note": debug_note[:500],
    }


def build_error_record(plate, err_text, screenshot_url=""):
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
        "screenshot_url": screenshot_url,
        "debug_note": err_text[:500],
    }


def create_browser_page(playwright):
    browser = playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
            "--disable-gpu",
            "--window-size=1536,2048",
        ]
    )

    context = browser.new_context(
        viewport={"width": 1536, "height": 2048},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="vi-VN",
        timezone_id="Asia/Ho_Chi_Minh",
        ignore_https_errors=True,
    )

    page = context.new_page()
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });
    """)
    page.set_default_timeout(20000)
    page.set_default_navigation_timeout(90000)

    return browser, context, page


def quick_network_check():
    try:
        r = requests.get(
            SOURCE_URL,
            timeout=25,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            },
        )
        return True, f"requests_status={r.status_code}"
    except Exception as e:
        return False, f"requests_error={str(e)}"


def goto_with_retry(page, url, retries=3):
    last_error = ""
    for i in range(1, retries + 1):
        try:
            logging.info("[STEP] goto page try %s/%s", i, retries)
            page.goto(url, wait_until="commit", timeout=90000)
            page.wait_for_timeout(4000)
            return True, f"goto_ok_try_{i}"
        except Exception as e:
            last_error = str(e)
            logging.warning("[WARN] goto fail try %s/%s: %s", i, retries, e)
            page.wait_for_timeout(3000)
    return False, last_error


def fetch_plate_data_and_screenshot(plate: str):
    plate = normalize_plate(plate)
    screenshot_url = ""

    net_ok, net_note = quick_network_check()
    if not net_ok:
        return [build_error_record(plate, f"Lỗi mạng từ Railway tới csgt.vn | {net_note}")]

    with sync_playwright() as p:
        browser, context, page = create_browser_page(p)

        try:
            ok, goto_note = goto_with_retry(page, SOURCE_URL, retries=3)
            if not ok:
                try:
                    os.makedirs("/tmp/phatnguoi", exist_ok=True)
                    local_file = f"/tmp/phatnguoi/{safe_filename(plate)}_{int(time.time())}_goto_fail.png"
                    page.screenshot(path=local_file, full_page=False)
                    screenshot_url = upload_file_to_drive(local_file, os.path.basename(local_file))
                except Exception:
                    pass

                browser.close()
                return [build_error_record(
                    plate,
                    f"Không mở được trang csgt.vn từ Railway | {net_note} | goto_error={goto_note}",
                    screenshot_url
                )]

            logging.info("[STEP] find input")
            input_box = page.locator("input[placeholder*='Nhập biển số xe']").first
            input_box.wait_for(state="visible", timeout=20000)

            logging.info("[STEP] type plate")
            input_box.click()
            try:
                page.keyboard.press("Control+A")
            except Exception:
                pass
            try:
                page.keyboard.press("Meta+A")
            except Exception:
                pass

            input_box.type(plate, delay=80)
            page.wait_for_timeout(800)

            logging.info("[STEP] click search")
            search_btn = page.locator("button:has-text('Tra cứu')").first
            search_btn.wait_for(state="visible", timeout=15000)
            search_btn.click()

            page.wait_for_timeout(5000)

            logging.info("[STEP] wait result")
            found_result = False
            last_text = ""

            for i in range(30):
                try:
                    last_text = page.locator("body").inner_text(timeout=5000) or ""
                except Exception:
                    last_text = ""

                signals = [
                    "Biển số:",
                    "Đã xử phạt",
                    "Chưa xử phạt",
                    "Loại xe:",
                    "Lỗi vi phạm:",
                    "Không tìm thấy",
                    "Không có kết quả",
                    "Chưa phát hiện lỗi vi phạm",
                ]

                if any(s in last_text for s in signals):
                    found_result = True
                    logging.info("[STEP] found result at loop %s", i + 1)
                    break

                page.wait_for_timeout(1000)

            os.makedirs("/tmp/phatnguoi", exist_ok=True)
            local_file = f"/tmp/phatnguoi/{safe_filename(plate)}_{int(time.time())}.png"
            page.screenshot(path=local_file, full_page=False)
            screenshot_url = upload_file_to_drive(local_file, os.path.basename(local_file))

            if not found_result:
                browser.close()
                return [build_error_record(
                    plate,
                    f"Không thấy tín hiệu kết quả | {net_note} | body={last_text[:250]}",
                    screenshot_url
                )]

            body_text = last_text

            no_hit_signals = [
                "Không tìm thấy kết quả",
                "Không tìm thấy vi phạm",
                "Chưa phát hiện lỗi vi phạm",
                "Không có kết quả",
                "Biển số không có lỗi vi phạm",
            ]
            if any(sig.lower() in body_text.lower() for sig in no_hit_signals):
                browser.close()
                return [build_no_violation_record(plate, screenshot_url, net_note)]

            blocks = extract_blocks_from_text(body_text)
            if not blocks and "Biển số:" in body_text and ("Loại xe:" in body_text or "Lỗi vi phạm:" in body_text):
                blocks = [body_text]

            if not blocks:
                browser.close()
                return [build_error_record(
                    plate,
                    f"Không parse được block kết quả | {net_note} | body={body_text[:300]}",
                    screenshot_url
                )]

            records = []
            for block in blocks:
                data = parse_block(block)
                if not data.get("plate_display"):
                    data["plate_display"] = display_plate(plate)
                data["screenshot_url"] = screenshot_url
                data["debug_note"] = net_note
                records.append(data)

            browser.close()
            return records

        except Exception as e:
            logging.exception("[ERROR] fetch_plate_data_and_screenshot")
            try:
                os.makedirs("/tmp/phatnguoi", exist_ok=True)
                local_file = f"/tmp/phatnguoi/{safe_filename(plate)}_{int(time.time())}_error.png"
                page.screenshot(path=local_file, full_page=False)
                screenshot_url = upload_file_to_drive(local_file, os.path.basename(local_file))
            except Exception:
                pass

            try:
                browser.close()
            except Exception:
                pass

            return [build_error_record(plate, str(e), screenshot_url)]


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
        msg = f"{title}\nKết quả: Không có vi phạm."
        if records[0].get("screenshot_url"):
            msg += f"\nẢnh kết quả: {records[0]['screenshot_url']}"
        return msg

    if len(records) == 1 and str(records[0]["result_status"]).startswith("LOI_TRA_CUU"):
        msg = f"{title}\nKết quả: {records[0]['result_status']}"
        if records[0].get("screenshot_url"):
            msg += f"\nẢnh debug: {records[0]['screenshot_url']}"
        return msg

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

    if records and records[0].get("screenshot_url"):
        lines.append("")
        lines.append(f"Ảnh kết quả: {records[0]['screenshot_url']}")

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
            "Không đọc được biển số.\nVui lòng gửi mỗi dòng 1 biển số, ví dụ:\n50H12314\n51L91000"
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
        records = fetch_plate_data_and_screenshot(plate)

        for r in records:
            all_rows.append(record_to_sheet_row(r, tg_meta, update_id, plate, normalize_plate(plate)))

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
