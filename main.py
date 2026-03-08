import os
import re
import json
import time
import logging
from datetime import datetime

import requests
import gspread
from flask import Flask, request, jsonify
from google.oauth2.service_account import Credentials
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# =========================
# CONFIG
# =========================
APP_NAME = "Tra cứu phạt nguội bot"
SOURCE_URL = "https://csgt.vn/tra-cuu-phat-nguoi"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_BASE_URL = os.getenv("WEBHOOK_BASE_URL", "").strip()  # ví dụ: https://your-app.up.railway.app
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Phạt Nguội").strip()

PORT = int(os.getenv("PORT", "8080"))

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Thiếu TELEGRAM_BOT_TOKEN")
if not GOOGLE_SERVICE_ACCOUNT_JSON:
    raise RuntimeError("Thiếu GOOGLE_SERVICE_ACCOUNT_JSON")
if not GOOGLE_SHEET_ID:
    raise RuntimeError("Thiếu GOOGLE_SHEET_ID")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

HEADERS = [
    "timestamp",
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
    "source_url",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

app = Flask(__name__)


# =========================
# UTIL
# =========================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_plate(text: str) -> str:
    text = (text or "").upper().strip()
    text = re.sub(r"[^A-Z0-9]", "", text)
    return text


def display_plate(plate: str) -> str:
    """
    Hiển thị đẹp kiểu 50H-389.74 nếu đủ điều kiện.
    Không đúng pattern thì trả nguyên.
    """
    p = normalize_plate(plate)
    # phổ biến: 3 ký tự đầu + 5 số
    if len(p) == 8 and re.match(r"^[0-9]{2}[A-Z][0-9]{5}$", p):
        return f"{p[:3]}-{p[3:6]}.{p[6:]}"
    # một số trường hợp biển 2 chữ cái
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
        # hỗ trợ ngăn cách bởi dấu phẩy / chấm phẩy trong cùng 1 dòng
        parts = re.split(r"[;,]+", line)
        for part in parts:
            v = normalize_plate(part)
            if v:
                raw_items.append(v)

    # giữ thứ tự, không trùng trong cùng 1 tin nhắn
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


# =========================
# TELEGRAM
# =========================
def tg_send_message(chat_id, text):
    for part in chunk_text(text):
        requests.post(
            f"{TELEGRAM_API}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": part,
            },
            timeout=30,
        )


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


# =========================
# GOOGLE SHEETS
# =========================
_gc = None
_ws = None


def get_gspread_client():
    global _gc
    if _gc is not None:
        return _gc

    info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    _gc = gspread.authorize(creds)
    return _gc


def get_worksheet():
    global _ws
    if _ws is not None:
        return _ws

    gc = get_gspread_client()
    sh = gc.open_by_key(GOOGLE_SHEET_ID)

    try:
        ws = sh.worksheet(GOOGLE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=GOOGLE_SHEET_NAME, rows=1000, cols=30)

    values = ws.get_all_values()
    if not values:
        ws.append_row(HEADERS, value_input_option="RAW")
    else:
        first_row = values[0]
        if first_row != HEADERS:
            ws.resize(rows=max(len(values), 1), cols=len(HEADERS))
            ws.update("A1", [HEADERS])

    _ws = ws
    return _ws


def append_sheet_rows(rows):
    if not rows:
        return
    ws = get_worksheet()
    ws.append_rows(rows, value_input_option="RAW")


# =========================
# PARSE RESULT TEXT
# =========================
def normalize_spaces(s: str) -> str:
    return re.sub(r"[ \t]+", " ", (s or "").strip())


def extract_blocks_from_text(page_text: str):
    """
    Tách từng block vi phạm.
    Heuristic:
    - Mỗi block thường bắt đầu bằng 'Biển số:'
    """
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
        if "Loại xe:" in part or "Chi tiết vi phạm" in part or "Thông tin xử lý" in part:
            blocks.append(part)

    return blocks


def parse_block(block_text: str):
    """
    Parse từng block text của 1 vi phạm.
    """
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
    }

    current = None
    side = None  # detected / resolved

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
            else:
                current = None
            continue

        if raw.startswith("Điện thoại:"):
            v = raw.replace("Điện thoại:", "", 1).strip()
            if side == "detected":
                data["detected_phone"] = v
                current = "detected_phone"
            elif side == "resolved":
                data["resolved_phone"] = v
                current = "resolved_phone"
            else:
                current = None
            continue

        # nối dòng bị wrap
        if current:
            if data.get(current):
                data[current] += " " + raw
            else:
                data[current] = raw

    if not data["result_status"]:
        # nếu không parse được badge trạng thái thì mặc định có vi phạm
        data["result_status"] = "CO_VI_PHAM"

    return data


def build_no_violation_record(plate):
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
    }


def build_error_record(plate, err_text):
    return {
        "plate_display": display_plate(plate),
        "result_status": f"LOI_TRA_CUU: {err_text[:200]}",
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
    }


# =========================
# SCRAPER
# =========================
def fetch_plate_data(plate: str):
    """
    Trả về list record.
    Mỗi vi phạm = 1 record.
    Không có vi phạm = 1 record KHONG_CO_VI_PHAM.
    """
    plate = normalize_plate(plate)
    if not plate:
        return [build_error_record(plate, "Biển số rỗng")]

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ]
        )
        page = browser.new_page(viewport={"width": 1400, "height": 2200})

        try:
            page.goto(SOURCE_URL, wait_until="domcontentloaded", timeout=90000)

            # điền biển số
            input_box = page.locator("input[placeholder*='Nhập biển số xe']").first
            input_box.wait_for(timeout=20000)
            input_box.click()
            input_box.fill(plate)

            # click tra cứu
            page.locator("button:has-text('Tra cứu')").first.click()

            # chờ kết quả
            page.wait_for_timeout(3500)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass

            body_text = page.locator("body").inner_text(timeout=10000)
            body_text = body_text or ""

            # các trường hợp "không có vi phạm" / "không thấy dữ liệu"
            no_hit_signals = [
                "Không tìm thấy kết quả",
                "Không tìm thấy vi phạm",
                "Chưa phát hiện lỗi vi phạm",
                "Không có kết quả",
                "Biển số không có lỗi vi phạm",
            ]
            if any(sig.lower() in body_text.lower() for sig in no_hit_signals):
                browser.close()
                return [build_no_violation_record(plate)]

            blocks = extract_blocks_from_text(body_text)
            if not blocks:
                # fallback: nếu body vẫn có biển số mà không tách block được
                if "Biển số:" in body_text and ("Loại xe:" in body_text or "Lỗi vi phạm:" in body_text):
                    blocks = [body_text]

            if not blocks:
                browser.close()
                return [build_no_violation_record(plate)]

            records = []
            for block in blocks:
                data = parse_block(block)
                if not data.get("plate_display"):
                    data["plate_display"] = display_plate(plate)
                records.append(data)

            browser.close()
            return records

        except PlaywrightTimeoutError:
            browser.close()
            return [build_error_record(plate, "Timeout khi tải trang hoặc chờ kết quả")]
        except Exception as e:
            browser.close()
            return [build_error_record(plate, str(e))]


# =========================
# FORMAT MESSAGE + SHEET ROW
# =========================
def record_to_sheet_row(record, tg_meta, plate_input, plate_normalized):
    return [
        now_str(),
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
        SOURCE_URL,
    ]


def format_plate_message(plate, records):
    title = f"Biển số: {display_plate(plate)}"

    if len(records) == 1 and records[0]["result_status"] == "KHONG_CO_VI_PHAM":
        return f"{title}\nKết quả: Không có vi phạm."

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
        if r.get("detected_address"):
            lines.append(f"Địa chỉ phát hiện: {r['detected_address']}")
        if r.get("detected_phone"):
            lines.append(f"SĐT phát hiện: {r['detected_phone']}")
        if r.get("resolved_by"):
            lines.append(f"Đơn vị giải quyết: {r['resolved_by']}")
        if r.get("resolved_address"):
            lines.append(f"Địa chỉ giải quyết: {r['resolved_address']}")
        if r.get("resolved_phone"):
            lines.append(f"SĐT giải quyết: {r['resolved_phone']}")
    return "\n".join(lines)


# =========================
# TELEGRAM UPDATE HANDLER
# =========================
def process_telegram_update(update: dict):
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

    if text.strip().lower() in ("/start", "start"):
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

        records = fetch_plate_data(plate)

        # sheet rows
        for r in records:
            all_rows.append(record_to_sheet_row(r, tg_meta, plate, normalize_plate(plate)))

        # telegram
        msg = format_plate_message(plate, records)
        tg_send_message(chat_id, msg)

        time.sleep(1)

    append_sheet_rows(all_rows)
    tg_send_message(chat_id, f"Hoàn tất. Đã xử lý {len(plates)} biển số và ghi Google Sheet.")


# =========================
# ROUTES
# =========================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "ok": True,
        "app": APP_NAME,
        "time": now_str()
    })


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
    try:
        update = request.get_json(force=True, silent=True) or {}
        logging.info("Telegram update: %s", update)
        process_telegram_update(update)
        return jsonify({"ok": True})
    except Exception as e:
        logging.exception("Webhook error")
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    # chạy local
    app.run(host="0.0.0.0", port=PORT)
