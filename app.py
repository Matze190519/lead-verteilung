"""
Lead-Verteilungs-Service v3.6-META
==================================
v3.6 Code mit Meta API statt Whapi (NUR WhatsApp geändert)
"""

import os
import json
import logging
import time
import threading
from datetime import datetime, timezone
from typing import Optional

import gspread
import stripe
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

# ─── Konfiguration ───────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("lead-verteilung")

# META API (Lina) - DAS IST DIE EINZIGE ÄNDERUNG!
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"

# Rest identisch zu v3.6
WHAPI_TOKEN = os.getenv("WHAPI_TOKEN", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
PAKET_PREIS = float(os.getenv("PAKET_PREIS", "50"))

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

MATZE_PHONE = os.getenv("MATZE_PHONE", "+491715060008")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

poll_lock = threading.Lock()

app = FastAPI(
    title="Lead-Verteilungs-Service",
    version="3.6-META",
)

# ─── Google Sheets (identisch zu v3.6) ───────────────────────────────────────
def get_google_client() -> gspread.Client:
    if GOOGLE_CREDENTIALS_JSON:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(creds_dict)
    else:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    return gc

def get_spreadsheet() -> gspread.Spreadsheet:
    gc = get_google_client()
    return gc.open_by_key(GOOGLE_SHEET_ID)

def get_sheet() -> gspread.Worksheet:
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet("Partner_Konto")
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("Tab 'Partner_Konto' nicht gefunden, verwende erstes Sheet")
        return spreadsheet.sheet1

def get_leads_sheet() -> gspread.Worksheet:
    return get_spreadsheet().worksheet("Tabellenblatt1")

def get_leads_log_sheet() -> gspread.Worksheet:
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet("Leads_Log")
    except gspread.exceptions.WorksheetNotFound:
        logger.info("Leads_Log Tab wird erstellt...")
        ws = spreadsheet.add_worksheet(title="Leads_Log", rows=1000, cols=10)
        headers = [
            "Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email",
            "Partner_Name", "Partner_Telefon", "Guthaben_Nachher",
            "WhatsApp_Partner", "WhatsApp_Lead", "Status"
        ]
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

def log_lead(lead_name: str, lead_phone: str, lead_email: str,
             partner_name: str, partner_phone: str, guthaben_nachher: float,
             wa_partner_ok: bool, wa_lead_ok: bool, status: str):
    try:
        log_sheet = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now, lead_name, lead_phone, lead_email,
            partner_name, partner_phone, guthaben_nachher,
            "OK" if wa_partner_ok else "FEHLER",
            "OK" if wa_lead_ok else "FEHLER/KEINE NR",
            status,
        ]
        log_sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"Lead geloggt: {lead_name} → {partner_name} ({status})")
    except Exception as e:
        logger.error(f"Fehler beim Lead-Logging: {e}")

# ─── NUR DIESE FUNKTION GEÄNDERT: Meta API statt Whapi ───────────────────────
def send_whatsapp(phone: str, message: str) -> dict:
    if not META_TOKEN:
        logger.error("META_TOKEN nicht gesetzt!")
        return {"error": "META_TOKEN nicht konfiguriert"}
    
    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Telefonnummer: '{phone}'")
        return {"error": f"Ungültige Telefonnummer: {phone}"}
    
    # Format für Meta API (ohne +, ohne Leerzeichen)
    to = phone.replace("+", "").replace(" ", "")
    
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": "text",
        "text": {"preview_url": False, "body": message}
    }
    
    try:
        logger.info(f"WhatsApp senden an {phone} (Meta API)...")
        response = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        logger.info(f"WhatsApp Response Status: {response.status_code}")
        
        if response.status_code >= 400:
            logger.error(f"Meta API Error: {response.text}")
            return {"error": response.text}
        
        result = response.json()
        logger.info(f"WhatsApp gesendet an {phone}: OK")
        return result
        
    except requests.exceptions.RequestException as e:
        logger.error(f"WhatsApp-Fehler an {phone}: {e}")
        return {"error": str(e)}

# ─── Rest IDENTISCH zu v3.6 ──────────────────────────────────────────────────
def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    if phone.startswith("p:"):
        phone = phone[2:]
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"):
        phone = "49" + phone[1:]
    if not phone.startswith("49") and len(phone) <= 11:
        phone = "49" + phone
    return phone

def get_all_partner_records(sheet: gspread.Worksheet) -> list:
    headers = ["Name", "Telefon", "Guthaben_Euro", "Leads_Geliefert", "Letzter_Lead_Am", "Status"]
    all_values = sheet.get_all_values()
    if len(all_values) <= 1:
        return []
    
    records = []
    for row in all_values[1:]:
        if len(row) >= 6:
            record = {headers[i]: row[i] for i in range(6)}
        else:
            record = {headers[i]: (row[i] if i < len(row) else "") for i in range(6)}
        if record.get("Name", "").strip():
            records.append(record)
    return records

def find_best_partner(sheet: gspread.Worksheet) -> Optional[dict]:
    try:
        all_records = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Fehler beim Lesen des Sheets: {e}")
        return None
    
    if not all_records:
        logger.warning("Keine Partner im Sheet gefunden!")
        return None
    
    aktive_partner = []
    for idx, record in enumerate(all_records):
        status = str(record.get("Status", "")).strip()
        try:
            guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
        except (ValueError, TypeError):
            guthaben = 0
        try:
            leads = int(record.get("Leads_Geliefert", 0))
        except (ValueError, TypeError):
            leads = 0
        
        letzter_lead = str(record.get("Letzter_Lead_Am", "")).strip()
        
        if status == "Aktiv" and guthaben >= LEAD_PREIS:
            aktive_partner.append({
                "index": idx,
                "row": idx + 2,
                "name": record.get("Name"),
                "phone": normalize_phone(record.get("Telefon")),
                "guthaben": guthaben,
                "leads": leads,
                "last": letzter_lead
            })
    
    if not aktive_partner:
        return None
    
    aktive_partner.sort(key=lambda x: (x["last"] or "0000-00-00", x["leads"]))
    return aktive_partner[0]

def update_partner(sheet: gspread.Worksheet, partner: dict) -> float:
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    new_bal = round(partner["guthaben"] - LEAD_PREIS, 2)
    
    try:
        sheet.update_cell(row, 3, new_bal)
        sheet.update_cell(row, 4, partner["leads"] + 1)
        sheet.update_cell(row, 5, now)
        
        if new_bal < LEAD_PREIS:
            sheet.update_cell(row, 6, "Pausiert")
            if MATZE_PHONE:
                send_whatsapp(MATZE_PHONE, f"⚠️ Partner {partner['name']} pausiert (Guthaben leer).")
        
        return new_bal
    except Exception as e:
        logger.error(f"Fehler beim Partner-Update: {e}")
        raise

def process_lead_distribution(name: str, phone: str, email: str, row_idx: int = None):
    if not phone:
        logger.warning(f"Keine Telefonnummer für Lead {name}")
        return
    
    try:
        sheet = get_sheet()
        partner = find_best_partner(sheet)
        
        if not partner:
            if MATZE_PHONE:
                send_whatsapp(MATZE_PHONE, f"⚠️ Kein Partner für Lead: {name}")
            if row_idx:
                try:
                    get_leads_sheet().update_cell(row_idx, 16, "KEIN_PARTNER")
                except:
                    pass
            return
        
        new_bal = update_partner(sheet, partner)
        
        msg_p = f"🔔 *Neuer Lead!*\n👤 {name}\n📞 {phone}\n📧 {email}\n💰 Rest: {new_bal}€"
        wa = send_whatsapp(partner["phone"], msg_p)
        
        if MATZE_PHONE:
            msg_a = f"✅ Lead verteilt: {name} -> {partner['name']}"
            send_whatsapp(MATZE_PHONE, msg_a)
        
        if row_idx:
            try:
                get_leads_sheet().update_cell(row_idx, 16, "VERTEILT")
            except:
                pass
        
        log_lead(name, phone, email, partner["name"], partner["phone"], 
                 new_bal, "error" not in str(wa).lower(), False, "VERTEILT")
        
    except Exception as e:
        logger.error(f"Process Error: {e}")

# ─── POLLING (identisch zu v3.6) ─────────────────────────────────────────────
def poll_loop():
    while True:
        time.sleep(POLL_INTERVAL)
        if not poll_lock.acquire(blocking=False):
            continue
        
        try:
            ls = get_leads_sheet()
            rows = ls.get_all_values()
            
            for i, r in enumerate(rows[1:], 2):
                if len(r) > 15 and r[15] == "CREATED":
                    ls.update_cell(i, 16, "PROCESSING")
                    
                    raw = [r[12] if len(r)>12 else "", r[13] if len(r)>13 else "", r[14] if len(r)>14 else ""]
                    name, email, phone = "Unbekannt", "", ""
                    
                    for v in raw:
                        v = v.strip()
                        if "@" in v:
                            email = v
                        elif any(c.isdigit() for c in v):
                            phone = normalize_phone(v)
                        elif v:
                            name = v
                    
                    process_lead_distribution(name, phone, email, i)
                    
        except Exception as e:
            logger.error(f"Poll Error: {e}")
        finally:
            poll_lock.release()

@app.on_event("startup")
def start_poll():
    threading.Thread(target=poll_loop, daemon=True).start()

# ─── WEBHOOKS (identisch zu v3.6) ────────────────────────────────────────────
@app.get("/")
def index():
    return {"status": "running v3.6-META"}

@app.get("/health")
def health():
    return {"
...(truncated)...
