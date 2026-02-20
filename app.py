"""
Lead-Verteilungs-Service v3.6-META (WORKING)
=============================================
v3.6 Code mit Meta API statt Whapi (NUR WhatsApp Funktion geändert)
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

# Environment-Variablen
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
PAKET_PREIS = float(os.getenv("PAKET_PREIS", "50"))

# Stripe
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# Matze's WhatsApp-Nummer
MATZE_PHONE = os.getenv("MATZE_PHONE", "")

# Google Credentials
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Facebook
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")

# Polling-Intervall (Sekunden)
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

# ─── Threading Lock ──────────────────────────
poll_lock = threading.Lock()

# ─── FastAPI App ─────────────────────────────
app = FastAPI(
    title="Lead-Verteilungs-Service",
    description="Verteilt Leads fair an Partner. Liest aus Google Sheet + Facebook Webhook + Stripe.",
    version="3.6-META",
)

# ─── Google Sheets Client ────────────────────
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
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet("Tabellenblatt1")

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

# ─── META WHATSAPP (NUR DIESE FUNKTION GEÄNDERT) ─────────────────────────────
def send_whatsapp(phone: str, message: str) -> dict:
    if not META_TOKEN:
        logger.error("META_TOKEN nicht gesetzt!")
        return {"error": "META_TOKEN nicht konfiguriert"}

    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Telefonnummer: '{phone}'")
        return {"error": f"Ungültige Telefonnummer: {phone}"}

    # Meta API Format: Nummer ohne + und ohne @s.whatsapp.net
    to = phone.replace("+", "").replace(" ", "").replace("@s.whatsapp.net", "")
    
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json",
    }
    
    # Meta API Payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message
        }
    }

    try:
        logger.info(f"WhatsApp senden an {phone} (Meta API)...")
        response = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        logger.info(f"WhatsApp Response Status: {response.status_code}")
        logger.info(f"WhatsApp Response Body: {response.text[:500]}")
        
        if response.status_code >= 400:
            logger.error(f"Meta API Error: {response.text}")
            return {"error": response.text, "status_code": response.status_code}
            
        result = response.json()
        logger.info(f"WhatsApp gesendet an {phone}: OK")
        return result
        
    except requests.exceptions.RequestException as e:
        error_body = ""
        if hasattr(e, 'response') and e.response is not None:
            error_body = e.response.text[:500]
        logger.error(f"WhatsApp-Fehler an {phone}: {e} | Response: {error_body}")
        return {"error": str(e), "response_body": error_body}

# ─── Telefonnummer normalisieren ─────────────────────────────────────────────
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

# ─── Partner-Suche und Update ────────────────────────────────────────────────
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
                "row": idx + 2,
                "name": str(record.get("Name", "Unbekannt")),
                "telefon": str(record.get("Telefon", "")),
                "guthaben": guthaben,
                "leads_geliefert": leads,
                "letzter_lead": letzter_lead,
                "status": status,
            })

    if not aktive_partner:
        logger.warning("Kein aktiver Partner mit ausreichend Guthaben gefunden!")
        return None

    def sort_key(p):
        datum = p["letzter_lead"]
        if not datum:
            return ("0000-00-00 00:00:00", p["leads_geliefert"])
        return (datum, p["leads_geliefert"])

    aktive_partner.sort(key=sort_key)
    best = aktive_partner[0]
    logger.info(f"Bester Partner: {best['name']} (Zeile {best['row']})")
    return best

def update_partner(sheet: gspread.Worksheet, partner: dict) -> bool:
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        neue_leads = partner["leads_geliefert"] + 1
        sheet.update_cell(row, 4, neue_leads)
        sheet.update_cell(row, 5, now)
        logger.info(f"Partner {partner['name']} aktualisiert: Guthaben {partner['guthaben']}€ → {neues_guthaben}€")
        
        if neues_guthaben < LEAD_PREIS:
            sheet.update_cell(row, 6, "Pausiert")
            logger.info(f"Partner {partner['name']} pausiert")
            if MATZE_PHONE:
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ Partner {partner['name']} pausiert (Guthaben: {neues_guthaben}€)"
                )
        return True
    except Exception as e:
        logger.error(f"Fehler beim Partner-Update: {e}")
        return False

def find_partner_by_phone(sheet: gspread.Worksheet, phone: str) -> Optional[dict]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    records = get_all_partner_records(sheet)
    for idx, record in enumerate(records):
        partner_phone = normalize_phone(str(record.get("Telefon", "")))
        if partner_phone and partner_phone == normalized:
            try:
                guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
            except (ValueError, TypeError):
                guthaben = 0
            return {
                "row": idx + 2,
                "name": str(record.get("Name", "")),
                "telefon": partner_phone,
                "guthaben": guthaben,
            }
    return None

def find_partner_by_name(sheet: gspread.Worksheet, name: str) -> Optional[dict]:
    if not name:
        return None
    records = get_all_partner_records(sheet)
    name_lower = name.lower().strip()
    for idx, record in enumerate(records):
        record_name = str(record.get("Name", "")).lower().strip()
        if record_name and (record_name in name_lower or name_lower in record_name):
            try:
                guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
            except (ValueError, TypeError):
                guthaben = 0
            return {
                "row": idx + 2,
                "name": str(record.get("Name", "")),
                "telefon": normalize_phone(str(record.get("Telefon", ""))),
                "guthaben": guthaben,
            }
    return None

def add_new_partner(sheet: gspread.Worksheet, name: str, phone: str, guthaben: float) -> bool:
    try:
        normalized_phone = normalize_phone(phone
...(truncated)...
