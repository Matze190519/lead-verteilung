"""
Lead-Verteilungs-Service v3.3 (FULL SAFE)
=========================================
Empfängt Facebook Lead Ads via Webhook ODER liest neue Leads aus dem
Google Sheet "Tabellenblatt1", verteilt Leads FAIR an aktive Partner
aus "Partner_Konto", zieht Guthaben ab und sendet WhatsApp-
Benachrichtigungen via Whapi API.

Inklusive: Stripe-Anbindung, Leads_Log, Threading-Lock.
SAFE MODE: Keine Nachrichten an Endkunden.
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

WHAPI_TOKEN = os.getenv("WHAPI_TOKEN", "")
WHAPI_URL = os.getenv("WHAPI_URL", "https://gate.whapi.cloud/messages/text")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
PAKET_PREIS = float(os.getenv("PAKET_PREIS", "50"))

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

MATZE_PHONE = os.getenv("MATZE_PHONE", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

poll_lock = threading.Lock()

app = FastAPI(
    title="Lead-Verteilungs-Service",
    description="Verteilt Leads fair an Partner. Liest aus Google Sheet + Facebook Webhook + Stripe.",
    version="3.3.0",
)

# ─── Google Sheets Client ────────────────────────────────────────────────────
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
        return spreadsheet.sheet1

def get_leads_sheet() -> gspread.Worksheet:
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet("Tabellenblatt1")

def get_leads_log_sheet() -> gspread.Worksheet:
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet("Leads_Log")
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title="Leads_Log", rows=1000, cols=10)
        headers = ["Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email", "Partner_Name", "Partner_Telefon", "Guthaben_Nachher", "WhatsApp_Partner", "WhatsApp_Lead", "Status"]
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws

def log_lead(lead_name, lead_phone, lead_email, partner_name, partner_phone, guthaben_nachher, wa_partner_ok, wa_lead_ok, status):
    try:
        log_sheet = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now, lead_name, lead_phone, lead_email,
            partner_name, partner_phone, guthaben_nachher,
            "OK" if wa_partner_ok else "FEHLER",
            "SKIPPED (SAFE MODE)",
            status,
        ]
        log_sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Fehler beim Lead-Logging: {e}")

# ─── Whapi WhatsApp ──────────────────────────────────────────────────────────
def send_whatsapp(phone: str, message: str) -> dict:
    if not WHAPI_TOKEN or not phone or len(phone) < 10:
        return {"error": "Token fehlt oder Nummer ungültig"}
    
    to = f"{phone}@s.whatsapp.net"
    headers = {"Authorization": f"Bearer {WHAPI_TOKEN}", "Content-Type": "application/json"}
    payload = {"to": to, "body": message}

    try:
        response = requests.post(WHAPI_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"WhatsApp-Fehler an {phone}: {e}")
        return {"error": str(e)}

# ─── Utils ───────────────────────────────────────────────────────────────────
def normalize_phone(phone: str) -> str:
    if not phone: return ""
    if phone.startswith("p:"): phone = phone[2:]
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"): phone = "49" + phone[1:]
    if not phone.startswith("49") and len(phone) <= 11: phone = "49" + phone
    return phone

def get_all_partner_records(sheet: gspread.Worksheet) -> list:
    headers = ["Name", "Telefon", "Guthaben_Euro", "Leads_Geliefert", "Letzter_Lead_Am", "Status"]
    all_values = sheet.get_all_values()
    if len(all_values) <= 1: return []
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
    except: return None
    if not all_records: return None

    aktive_partner = []
    for idx, record in enumerate(all_records):
        status = str(record.get("Status", "")).strip()
        try:
            guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
        except: guthaben = 0
        try:
            leads = int(record.get("Leads_Geliefert", 0))
        except: leads = 0
        letzter_lead = str(record.get("Letzter_Lead_Am", "")).strip()

        if status == "Aktiv" and guthaben > LEAD_PREIS - 0.01:
            aktive_partner.append({
                "row": idx + 2,
                "name": str(record.get("Name", "Unbekannt")),
                "telefon": str(record.get("Telefon", "")),
                "guthaben": guthaben,
                "leads_geliefert": leads,
                "letzter_lead": letzter_lead,
                "status": status,
            })

    if not aktive_partner: return None
    
    # Sortierung: Datum ASC (älteste zuerst)
    def sort_key(p):
        datum = p["letzter_lead"]
        if not datum: return ("0000-00-00 00:00:00", p["leads_geliefert"])
        return (datum, p["leads_geliefert"])

    aktive_partner.sort(key=sort_key)
    return aktive_partner[0]

def update_partner(sheet: gspread.Worksheet, partner: dict) -> bool:
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        sheet.update_cell(row, 4, partner["leads_geliefert"] + 1)
        sheet.update_cell(row, 5, now)
        
        if neues_guthaben < LEAD_PREIS:
            sheet.update_cell(row, 6, "Pausiert")
            if MATZE_PHONE:
                send_whatsapp(MATZE_PHONE, f"⚠️ Partner {partner['name']} pausiert (Guthaben leer).")
        return True
    except Exception as e:
        logger.error(f"Partner-Update Fehler: {e}")
        return False

# ─── Partner Helper (Stripe) ─────────────────────────────────────────────────
def find_partner_by_name(sheet, name):
    if not name: return None
    records = get_all_partner_records(sheet)
    name_lower = name.lower().strip()
    for idx, r in enumerate(records):
        rn = str(r.get("Name", "")).lower().strip()
        if rn and (rn in name_lower or name_lower in rn):
            try: g = float(str(r.get("Guthaben_Euro", 0)).replace(",", "."))
            except: g = 0
            return {"row": idx + 2, "name": r.get("Name"), "telefon": normalize_phone(r.get("Telefon")), "guthaben": g}
    return None

def find_partner_by_phone(sheet, phone):
    norm = normalize_phone(phone)
    if not norm: return None
    records = get_all_partner_records(sheet)
    for idx, r in enumerate(records):
        rp = normalize_phone(str(r.get("Telefon", "")))
        if rp == norm:
            try: g = float(str(r.get("Guthaben_Euro", 0)).replace(",", "."))
            except: g = 0
            return {"row": idx + 2, "name": r.get("Name"), "telefon": rp, "guthaben": g}
    return None

def update_partner_guthaben(sheet, partner, amount):
    try:
        new_g = round(partner["guthaben"] + amount, 2)
        sheet.update_cell(partner["row"], 3, new_g)
        sheet.update_cell(partner["row"], 6, "Aktiv")
        return True
    except: return False

def add_new_partner(sheet, name, phone, amount):
    try:
        p = normalize_phone(phone)
        sheet.append_row([name, p, amount, 0, "", "Aktiv"], value_input_option="USER_ENTERED")
        return True
    except: return False

def process_stripe_payment(name, phone, email, amount):
    try:
        sheet = get_sheet()
        p = find_partner_by_phone(sheet, phone) or find_partner_by_name(sheet, name)
        
        if p:
            update_partner_guthaben(sheet, p, amount)
            act = "GUTHABEN ERHÖHT"
            new_bal = round(p["guthaben"] + amount, 2)
        else:
            add_new_partner(sheet, name, phone, amount)
            act = "NEUER PARTNER"
            new_bal = amount
            
        if MATZE_PHONE:
            msg = f"💰 *Stripe:* {name} | {amount}€\n✅ {act}\n📊 Neu: {new_bal}€"
            send_whatsapp(MATZE_PHONE, msg)
    except Exception as e:
        logger.error(f"Stripe Error: {e}")

# ─── POLLING LOGIC (SAFE MODE) ───────────────────────────────────────────────
def poll_new_leads():
    if not poll_lock.acquire(blocking=False): return
    try:
        return _do_poll_new_leads()
    finally:
        poll_lock.release()

def _do_poll_new_leads():
    try:
        leads_sheet = get_leads_sheet()
        partner_sheet = get_sheet()
    except: return
    
    all_values = leads_sheet.get_all_values()
    if len(all_values) <= 1: return

    new_leads = []
    for row_idx, row in enumerate(all_values[1:], start=2):
        status = row[15] if len(row) > 15 else ""
        if status == "CREATED":
            try: leads_sheet.update_cell(row_idx, 16, "PROCESSING")
            except: continue
            
            raw = [row[12] if len(row)>12 else "", row[13] if len(row)>13 else "", row[14] if len(row)>14 else ""]
            name, email, phone = "Unbekannt", "", ""
            for v in raw:
                v = v.strip()
                if "@" in v: email = v
                elif any(c.isdigit() for c in v): phone = normalize
...(truncated)...
