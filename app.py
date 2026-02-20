"""
Lead-Verteilungs-Service v4.2 FINAL (STABLE)
=============================================
- WhatsApp: Meta Cloud API (Lina) - NEU
- Stripe: v3.6 Code (bewährt, funktioniert) - STABIL
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

# ─── CONFIG ─────────────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("lead-verteilung")

# META API (Lina) - NEU
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"

# Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Preise
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
PAKET_PREIS = float(os.getenv("PAKET_PREIS", "50"))

# Stripe (aus v3.6 - funktioniert!)
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# Admin
MATZE_PHONE = os.getenv("MATZE_PHONE", "+491715060008")

# Polling
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

poll_lock = threading.Lock()

app = FastAPI(title="Lead-Verteilung v4.2 FINAL", version="4.2.0")

# ─── GOOGLE SHEETS (aus v3.6) ────────────────────────────────────────────────
def get_google_client():
    if GOOGLE_CREDENTIALS_JSON:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        return gspread.service_account_from_dict(creds_dict)
    return gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)

def get_spreadsheet():
    return get_google_client().open_by_key(GOOGLE_SHEET_ID)

def get_sheet():
    try:
        return get_spreadsheet().worksheet("Partner_Konto")
    except:
        return get_spreadsheet().sheet1

def get_leads_sheet():
    return get_spreadsheet().worksheet("Tabellenblatt1")

def get_leads_log_sheet():
    try:
        return get_spreadsheet().worksheet("Leads_Log")
    except:
        ws = get_spreadsheet().add_worksheet(title="Leads_Log", rows=1000, cols=10)
        headers = ["Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email",
                   "Partner_Name", "Partner_Telefon", "Guthaben_Nachher",
                   "WhatsApp_Partner", "WhatsApp_Lead", "Status"]
        ws.append_row(headers)
        return ws

# ─── META WHATSAPP (NEU - statt Whapi) ──────────────────────────────────────
def send_whatsapp(phone: str, message: str) -> bool:
    if not phone or not META_TOKEN:
        return False
    
    to = phone.replace("+", "").replace(" ", "")
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": "text",
        "text": {"body": message}
    }
    
    try:
        res = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        if res.status_code != 200:
            logger.error(f"Meta API Error: {res.text}")
        return res.status_code == 200
    except Exception as e:
        logger.error(f"WhatsApp Exception: {e}")
        return False

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

# ─── STRIPE (aus v3.6 - bewährt!) ───────────────────────────────────────────
@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    """Stripe Webhook Handler - aus v3.6, funktioniert!"""
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    
    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except ValueError:
        return JSONResponse({"error": "Invalid payload"}, status_code=400)
    except stripe.error.SignatureVerificationError:
        return JSONResponse({"error": "Invalid signature"}, status_code=400)
    
    if event.get("type") == "checkout.session.completed":
        session = event["data"]["object"]
        handle_checkout_session(session)
    
    return {"status": "success"}

def handle_checkout_session(session):
    """Stripe Zahlung verarbeiten - Guthaben aufladen"""
    try:
        customer_email = session.get("customer_details", {}).get("email", "")
        customer_name = session.get("customer_details", {}).get("name", "")
        amount = session.get("amount_total", 0) / 100  # Cent zu Euro
        
        sheet = get_sheet()
        all_records = get_all_partner_records(sheet)
        
        partner_found = False
        for idx, record in enumerate(all_records):
            if (record.get("Email") == customer_email or 
                record.get("Name") == customer_name):
                
                current_balance = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
                new_balance = current_balance + amount
                
                # Update Sheet (Zeile idx+2 wegen Header)
                sheet.update_cell(idx + 2, 3, new_balance)  # Spalte C = Guthaben
                sheet.update_cell(idx + 2, 6, "Aktiv")      # Spalte F = Status
                
                partner_found = True
                
                # WhatsApp an Matze
                msg = f"💰 *Stripe-Zahlung*\n{record.get('Name')} hat {amount}€ aufgeladen\nNeues Guthaben: {new_balance}€"
                send_whatsapp(MATZE_PHONE, msg)
                logger.info(f"Guthaben aufgeladen: {record.get('Name')} +{amount}€")
                break
        
        if not partner_found:
            # Neuer Partner anlegen
            new_row = [customer_name, "", amount, 0, "", "Aktiv", customer_email]
            sheet.append_row(new_row)
            send_whatsapp(MATZE_PHONE, f"⚠️ Neuer Partner via Stripe: {customer_name} ({amount}€)")
            logger.info(f"Neuer Partner angelegt: {customer_name}")
            
    except Exception as e:
        logger.error(f"Stripe Fehler: {e}")

# ─── PARTNER LOGIC (aus v3.6) ───────────────────────────────────────────────
def get_all_partner_records(sheet):
    """Liest alle Partner-Daten aus Spalten A-F"""
    headers = ["Name", "Telefon", "Guthaben_Euro", "Leads_Geliefert", "Letzter_Lead_Am", "Status", "Email"]
    all_values = sheet.get_all_values()
    if len(all_values) <= 1:
        return []
    
    records = []
    for row in all_values[1:]:
        if len(row) >= 1 and row[0].strip():
            record = {}
            for i, header in enumerate(headers):
                record[header] = row[i] if i < len(row) else ""
            records.append(record)
    return records

def find_best_partner(sheet):
    """Faire Verteilung: Wer am längsten wartet, ist dran"""
    try:
        all_records = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Fehler beim Lesen: {e}")
        return None
    
    aktive = []
    for idx, record in enumerate(all_records):
        status = str(record.get("Status", "")).strip()
        try:
            guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
        except:
            guthaben = 0
        
        if status == "Aktiv" and guthaben >= LEAD_PREIS:
            aktive.append({
                "row": idx + 2,
                "name": record.get("Name"),
                "phone": normalize_phone(record.get("Telefon")),
                "guthaben": guthaben,
                "leads": int(record.get("Leads_Geliefert", 0) or 0),
                "last": str(record.get("Letzter_Lead_Am", ""))
            })
    
    if not aktive:
        return None
    
    # Sortieren: Leeres Datum zuerst, dann ältestes Datum
    aktive.sort(key=lambda x: (x["last"] or "0000-00-00", x["leads"]))
    return aktive[0]

def update_partner(sheet, partner):
    """Partner aktualisieren nach Lead-Vergabe"""
    new_bal = partner["guthaben"] - LEAD_PREIS
    new_leads = partner["leads"] + 1
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    
    sheet.update_cell(partner["row"], 3, new_bal)      # Guthaben
    sheet.update_cell(partner["row"], 4, new_leads)    # Leads
    sheet.update_cell(partner["row"], 5, now)          # Letzter Lead
    
    if new_bal < LEAD_PREIS:
        sheet.update_cell(partner["row"], 6, "Pausiert")
        send_whatsapp(MATZE_PHONE, f"⚠️ {partner['name']} pausiert (Guthaben leer)")
    
    return new_bal

# ─── LEAD VERARBEITUNG ──────────────────────────────────────────────────────
@app.post("/webhook")
async def receive_lead(request: Request):
    """Facebook Lead Webhook"""
    try:
        data = await request.json()
        lead_name = data.get("name", "Unbekannt")
        lead_phone = normalize_phone(data.get("phone", ""))
        lead_email = data.get("email", "")
        
        sheet = get_sheet()
        partner = find_best_partner(sheet)
        
        if not partner:
            send_whatsapp(MATZE_PHONE, f"❌ Kein Partner für Lead {lead_name}")
            return {"status": "no_partner"}
        
        new_bal = update_partner(sheet, partner)
        
        # An Partner
        msg_p = f"🎯 *Neuer Lead!*\nName: {lead_name}\nTel: {lead_phone}\nEmail: {lead_email}\nDein Restguthaben: {new_bal}€"
        wa_partner = send_whatsapp(partner["phone"], msg_p)
        
        # An Matze
        msg_m = f"✅ Lead {lead_name} → {partner['name']} (Rest: {new_bal}€)"
        send_whatsapp(MATZE_PHONE, msg_m)
        
        # Loggen
        try:
            log_sheet = get_leads_log_sheet()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            log_sheet.append_row([
                now, lead_name, lead_phone, lead_email,
                partner["name"], partner["phone"], new_bal,
                "OK" if wa_partner else "FEHLER", "SKIPPED", "VERTEILT"
            ])
        except Exception as e:
            logger.error(f"Log-Fehler: {e}")
        
        return {"status": "distributed", "partner": partner["name"]}
        
    except Exception as e:
        logger.error(f"Webhook Fehler: {e}")
        return {"status": "error", "message": str(e)}

# ─── POLLING (optional, für Sheet-basierte Leads) ───────────────────────────
def poll_loop():
    while True:
        time.sleep(POLL_INTERVAL)
        if not poll_lock.acquire(blocking=False):
            continue
        
        try:
            ls = get_leads_sheet()
            rows = ls.get_all_values()
            
            for i, row in enumerate(rows[1:], 2):
                if len(row) > 15 and row[15] == "CREATED":
                    ls.update_cell(i, 16, "PROCESSING")
                    
                    name = row[12] if len(row) > 12 else "Unbekannt"
                    email = row[13] if len(row) > 13 else ""
                    phone = normalize_phone(row[14] if len(row) > 14 else "")
                    
                    # Webhook-Logik wiederverwenden
                    sheet = get_sheet()
                    partner = find_best_partner(sheet)
                    
                    if partner:
                        
...(truncated)...
