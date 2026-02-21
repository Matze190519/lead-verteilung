"""
Lead-Verteilungs-Service v4.0 FINAL (META API)
================================================
- Meta API für WhatsApp (offiziell)
- Automatisches Polling alle 60 Sekunden
- Stripe Integration
- Lina (4915170605019) bekommt alle Infos
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
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

# ─── KONFIGURATION ───────────────────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("lead-verteilung")

# Meta API (aus Environment oder hardcoded als Fallback)
META_TOKEN = os.getenv("META_TOKEN", "EAARgaZCn3eoYBO0Tr9nSqfmJYOcx3gx3NAzSdwekRpZB5rfmWH2poZAvKSXXVBdR0HDqiXAEbfESzfejzSYLTCkhZAxs0bVZCMufcy51ZBN16zkDlpy8bcaUL5Omu6FTLW37O30I9uO51HSgfZBZBYz6qPEQ49RVEMWNrJmnrvvmrwCgAlJaJB7eHk2GvDdU8pKYkwZDZD")
META_PHONE_ID = os.getenv("META_PHONE_ID", "623007617563961")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"

# Weitere Config
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))

# Linas Nummer für Benachrichtigungen
LINA_PHONE = "4915170605019"

# Google Credentials
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Facebook & Stripe
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

# ─── THREADING LOCK ──────────────────────────
poll_lock = threading.Lock()

# ─── FASTAPI APP ─────────────────────────────
app = FastAPI(
    title="Lead-Verteilungs-Service",
    version="4.0-META",
)
# ─── GOOGLE SHEETS ───────────────────────────
def get_google_client():
    if GOOGLE_CREDENTIALS_JSON:
        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        return gspread.service_account_from_dict(creds)
    return gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)


def get_spreadsheet():
    return get_google_client().open_by_key(GOOGLE_SHEET_ID)


def get_partner_sheet():
    return get_spreadsheet().worksheet("Partner_Konto")


def get_leads_sheet():
    return get_spreadsheet().worksheet("Tabellenblatt1")


def get_log_sheet():
    try:
        return get_spreadsheet().worksheet("Leads_Log")
    except:
        ws = get_spreadsheet().add_worksheet(title="Leads_Log", rows=1000, cols=10)
        ws.append_row(["Zeit", "Lead", "Telefon", "Email", "Partner", "Partner_Tel", "Guthaben", "Status"], value_input_option="USER_ENTERED")
        return ws


def log_entry(lead_name, lead_phone, lead_email, partner_name, partner_phone, guthaben, status):
    try:
        ws = get_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([now, lead_name, lead_phone, lead_email, partner_name, partner_phone, guthaben, status], value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Log error: {e}")
        # ─── META WHATSAPP ───────────────────────────
def send_whatsapp(phone: str, message: str) -> bool:
    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Nummer: {phone}")
        return False
    
    if not META_TOKEN or not META_PHONE_ID:
        logger.error("META_TOKEN oder META_PHONE_ID nicht gesetzt!")
        return False
    
    to = phone.replace("+", "").replace(" ", "").replace("@s.whatsapp.net", "")
    
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
        res = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        if res.status_code >= 400:
            logger.error(f"Meta API Error: {res.text}")
            return False
        logger.info(f"WhatsApp OK an {phone}")
        return True
    except Exception as e:
        logger.error(f"WhatsApp Exception: {e}")
        return False


def normalize_phone(phone):
    if not phone:
        return ""
    phone = str(phone).replace("p:", "")
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"):
        phone = "49" + phone[1:]
    return phone
    # ─── PARTNER LOGIC ───────────────────────────
def get_all_partners(sheet):
    records = []
    try:
        for i, row in enumerate(sheet.get_all_records(), 2):
            try:
                guthaben = float(str(row.get("Guthaben_Euro", 0)).replace(",", "."))
                records.append({
                    "row": i,
                    "name": row.get("Name", ""),
                    "phone": normalize_phone(str(row.get("Telefon", ""))),
                    "guthaben": guthaben,
                    "leads": int(row.get("Leads_Geliefert", 0)),
                    "last": str(row.get("Letzter_Lead_Am", "")),
                    "status": str(row.get("Status", "")).strip(),
                })
            except:
                continue
    except Exception as e:
        logger.error(f"Fehler beim Lesen: {e}")
    return records


def find_best_partner(sheet):
    partners = [p for p in get_all_partners(sheet) if p["status"] == "Aktiv" and p["guthaben"] >= LEAD_PREIS]
    if not partners:
        return None
    partners.sort(key=lambda x: (x["last"] or "0000", x["leads"]))
    return partners[0]


def update_partner(sheet, partner):
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    new_bal = round(partner["guthaben"] - LEAD_PREIS, 2)
    
    sheet.update_cell(row, 3, new_bal)
    sheet.update_cell(row, 4, partner["leads"] + 1)
    sheet.update_cell(row, 5, now)
    
    if new_bal < LEAD_PREIS:
        sheet.update_cell(row, 6, "Pausiert")
        send_whatsapp(LINA_PHONE, f"⚠️ Partner {partner['name']} pausiert (Guthaben: {new_bal}€)")
    
    return new_bal
    # ─── LEAD VERARBEITUNG ───────────────────────
def process_lead(name, phone, email, row_idx=None):
    logger.info(f"Verarbeite Lead: {name} ({email})")
    
    try:
        sheet = get_partner_sheet()
        partner = find_best_partner(sheet)
        
        if not partner:
            logger.error("Kein Partner verfügbar!")
            if row_idx:
                get_leads_sheet().update_cell(row_idx, 16, "KEIN_PARTNER")
            return False
        
        new_bal = update_partner(sheet, partner)
        
        # 1. Partner benachrichtigen
        msg_partner = f"🔔 *Neuer Lead!*\n\n👤 {name}\n📞 {phone}\n📧 {email}\n\n💰 Guthaben: {new_bal}€"
        send_whatsapp(partner["phone"], msg_partner)
        
        # 2. LINA (du) benachrichtigen
        msg_lina = f"✅ Lead verteilt\n\n👤 {name}\n📞 {phone}\n📧 {email}\n\n➡️ {partner['name']}\n💰 {new_bal}€"
        send_whatsapp(LINA_PHONE, msg_lina)
        
        # 3. Status aktualisieren
        if row_idx:
            get_leads_sheet().update_cell(row_idx, 16, "VERTEILT")
        
        # 4. Loggen
        log_entry(name, phone, email, partner["name"], partner["phone"], new_bal, "VERTEILT")
        
        logger.info(f"Lead {name} → {partner['name']} verteilt")
        return True
        
    except Exception as e:
        logger.error(f"Fehler: {e}")
        return False
        # ─── POLLING ─────────────────────────────────
def poll_leads():
    if not poll_lock.acquire(blocking=False):
        return
    
    try:
        sheet = get_leads_sheet()
        rows = sheet.get_all_values()
        
        if len(rows) <= 1:
            return
        
        headers = rows[0]
        
        # KORRIGIERTE Spalten-Indizes (basierend auf der Header-Zeile):
        # id=0, created_time=1, ad_id=2, ad_name=3, adset_id=4, adset_name=5, 
        # campaign_id=6, campaign_name=7, form_id=8, form_name=9, is_organic=10, 
        # platform=11, e-mail-adresse=12, vollständiger_name=13, telefonnummer=14, lead_status=15
        name_col = 13   # vollständiger_name - WAR VORHER 3 (ad_name)!
        email_col = 12  # e-mail-adresse  
        phone_col = 14  # telefonnummer
        status_col = 15 # lead_status
        
        logger.info(f"KORRIGIERT - Spalten: Name={name_col}, Email={email_col}, Phone={phone_col}, Status={status_col}")
        
        for i, row in enumerate(rows[1:], 2):
            if len(row) <= status_col:
                continue
            
            status = row[status_col].strip().upper()
            if status not in ["", "NEU", "CREATED"]:
                continue
            
            # Als PROCESSING markieren
            sheet.update_cell(i, status_col + 1, "PROCESSING")
            
            name = row[name_col] if len(row) > name_col else ""
            email = row[email_col] if len(row) > email_col else ""
            phone = normalize_phone(row[phone_col]) if len(row) > phone_col else ""
            
            process_lead(name, phone, email, i)
            
    except Exception as e:
        logger.error(f"Polling Fehler: {e}")
    finally:
        poll_lock.release()


def poll_loop():
    logger.info(f"Polling gestartet (alle {POLL_INTERVAL}s)")
    while True:
        time.sleep(POLL_INTERVAL)
        poll_leads()
       # ─── API ENDPOINTS ───────────────────────────
@app.get("/")
def root():
    return {"status": "ok", "version": "4.0-META", "meta_url": META_URL}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/poll")
def manual_poll():
    poll_leads()
    return {"status": "ok", "message": "Polling ausgeführt"}


@app.get("/webhook/facebook")
def fb_verify(request: Request):
    params = dict(request.query_params)
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == FB_VERIFY_TOKEN:
        return int(params.get("hub.challenge", 0))
    raise HTTPException(403)


@app.post("/webhook/facebook")
async def fb_webhook(request: Request):
    try:
        data = await request.json()
        entry = data.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value = changes.get("value", {})
        lead_id = value.get("leadgen_id")
        
        if lead_id and FB_ACCESS_TOKEN:
            fb_url = f"https://graph.facebook.com/v18.0/{lead_id}?access_token={FB_ACCESS_TOKEN}"
            res = requests.get(fb_url, timeout=30)
            if res.status_code == 200:
                lead_data = res.json()
                fields = {f["name"]: f["values"][0] for f in lead_data.get("field_data", []) if f.get("values")}
                
                sheet = get_leads_sheet()
                sheet.append_row([
                    fields.get("full_name", ""),
                    fields.get("email", ""),
                    normalize_phone(fields.get("phone_number", "")),
                    "NEU",
                    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                ], value_input_option="USER_ENTERED")
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"FB Webhook Fehler: {e}")
        return {"status": "error"}
        @app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    try:
        payload = await request.body()
        sig = request.headers.get("stripe-signature", "")
        
        if STRIPE_WEBHOOK_SECRET and sig:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
        
        if event.get("type") == "checkout.session.completed":
            data = event["data"]["object"]
            amount = data.get("amount_total", 0) / 100
            customer = data.get("customer_details", {})
            customer_email = customer.get("email", "")
            customer_name = customer.get("name", "")
            
            # Partner finden oder erstellen
            sheet = get_partner_sheet()
            partners = get_all_partners(sheet)
            
            partner = None
            partner_phone = ""
            
            for p in partners:
                if customer_email.lower() in p["name"].lower():
                    partner = p
                    partner_phone = p["phone"]
                    break
            
            if partner:
                # Bestehender Partner - Guthaben aufladen
                new_g = partner["guthaben"] + amount
                sheet.update_cell(partner["row"], 3, new_g)
                sheet.update_cell(partner["row"], 6, "Aktiv")
                
                # Partner benachrichtigen
                if partner_phone:
                    msg_partner = f"✅ *Zahlung erhalten!*\n\n💰 {amount}€ wurden aufgeladen\n📊 Neues Guthaben: {new_g}€\n\nDu bist aktiv und erhältst Leads!"
                    send_whatsapp(partner_phone, msg_partner)
                
                # Lina benachrichtigen
                msg_lina = f"💰 Stripe Zahlung (Aufladung)\n\n👤 {customer_name}\n📧 {customer_email}\n💵 {amount}€\n✅ Aufgeladen\n📊 Guthaben: {new_g}€"
                send_whatsapp(LINA_PHONE, msg_lina)
                
            else:
                # Neuer Partner - anlegen
                customer_phone = normalize_phone(customer.get("phone", ""))
                
                sheet.append_row([customer_name, customer_phone, amount, 0, "", "Aktiv"], value_input_option="USER_ENTERED")
                
                # Partner benachrichtigen (wenn Telefon bekannt)
                if customer_phone:
                    msg_partner = f"🎉 *Willkommen!*\n\nDeine Anmeldung war erfolgreich!\n💰 {amount}€ Guthaben\n\nDu wirst jetzt automatisch Leads erhalten. Viel Erfolg! 🚀"
                    send_whatsapp(customer_phone, msg_partner)
                
                # Lina benachrichtigen
                msg_lina = f"🆕 *Neuer Partner!*\n\n👤 {customer_name}\n📧 {customer_email}\n📞 {customer_phone}\n💵 {amount}€\n✅ Neuer Partner angelegt"
                send_whatsapp(LINA_PHONE, msg_lina)
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Stripe Fehler: {e}")
        return {"status": "error"}


# ─── STARTUP ─────────────────────────────────
@app.on_event("startup")
def startup():
    logger.info("Lead-Verteilung v4.0 gestartet")
    logger.info(f"Meta URL: {META_URL}")
    logger.info(f"Lina Phone: {LINA_PHONE}")
    threading.Thread(target=poll_loop, daemon=True).start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
