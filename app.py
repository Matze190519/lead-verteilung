"""
Lead-Verteilungs-Service v4.0 META (WORKING)
=============================================
- Meta API fur WhatsApp (Lina)
- Stripe funktioniert
"""

import os
import json
import logging
import time
import threading
from datetime import datetime, timezone

import gspread
import stripe
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lead-verteilung")

# Meta API (Lina)
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"

# Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")

# Stripe
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# Config
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
MATZE_PHONE = os.getenv("MATZE_PHONE", "+491715060008")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

poll_lock = threading.Lock()
app = FastAPI(title="Lead-Verteilung v4.0", version="4.0.0")

def get_sheet():
    if GOOGLE_CREDENTIALS_JSON:
        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(creds)
    else:
        gc = gspread.service_account(filename="credentials.json")
    return gc.open_by_key(GOOGLE_SHEET_ID).worksheet("Partner_Konto")

def get_leads_sheet():
    if GOOGLE_CREDENTIALS_JSON:
        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(creds)
    else:
        gc = gspread.service_account(filename="credentials.json")
    return gc.open_by_key(GOOGLE_SHEET_ID).worksheet("Tabellenblatt1")

def send_whatsapp(phone, message):
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
        r = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        return r.status_code == 200
    except Exception as e:
        logger.error(f"WhatsApp error: {e}")
        return False

def normalize_phone(phone):
    if not phone:
        return ""
    phone = str(phone).replace("p:", "")
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"):
        phone = "49" + phone[1:]
    return phone

def get_all_records(sheet):
    rows = sheet.get_all_values()
    if len(rows) <= 1:
        return []
    headers = ["Name", "Telefon", "Guthaben_Euro", "Leads_Geliefert", "Letzter_Lead_Am", "Status"]
    records = []
    for row in rows[1:]:
        if row and row[0]:
            rec = {}
            for i, h in enumerate(headers):
                rec[h] = row[i] if i < len(row) else ""
            records.append(rec)
    return records

def find_partner(sheet):
    records = get_all_records(sheet)
    candidates = []
    for i, r in enumerate(records):
        try:
            g = float(str(r.get("Guthaben_Euro", 0)).replace(",", "."))
            if str(r.get("Status", "")) == "Aktiv" and g >= LEAD_PREIS:
                candidates.append({
                    "row": i + 2,
                    "name": r.get("Name"),
                    "phone": normalize_phone(r.get("Telefon")),
                    "guthaben": g,
                    "leads": int(r.get("Leads_Geliefert", 0) or 0),
                    "last": str(r.get("Letzter_Lead_Am", ""))
                })
        except:
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x["last"] or "0000", x["leads"]))
    return candidates[0]

def update_partner(sheet, p):
    new_bal = round(p["guthaben"] - LEAD_PREIS, 2)
    new_count = p["leads"] + 1
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sheet.update_cell(p["row"], 3, new_bal)
    sheet.update_cell(p["row"], 4, new_count)
    sheet.update_cell(p["row"], 5, now)
    if new_bal < LEAD_PREIS:
        sheet.update_cell(p["row"], 6, "Pausiert")
        send_whatsapp(MATZE_PHONE, f"Partner {p['name']} pausiert")
    return new_bal

@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        name = data.get("name", "Unbekannt")
        phone = normalize_phone(data.get("phone", ""))
        email = data.get("email", "")
        
        sheet = get_sheet()
        partner = find_partner(sheet)
        
        if not partner:
            send_whatsapp(MATZE_PHONE, f"Kein Partner fur {name}")
            return {"status": "no_partner"}
        
        new_bal = update_partner(sheet, partner)
        
        msg = f"Neuer Lead: {name}, Tel: {phone}, Rest: {new_bal}EUR"
        send_whatsapp(partner["phone"], msg)
        send_whatsapp(MATZE_PHONE, f"Lead {name} an {partner['name']}")
        
        return {"status": "ok", "partner": partner["name"]}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"status": "error"}

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    try:
        payload = await request.body()
        sig = request.headers.get("stripe-signature", "")
        
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
        
        if event.get("type") == "checkout.session.completed":
            s = event["data"]["object"]
            amt = s.get("amount_total", 0) / 100
            cd = s.get("customer_details", {}) or {}
            email = cd.get("email", "")
            name = cd.get("name", "")
            phone = normalize_phone(cd.get("phone", ""))
            
            sheet = get_sheet()
            records = get_all_records(sheet)
            found = False
            
            for i, r in enumerate(records):
                if email and r.get("Name") == name:
                    try:
                        g = float(str(r.get("Guthaben_Euro", 0)).replace(",", "."))
                        new_g = round(g + amt, 2)
                        sheet.update_cell(i + 2, 3, new_g)
                        sheet.update_cell(i + 2, 6, "Aktiv")
                        found = True
                        send_whatsapp(MATZE_PHONE, f"Stripe: {name} +{amt}EUR = {new_g}EUR")
                        break
                    except:
                        pass
            
            if not found:
                sheet.append_row([name, phone, amt, 0, "", "Aktiv", email])
                send_whatsapp(MATZE_PHONE, f"Stripe NEU: {name} {amt}EUR")
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Stripe error: {e}")
        return {"status": "error"}

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
                    
                    sheet = get_sheet()
                    partner = find_partner(sheet)
                    if partner:
                        new_bal = update_partner(sheet, partner)
                        send_whatsapp(partner["phone"], f"Lead: {name}, Rest: {new_bal}EUR")
                        send_whatsapp(MATZE_PHONE, f"Lead {name} an {partner['name']}")
                        ls.update_cell(i, 16, "VERTEILT")
                    else:
                        ls.update_cell(i, 16, "KEIN_PARTNER")
        except Exception as e:
            logger.error(f"Poll error: {e}")
        finally:
            poll_lock.release()

@app.on_event("startup")
def start():
    threading.Thread(target=poll_loop, daemon=True).start()

@app.get("/")
def health():
    return {"status": "v4.0 running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
