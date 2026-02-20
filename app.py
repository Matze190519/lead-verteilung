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

# ─── CONFIG ───
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lead-verteilung")

WHAPI_TOKEN = os.getenv("WHAPI_TOKEN", "")
WHAPI_URL = "https://gate.whapi.cloud/messages/text"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
MATZE_PHONE = os.getenv("MATZE_PHONE", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

poll_lock = threading.Lock()
app = FastAPI(title="Lead-Verteilungs-Service v3.6 (SAFE)")

# ─── GOOGLE SHEETS ───
def get_spreadsheet():
    if GOOGLE_CREDENTIALS_JSON:
        creds = json.loads(GOOGLE_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(creds)
    else:
        gc = gspread.service_account(filename="credentials.json")
    return gc.open_by_key(GOOGLE_SHEET_ID)

def get_sheet():
    return get_spreadsheet().worksheet("Partner_Konto")

def get_leads_sheet():
    return get_spreadsheet().worksheet("Tabellenblatt1")

def get_leads_log_sheet():
    try:
        return get_spreadsheet().worksheet("Leads_Log")
    except:
        ws = get_spreadsheet().add_worksheet(title="Leads_Log", rows=1000, cols=10)
        return ws

def log_lead(lead_name, lead_phone, lead_email, partner_name, partner_phone, guthaben_nachher, wa_partner_ok, status):
    try:
        ws = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [now, lead_name, lead_phone, lead_email, partner_name, partner_phone, guthaben_nachher, "OK" if wa_partner_ok else "FEHLER", "SKIPPED", status]
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Log Error: {e}")

# ─── WHAPI ───
def send_whatsapp(phone, message):
    if not WHAPI_TOKEN or not phone: return {"error": "No Token/Phone"}
    to = f"{phone}@s.whatsapp.net"
    try:
        res = requests.post(WHAPI_URL, json={"to": to, "body": message}, headers={"Authorization": f"Bearer {WHAPI_TOKEN}"})
        return res.json()
    except Exception as e:
        return {"error": str(e)}

def normalize_phone(phone):
    if not phone: return ""
    phone = str(phone).replace("p:", "")
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"): phone = "49" + phone[1:]
    return phone

# ─── LOGIC ───
def find_best_partner(sheet):
    try:
        records = sheet.get_all_records()
    except: return None
    candidates = []
    for i, r in enumerate(records):
        try:
            g = float(str(r.get("Guthaben_Euro", 0)).replace(",", "."))
            if str(r.get("Status")).strip() == "Aktiv" and g >= LEAD_PREIS:
                candidates.append({
                    "row": i + 2, "name": r.get("Name"), "phone": normalize_phone(str(r.get("Telefon"))),
                    "guthaben": g, "leads": r.get("Leads_Geliefert", 0),
                    "last": str(r.get("Letzter_Lead_Am", ""))
                })
        except: continue
    if not candidates: return None
    candidates.sort(key=lambda x: (x["last"] or "0000", x["leads"]))
    return candidates[0]

def update_partner(sheet, partner):
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    new_bal = round(partner["guthaben"] - LEAD_PREIS, 2)
    sheet.update_cell(row, 3, new_bal)
    sheet.update_cell(row, 4, partner["leads"] + 1)
    sheet.update_cell(row, 5, now)
    if new_bal < LEAD_PREIS:
        sheet.update_cell(row, 6, "Pausiert")
        if MATZE_PHONE: send_whatsapp(MATZE_PHONE, f"⚠️ Partner {partner['name']} pausiert (Guthaben leer).")
    return new_bal

def process_lead_distribution(name, phone, email, row_idx=None):
    if not phone: return
    try:
        sheet = get_sheet()
        partner = find_best_partner(sheet)
        
        if not partner:
            if MATZE_PHONE: send_whatsapp(MATZE_PHONE, f"⚠️ Kein Partner für Lead: {name}")
            if row_idx: get_leads_sheet().update_cell(row_idx, 16, "KEIN_PARTNER")
            return

        new_bal = update_partner(sheet, partner)
        
        # Info an Partner
        msg_p = f"🔔 *Neuer Lead!*\n👤 {name}\n📞 {phone}\n📧 {email}\n💰 Rest: {new_bal}€"
        wa = send_whatsapp(partner["phone"], msg_p)
        
        # Info an Admin
        if MATZE_PHONE:
            msg_a = f"✅ Lead verteilt: {name} -> {partner['name']}"
            send_whatsapp(MATZE_PHONE, msg_a)
            
        # SAFE MODE: KEINE Nachricht an Lead.
        
        if row_idx: get_leads_sheet().update_cell(row_idx, 16, "VERTEILT")
        log_lead(name, phone, email, partner["name"], partner["phone"], new_bal, "error" not in wa, "VERTEILT")
    except Exception as e:
        logger.error(f"Process Error: {e}")

# ─── POLLING ───
def poll_loop():
    while True:
        time.sleep(POLL_INTERVAL)
        if not poll_lock.acquire(blocking=False): continue
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
                        if "@" in v: email = v
                        elif any(c.isdigit() for c in v): phone = normalize_phone(v)
                        elif v: name = v
                    process_lead_distribution(name, phone, email, i)
        except Exception as e:
            logger.error(f"Poll Error: {e}")
        finally:
            poll_lock.release()

@app.on_event("startup")
def start_poll():
    threading.Thread(target=poll_loop, daemon=True).start()

# ─── WEBHOOKS ───
@app.get("/")
def index(): return {"status": "running v3.6"}

@app.get("/health")
def health(): return {"status": "ok"}

@app.get("/webhook")
def verify(request: Request):
    if request.query_params.get("hub.verify_token") == FB_VERIFY_TOKEN:
        return int(request.query_params.get("hub.challenge"))
    raise HTTPException(403)

@app.post("/webhook")
async def receive(request: Request, bg: BackgroundTasks):
    return {"status": "received"}

@app.post("/stripe-webhook")
async def stripe_wh(request: Request, bg: BackgroundTasks):
    try:
        payload = await request.body()
        sig = request.headers.get("stripe-signature", "")
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET) if STRIPE_WEBHOOK_SECRET else json.loads(payload)
        if event.get("type") == "checkout.session.completed":
            s = event["data"]["object"]
            amt = s.get("amount_total", 0) / 100.0
            cd = s.get("customer_details", {}) or {}
            bg.add_task(process_stripe_payment, cd.get("name",""), cd.get("phone",""), cd.get("email",""), amt)
    except: pass
    return {"status": "ok"}

# ─── STRIPE HELPER ───
def process_stripe_payment(name, phone, email, amount):
    try:
        sheet = get_sheet()
        p = find_best_partner(sheet) # Placeholder search logic needed here? No, find specific partner.
        # Quick fix for stripe logic:
        records = get_all_partner_records(sheet)
        target = None
        norm_phone = normalize_phone(phone)
        for i, r in enumerate(records):
            if (norm_phone and normalize_phone(str(r.get("Telefon"))) == norm_phone) or \
               (name and str(r.get("Name")).lower() in name.lower()):
                target = {"row": i+2, "guthaben": float(str(r.get("Guthaben_Euro",0)).replace(",","."))}
                break
        
        if target:
            new_g = round(target["guthaben"] + amount, 2)
            sheet.update_cell(target["row"], 3, new_g)
            sheet.update_cell(target["row"], 6, "Aktiv")
            act = "GUTHABEN ERHÖHT"
        else:
            sheet.append_row([name, normalize_phone(phone), amount, 0, "", "Aktiv"], value_input_option="USER_ENTERED")
            act = "NEUER PARTNER"
            new_g = amount
            
        if MATZE_PHONE: send_whatsapp(MATZE_PHONE, f"💰 *Stripe:* {name} | {amount}€\n✅ {act}\n📊 Neu: {new_g}€")
    except Exception as e:
        logger.error(f"Stripe Error: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
