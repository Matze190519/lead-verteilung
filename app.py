"""
Lead-Verteilungs-Service v4.3 FINAL + STRIPE-FIX
=================================================
- Meta Cloud API (Lina's Account zum SENDEN)
- Alle Benachrichtigungen an Matze (491715060008)
- Partner bekommen Leads von ihrer Partnernummer
- Matze sieht ALLES (Lead-Verteilungen, Stripe, Fehler)
- Stripe-Benachrichtigungen an Matze hinzugefügt (FIX!)
- Keine WhAPI (Meta-konform!)

Architektur:
  Lina's Business (4915170605019) → SENDET
  Matze (491715060008) → EMPFÄNGT alle Admin-Infos
  Partner (diverse) → Bekommen ihre Leads
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

# META API (Lina's Business Account - nur zum SENDEN!)
META_TOKEN = os.getenv("META_TOKEN", "EAARgaZCn3eoYBO0Tr9nSqfmJYOcx3gx3NAzSdwekRpZB5rfmWH2poZAvKSXXVBdR0HDqiXAEbfESzfejzSYLTCkhZAxs0bVZCMufcy51ZBN16zkDlpy8bcaUL5Omu6FTLW37O30I9uO51HSgfZBZBYz6qPEQ49RVEMWNrJmnrvvmrwCgAlJaJB7eHk2GvDdU8pKYkwZDZD")
META_PHONE_ID = os.getenv("META_PHONE_ID", "623007617563961")
META_URL = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"

# MATZE'S NUMMER (empfängt ALLE Admin-Benachrichtigungen!)
MATZE_PHONE = "491715060008"  # ← Hardcoded, keine Env-Variable!

# Weitere Config
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))

# Google Credentials
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Facebook & Stripe
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

# ─── Threading Lock ──────────────────────────
poll_lock = threading.Lock()

# ─── FastAPI App ─────────────────────────────
app = FastAPI(
    title="Lead-Verteilungs-Service",
    version="4.3-FINAL-STRIPE-FIX",
)

logger.info(f"✅ System gestartet | Admin-Benachrichtigungen → {MATZE_PHONE}")


# ─── Google Sheets ───────────────────────────
def get_google_client():
    if GOOGLE_CREDENTIALS_JSON:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        return gspread.service_account_from_dict(creds_dict)
    return gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)


def get_spreadsheet():
    return get_google_client().open_by_key(GOOGLE_SHEET_ID)


def get_partner_sheet():
    return get_spreadsheet().worksheet("Partner_Konto")


def get_leads_sheet():
    return get_spreadsheet().worksheet("Tabellenblatt1")


def get_leads_log_sheet():
    try:
        return get_spreadsheet().worksheet("Leads_Log")
    except:
        ws = get_spreadsheet().add_worksheet(title="Leads_Log", rows=1000, cols=10)
        ws.append_row(["Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email",
                       "Partner_Name", "Partner_Telefon", "Guthaben_Nachher",
                       "WhatsApp_Partner", "Status"], value_input_option="USER_ENTERED")
        return ws


def log_lead(lead_name, lead_phone, lead_email, partner_name, partner_phone, 
             guthaben_nachher, wa_partner_ok, status):
    try:
        log_sheet = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [now, lead_name, lead_phone, lead_email, partner_name, 
               partner_phone, guthaben_nachher, "OK" if wa_partner_ok else "FEHLER", status]
        log_sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Log error: {e}")


# ─── Meta WhatsApp (OFFICIAL CLOUD API) ───────
def send_whatsapp(phone, message):
    """
    Sendet WhatsApp über Meta Cloud API (Lina's Business Account)
    """
    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Nummer: {phone}")
        return {"error": "Invalid phone"}
    
    if not META_TOKEN or not META_PHONE_ID:
        logger.error("META_TOKEN oder META_PHONE_ID nicht gesetzt!")
        return {"error": "Not configured"}
    
    # Nummer normalisieren (ohne +, ohne Leerzeichen)
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
        
        logger.info(f"[META_RESPONSE] Status={res.status_code} | Phone={to}")
        
        if res.status_code >= 400:
            logger.error(f"Meta API Error: {res.text}")
            return {"error": res.text}
        
        logger.info(f"✅ WhatsApp OK an {phone}")
        return {"success": True}
        
    except Exception as e:
        logger.error(f"WhatsApp Exception: {e}")
        return {"error": str(e)}


def normalize_phone(phone):
    if not phone:
        return ""
    phone = str(phone).replace("p:", "")
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"):
        phone = "49" + phone[1:]
    return phone


# ─── Partner Logik ───────────────────────────
def get_all_partner_records(sheet):
    records = []
    try:
        for i, row in enumerate(sheet.get_all_records(), 2):
            try:
                guthaben = float(str(row.get("Guthaben_Euro", 0)).replace(",", "."))
                records.append({
                    "row": i,
                    "name": row.get("Name", ""),
                    "telefon": normalize_phone(str(row.get("Telefon", ""))),
                    "guthaben": guthaben,
                    "leads_geliefert": int(row.get("Leads_Geliefert", 0)),
                    "letzter_lead": str(row.get("Letzter_Lead_Am", "")),
                    "status": str(row.get("Status", "")).strip(),
                })
            except:
                continue
    except Exception as e:
        logger.error(f"Fehler beim Lesen: {e}")
    return records


def find_best_partner(sheet):
    try:
        all_records = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Fehler beim Lesen: {e}")
        return None

    aktive_partner = []
    for record in all_records:
        status = str(record.get("status", "")).strip()
        guthaben = record.get("guthaben", 0)
        
        if status == "Aktiv" and guthaben >= LEAD_PREIS:
            aktive_partner.append(record)

    if not aktive_partner:
        return None

    def sort_key(p):
        datum = p["letzter_lead"]
        if not datum:
            return ("0000-00-00 00:00:00", p["leads_geliefert"])
        return (datum, p["leads_geliefert"])

    aktive_partner.sort(key=sort_key)
    return aktive_partner[0]


def update_partner(sheet, partner):
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        sheet.update_cell(row, 4, partner["leads_geliefert"] + 1)
        sheet.update_cell(row, 5, now)
        
        if neues_guthaben < LEAD_PREIS:
            sheet.update_cell(row, 6, "Pausiert")
            # Matze benachrichtigen
            send_whatsapp(MATZE_PHONE, 
                f"⚠️ Partner {partner['name']} pausiert (Guthaben: {neues_guthaben}€)")
        
        return neues_guthaben
    except Exception as e:
        logger.error(f"Fehler beim Update: {e}")
        return partner["guthaben"]


def find_partner_by_phone(sheet, phone):
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    records = get_all_partner_records(sheet)
    for record in records:
        partner_phone = normalize_phone(str(record.get("telefon", "")))
        if partner_phone == normalized:
            return record
    return None


def find_partner_by_name(sheet, name):
    if not name:
        return None
    records = get_all_partner_records(sheet)
    name_lower = name.lower().strip()
    for record in records:
        record_name = str(record.get("name", "")).lower().strip()
        if record_name and (record_name in name_lower or name_lower in record_name):
            return record
    return None


def add_new_partner(sheet, name, phone, guthaben):
    try:
        normalized_phone = normalize_phone(phone)
        sheet.append_row([name, normalized_phone, guthaben, 0, "", "Aktiv"], 
                        value_input_option="USER_ENTERED")
        logger.info(f"Neuer Partner: {name}, {guthaben}€")
        return True
    except Exception as e:
        logger.error(f"Fehler: {e}")
        return False


def update_partner_guthaben(sheet, partner, betrag):
    row = partner["row"]
    try:
        neues_guthaben = round(partner["guthaben"] + betrag, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        sheet.update_cell(row, 6, "Aktiv")
        return neues_guthaben
    except Exception as e:
        logger.error(f"Fehler: {e}")
        return partner["guthaben"]


# ─── Lead-Verteilung ─────────────────────────
def process_lead(lead_data):
    lead_name = lead_data.get("name", "Unbekannt")
    lead_phone = normalize_phone(lead_data.get("phone", ""))
    lead_email = lead_data.get("email", "")

    logger.info(f"=== Lead: {lead_name} | {lead_phone} ===")

    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return {"error": str(e)}

    partner = find_best_partner(sheet)
    if not partner:
        # Matze benachrichtigen - kein Partner
        send_whatsapp(MATZE_PHONE,
            f"⚠️ *Lead ohne Partner!*\n\n"
            f"👤 {lead_name}\n📞 {lead_phone}\n📧 {lead_email}")
        log_lead(lead_name, lead_phone, lead_email, "KEIN PARTNER", "", 0, False, "KEIN_PARTNER")
        return {"error": "Kein Partner"}

    neues_guthaben = update_partner(sheet, partner)
    
    # 1. Partner benachrichtigen
    partner_msg = (f"🔔 *Neuer Lead!*\n\n"
                   f"👤 {lead_name}\n"
                   f"📞 {lead_phone}\n"
                   f"📧 {lead_email}\n\n"
                   f"💰 Rest: {neues_guthaben}€")
    wa_result = send_whatsapp(partner["telefon"], partner_msg)

    time.sleep(2)  # Rate-Limit-Schutz

    # 2. MATZE benachrichtigen (ALLE Infos!)
    matze_msg = (f"✅ *Lead verteilt*\n\n"
                 f"👤 {lead_name}\n"
                 f"📞 {lead_phone}\n"
                 f"📧 {lead_email}\n\n"
                 f"➡️ {partner['name']}\n"
                 f"💰 Rest: {neues_guthaben}€")
    send_whatsapp(MATZE_PHONE, matze_msg)

    log_lead(lead_name, lead_phone, lead_email, partner["name"], 
             partner["telefon"], neues_guthaben, "error" not in wa_result, "VERTEILT")

    return {"success": True, "partner": partner["name"], "guthaben": neues_guthaben}


# ─── Stripe Zahlung verarbeiten ──────────────
def process_stripe_payment(customer_name, customer_phone, customer_email, amount):
    logger.info(f"=== Stripe: {customer_name} | {amount}€ ===")
    
    # DEBUG: Prüfe MATZE_PHONE
    logger.info(f"🔍 DEBUG: MATZE_PHONE = '{MATZE_PHONE}' | Länge={len(MATZE_PHONE)}")
    
    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return

    # Partner suchen (Telefon ODER Name)
    partner = None
    if customer_phone:
        partner = find_partner_by_phone(sheet, customer_phone)
    if not partner and customer_name:
        partner = find_partner_by_name(sheet, customer_name)

    if partner:
        neues_guthaben = update_partner_guthaben(sheet, partner, amount)
        action = "GUTHABEN ERHÖHT"
        partner_name = partner["name"]
    else:
        # Neuer Partner
        add_new_partner(sheet, customer_name, customer_phone, amount)
        neues_guthaben = amount
        action = "NEUER PARTNER"
        partner_name = customer_name

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1️⃣ PARTNER BENACHRICHTIGEN (optional - wenn Tel vorhanden)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if customer_phone and normalize_phone(customer_phone):
        partner_msg = (
            f"✅ *Zahlung erhalten!*\n\n"
            f"💰 {amount}€ aufgeladen\n"
            f"📊 Neues Guthaben: {neues_guthaben}€\n\n"
            f"Du bist aktiv und erhältst Leads!"
        )
        partner_result = send_whatsapp(normalize_phone(customer_phone), partner_msg)
        if "error" not in partner_result:
            logger.info(f"✅ Partner-Benachrichtigung gesendet an {customer_phone}")
    
    time.sleep(2)  # Rate-Limit-Schutz
    
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2️⃣ MATZE BENACHRICHTIGEN (ADMIN-INFO - WIE BEI LEADS!)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    matze_msg = (
        f"💰 *Stripe-Zahlung eingegangen!*\n\n"
        f"👤 {customer_name}\n"
        f"📞 {customer_phone}\n"
        f"📧 {customer_email}\n"
        f"💵 {amount}€\n\n"
        f"✅ {action}\n"
        f"📊 Neues Guthaben: {neues_guthaben}€\n"
        f"👤 Partner: {partner_name}"
    )
    
    logger.info("📤 Sende Stripe-Admin-Info an Matze...")
    matze_result = send_whatsapp(MATZE_PHONE, matze_msg)
    
    if "error" in matze_result:
        logger.error(f"❌ Matze-Benachrichtigung fehlgeschlagen: {matze_result}")
    else:
        logger.info(f"✅ Matze-Benachrichtigung gesendet!")
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    
    logger.info(f"Stripe fertig: {action}")


# ─── Sheet Polling ───────────────────────────
def poll_new_leads():
    acquired = poll_lock.acquire(blocking=False)
    if not acquired:
        return {"processed": 0, "message": "Bereits aktiv"}

    try:
        return _do_poll()
    finally:
        poll_lock.release()


def _do_poll():
    logger.info("=== Polling gestartet ===")
    
    try:
        leads_sheet = get_leads_sheet()
        partner_sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return {"error": str(e)}

    all_values = leads_sheet.get_all_values()
    if len(all_values) <= 1:
        return {"processed": 0}

    new_leads = []
    for row_idx, row in enumerate(all_values[1:], start=2):
        lead_status = row[15] if len(row) > 15 else ""
        
        if lead_status == "CREATED":
            try:
                leads_sheet.update_cell(row_idx, 16, "PROCESSING")
            except Exception as e:
                logger.error(f"Fehler beim Status-Update: {e}")
                continue

            col_m = row[12] if len(row) > 12 else ""
            col_n = row[13] if len(row) > 13 else ""
            col_o = row[14] if len(row) > 14 else ""
            
            raw_values = [col_m, col_n, col_o]
            name = "Unbekannt"
            email = ""
            phone_raw = ""
            
            for val in raw_values:
                val_stripped = val.strip()
                if not val_stripped:
                    continue
                if (val_stripped.startswith("p:") or 
                    val_stripped.startswith("+49") or
                    val_stripped.startswith("49") or
                    (val_stripped.startswith("0") and len(val_stripped) > 8)):
                    phone_raw = val_stripped
                elif "@" in val_stripped:
                    email = val_stripped
                else:
                    name = val_stripped
            
            new_leads.append({
                "row": row_idx,
                "name": name,
                "email": email,
                "phone": normalize_phone(phone_raw),
            })

    if not new_leads:
        return {"processed": 0}

    logger.info(f"🔥 {len(new_leads)} neue Leads")

    processed = 0
    for lead in new_leads:
        try:
            result = process_lead(lead)
            if "error" not in result:
                leads_sheet.update_cell(lead["row"], 16, "VERTEILT")
                processed += 1
            else:
                leads_sheet.update_cell(lead["row"], 16, "FEHLER")
        except Exception as e:
            logger.error(f"Fehler bei Lead {lead['name']}: {e}")
            try:
                leads_sheet.update_cell(lead["row"], 16, "FEHLER")
            except:
                pass
        
        time.sleep(2)

    return {"processed": processed, "total": len(new_leads)}


def polling_loop():
    logger.info(f"📡 Polling gestartet ({POLL_INTERVAL}s)")
    while True:
        try:
            poll_new_leads()
        except Exception as e:
            logger.error(f"Polling-Fehler: {e}")
        time.sleep(POLL_INTERVAL)


# ─── API Endpoints ───────────────────────────
@app.on_event("startup")
def startup():
    logger.info("🚀 Lead-Verteilung v4.3 FINAL + STRIPE-FIX gestartet")
    threading.Thread(target=polling_loop, daemon=True).start()


@app.get("/")
def root():
    return {"status": "ok", "version": "4.3-FINAL-STRIPE-FIX", "admin": MATZE_PHONE}


@app.get("/webhook/facebook")
def fb_verify(request: Request):
    params = dict(request.query_params)
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == FB_VERIFY_TOKEN:
        return int(params.get("hub.challenge", 0))
    raise HTTPException(403)


@app.post("/webhook/facebook")
async def fb_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except:
        return {"error": "Invalid JSON"}
    
    # Lead-Daten extrahieren (vereinfacht - du musst deine Logik hier einfügen)
    lead_data = {"name": "Test", "phone": "491234567890", "email": "test@test.de"}
    
    background_tasks.add_task(process_lead, lead_data)
    return {"status": "received"}


@app.post("/stripe-webhook")
async def stripe_webhook(request: Request, background_tasks: BackgroundTasks):
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")

    try:
        if STRIPE_WEBHOOK_SECRET and sig:
            event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        else:
            event = json.loads(payload)
    except Exception as e:
        logger.error(f"Stripe Fehler: {e}")
        raise HTTPException(400, "Invalid")

    if event.get("type") == "checkout.session.completed":
        data = event["data"]["object"]
        amount = data.get("amount_total", 0) / 100
        cd = data.get("customer_details", {})
        
        customer_name = cd.get("name", "")
        customer_email = cd.get("email", "")
        customer_phone = cd.get("phone", "")
        
        if not customer_name:
            customer_name = customer_email.split("@")[0] if customer_email else "Unbekannt"

        background_tasks.add_task(process_stripe_payment, 
                                  customer_name, customer_phone, customer_email, amount)
        return {"status": "received"}

    return {"status": "ignored"}


@app.get("/poll")
def manual_poll():
    result = poll_new_leads()
    return {"status": "ok", "result": result}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
