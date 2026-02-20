"""
Lead-Verteilungs-Service v4.0 EMERGENCY FIX
============================================
Fix: Polling-Loop korrigiert, Spalten-Erkennung stabil
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

# Meta API
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"

# Stripe
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# Config
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
MATZE_PHONE = os.getenv("MATZE_PHONE", "+491715060008")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

poll_lock = threading.Lock()
app = FastAPI(title="Lead-Verteilung v4.0", version="4.0.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "v4.0 running"}

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
        logger.error(f"WhatsApp fehlgeschlagen: phone={phone}, token={'ja' if META_TOKEN else 'nein'}")
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
        if r.status_code != 200:
            logger.error(f"Meta API Error: {r.status_code} - {r.text}")
        return r.status_code == 200
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
            status = str(r.get("Status", "")).strip()
            guthaben_str = str(r.get("Guthaben_Euro", "0")).replace(",", ".")
            guthaben = float(guthaben_str)
            
            if status == "Aktiv" and guthaben >= LEAD_PREIS:
                candidates.append({
                    "row": i + 2,
                    "name": r.get("Name"),
                    "phone": normalize_phone(r.get("Telefon")),
                    "guthaben": guthaben,
                    "leads": int(float(str(r.get("Leads_Geliefert", "0")).replace(",", "."))),
                    "last": str(r.get("Letzter_Lead_Am", ""))
                })
        except Exception as e:
            logger.error(f"Fehler bei Partner {i}: {e}")
            continue
    
    if not candidates:
        logger.warning("Keine aktiven Partner mit Guthaben gefunden!")
        return None
    
    candidates.sort(key=lambda x: (x["last"] or "0000-00-00", x["leads"]))
    logger.info(f"Partner gefunden: {candidates[0]['name']}")
    return candidates[0]

def update_partner(sheet, p):
    try:
        new_bal = round(p["guthaben"] - LEAD_PREIS, 2)
        new_count = p["leads"] + 1
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        
        sheet.update_cell(p["row"], 3, new_bal)
        sheet.update_cell(p["row"], 4, new_count)
        sheet.update_cell(p["row"], 5, now)
        
        if new_bal < LEAD_PREIS:
            sheet.update_cell(p["row"], 6, "Pausiert")
            send_whatsapp(MATZE_PHONE, f"⚠️ Partner {p['name']} pausiert (Guthaben leer)")
        
        logger.info(f"Partner {p['name']} aktualisiert: Guthaben={new_bal}, Leads={new_count}")
        return new_bal
    except Exception as e:
        logger.error(f"Fehler beim Partner-Update: {e}")
        raise

# === DER WICHTIGE FIX: POLLING-LOOP ===
def poll_loop():
    logger.info("Polling-Loop gestartet")
    while True:
        time.sleep(POLL_INTERVAL)
        
        if not poll_lock.acquire(blocking=False):
            logger.debug("Polling übersprungen (noch aktiv)")
            continue
        
        try:
            ls = get_leads_sheet()
            all_rows = ls.get_all_values()
            
            if len(all_rows) <= 1:
                logger.debug("Keine Daten im Sheet")
                continue
            
            logger.info(f"Prüfe {len(all_rows)-1} Leads...")
            
            for i, row in enumerate(all_rows[1:], start=2):
                try:
                    # Spalte P (Index 15) prüfen
                    if len(row) <= 15:
                        continue
                    
                    status = str(row[15]).strip() if row[15] else ""
                    
                    if status == "CREATED":
                        logger.info(f"Neuer Lead gefunden in Zeile {i}")
                        
                        # Status sofort auf PROCESSING setzen (wichtig!)
                        ls.update_cell(i, 16, "PROCESSING")
                        logger.info(f"Zeile {i} auf PROCESSING gesetzt")
                        
                        # Daten extrahieren (Spalten M, N, O = Index 12, 13, 14)
                        raw1 = row[12] if len(row) > 12 else ""
                        raw2 = row[13] if len(row) > 13 else ""
                        raw3 = row[14] if len(row) > 14 else ""
                        
                        logger.info(f"Rohdaten: {raw1}, {raw2}, {raw3}")
                        
                        # Intelligente Zuordnung
                        name, email, phone = "Unbekannt", "", ""
                        
                        for raw in [raw1, raw2, raw3]:
                            if not raw:
                                continue
                            raw = str(raw).strip()
                            if "@" in raw:
                                email = raw
                            elif any(c.isdigit() for c in raw):
                                phone = normalize_phone(raw)
                            elif raw:
                                name = raw
                        
                        logger.info(f"Extrahiert: Name={name}, Email={email}, Tel={phone}")
                        
                        # Partner finden und verteilen
                        sheet = get_sheet()
                        partner = find_partner(sheet)
                        
                        if partner:
                            new_bal = update_partner(sheet, partner)
                            
                            # WhatsApp an Partner
                            msg = f"🎯 Neuer Lead!\n👤 {name}\n📧 {email}\n📞 {phone}\n💰 Restguthaben: {new_bal}€"
                            wa_ok = send_whatsapp(partner["phone"], msg)
                            
                            # WhatsApp an Matze
                            send_whatsapp(MATZE_PHONE, f"✅ Lead verteilt: {name} → {partner['name']}")
                            
                            # Status auf VERTEILT
                            ls.update_cell(i, 16, "VERTEILT")
                            logger.info(f"Lead {name} erfolgreich verteilt an {partner['name']}")
                        else:
                            ls.update_cell(i, 16, "KEIN_PARTNER")
                            send_whatsapp(MATZE_PHONE, f"❌ Kein Partner für Lead: {name}")
                            logger.warning(f"Kein Partner für {name}")
                            
                except Exception as e:
                    logger.error(f"Fehler bei Zeile {i}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Polling-Fehler: {e}")
        finally:
            poll_lock.release()

@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        name = data.get("name", "Unbekannt")
        phone = normalize_phone(data.get("phone", ""))
        email = data.get("email", "")
        
        logger.info(f"Webhook erhalten: {name}, {phone}, {email}")
        
        sheet = get_sheet()
        partner = find_partner(sheet)
        
        if not partner:
            send_whatsapp(MATZE_PHONE, f"❌ Kein Partner für: {name}")
            return {"status": "no_partner"}
        
        new_bal = update_partner(sheet, partner)
        
        msg = f"🎯 Neuer Lead!\n👤 {name}\n📧 {email}\n📞 {phone}\n💰 Rest: {new_bal}€"
        send_whatsapp(partner["phone"], msg)
        send_whatsapp(MATZE_PHONE, f"✅ {name} → {partner['name']}")
        
        return {"status": "ok", "partner": partner["name"]}
    except Exception as e:
        logger.error(f"Webhook-Fehler: {e}")
        return {"status": "error", "message": str(e)}

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
            name = cd.get("name", "Unbekannt")
            phone = normalize_phone(cd.get("phone", ""))
            
            logger.info(f"Stripe-Zahlung: {name}, {amt}€")
            
            sheet = get_sheet()
            records = get_all_records(sheet)
            found = False
            
            for i, r in enumerate(records):
                try:
                    if email and r.get("Name") == name:
                        guthaben = float(str(r.get("Guthaben_Euro", "0")).replace(",", "."))
                        new_g = round(guthaben + amt, 2)
                        
                        sheet.update_cell(i + 2, 3, new_g)
                        sheet.update_cell(i + 2, 6, "Aktiv")
                        
                        send_whatsapp(MATZE_PHONE, f"💰 {name} +{amt}€ = {n
...(truncated)...
