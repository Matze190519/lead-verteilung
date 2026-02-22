"""
Lead-Verteilungs-Service v4.7 PRODUCTION READY
===============================================
✅ Worksheet: "Tabellenblatt1"
✅ Text-Info statt Ad-Link (funktioniert!)
✅ UTM-Tracking (Spalten C, D, H)
✅ Spalten M=Email, N=Name, O=Phone, P=Status
✅ Stripe-Integration (automatische Guthaben-Aufladung)
✅ Tägliche Erinnerungen (08:00 Uhr) - hält 24h-Fenster offen
✅ 24h-Fenster-Erkennung (Error Code 100)
✅ Aktuelles Guthaben in jeder Nachricht
✅ Automatische Partner-Pausierung bei zu wenig Guthaben
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
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# ─── Konfiguration ───────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("lead-verteilung")

# META API
META_TOKEN = os.getenv("META_TOKEN", "EAARgaZCn3eoYBO0Tr9nSqfmJYOcx3gx3NAzSdwekRpZB5rfmWH2poZAvKSXXVBdR0HDqiXAEbfESzfejzSYLTCkhZAxs0bVZCMufcy51ZBN16zkDlpy8bcaUL5Omu6FTLW37O30I9uO51HSgfZBZBYz6qPEQ49RVEMWNrJmnrvvmrwCgAlJaJB7eHk2GvDdU8pKYkwZDZD")
META_PHONE_ID = os.getenv("META_PHONE_ID", "623007617563961")
META_URL = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"

MATZE_PHONE = "491715060008"
LINA_PHONE = "4915170605019"

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

LEAD_PREIS = 5.0
POLL_INTERVAL = 60
BERLIN_TZ = pytz.timezone('Europe/Berlin')

poll_lock = threading.Lock()

app = FastAPI(title="Lead-Verteilungs-Service", version="4.7-PRODUCTION")

scheduler = BackgroundScheduler(timezone=BERLIN_TZ)

logger.info(f"✅ System v4.7 PRODUCTION gestartet | Admin → {MATZE_PHONE}")


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
        ws = get_spreadsheet().add_worksheet(title="Leads_Log", rows=1000, cols=12)
        ws.append_row([
            "Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email",
            "Partner_Name", "Partner_Telefon", "Guthaben_Nachher",
            "WhatsApp_Status", "Status", "Kampagne", "Anzeige", "Ad_ID"
        ], value_input_option="USER_ENTERED")
        return ws


def log_lead(lead_name, lead_phone, lead_email, partner_name, partner_phone,
             guthaben_nachher, wa_ok, status, campaign="", ad_name="", ad_id=""):
    try:
        log_sheet = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now, lead_name, lead_phone, lead_email, partner_name,
            partner_phone, guthaben_nachher, "OK" if wa_ok else "FEHLER",
            status, campaign, ad_name, ad_id
        ]
        log_sheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error(f"Log-Fehler: {e}")


# ─── Meta WhatsApp (MIT 24H-FENSTER-ERKENNUNG) ───
def send_whatsapp(phone, message):
    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Nummer: {phone}")
        return {"error": "Invalid phone"}

    if not META_TOKEN or not META_PHONE_ID:
        logger.error("META_TOKEN oder META_PHONE_ID fehlt!")
        return {"error": "Not configured"}

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
        "text": {"preview_url": True, "body": message}
    }

    try:
        res = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        logger.info(f"[META] Status={res.status_code} | To={to}")

        if res.status_code >= 400:
            error_text = res.text
            logger.error(f"Meta API Error: {error_text}")

            # 24H-FENSTER-ERKENNUNG
            if '"code":100' in error_text or "outside the 24-hour window" in error_text.lower():
                logger.warning(f"⚠️ 24h-Fenster geschlossen für {phone}")
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ *24h-Fenster geschlossen!*\n\n"
                    f"Partner: {phone}\n\n"
                    f"Bitte Lina ({LINA_PHONE}) kontaktieren,\n"
                    f"damit sie den Partner anschreibt!")
                return {"error": "24h_window_closed", "code": 100}

            return {"error": error_text}

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


# ─── Partner-Logik ───────────────────────────
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
        logger.error(f"Partner-Lese-Fehler: {e}")
    return records


def find_best_partner(sheet):
    try:
        all_records = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Fehler: {e}")
        return None

    aktive_partner = [
        p for p in all_records
        if p.get("status", "").strip() == "Aktiv" and p.get("guthaben", 0) >= LEAD_PREIS
    ]

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
            send_whatsapp(MATZE_PHONE,
                f"⚠️ Partner {partner['name']} automatisch pausiert\n"
                f"💰 Guthaben: {neues_guthaben}€ (unter {LEAD_PREIS}€)")

        return neues_guthaben
    except Exception as e:
        logger.error(f"Update-Fehler: {e}")
        return partner["guthaben"]


def find_partner_by_phone(sheet, phone):
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    records = get_all_partner_records(sheet)
    for record in records:
        if normalize_phone(str(record.get("telefon", ""))) == normalized:
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
        logger.info(f"✅ Neuer Partner: {name}, {guthaben}€")
        return True
    except Exception as e:
        logger.error(f"Partner-Add-Fehler: {e}")
        return False


def update_partner_guthaben(sheet, partner, betrag):
    row = partner["row"]
    try:
        neues_guthaben = round(partner["guthaben"] + betrag, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        sheet.update_cell(row, 6, "Aktiv")
        return neues_guthaben
    except Exception as e:
        logger.error(f"Guthaben-Update-Fehler: {e}")
        return partner["guthaben"]


# ─── Lead-Verteilung (✅ TEXT-INFO STATT LINK!) ───
def process_lead(lead_data):
    lead_name = lead_data.get("name", "Unbekannt")
    lead_phone = normalize_phone(lead_data.get("phone", ""))
    lead_email = lead_data.get("email", "")

    # UTM-Tracking
    ad_id = lead_data.get("ad_id", "")
    ad_name = lead_data.get("ad_name", "")
    campaign_name = lead_data.get("campaign_name", "")

    logger.info(f"=== LEAD: {lead_name} | {lead_phone} | Kampagne: {campaign_name} ===")

    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return {"error": str(e)}

    partner = find_best_partner(sheet)
    if not partner:
        send_whatsapp(MATZE_PHONE,
            f"⚠️ *Lead ohne Partner!*\n\n"
            f"👤 {lead_name}\n📞 {lead_phone}\n📧 {lead_email}\n\n"
            f"Kein aktiver Partner mit Guthaben verfügbar!")
        log_lead(lead_name, lead_phone, lead_email, "KEIN PARTNER", "", 0,
                False, "KEIN_PARTNER", campaign_name, ad_name, ad_id)
        return {"error": "Kein Partner"}

    neues_guthaben = update_partner(sheet, partner)

    # ✅ PARTNER-NACHRICHT (TEXT-INFO STATT LINK!)
    partner_msg = f"🔔 *Neuer Lead!*\n\n👤 {lead_name}\n📞 {lead_phone}\n📧 {lead_email}\n"

    if campaign_name:
        partner_msg += f"\n📊 Kampagne: {campaign_name}"
    if ad_name:
        partner_msg += f"\n🎯 Anzeige: {ad_name}"

    # Lead-Quelle beschreiben (OHNE Link!)
    partner_msg += f"\n\n💬 *Lead kam über:*\n"

    if ad_name:
        ad_name_lower = ad_name.lower()
        
        if "porsche" in ad_name_lower or "reel" in ad_name_lower:
            partner_msg += "➡️ Reel über LR & Porsche\n   (Lifestyle, Autos, Erfolg)"
        elif "wage" in ad_name_lower or "clean" in ad_name_lower or "geld" in ad_name_lower:
            partner_msg += "➡️ Online Geld verdienen\n   (Nebeneinkommen, Flexibilität)"
        else:
            partner_msg += f"➡️ {ad_name}"
    else:
        partner_msg += f"➡️ {campaign_name}" if campaign_name else "➡️ Werbeanzeige"

    partner_msg += f"\n\n💰 *Dein aktuelles Guthaben: {neues_guthaben}€*"

    wa_result = send_whatsapp(partner["telefon"], partner_msg)
    time.sleep(2)

    # ADMIN-NACHRICHT
    matze_msg = f"✅ *Lead verteilt*\n\n👤 {lead_name}\n📞 {lead_phone}\n📧 {lead_email}\n"
    if campaign_name:
        matze_msg += f"\n📊 {campaign_name}"
    if ad_name:
        matze_msg += f"\n🎯 {ad_name}"
    matze_msg += f"\n\n➡️ Partner: {partner['name']}\n💰 Guthaben danach: {neues_guthaben}€"

    send_whatsapp(MATZE_PHONE, matze_msg)

    log_lead(lead_name, lead_phone, lead_email, partner["name"],
            partner["telefon"], neues_guthaben, "error" not in wa_result,
            "VERTEILT", campaign_name, ad_name, ad_id)

    return {"success": True, "partner": partner["name"], "guthaben": neues_guthaben}


# ─── Stripe-Zahlung ──────────────────────────
def process_stripe_payment(customer_name, customer_phone, customer_email, amount):
    logger.info(f"=== STRIPE: {customer_name} | {amount}€ ===")

    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return

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
        add_new_partner(sheet, customer_name, customer_phone, amount)
        neues_guthaben = amount
        action = "NEUER PARTNER ANGELEGT"
        partner_name = customer_name

    if customer_phone and normalize_phone(customer_phone):
        partner_msg = (
            f"✅ *Zahlung erhalten!*\n\n"
            f"💰 +{amount}€ aufgeladen\n"
            f"📊 *Neues Guthaben: {neues_guthaben}€*\n\n"
            f"Du bist jetzt aktiv und erhältst Leads! 🚀"
        )
        send_whatsapp(normalize_phone(customer_phone), partner_msg)

    time.sleep(2)

    matze_msg = (
        f"💰 *Stripe-Zahlung eingegangen!*\n\n"
        f"👤 {customer_name}\n"
        f"📞 {customer_phone}\n"
        f"📧 {customer_email}\n"
        f"💵 +{amount}€\n\n"
        f"✅ {action}\n"
        f"📊 Guthaben jetzt: {neues_guthaben}€\n"
        f"👤 Partner: {partner_name}"
    )
    send_whatsapp(MATZE_PHONE, matze_msg)


# ─── Tägliche Erinnerungen (08:00 UHR) ───────
def send_daily_reminders():
    logger.info("=== TÄGLICHE ERINNERUNGEN (08:00 Uhr) ===")

    try:
        sheet = get_partner_sheet()
        all_partners = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Reminder-Fehler: {e}")
        return

    aktive_partner = [p for p in all_partners if p.get("status", "").strip() == "Aktiv"]

    if not aktive_partner:
        logger.info("Keine aktiven Partner für Reminder")
        return

    logger.info(f"📤 Sende Reminder an {len(aktive_partner)} Partner")

    erfolg = 0
    fehler = 0

    for partner in aktive_partner:
        try:
            reminder_msg = (
                f"🌅 *Guten Morgen, {partner['name']}!*\n\n"
                f"📊 *Dein aktuelles Guthaben: {partner['guthaben']}€*\n"
                f"🎯 Leads geliefert: {partner['leads_geliefert']}\n\n"
                f"💬 Antworte kurz (z.B. 'OK' oder 👍), damit du heute Leads bekommst!\n\n"
                f"Viel Erfolg heute! 🚀"
            )

            result = send_whatsapp(partner["telefon"], reminder_msg)

            if "error" in result:
                logger.warning(f"Reminder-Fehler für {partner['name']}: {result}")
                fehler += 1
            else:
                logger.info(f"✅ Reminder gesendet an {partner['name']}")
                erfolg += 1

            time.sleep(3)

        except Exception as e:
            logger.error(f"Reminder-Exception für {partner['name']}: {e}")
            fehler += 1

    send_whatsapp(MATZE_PHONE,
        f"✅ *Tägliche Reminder versendet*\n\n"
        f"📤 {erfolg} erfolgreich\n"
        f"❌ {fehler} Fehler\n"
        f"📊 Gesamt: {len(aktive_partner)} Partner")


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
    logger.info("=== POLLING ===")

    try:
        leads_sheet = get_leads_sheet()
    except Exception as e:
        logger.error(f"❌ Sheet-Fehler: {e}")
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
                logger.error(f"Status-Update-Fehler: {e}")
                continue

            # UTM-Daten
            ad_id = row[2] if len(row) > 2 else ""
            ad_name = row[3] if len(row) > 3 else ""
            campaign_name = row[7] if len(row) > 7 else ""

            # Lead-Daten (M, N, O)
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
                "ad_id": ad_id,
                "ad_name": ad_name,
                "campaign_name": campaign_name,
            })

    if not new_leads:
        return {"processed": 0}

    logger.info(f"🔥 {len(new_leads)} neue Leads gefunden")

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
            logger.error(f"Lead-Verarbeitung fehlgeschlagen: {e}")
            try:
                leads_sheet.update_cell(lead["row"], 16, "FEHLER")
            except:
                pass

        time.sleep(2)

    return {"processed": processed, "total": len(new_leads)}


def polling_loop():
    logger.info(f"📡 Polling gestartet (alle {POLL_INTERVAL}s)")
    while True:
        try:
            poll_new_leads()
        except Exception as e:
            logger.error(f"Polling-Loop-Fehler: {e}")
        time.sleep(POLL_INTERVAL)


# ─── API Endpoints ───────────────────────────
@app.on_event("startup")
def startup():
    logger.info("🚀 Lead-Verteilung v4.7 PRODUCTION READY gestartet")

    scheduler.add_job(
        send_daily_reminders,
        trigger=CronTrigger(hour=8, minute=0, timezone=BERLIN_TZ),
        id="daily_reminder",
        name="Tägliche Partner-Erinnerungen",
        replace_existing=True
    )
    scheduler.start()
    logger.info("✅ Scheduler gestartet (tägliche Erinnerungen um 08:00 Uhr)")

    threading.Thread(target=polling_loop, daemon=True).start()
    logger.info("✅ Lead-Polling gestartet")

    send_whatsapp(MATZE_PHONE,
        f"🚀 *System gestartet!*\n\n"
        f"Lead-Verteilungs-Service v4.7\n\n"
        f"✅ Lead-Polling aktiv\n"
        f"✅ WhatsApp-Integration aktiv\n"
        f"✅ Stripe-Webhook aktiv\n"
        f"✅ UTM-Tracking (Text-Info)\n"
        f"✅ Tägliche Erinnerungen (08:00 Uhr)\n"
        f"✅ 24h-Fenster-Erkennung aktiv\n"
        f"✅ Aktuelles Guthaben in jeder Nachricht\n"
        f"💰 Lead-Preis: {LEAD_PREIS}€\n\n"
        f"Alle Systeme bereit! 🎯")


@app.get("/")
def root():
    return {
        "status": "ok",
        "version": "4.7-PRODUCTION",
        "admin": MATZE_PHONE,
        "features": [
            "Automatische Lead-Verteilung",
            "WhatsApp-Benachrichtigungen",
            "Stripe-Integration",
            "Tägliche Erinnerungen (08:00 Uhr)",
            "24h-Fenster-Erkennung",
            "UTM-Tracking (Text-Info)",
            "Aktuelles Guthaben in jeder Nachricht",
            "Automatische Partner-Pausierung"
        ]
    }


@app.get("/health")
def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


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
        logger.error(f"Stripe-Fehler: {e}")
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

        background_tasks.add_task(
            process_stripe_payment,
            customer_name, customer_phone, customer_email, amount
        )
        return {"status": "received"}

    return {"status": "ignored"}


@app.get("/poll")
def manual_poll():
    result = poll_new_leads()
    return {"status": "ok", "result": result}


@app.get("/test-reminder")
def test_reminder():
    send_daily_reminders()
    return {"status": "reminder_sent"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
