from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
import requests
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import time
from datetime import datetime
import pytz
import threading
import stripe
from apscheduler.schedulers.background import BackgroundScheduler

app = FastAPI()

# ─── KONFIGURATION ────────────────────────────────────────────────
META_TOKEN            = os.environ.get("META_TOKEN", "")
META_PHONE_ID         = os.environ.get("META_PHONE_ID", "")
MATZE_PHONE           = os.environ.get("MATZE_PHONE", "")
GOOGLE_SHEET_ID       = os.environ.get("GOOGLE_SHEET_ID", "")
STRIPE_SECRET_KEY     = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
APP_URL               = os.environ.get("APP_URL", "https://lead-verteilung.onrender.com")
LINA_WA_NUMBER        = os.environ.get("LINA_WA_NUMBER", "4915735989735")

LEAD_PREIS            = 5.0
POLL_INTERVAL         = 60
STRIPE_PAYMENT_LINK   = "https://buy.stripe.com/aFa6oH64Wei20Robnte7m01"
LINA_ONBOARDING_LINK  = "https://onboarding.pro"

BERLIN_TZ = pytz.timezone("Europe/Berlin")
stripe.api_key = STRIPE_SECRET_KEY

LEADS_SHEET_NAME   = "Tabellenblatt1"
PARTNER_SHEET_NAME = "Partner_Konto"
LOG_SHEET_NAME     = "Leads_Log"

# Partner_Konto Spalten (0-indexed)
# A=Name B=Telefon C=Guthaben_Euro D=Leads_Geliefert E=Letzter_Lead_Am F=Status G=Zeitfenster H=Email
COL_NAME            = 0
COL_TELEFON         = 1
COL_GUTHABEN        = 2
COL_LEADS_GELIEFERT = 3
COL_LETZTER_LEAD    = 4
COL_STATUS          = 5
COL_ZEITFENSTER     = 6
COL_EMAIL           = 7

# Tabellenblatt1 Spalten (0-indexed)
COL_LEAD_EMAIL  = 12
COL_LEAD_NAME   = 13
COL_LEAD_PHONE  = 14
COL_LEAD_STATUS = 15

# Leads_Log Spalten (0-indexed)
COL_LOG_ZEITSTEMPEL     = 0
COL_LOG_LEAD_NAME       = 1
COL_LOG_LEAD_TELEFON    = 2
COL_LOG_LEAD_EMAIL      = 3
COL_LOG_PARTNER_NAME    = 4
COL_LOG_PARTNER_TELEFON = 5
COL_LOG_GUTHABEN_NACH   = 6
COL_LOG_WA_PARTNER      = 7
COL_LOG_WA_LEAD         = 8
COL_LOG_STATUS          = 9

# ─── GOOGLE SHEETS ────────────────────────────────────────────────
def get_sheets():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON ist nicht gesetzt!")
    try:
        creds_dict = json.loads(creds_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_CREDENTIALS_JSON ist kein gueltiges JSON: {e}")
    scopes     = ["https://www.googleapis.com/auth/spreadsheets"]
    creds      = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client     = gspread.authorize(creds)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    leads_ws   = spreadsheet.worksheet(LEADS_SHEET_NAME)
    partner_ws = spreadsheet.worksheet(PARTNER_SHEET_NAME)
    log_ws     = spreadsheet.worksheet(LOG_SHEET_NAME)
    return leads_ws, partner_ws, log_ws

# ─── HILFSFUNKTIONEN ──────────────────────────────────────────────
def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    p = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if p.startswith("+"):
        return p[1:]
    if p.startswith("00"):
        return p[2:]
    if p.startswith("0"):
        return "49" + p[1:]
    return p

def is_within_zeitfenster(zeitfenster: str) -> bool:
    now = datetime.now(BERLIN_TZ)
    h   = now.hour
    zf  = zeitfenster.strip() if zeitfenster else ""
    if zf in ("Ganztag", "ganztag", ""):
        return True
    if zf == "Vormittag":
        return 8 <= h < 12
    if zf == "Nachmittag":
        return 12 <= h < 17
    if zf == "Abend":
        return 17 <= h < 22
    return True

def get_all_partner_records(partner_ws):
    return partner_ws.get_all_records()

def find_partner_by_phone(records, phone_norm):
    for i, r in enumerate(records):
        if normalize_phone(str(r.get("Telefon", ""))) == phone_norm:
            return i, r
    return None, None

def find_partner_by_email(records, email):
    email_lower = email.strip().lower()
    for i, r in enumerate(records):
        partner_email = str(r.get("Email", "")).strip().lower()
        if partner_email and partner_email == email_lower:
            return i, r
    return None, None

def validate_sheet_headers(partner_ws):
    try:
        headers = partner_ws.row_values(1)
        if len(headers) < 8 or headers[7].strip() != "Email":
            send_whatsapp(MATZE_PHONE,
                f"🚨 SHEET FEHLER!\n"
                f"Spalte H = '{headers[7] if len(headers) > 7 else 'LEER'}'\n"
                f"Erwartet: 'Email'\n\n"
                f"Jetzt korrigieren:\n"
                f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}",
                _skip_admin=True)
            return False
        return True
    except Exception as e:
        print(f"[HEADER-CHECK-ERROR] {e}")
        return False

# ─── WHATSAPP ─────────────────────────────────────────────────────
def send_whatsapp(to: str, message: str, _skip_admin: bool = False) -> bool:
    url     = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": message}
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        if r.status_code == 200:
            return True
        err_data = {}
        try:
            err_data = r.json()
        except Exception:
            pass
        err_code = err_data.get("error", {}).get("code", 0)
        if err_code == 131047 and not _skip_admin:
            send_whatsapp(MATZE_PHONE,
                f"⚠️ 24h-Fenster geschlossen!\n"
                f"Empfaenger: {to}\n\n"
                f"Partner muss zuerst Lina schreiben:\n"
                f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                f"Danach erneut versuchen.",
                _skip_admin=True)
        return False
    except Exception as e:
        print(f"[WA-ERROR] to={to} err={e}")
        return False

# ─── LEAD VERTEILEN ───────────────────────────────────────────────
def process_lead(lead_row, lead_row_index, partner_record,
                 partner_row_index, partner_ws, log_ws, leads_ws):
    lead_email    = lead_row[COL_LEAD_EMAIL]  if len(lead_row) > COL_LEAD_EMAIL  else ""
    lead_name     = lead_row[COL_LEAD_NAME]   if len(lead_row) > COL_LEAD_NAME   else ""
    lead_phone    = normalize_phone(str(lead_row[COL_LEAD_PHONE])) if len(lead_row) > COL_LEAD_PHONE else ""
    partner_name  = partner_record.get("Name", "")
    partner_phone = normalize_phone(str(partner_record.get("Telefon", "")))

    guthaben = float(str(partner_record.get("Guthaben_Euro", "0")).replace(",", "."))
    if guthaben < LEAD_PREIS:
        send_whatsapp(MATZE_PHONE,
            f"⚠️ Guthaben reicht nicht!\n"
            f"Lead: {lead_name}\n"
            f"Kein aktiver Partner mit ausreichend Guthaben.",
            _skip_admin=True)
        return

    ad_name   = lead_row[3] if len(lead_row) > 3 else ""
    camp_name = lead_row[7] if len(lead_row) > 7 else ""
    if ad_name or camp_name:
        source_text = f"📢 Quelle: {ad_name or camp_name}"
    else:
        source_text = "📢 Quelle: Organisch"

    new_guthaben = guthaben - LEAD_PREIS

    # ── WhatsApp an Lead ──
    wa_lead_sent = send_whatsapp(lead_phone,
        f"Hallo {lead_name} 👋\n\n"
        f"Vielen Dank fuer dein Interesse!\n"
        f"Jemand aus unserem Team wird sich in Kuerze bei dir melden.\n\n"
        f"Bis gleich! 🙂")

    # ── WhatsApp an Partner ──
    partner_msg = (
        f"🎯 Neuer Lead fuer dich!\n\n"
        f"👤 Name: {lead_name}\n"
        f"📧 E-Mail: {lead_email}\n"
        f"📱 Telefon: +{lead_phone}\n"
        f"{source_text}\n\n"
        f"💰 Guthaben nach Abzug: {new_guthaben:.2f} €\n\n"
        f"⚠️ Bitte melde dich zeitnah beim Lead!"
    )
    if new_guthaben < 15:
        partner_msg += (
            f"\n\n💳 Guthaben wird knapp!\n"
            f"👉 Jetzt aufladen bei Lina: https://wa.me/{LINA_WA_NUMBER}\n"
            f"Oder direkt: {STRIPE_PAYMENT_LINK}"
        )
    wa_partner_sent = send_whatsapp(partner_phone, partner_msg)

    # ── Sheet updaten ──
    leads_delivered = int(partner_record.get("Leads_Geliefert", 0)) + 1
    now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
    partner_ws.update_cell(partner_row_index + 2, COL_GUTHABEN        + 1, new_guthaben)
    partner_ws.update_cell(partner_row_index + 2, COL_LEADS_GELIEFERT + 1, leads_delivered)
    partner_ws.update_cell(partner_row_index + 2, COL_LETZTER_LEAD    + 1, now_str)
    leads_ws.update_cell(lead_row_index + 2, COL_LEAD_STATUS + 1, "VERTEILT")

    # ── Admin Benachrichtigung (detailliert) ──
    send_whatsapp(MATZE_PHONE,
        f"✅ Lead verteilt!\n\n"
        f"👤 Lead: {lead_name}\n"
        f"📧 E-Mail: {lead_email}\n"
        f"📱 Tel: +{lead_phone}\n"
        f"{source_text}\n\n"
        f"🤝 Partner: {partner_name}\n"
        f"💰 Guthaben jetzt: {new_guthaben:.2f} €\n"
        f"📊 Leads gesamt: {leads_delivered}\n\n"
        f"📲 WA Lead: {'✅' if wa_lead_sent else '❌'}\n"
        f"📲 WA Partner: {'✅' if wa_partner_sent else '❌'}",
        _skip_admin=True)

    # ── Log ──
    log_ws.append_row([
        now_str, lead_name, lead_phone, lead_email,
        partner_name, partner_phone, new_guthaben,
        "✅" if wa_lead_sent else "❌",
        "✅" if wa_partner_sent else "❌",
        "VERTEILT"
    ])

# ─── POLLING ──────────────────────────────────────────────────────
def poll_leads():
    while True:
        try:
            leads_ws, partner_ws, log_ws = get_sheets()
            all_leads    = leads_ws.get_all_values()
            all_partners = get_all_partner_records(partner_ws)

            active_partners = [
                (i, r) for i, r in enumerate(all_partners)
                if str(r.get("Status", "")).strip().lower() == "aktiv"
                and float(str(r.get("Guthaben_Euro", "0")).replace(",", ".")) >= LEAD_PREIS
                and is_within_zeitfenster(str(r.get("Zeitfenster", "")))
            ]

            if not active_partners:
                time.sleep(POLL_INTERVAL)
                continue

            partner_idx = 0
            for row_i, lead_row in enumerate(all_leads[1:], start=1):
                if len(lead_row) <= COL_LEAD_STATUS:
                    continue
                if str(lead_row[COL_LEAD_STATUS]).strip().upper() in ("VERTEILT", "ARCHIV", "DUPLIKAT"):
                    continue
                if partner_idx >= len(active_partners):
                    break
                p_row_i, p_record = active_partners[partner_idx]
                process_lead(lead_row, row_i, p_record, p_row_i, partner_ws, log_ws, leads_ws)
                partner_idx += 1

        except Exception as e:
            print(f"[POLL-ERROR] {e}")
            send_whatsapp(MATZE_PHONE, f"🚨 Poll-Fehler:\n{e}", _skip_admin=True)
        time.sleep(POLL_INTERVAL)

# ─── DAILY REMINDER 08:00 ─────────────────────────────────────────
def send_daily_reminders():
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)
        sent_list, failed_list = [], []

        for r in records:
            if str(r.get("Status", "")).strip().lower() != "aktiv":
                continue
            phone       = normalize_phone(str(r.get("Telefon", "")))
            name        = r.get("Name", "Unbekannt")
            guthaben    = float(str(r.get("Guthaben_Euro", "0")).replace(",", "."))
            zeitfenster = str(r.get("Zeitfenster", "Ganztag")).strip() or "Ganztag"

            msg = (
                f"🚨🚨🚨 GUTEN MORGEN {name.upper()}! 🚨🚨🚨\n\n"
                f"Dein Lead-System ist AKTIV und bereit! 💪\n\n"
                f"💰 Guthaben: {guthaben:.2f} €\n"
                f"⏰ Zeitfenster: {zeitfenster}\n"
            )
            if guthaben < 15:
                msg += (
                    f"\n⚠️ ACHTUNG: Guthaben wird knapp!\n"
                    f"👉 Jetzt aufladen bei Lina: https://wa.me/{LINA_WA_NUMBER}\n"
                    f"Oder direkt: {STRIPE_PAYMENT_LINK}\n"
                )
            elif guthaben < 30:
                msg += (
                    f"\n💡 Tipp: Bald aufladen damit keine Leads verloren gehen.\n"
                    f"👉 Lina: https://wa.me/{LINA_WA_NUMBER}\n"
                )
            else:
                msg += f"\n✅ Guthaben ausreichend!\n"

            msg += (
                f"\n⏰ Zeitfenster aendern:\n"
                f"Ganztag: {APP_URL}/zeitfenster?phone={phone}&wahl=Ganztag\n"
                f"Vormittag: {APP_URL}/zeitfenster?phone={phone}&wahl=Vormittag\n"
                f"Nachmittag: {APP_URL}/zeitfenster?phone={phone}&wahl=Nachmittag\n"
                f"Abend: {APP_URL}/zeitfenster?phone={phone}&wahl=Abend\n\n"
                f"Viel Erfolg heute! 🚀"
            )

            if send_whatsapp(phone, msg):
                sent_list.append(f"✅ {name} ({guthaben:.2f}€, {zeitfenster})")
            else:
                failed_list.append(f"❌ {name} ({phone})")

        # ── Admin Summary ──
        now_str = datetime.now(BERLIN_TZ).strftime("%d.%m.%Y %H:%M")
        summary = f"📊 TAGES-REPORT {now_str}\n\n"

        if sent_list:
            summary += f"✅ Gesendet ({len(sent_list)}):\n" + "\n".join(sent_list) + "\n\n"
        if failed_list:
            summary += (
                f"❌ Fehlgeschlagen ({len(failed_list)}):\n"
                + "\n".join(failed_list)
                + f"\n\n👉 Diese Partner muessen Lina schreiben:\nhttps://wa.me/{LINA_WA_NUMBER}\n\n"
            )
        if not sent_list and not failed_list:
            summary += "ℹ️ Keine aktiven Partner gefunden.\n"

        summary += f"📈 Gesamt aktiv: {len(sent_list) + len(failed_list)} Partner"
        send_whatsapp(MATZE_PHONE, summary, _skip_admin=True)

    except Exception as e:
        print(f"[REMINDER-ERROR] {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 Reminder-Fehler:\n{e}", _skip_admin=True)

# ─── STRIPE PAYMENT ───────────────────────────────────────────────
def process_stripe_payment(session):
    try:
        leads_ws, partner_ws, log_ws = get_sheets()
        customer_email = (session.get("customer_details") or {}).get("email", "").strip()
        amount_total   = (session.get("amount_total") or 0) / 100

        if not customer_email:
            send_whatsapp(MATZE_PHONE,
                f"⚠️ Stripe-Zahlung ohne E-Mail!\n"
                f"💰 Betrag: {amount_total:.2f} €\n"
                f"Bitte manuell nachbearbeiten.",
                _skip_admin=True)
            return

        records = get_all_partner_records(partner_ws)
        p_idx, p_record = find_partner_by_email(records, customer_email)

        if p_record is None:
            # ── NEUER PARTNER ──
            raw_phone     = (session.get("customer_details") or {}).get("phone", "")
            partner_phone = normalize_phone(str(raw_phone)) if raw_phone else ""
            partner_name  = (session.get("customer_details") or {}).get("name", "") or customer_email
            partner_ws.append_row([
                partner_name, partner_phone, amount_total, 0, "",
                "aktiv", "Ganztag", customer_email
            ])
            records = get_all_partner_records(partner_ws)
            p_idx, p_record = find_partner_by_email(records, customer_email)

            if partner_phone:
                welcome_msg = (
                    f"✅ Willkommen im Lead-System, {partner_name}! 🎉\n\n"
                    f"💰 {amount_total:.2f} € Guthaben eingebucht.\n\n"
                    f"⚠️ WICHTIG - 2 Schritte:\n\n"
                    f"1️⃣ Schreib Lina kurz 'Hallo' damit dein System startet:\n"
                    f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"2️⃣ Waehle dein Zeitfenster fuer Leads:\n"
                    f"Ganztag (08-22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"Vormittag (08-12h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"Nachmittag (12-17h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"Abend (17-22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend\n\n"
                    f"Klick einfach auf den Link deiner Wahl! 👆"
                )
                wa_sent = send_whatsapp(partner_phone, welcome_msg)
                if not wa_sent:
                    send_whatsapp(MATZE_PHONE,
                        f"⚠️ WA an neuen Partner fehlgeschlagen!\n"
                        f"👤 {partner_name}\n"
                        f"📱 {partner_phone}\n"
                        f"📧 {customer_email}\n"
                        f"💰 +{amount_total:.2f} €\n\n"
                        f"Bitte manuell kontaktieren und Zeitfenster-Links schicken:\n"
                        f"Ganztag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                        f"Vormittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                        f"Nachmittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                        f"Abend: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend",
                        _skip_admin=True)
            else:
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ Neuer Partner OHNE Telefonnummer!\n"
                    f"👤 {partner_name}\n"
                    f"📧 {customer_email}\n"
                    f"💰 +{amount_total:.2f} € eingebucht.\n\n"
                    f"➡️ Bitte Telefonnummer manuell im Sheet eintragen!",
                    _skip_admin=True)

            # ── Admin: Neuer Partner ──
            send_whatsapp(MATZE_PHONE,
                f"🆕 NEUER PARTNER!\n\n"
                f"👤 {partner_name}\n"
                f"📧 {customer_email}\n"
                f"📱 {partner_phone or 'Keine Nummer!'}\n"
                f"💰 +{amount_total:.2f} € Startguthaben\n"
                f"⏰ Zeitfenster: Ganztag\n"
                f"📲 WA gesendet: {'✅' if (partner_phone and wa_sent) else '❌'}",
                _skip_admin=True)

        else:
            # ── BESTEHENDER PARTNER ──
            partner_phone    = normalize_phone(str(p_record.get("Telefon", "")))
            current_guthaben = float(str(p_record.get("Guthaben_Euro", "0")).replace(",", "."))
            new_guthaben     = current_guthaben + amount_total
            partner_name     = p_record.get("Name", customer_email)
            zeitfenster      = str(p_record.get("Zeitfenster", "Ganztag")).strip() or "Ganztag"
            partner_ws.update_cell(p_idx + 2, COL_GUTHABEN + 1, new_guthaben)

            if partner_phone:
                confirm_msg = (
                    f"✅ Zahlung erhalten! 🎉\n\n"
                    f"💰 +{amount_total:.2f} € aufgeladen\n"
                    f"💳 Neues Guthaben: {new_guthaben:.2f} €\n"
                    f"⏰ Dein Zeitfenster: {zeitfenster}\n\n"
                    f"⚠️ Wichtig: Schreib Lina kurz damit dein Zeitfenster offen bleibt:\n"
                    f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"⏰ Zeitfenster aendern:\n"
                    f"Ganztag (08-22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"Vormittag (08-12h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"Nachmittag (12-17h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"Abend (17-22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"
                )
                wa_sent = send_whatsapp(partner_phone, confirm_msg)
                if not wa_sent:
                    send_whatsapp(MATZE_PHONE,
                        f"⚠️ WA an {partner_name} fehlgeschlagen!\n"
                        f"📱 {partner_phone}\n"
                        f"💰 +{amount_total:.2f} € eingebucht\n"
                        f"💳 Neues Guthaben: {new_guthaben:.2f} €\n\n"
                        f"Bitte manuell kontaktieren!",
                        _skip_admin=True)
            else:
                wa_sent = False
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ {partner_name} hat keine Telefonnummer!\n"
                    f"💰 +{amount_total:.2f} € eingebucht\n"
                    f"💳 Neues Guthaben: {new_guthaben:.2f} €",
                    _skip_admin=True)

            # ── Admin: Aufladung ──
            send_whatsapp(MATZE_PHONE,
                f"💳 AUFLADUNG!\n\n"
                f"👤 {partner_name}\n"
                f"📧 {customer_email}\n"
                f"💰 +{amount_total:.2f} €\n"
                f"💳 Neues Guthaben: {new_guthaben:.2f} €\n"
                f"⏰ Zeitfenster: {zeitfenster}\n"
                f"📲 WA gesendet: {'✅' if (partner_phone and wa_sent) else '❌'}",
                _skip_admin=True)

    except Exception as e:
        print(f"[STRIPE-ERROR] {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 Stripe-Fehler:\n{e}", _skip_admin=True)

# ─── ENDPOINTS ────────────────────────────────────────────────────
@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid signature")
    if event["type"] == "checkout.session.completed":
        threading.Thread(
            target=process_stripe_payment,
            args=(event["data"]["object"],),
            daemon=True
        ).start()
    return JSONResponse({"status": "ok"})

@app.get("/zeitfenster")
async def set_zeitfenster(phone: str, wahl: str):
    valid = {"Ganztag", "Vormittag", "Nachmittag", "Abend"}
    if wahl not in valid:
        raise HTTPException(status_code=400, detail="Ungueltige Wahl. Erlaubt: Ganztag, Vormittag, Nachmittag, Abend")
    phone_norm = normalize_phone(phone)
    _, partner_ws, _ = get_sheets()
    records = get_all_partner_records(partner_ws)
    idx, record = find_partner_by_phone(records, phone_norm)
    if record is None:
        raise HTTPException(status_code=404, detail="Partner nicht gefunden")
    partner_ws.update_cell(idx + 2, COL_ZEITFENSTER + 1, wahl)
    partner_name = record.get("Name", phone_norm)

    # WA Bestaetigung an Partner
    send_whatsapp(phone_norm,
        f"✅ Zeitfenster gesetzt: {wahl}\n\n"
        f"Du erhaeltst jetzt Leads in diesem Zeitraum:\n"
        f"• Ganztag: 08-22 Uhr\n"
        f"• Vormittag: 08-12 Uhr\n"
        f"• Nachmittag: 12-17 Uhr\n"
        f"• Abend: 17-22 Uhr\n\n"
        f"Aendern jederzeit moeglich! 🚀",
        _skip_admin=False)

    # Admin Info
    send_whatsapp(MATZE_PHONE,
        f"⏰ Zeitfenster geaendert!\n"
        f"👤 {partner_name}\n"
        f"📱 {phone_norm}\n"
        f"⏰ Neues Fenster: {wahl}",
        _skip_admin=True)

    return HTMLResponse(
        f"<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'></head>"
        f"<body style='font-family:sans-serif;text-align:center;padding:40px;background:#f0f9f0'>"
        f"<h2 style='color:#2d7d2d'>✅ Zeitfenster gesetzt!</h2>"
        f"<p><b>{partner_name}</b></p>"
        f"<p style='font-size:1.3em;color:#333'>{wahl}</p>"
        f"<p style='color:#666'>Du kannst dein Zeitfenster jederzeit aendern.</p>"
        f"<p style='margin-top:20px'><a href='https://wa.me/{LINA_WA_NUMBER}' "
        f"style='background:#25D366;color:white;padding:12px 24px;border-radius:8px;"
        f"text-decoration:none;font-weight:bold'>Lina schreiben</a></p>"
        f"</body></html>"
    )

@app.get("/health")
async def health():
    return {"status": "ok", "version": "5.4"}

@app.get("/status")
async def status_check():
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)
        active = [r for r in records if str(r.get("Status", "")).lower() == "aktiv"]
        return {
            "version": "5.4",
            "active_partners": len(active),
            "total_partners": len(records),
            "status": "ok"
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/partner")
async def list_partners():
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)
        return JSONResponse({"partners": [
            {"name": r.get("Name", ""), "phone": r.get("Telefon", ""),
             "guthaben": r.get("Guthaben_Euro", 0), "status": r.get("Status", ""),
             "zeitfenster": r.get("Zeitfenster", ""), "email": r.get("Email", "")}
            for r in records
        ], "count": len(records)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── STARTUP ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    threading.Thread(target=poll_leads, daemon=True).start()

    scheduler = BackgroundScheduler(timezone=BERLIN_TZ)
    scheduler.add_job(send_daily_reminders, "cron", hour=8, minute=0)
    scheduler.start()

    # Sheet-Header pruefen
    try:
        _, partner_ws, _ = get_sheets()
        header_ok = validate_sheet_headers(partner_ws)
    except Exception as e:
        header_ok = False
        send_whatsapp(MATZE_PHONE, f"⚠️ Startup Sheet-Check Fehler:\n{e}", _skip_admin=True)

    send_whatsapp(MATZE_PHONE,
        f"🚀 Lead-System v5.4 gestartet!\n\n"
        f"✅ Polling aktiv (alle {POLL_INTERVAL}s)\n"
        f"✅ Tages-Erinnerung aktiv (08:00 Berlin)\n"
        f"✅ Stripe Webhook aktiv\n"
        f"✅ Zeitfenster-Links aktiv\n"
        f"✅ Admin-Benachrichtigungen aktiv\n"
        f"✅ Email-Match via Spalte H\n"
        f"{'✅' if header_ok else '❌'} Sheet-Header geprueft",
        _skip_admin=True)
