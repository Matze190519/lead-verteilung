/mnt/user-data/outputs/lead-system/app_v5_2.py
Copyfrom fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse
import requests
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import logging

logging.basicConfig(level=logging.INFO)
import time
from datetime import datetime, timedelta
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

# ─── 0-INDEXED COLUMN CONSTANTS ───────────────────────────────────
# Partner_Konto: Name | Telefon | Guthaben_Euro | Leads_Geliefert | Letzter_Lead_Am | Status | Zeitfenster | Email
COL_NAME            = 0
COL_TELEFON         = 1
COL_GUTHABEN        = 2
COL_LEADS_GELIEFERT = 3
COL_LETZTER_LEAD    = 4
COL_STATUS          = 5
COL_ZEITFENSTER     = 6
COL_EMAIL           = 7   # Column H

# Tabellenblatt1: ... | M=Email | N=Name | O=Phone | P=Status
COL_LEAD_EMAIL  = 12
COL_LEAD_NAME   = 13
COL_LEAD_PHONE  = 14
COL_LEAD_STATUS = 15

# Leads_Log columns
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
    """Connect to Google Sheets using gspread 6.x compatible API.
    Uses service_account_from_dict() instead of deprecated gspread.authorize()."""
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON ist nicht gesetzt!")
    try:
        creds_dict = json.loads(creds_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_CREDENTIALS_JSON ist kein gültiges JSON: {e}")
    # gspread 6.x: service_account_from_dict statt authorize()
    client      = gspread.service_account_from_dict(creds_dict)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    leads_ws    = spreadsheet.worksheet(LEADS_SHEET_NAME)
    partner_ws  = spreadsheet.worksheet(PARTNER_SHEET_NAME)
    log_ws      = spreadsheet.worksheet(LOG_SHEET_NAME)
    return leads_ws, partner_ws, log_ws

# ─── HILFSFUNKTIONEN ──────────────────────────────────────────────
def normalize_phone(phone: str) -> str:
    """Normalize phone to E.164 format without leading +.
    Handles DE/AT/CH numbers."""
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
    """Check if current Berlin time is within the partner's chosen time window."""
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
    return True  # Unknown values: always deliver

def get_all_partner_records(partner_ws) -> list:
    """Return all partner records as list of dicts using sheet headers as keys.
    Headers expected: Name, Telefon, Guthaben_Euro, Leads_Geliefert,
                      Letzter_Lead_Am, Status, Zeitfenster, Email"""
    return partner_ws.get_all_records()

def find_partner_by_phone(records: list, phone_norm: str):
    """Find partner by normalized phone number. Returns (index, record) or (None, None)."""
    for i, r in enumerate(records):
        if normalize_phone(str(r.get("Telefon", ""))) == phone_norm:
            return i, r
    return None, None

def find_partner_by_email(records: list, email: str):
    """Find partner by email address (case-insensitive). Returns (index, record) or (None, None).
    NOTE: Requires column H header to be exactly 'Email' in the Google Sheet."""
    email_lower = email.strip().lower()
    for i, r in enumerate(records):
        partner_email = str(r.get("Email", "")).strip().lower()
        if partner_email and partner_email == email_lower:
            return i, r
    return None, None

def validate_sheet_headers(partner_ws) -> bool:
    """Validate that Partner_Konto has correct headers in row 1.
    Returns True if OK, False if Email header is missing."""
    try:
        headers = partner_ws.row_values(1)
        if len(headers) < 8 or headers[7].strip() != "Email":
            send_whatsapp(MATZE_PHONE,
                f"🚨 SHEET FEHLER: Spalte H in Partner_Konto ist '{headers[7] if len(headers) > 7 else 'LEER'}' "
                f"statt 'Email'!\n"
                f"➡️ Bitte jetzt in Google Sheet korrigieren:\n"
                f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}",
                _skip_admin=True)
            return False
        return True
    except Exception as e:
        print(f"[HEADER-VALIDATE-ERROR] {e}")
        return False

# ─── WHATSAPP ─────────────────────────────────────────────────────
def send_whatsapp(to: str, message: str, _skip_admin: bool = False) -> bool:
    """Send a WhatsApp text message. Returns True on success.
    _skip_admin=True prevents recursive admin alerts for admin messages."""
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
        # Detect 24-hour window error (code 131047)
        err_data = {}
        ct = r.headers.get("content-type", "")
        if "application/json" in ct:
            err_data = r.json()
        err_code = err_data.get("error", {}).get("code", 0)
        if err_code == 131047 and not _skip_admin:
            admin_msg = (
                f"⚠️ 24h-Fenster geschlossen!\n"
                f"Empfänger: {to}\n\n"
                f"Partner muss zuerst Lina schreiben:\n"
                f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                f"Danach erneut versuchen."
            )
            send_whatsapp(MATZE_PHONE, admin_msg, _skip_admin=True)
        return False
    except Exception as e:
        print(f"[WA-ERROR] to={to} err={e}")
        return False

# ─── LEAD PROCESSING ──────────────────────────────────────────────
def process_lead(lead_row: list, lead_row_index: int, partner_record: dict,
                 partner_row_index: int, partner_ws, log_ws, leads_ws):
    """Distribute one lead to one partner and update all sheets + send WhatsApp messages."""
    lead_email    = lead_row[COL_LEAD_EMAIL]  if len(lead_row) > COL_LEAD_EMAIL  else ""
    lead_name     = lead_row[COL_LEAD_NAME]   if len(lead_row) > COL_LEAD_NAME   else ""
    lead_phone    = normalize_phone(str(lead_row[COL_LEAD_PHONE]))  if len(lead_row) > COL_LEAD_PHONE else ""
    partner_name  = partner_record.get("Name", "")
    partner_phone = normalize_phone(str(partner_record.get("Telefon", "")))

    guthaben = float(str(partner_record.get("Guthaben_Euro", "0")).replace(",", "."))
    if guthaben < LEAD_PREIS:
        send_whatsapp(MATZE_PHONE,
            f"⚠️ Kein aktiver Partner mit ausreichend Guthaben für Lead {lead_name}!",
            _skip_admin=True)
        return

    # Ad source text
    ad_name   = lead_row[3] if len(lead_row) > 3 else ""
    camp_name = lead_row[7] if len(lead_row) > 7 else ""
    if ad_name or camp_name:
        source_text = f"📢 Quelle: {ad_name or camp_name}"
    else:
        source_text = "📢 Quelle: Organisch"

    new_guthaben = guthaben - LEAD_PREIS

    # ── WhatsApp to Lead ──
    lead_msg = (
        f"Hallo {lead_name} 👋\n"
        f"Ich freue mich, von dir gehört zu haben!\n"
        f"Jemand aus meinem Team wird sich in Kürze bei dir melden.\n"
        f"Deine Telefonnummer: {lead_phone}"
    )
    wa_lead_sent = send_whatsapp(lead_phone, lead_msg)

    # ── WhatsApp to Partner ──
    partner_msg = (
        f"🎯 Neuer Lead für dich!\n\n"
        f"👤 Name: {lead_name}\n"
        f"📧 E-Mail: {lead_email}\n"
        f"📱 Telefon: +{lead_phone}\n"
        f"{source_text}\n\n"
        f"💰 Guthaben nach Abzug: {new_guthaben:.2f} €\n"
        f"⚠️ Bitte melde dich zeitnah!\n"
    )
    if new_guthaben < 15:
        partner_msg += (
            f"\n💳 Guthaben wird knapp! Jetzt aufladen:\n"
            f"👉 Schreib Lina: https://wa.me/{LINA_WA_NUMBER}\n"
        )
    wa_partner_sent = send_whatsapp(partner_phone, partner_msg)

    # ── Update Google Sheet: Partner_Konto ──
    leads_delivered = int(partner_record.get("Leads_Geliefert", 0)) + 1
    now_str         = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")

    partner_ws.update_cell(partner_row_index + 2, COL_GUTHABEN        + 1, new_guthaben)
    partner_ws.update_cell(partner_row_index + 2, COL_LEADS_GELIEFERT + 1, leads_delivered)
    partner_ws.update_cell(partner_row_index + 2, COL_LETZTER_LEAD    + 1, now_str)

    # ── Update Google Sheet: Tabellenblatt1 ──
    leads_ws.update_cell(lead_row_index + 2, COL_LEAD_STATUS + 1, "VERTEILT")

    # ── Admin Notification ──
    admin_msg = (
        f"✅ Lead verteilt!\n\n"
        f"👤 Lead:    {lead_name}\n"
        f"📧 E-Mail:  {lead_email}\n"
        f"📱 Tel:     +{lead_phone}\n"
        f"🤝 Partner: {partner_name}\n"
        f"💰 Guthaben jetzt: {new_guthaben:.2f} €\n"
        f"📲 WA Lead:    {'✅' if wa_lead_sent    else '❌'}\n"
        f"📲 WA Partner: {'✅' if wa_partner_sent else '❌'}"
    )
    send_whatsapp(MATZE_PHONE, admin_msg, _skip_admin=True)

    # ── Leads_Log ──
    log_ws.append_row([
        now_str,
        lead_name,
        lead_phone,
        lead_email,
        partner_name,
        partner_phone,
        new_guthaben,
        "✅" if wa_lead_sent    else "❌",
        "✅" if wa_partner_sent else "❌",
        "VERTEILT"
    ])

# ─── POLLING LOOP ─────────────────────────────────────────────────
def poll_leads():
    """Background thread: continuously polls for new leads and distributes them."""
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
                status = str(lead_row[COL_LEAD_STATUS]).strip().upper()
                if status in ("VERTEILT", "ARCHIV", "DUPLIKAT"):
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

# ─── DAILY REMINDER (08:00 Berlin) ────────────────────────────────
def send_daily_reminders():
    """Send daily reminder to all active partners + admin summary."""
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)

        sent_list          = []
        failed_list        = []

        for r in records:
            if str(r.get("Status", "")).strip().lower() != "aktiv":
                continue
            phone       = normalize_phone(str(r.get("Telefon", "")))
            name        = r.get("Name", "Unbekannt")
            guthaben    = float(str(r.get("Guthaben_Euro", "0")).replace(",", "."))

            msg = (
                f"🚨🚨🚨 TAGES-ERINNERUNG 🚨🚨🚨\n\n"
                f"Hallo {name}! 👋\n"
                f"Dein Lead-System ist aktiv.\n\n"
                f"💰 Aktuelles Guthaben: {guthaben:.2f} €\n"
            )
            if guthaben < 15:
                msg += (
                    f"⚠️ Guthaben wird knapp! Jetzt aufladen:\n"
                    f"👉 Schreib Lina: https://wa.me/{LINA_WA_NUMBER}\n"
                )
            else:
                msg += "✅ Guthaben ausreichend.\n"
            msg += f"\nViel Erfolg heute! 🚀"

            success = send_whatsapp(phone, msg)
            if success:
                sent_list.append(f"✅ {name} ({phone})")
            else:
                failed_list.append(f"❌ {name} ({phone}) – 24h-Fenster evtl. geschlossen")

        # ── Admin Summary ──
        summary = f"📊 Tages-Erinnerung – {datetime.now(BERLIN_TZ).strftime('%d.%m.%Y %H:%M')}\n\n"
        if sent_list:
            summary += "✅ Erfolgreich gesendet:\n" + "\n".join(sent_list) + "\n\n"
        if failed_list:
            summary += (
                "❌ Fehlgeschlagen:\n"
                + "\n".join(failed_list)
                + f"\n\n👉 Diese Partner müssen Lina schreiben:\nhttps://wa.me/{LINA_WA_NUMBER}"
            )
        if not sent_list and not failed_list:
            summary += "ℹ️ Keine aktiven Partner gefunden."

        send_whatsapp(MATZE_PHONE, summary, _skip_admin=True)

    except Exception as e:
        print(f"[REMINDER-ERROR] {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 Reminder-Fehler:\n{e}", _skip_admin=True)

# ─── STRIPE WEBHOOK HANDLER ───────────────────────────────────────
def process_stripe_payment(session: dict):
    """Process a completed Stripe checkout session:
    - Credit partner's balance
    - Send WhatsApp confirmation with time-window links
    - Notify admin in all cases"""
    try:
        leads_ws, partner_ws, log_ws = get_sheets()
        customer_email = (session.get("customer_details") or {}).get("email", "").strip()
        amount_total   = (session.get("amount_total") or 0) / 100

        if not customer_email:
            send_whatsapp(MATZE_PHONE,
                f"⚠️ Stripe-Zahlung ohne E-Mail!\n"
                f"Betrag: {amount_total:.2f} €\n"
                f"Bitte manuell nachbearbeiten.",
                _skip_admin=True)
            return

        records     = get_all_partner_records(partner_ws)
        p_idx, p_record = find_partner_by_email(records, customer_email)

        if p_record is None:
            # ── NEW PARTNER ──
            raw_phone    = (session.get("customer_details") or {}).get("phone", "")
            partner_phone = normalize_phone(str(raw_phone)) if raw_phone else ""
            partner_name  = (session.get("customer_details") or {}).get("name", "") or customer_email

            new_row = [
                partner_name,
                partner_phone,
                amount_total,
                0,
                "",
                "aktiv",
                "Ganztag",
                customer_email
            ]
            partner_ws.append_row(new_row)

            # Re-fetch to get the new row index
            records         = get_all_partner_records(partner_ws)
            p_idx, p_record = find_partner_by_email(records, customer_email)

            if partner_phone:
                welcome_msg = (
                    f"✅ Willkommen {partner_name}!\n"
                    f"💰 {amount_total:.2f} € Guthaben eingebucht.\n\n"
                    f"⚠️ WICHTIG: Schreib Lina kurz 'Hallo' damit dein Lead-System startet:\n"
                    f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"⏰ Dann wähle dein Zeitfenster:\n"
                    f"Ganztag:    {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"Vormittag:  {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"Nachmittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"Abend:      {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"
                )
                wa_sent = send_whatsapp(partner_phone, welcome_msg)
                if not wa_sent:
                    send_whatsapp(MATZE_PHONE,
                        f"⚠️ WA an neuen Partner {partner_name} fehlgeschlagen!\n"
                        f"Tel: {partner_phone}\n"
                        f"E-Mail: {customer_email}\n"
                        f"Betrag: +{amount_total:.2f} €\n\n"
                        f"Manuell Zeitfenster-Links schicken:\n"
                        f"Ganztag:    {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                        f"Vormittag:  {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                        f"Nachmittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                        f"Abend:      {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend",
                        _skip_admin=True)
            else:
                # No phone from Stripe → alert admin
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ Neuer Partner ohne Telefon!\n"
                    f"Name: {partner_name}\n"
                    f"E-Mail: {customer_email}\n"
                    f"Betrag: +{amount_total:.2f} € eingebucht.\n\n"
                    f"➡️ Bitte Telefonnummer manuell eintragen und Zeitfenster-Link schicken.",
                    _skip_admin=True)

        else:
            # ── EXISTING PARTNER ──
            partner_phone    = normalize_phone(str(p_record.get("Telefon", "")))
            current_guthaben = float(str(p_record.get("Guthaben_Euro", "0")).replace(",", "."))
            new_guthaben     = current_guthaben + amount_total
            partner_name     = p_record.get("Name", customer_email)

            partner_ws.update_cell(p_idx + 2, COL_GUTHABEN + 1, new_guthaben)

            if partner_phone:
                confirm_msg = (
                    f"✅ Zahlung erhalten!\n"
                    f"💰 +{amount_total:.2f} € aufgeladen.\n"
                    f"💳 Neues Guthaben: {new_guthaben:.2f} €\n\n"
                    f"⚠️ Denk daran: Schreib Lina kurz damit dein Zeitfenster offen bleibt:\n"
                    f"👉 https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"⏰ Zeitfenster anpassen:\n"
                    f"Ganztag:    {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"Vormittag:  {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"Nachmittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"Abend:      {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"
                )
                wa_sent = send_whatsapp(partner_phone, confirm_msg)
                if not wa_sent:
                    send_whatsapp(MATZE_PHONE,
                        f"⚠️ WA an Partner {partner_name} fehlgeschlagen!\n"
                        f"Tel: {partner_phone}\n"
                        f"Betrag: +{amount_total:.2f} €\n"
                        f"Neues Guthaben: {new_guthaben:.2f} €\n\n"
                        f"Manuell Zeitfenster-Links schicken:\n"
                        f"Ganztag:    {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                        f"Vormittag:  {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                        f"Nachmittag: {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                        f"Abend:      {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend",
                        _skip_admin=True)
            else:
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ Partner {partner_name} hat keine Telefonnummer!\n"
                    f"Betrag: +{amount_total:.2f} € wurde eingebucht.\n"
                    f"Neues Guthaben: {new_guthaben:.2f} €",
                    _skip_admin=True)

        # ── Admin Notification (always sent) ──
        p_name_display = p_record.get("Name", customer_email) if p_record else customer_email
        send_whatsapp(MATZE_PHONE,
            f"💳 Neue Zahlung!\n\n"
            f"👤 {p_name_display}\n"
            f"📧 {customer_email}\n"
            f"💰 +{amount_total:.2f} €",
            _skip_admin=True)

    except Exception as e:
        print(f"[STRIPE-ERROR] {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 Stripe-Fehler:\n{e}", _skip_admin=True)

# ─── FASTAPI ENDPOINTS ────────────────────────────────────────────
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
    """Set a partner's time window via URL link."""
    valid = {"Ganztag", "Vormittag", "Nachmittag", "Abend"}
    if wahl not in valid:
        raise HTTPException(status_code=400, detail="Ungültige Wahl. Erlaubt: Ganztag, Vormittag, Nachmittag, Abend")
    phone_norm = normalize_phone(phone)
    _, partner_ws, _ = get_sheets()
    records   = get_all_partner_records(partner_ws)
    idx, record = find_partner_by_phone(records, phone_norm)
    if record is None:
        raise HTTPException(status_code=404, detail="Partner nicht gefunden")
    partner_ws.update_cell(idx + 2, COL_ZEITFENSTER + 1, wahl)
    partner_name = record.get("Name", phone_norm)
    # Confirm to partner
    send_whatsapp(phone_norm,
        f"✅ Zeitfenster gesetzt: {wahl}\n"
        f"Du erhältst jetzt Leads in diesem Zeitraum.\n\n"
        f"Viel Erfolg! 🚀",
        _skip_admin=False)
    return HTMLResponse(
        content=f"""
        <html><body style="font-family:sans-serif;text-align:center;padding:40px">
        <h2>✅ Zeitfenster gesetzt!</h2>
        <p><b>{partner_name}</b>: {wahl}</p>
        <p>Du kannst dieses Fenster jederzeit ändern.</p>
        </body></html>
        """,
        status_code=200
    )

@app.get("/health")
async def health():
    return {"status": "ok", "version": "5.2"}

@app.get("/status")
async def status_check():
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)
        active  = [r for r in records if str(r.get("Status", "")).lower() == "aktiv"]
        total   = len(records)
        return {
            "version":         "5.2",
            "active_partners": len(active),
            "total_partners":  total,
            "status":          "ok"
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/partner")
async def list_partners():
    """Admin endpoint: list all partners with current balance and status."""
    try:
        _, partner_ws, _ = get_sheets()
        records = get_all_partner_records(partner_ws)
        result  = []
        for r in records:
            result.append({
                "name":       r.get("Name", ""),
                "phone":      r.get("Telefon", ""),
                "guthaben":   r.get("Guthaben_Euro", 0),
                "status":     r.get("Status", ""),
                "zeitfenster": r.get("Zeitfenster", ""),
                "email":      r.get("Email", ""),
            })
        return JSONResponse({"partners": result, "count": len(result)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─── STARTUP ──────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    # Start lead polling in background thread
    threading.Thread(target=poll_leads, daemon=True).start()

    # Start daily reminder scheduler (08:00 Berlin time)
    scheduler = BackgroundScheduler(timezone=BERLIN_TZ)
    scheduler.add_job(send_daily_reminders, "cron", hour=8, minute=0)
    scheduler.start()

    # Validate sheet headers at startup
    try:
        _, partner_ws, _ = get_sheets()
        validate_sheet_headers(partner_ws)
    except Exception as e:
        send_whatsapp(MATZE_PHONE, f"⚠️ Startup Sheet-Check Fehler:\n{e}", _skip_admin=True)

    # Startup confirmation to admin
    send_whatsapp(MATZE_PHONE,
        f"🚀 Lead-System v5.2 gestartet!\n"
        f"✅ Polling aktiv ({POLL_INTERVAL}s)\n"
        f"✅ Daily Reminders aktiv (08:00 Berlin)\n"
        f"✅ Stripe Webhook aktiv\n"
        f"✅ Admin Notifications aktiv\n"
        f"✅ Sheet Header validiert\n"
        f"✅ Email-Match via Spalte H",
        _skip_admin=True)
