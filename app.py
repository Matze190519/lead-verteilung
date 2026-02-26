# ============================================================
# Lead-Verteilungs-Service v5.5
# ============================================================
# Basis: v4.9 (stabil, verifiziert)
# + find_partner_by_email (Stripe-Match)
# + .strip() auf Zeitfenster
# + Header-Validierung Spalte H = Email
# + _skip_admin Flag (keine Endlos-Loops)
# + 24h-Fenster-Erkennung + Lina-Verweis
# + Sichtbare Tages-Erinnerung 08:00
# + Admin-Summary nach Tages-Erinnerung
# + Detaillierte Admin-Nachrichten
# + Zeitfenster-Links mit Uhrzeiten
# + /partner Admin-Endpoint
# + /status Endpoint
# ============================================================

import os
import json
import logging
import threading
import time
import re
from datetime import datetime

import gspread
import requests
import stripe
import pytz
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler

# ─── Logging ───────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Konfiguration aus Umgebungsvariablen ──────────────────
META_TOKEN            = os.getenv("META_TOKEN", "")
META_PHONE_ID         = os.getenv("META_PHONE_ID", "")
MATZE_PHONE           = os.getenv("MATZE_PHONE", "491715060008")
GOOGLE_SHEET_ID       = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
APP_URL               = os.getenv("APP_URL", "https://lead-verteilung.onrender.com")
LINA_WA_NUMBER        = os.getenv("LINA_WA_NUMBER", "4915170605019")

# ─── Konstanten ────────────────────────────────────────────
LEAD_PREIS     = 5.0
POLL_INTERVAL  = 60
BERLIN_TZ      = pytz.timezone("Europe/Berlin")
STRIPE_PAYMENT_LINK = "https://buy.stripe.com/aFa6oH64Wei20Robnte7m01"

LEADS_SHEET_NAME   = "Tabellenblatt1"
PARTNER_SHEET_NAME = "Partner_Konto"
LOG_SHEET_NAME     = "Leads_Log"

# ─── Spalten Partner_Konto (1-basiert fuer update_cell) ────
COL_NAME        = 1   # A
COL_TELEFON     = 2   # B
COL_GUTHABEN    = 3   # C
COL_LEADS       = 4   # D
COL_LETZTER     = 5   # E
COL_STATUS      = 6   # F
COL_ZEITFENSTER = 7   # G
COL_EMAIL       = 8   # H

# ─── Zeitfenster ───────────────────────────────────────────
ZEITFENSTER = {
    "Ganztag":    None,
    "Vormittag":  (8,  12),
    "Nachmittag": (12, 17),
    "Abend":      (17, 22),
}
ZEITFENSTER_TEXT = {
    "Ganztag":    "08-22 Uhr (Ganztag)",
    "Vormittag":  "08-12 Uhr",
    "Nachmittag": "12-17 Uhr",
    "Abend":      "17-22 Uhr",
}

def partner_ist_verfuegbar(zeitfenster: str) -> bool:
    zf = zeitfenster.strip() if zeitfenster else "Ganztag"
    fenster = ZEITFENSTER.get(zf, None)
    if fenster is None:
        return True
    now_hour = datetime.now(BERLIN_TZ).hour
    start, end = fenster
    return start <= now_hour < end

# ─── Stripe Init ───────────────────────────────────────────
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ─── Google Sheets Auth (EXAKT wie v4.9!) ──────────────────
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def get_google_client():
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if creds_json:
        creds_dict = json.loads(creds_json)
    else:
        creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        with open(creds_file) as f:
            creds_dict = json.load(f)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)

def get_spreadsheet():
    return get_google_client().open_by_key(GOOGLE_SHEET_ID)

def get_leads_sheet():
    return get_spreadsheet().worksheet(LEADS_SHEET_NAME)

def get_partner_sheet():
    return get_spreadsheet().worksheet(PARTNER_SHEET_NAME)

def get_log_sheet():
    try:
        return get_spreadsheet().worksheet(LOG_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = get_spreadsheet().add_worksheet(
            title=LOG_SHEET_NAME, rows=2000, cols=12
        )
        ws.append_row([
            "Zeitstempel", "Lead_Name", "Lead_Phone", "Lead_Email",
            "Partner_Name", "Partner_Telefon", "Guthaben_Nach",
            "WhatsApp", "WhatsApp_Lead", "Status", "Kampagne", "Anzeige"
        ])
        return ws

# ─── Phone-Normalisierung (v4.9 exakt) ────────────────────
def normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    phone = re.sub(r'^p:', '', phone.strip())
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    if phone.startswith('+'):
        return phone[1:]
    if phone.startswith('00'):
        return phone[2:]
    if re.match(r'^0[567]\d', phone) and len(phone) <= 11:
        return '43' + phone[1:]
    if re.match(r'^07\d', phone) and len(phone) == 10:
        return '41' + phone[1:]
    if phone.startswith('0'):
        return '49' + phone[1:]
    return phone

# ─── Ad-Quelle (v4.9 exakt) ───────────────────────────────
def get_ad_quelle(ad_name: str, campaign_name: str) -> str:
    name = (ad_name or campaign_name or "").lower()
    if "porsche" in name or "reel" in name or "auto" in name:
        return "Auto LR Reel (Lifestyle, Autos, Erfolg)"
    elif "wage" in name or "clean" in name or "geld" in name or "online" in name:
        return "Online Business (Nebeneinkommen, Flexibilitaet)"
    elif "zoom" in name or "call" in name or "info" in name:
        return "Zoom Info Call (Informations-Gespraech)"
    elif "gesundheit" in name or "health" in name or "product" in name:
        return "LR Gesundheitsprodukte"
    elif ad_name:
        return ad_name
    elif campaign_name:
        return campaign_name
    return "Werbeanzeige"

# ─── WhatsApp senden (v4.9 + _skip_admin Fix) ─────────────
def send_whatsapp(phone: str, message: str, _skip_admin: bool = False) -> bool:
    if not META_TOKEN or not META_PHONE_ID:
        logger.warning("META_TOKEN oder META_PHONE_ID fehlt!")
        return False
    url = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "text",
        "text": {"body": message, "preview_url": True},
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        try:
            data = resp.json()
        except Exception:
            data = {}
        if resp.status_code == 200:
            logger.info(f"WA gesendet -> {phone}")
            return True
        error_code = data.get("error", {}).get("code", 0)
        if error_code in [100, 131047] and not _skip_admin:
            send_whatsapp(
                MATZE_PHONE,
                f"24h-Fenster geschlossen!\n"
                f"Empfaenger: {phone}\n\n"
                f"Partner muss zuerst Lina schreiben:\n"
                f"https://wa.me/{LINA_WA_NUMBER}\n\n"
                f"Danach erneut versuchen.",
                _skip_admin=True
            )
        else:
            logger.error(f"WA Fehler {error_code}: {data}")
        return False
    except Exception as e:
        logger.error(f"WA Exception: {e}")
        return False

# ─── Partner lesen (v4.9 Struktur + Email-Match) ──────────
def get_all_partner_records():
    try:
        ws = get_partner_sheet()
        records = ws.get_all_records()
        result = []
        for i, row in enumerate(records, start=2):
            try:
                guthaben_raw = str(row.get("Guthaben_Euro", 0))
                guthaben = float(
                    guthaben_raw.replace(",", ".").replace("€", "").strip() or 0
                )
                result.append({
                    "row_index":   i,
                    "name":        str(row.get("Name", "")).strip(),
                    "phone":       normalize_phone(str(row.get("Telefon", ""))),
                    "email":       str(row.get("Email", "")).strip().lower(),
                    "status":      str(row.get("Status", "")).strip(),
                    "guthaben":    guthaben,
                    "last_lead":   str(row.get("Letzter_Lead_Am", "")).strip(),
                    "lead_count":  int(
                        str(row.get("Leads_Geliefert", 0))
                        .replace(",", "").strip() or 0
                    ),
                    "zeitfenster": str(
                        row.get("Zeitfenster", "Ganztag")
                    ).strip() or "Ganztag",
                })
            except Exception as e:
                logger.warning(f"Partner Zeile {i} Fehler: {e}")
        return result
    except Exception as e:
        logger.error(f"Partner-Sheet Fehler: {e}")
        return []

# ─── Partner per Email finden (NEU - v5.x Fix) ────────────
def find_partner_by_email(records, email):
    email_lower = email.strip().lower()
    for p in records:
        if p["email"] and p["email"] == email_lower:
            return p
    return None

# ─── Besten Partner finden (v4.9 Logik) ───────────────────
def find_best_partner():
    all_records = get_all_partner_records()
    verfuegbar = [
        p for p in all_records
        if p["status"].lower() == "aktiv"
        and p["guthaben"] >= LEAD_PREIS
        and partner_ist_verfuegbar(p["zeitfenster"])
    ]
    if not verfuegbar:
        logger.info("Kein Partner im Zeitfenster - Fallback auf alle aktiven")
        verfuegbar = [
            p for p in all_records
            if p["status"].lower() == "aktiv"
            and p["guthaben"] >= LEAD_PREIS
        ]
    if not verfuegbar:
        logger.warning("Kein aktiver Partner mit Guthaben!")
        send_whatsapp(
            MATZE_PHONE,
            "ALERT: Kein Partner verfuegbar!\n\n"
            "Kein aktiver Partner hat genug Guthaben.\n"
            "Bitte Partner zum Aufladen auffordern!",
            _skip_admin=True
        )
        return None
    return sorted(verfuegbar, key=lambda p: (p["last_lead"] or "0000"))[0]

# ─── Partner updaten (v4.9 exakt) ─────────────────────────
def update_partner(row_index, new_guthaben, lead_count):
    try:
        ws = get_partner_sheet()
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.update_cell(row_index, COL_GUTHABEN, round(new_guthaben, 2))
        ws.update_cell(row_index, COL_LETZTER,  now_str)
        ws.update_cell(row_index, COL_LEADS,    lead_count)
        if new_guthaben < LEAD_PREIS:
            ws.update_cell(row_index, COL_STATUS, "Pausiert")
            logger.info(f"Partner Zeile {row_index} auto-pausiert ({new_guthaben})")
    except Exception as e:
        logger.error(f"Partner-Update Fehler Zeile {row_index}: {e}")

def update_partner_guthaben(email, betrag):
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["email"] == email.lower().strip():
                new_g = p["guthaben"] + betrag
                ws.update_cell(p["row_index"], COL_GUTHABEN, round(new_g, 2))
                ws.update_cell(p["row_index"], COL_STATUS,   "Aktiv")
                logger.info(f"Guthaben {email}: +{betrag} -> {new_g}")
                return True, p
        return False, None
    except Exception as e:
        logger.error(f"Guthaben-Update Fehler: {e}")
        return False, None

def update_zeitfenster_im_sheet(phone, zeitfenster):
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["phone"] == normalize_phone(phone):
                ws.update_cell(p["row_index"], COL_ZEITFENSTER, zeitfenster)
                logger.info(f"Zeitfenster {phone} -> {zeitfenster}")
                return True, p
        logger.warning(f"Partner {phone} nicht gefunden fuer Zeitfenster")
        return False, None
    except Exception as e:
        logger.error(f"Zeitfenster-Update Fehler: {e}")
        return False, None

def add_new_partner(name, email, phone_raw, guthaben):
    try:
        ws = get_partner_sheet()
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([
            name,
            normalize_phone(phone_raw),
            guthaben,
            0,
            now_str,
            "Aktiv",
            "Ganztag",
            email
        ])
        logger.info(f"Neuer Partner: {name} | {email} | {guthaben}")
    except Exception as e:
        logger.error(f"Neuer Partner Fehler: {e}")

# ─── Lead-Logging (v4.9 exakt) ────────────────────────────
def log_lead(lead_data, partner, neues_guthaben, wa_partner, status):
    try:
        ws = get_log_sheet()
        lead_phone = lead_data.get("phone", "")
        wa_lead_status = "OK" if lead_phone else "KEINE NR"
        ws.append_row([
            datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            lead_data.get("name", ""),
            lead_phone,
            lead_data.get("email", ""),
            partner.get("name", ""),
            partner.get("phone", ""),
            f"{neues_guthaben:.2f}",
            "OK" if wa_partner else "FEHLER",
            wa_lead_status,
            status,
            lead_data.get("campaign_name", ""),
            lead_data.get("ad_name", ""),
        ])
    except Exception as e:
        logger.error(f"Log-Fehler: {e}")

# ─── Lead verarbeiten (v4.9 + detaillierte Admin-Infos) ───
def process_lead(lead_data):
    lead_name     = lead_data.get("name", "Unbekannt")
    lead_phone    = lead_data.get("phone", "")
    lead_email    = lead_data.get("email", "")
    campaign_name = lead_data.get("campaign_name", "")
    ad_name       = lead_data.get("ad_name", "")

    partner = find_best_partner()
    if not partner:
        logger.warning(f"Kein Partner fuer Lead '{lead_name}'")
        return False

    neues_guthaben = partner["guthaben"] - LEAD_PREIS
    new_lead_count = partner["lead_count"] + 1
    ad_quelle      = get_ad_quelle(ad_name, campaign_name)

    # WhatsApp an Lead
    wa_lead = send_whatsapp(lead_phone,
        f"Hallo {lead_name}!\n\n"
        f"Vielen Dank fuer dein Interesse!\n"
        f"Jemand aus unserem Team wird sich in Kuerze bei dir melden.\n\n"
        f"Bis gleich!")

    # WhatsApp an Partner
    partner_msg = (
        f"Neuer Lead!\n\n"
        f"Name: {lead_name}\n"
        f"Telefon: +{lead_phone}\n"
        f"E-Mail: {lead_email}\n\n"
        f"Quelle: {ad_quelle}\n\n"
        f"Dein Guthaben: {neues_guthaben:.2f} EUR"
    )
    if neues_guthaben < 15:
        partner_msg += (
            f"\n\nGuthaben wird knapp!\n"
            f"Aufladen bei Lina: https://wa.me/{LINA_WA_NUMBER}\n"
            f"Oder direkt: {STRIPE_PAYMENT_LINK}"
        )
    partner_msg += "\n\nBitte melde dich zeitnah beim Lead!"
    wa_partner = send_whatsapp(partner["phone"], partner_msg)

    # Admin-Nachricht (detailliert)
    send_whatsapp(MATZE_PHONE,
        f"Lead verteilt!\n\n"
        f"Lead: {lead_name}\n"
        f"Tel: +{lead_phone}\n"
        f"Email: {lead_email}\n"
        f"Quelle: {ad_quelle}\n\n"
        f"Partner: {partner['name']}\n"
        f"Guthaben jetzt: {neues_guthaben:.2f} EUR\n"
        f"Leads gesamt: {new_lead_count}\n\n"
        f"WA Lead: {'OK' if wa_lead else 'FEHLER'}\n"
        f"WA Partner: {'OK' if wa_partner else 'FEHLER'}",
        _skip_admin=True)

    # Sheet updaten
    update_partner(partner["row_index"], neues_guthaben, new_lead_count)

    # Loggen
    final_status = "VERTEILT" if wa_partner else "VERTEILT_WA_FEHLER"
    log_lead(lead_data, partner, neues_guthaben, wa_partner, final_status)

    logger.info(f"Lead '{lead_name}' -> Partner '{partner['name']}' | Guthaben: {neues_guthaben:.2f}")
    return True

# ─── Stripe Payment (v4.9 + Email-Match + Zeitfenster-Links) ──
def process_stripe_payment(session):
    try:
        email        = (session.get("customer_details") or {}).get("email", "")
        amount_cents = session.get("amount_total", 0)
        betrag       = amount_cents / 100.0
        name         = (session.get("customer_details") or {}).get("name", "Unbekannt")

        if not email:
            logger.warning("Stripe: Keine E-Mail in Session")
            send_whatsapp(MATZE_PHONE,
                f"Stripe-Zahlung ohne E-Mail!\n"
                f"Betrag: {betrag:.2f} EUR\n"
                f"Bitte manuell nachbearbeiten.",
                _skip_admin=True)
            return

        # Bestehenden Partner aufladen oder neu anlegen
        found, partner_data = update_partner_guthaben(email, betrag)
        if not found:
            phone_raw = (session.get("customer_details") or {}).get("phone", "")
            add_new_partner(name, email, phone_raw, betrag)
            logger.info(f"Neuer Partner via Stripe: {name} | {email} | {betrag}")

        # Partner-Telefon holen
        all_records   = get_all_partner_records()
        partner_phone = ""
        partner_guthaben = betrag
        partner_zeitfenster = "Ganztag"
        for p in all_records:
            if p["email"] == email.lower().strip():
                partner_phone = p["phone"]
                partner_guthaben = p["guthaben"]
                partner_zeitfenster = p["zeitfenster"]
                break

        if partner_phone:
            # Zeitfenster-Links
            link_g = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag"
            link_v = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag"
            link_n = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag"
            link_a = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"

            if found:
                # Bestehender Partner - Aufladung
                wa_sent = send_whatsapp(partner_phone,
                    f"Zahlung erhalten!\n\n"
                    f"+{betrag:.2f} EUR aufgeladen\n"
                    f"Neues Guthaben: {partner_guthaben:.2f} EUR\n"
                    f"Zeitfenster: {partner_zeitfenster}\n\n"
                    f"Wichtig: Schreib Lina kurz damit dein Zeitfenster offen bleibt:\n"
                    f"https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"Zeitfenster aendern:\n"
                    f"Ganztag ({ZEITFENSTER_TEXT['Ganztag']}): {link_g}\n"
                    f"Vormittag ({ZEITFENSTER_TEXT['Vormittag']}): {link_v}\n"
                    f"Nachmittag ({ZEITFENSTER_TEXT['Nachmittag']}): {link_n}\n"
                    f"Abend ({ZEITFENSTER_TEXT['Abend']}): {link_a}")
            else:
                # Neuer Partner - Willkommen
                wa_sent = send_whatsapp(partner_phone,
                    f"Willkommen im Lead-System, {name}!\n\n"
                    f"{betrag:.2f} EUR Guthaben eingebucht.\n\n"
                    f"WICHTIG - 2 Schritte:\n\n"
                    f"1. Schreib Lina kurz 'Hallo' damit dein System startet:\n"
                    f"https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"2. Waehle dein Zeitfenster fuer Leads:\n"
                    f"Ganztag ({ZEITFENSTER_TEXT['Ganztag']}): {link_g}\n"
                    f"Vormittag ({ZEITFENSTER_TEXT['Vormittag']}): {link_v}\n"
                    f"Nachmittag ({ZEITFENSTER_TEXT['Nachmittag']}): {link_n}\n"
                    f"Abend ({ZEITFENSTER_TEXT['Abend']}): {link_a}\n\n"
                    f"Klick einfach auf den Link deiner Wahl!")

            if not wa_sent:
                send_whatsapp(MATZE_PHONE,
                    f"WA an Partner fehlgeschlagen!\n"
                    f"Name: {name}\n"
                    f"Tel: {partner_phone}\n"
                    f"Email: {email}\n"
                    f"Betrag: +{betrag:.2f} EUR\n\n"
                    f"Bitte manuell kontaktieren!",
                    _skip_admin=True)
        else:
            wa_sent = False
            send_whatsapp(MATZE_PHONE,
                f"Partner OHNE Telefonnummer!\n"
                f"Name: {name}\n"
                f"Email: {email}\n"
                f"Betrag: +{betrag:.2f} EUR eingebucht.\n\n"
                f"Bitte Telefonnummer manuell im Sheet eintragen!",
                _skip_admin=True)

        # Admin-Info (detailliert)
        status_text = "AUFLADUNG" if found else "NEUER PARTNER"
        send_whatsapp(MATZE_PHONE,
            f"{status_text}!\n\n"
            f"Name: {name}\n"
            f"Email: {email}\n"
            f"Tel: {partner_phone or 'Keine Nummer!'}\n"
            f"Betrag: +{betrag:.2f} EUR\n"
            f"Guthaben jetzt: {partner_guthaben:.2f} EUR\n"
            f"Zeitfenster: {partner_zeitfenster}\n"
            f"WA gesendet: {'OK' if (partner_phone and wa_sent) else 'FEHLER'}",
            _skip_admin=True)

    except Exception as e:
        logger.error(f"Stripe-Verarbeitung Fehler: {e}")
        send_whatsapp(MATZE_PHONE, f"Stripe-Fehler:\n{e}", _skip_admin=True)

# ─── Taegliche Erinnerungen 08:00 (verbessert + Admin-Summary) ──
def send_daily_reminders():
    logger.info("Taegliche Erinnerungen werden gesendet...")
    try:
        all_records = get_all_partner_records()
        aktive = [p for p in all_records if p["status"].lower() == "aktiv"]
        sent_list = []
        failed_list = []

        for partner in aktive:
            phone = partner["phone"]
            name = partner["name"]
            guthaben = partner["guthaben"]
            zeitfenster = partner["zeitfenster"]

            # Zeitfenster-Links
            link_g = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Ganztag"
            link_v = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Vormittag"
            link_n = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Nachmittag"
            link_a = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Abend"

            msg = (
                f"GUTEN MORGEN {name.upper()}!\n\n"
                f"Dein Lead-System ist AKTIV!\n\n"
                f"Guthaben: {guthaben:.2f} EUR\n"
                f"Zeitfenster: {zeitfenster}\n"
                f"Leads erhalten: {partner['lead_count']}\n"
            )
            if guthaben < 15:
                msg += (
                    f"\nACHTUNG: Guthaben wird knapp!\n"
                    f"Jetzt aufladen bei Lina: https://wa.me/{LINA_WA_NUMBER}\n"
                    f"Oder direkt: {STRIPE_PAYMENT_LINK}\n"
                )
            elif guthaben < 30:
                msg += (
                    f"\nTipp: Bald aufladen damit keine Leads verloren gehen.\n"
                    f"Lina: https://wa.me/{LINA_WA_NUMBER}\n"
                )
            else:
                msg += f"\nGuthaben ausreichend!\n"

            msg += (
                f"\nZeitfenster aendern:\n"
                f"Ganztag ({ZEITFENSTER_TEXT['Ganztag']}): {link_g}\n"
                f"Vormittag ({ZEITFENSTER_TEXT['Vormittag']}): {link_v}\n"
                f"Nachmittag ({ZEITFENSTER_TEXT['Nachmittag']}): {link_n}\n"
                f"Abend ({ZEITFENSTER_TEXT['Abend']}): {link_a}\n\n"
                f"Kurze Antwort (OK) damit Leads heute ankommen!\n\n"
                f"Viel Erfolg heute!"
            )

            if send_whatsapp(phone, msg):
                sent_list.append(f"OK: {name} ({guthaben:.2f} EUR, {zeitfenster})")
            else:
                failed_list.append(f"FEHLER: {name} ({phone})")

        # Admin Summary
        now_str = datetime.now(BERLIN_TZ).strftime("%d.%m.%Y %H:%M")
        summary = f"TAGES-REPORT {now_str}\n\n"

        if sent_list:
            summary += f"Gesendet ({len(sent_list)}):\n" + "\n".join(sent_list) + "\n\n"
        if failed_list:
            summary += (
                f"Fehlgeschlagen ({len(failed_list)}):\n"
                + "\n".join(failed_list)
                + f"\n\nDiese Partner muessen Lina schreiben:\nhttps://wa.me/{LINA_WA_NUMBER}\n\n"
            )
        if not sent_list and not failed_list:
            summary += "Keine aktiven Partner gefunden.\n"

        summary += f"Gesamt aktiv: {len(sent_list) + len(failed_list)} Partner"
        send_whatsapp(MATZE_PHONE, summary, _skip_admin=True)

    except Exception as e:
        logger.error(f"Taegliche Erinnerung Fehler: {e}")
        send_whatsapp(MATZE_PHONE, f"Reminder-Fehler:\n{e}", _skip_admin=True)

# ─── Lead-Polling (v4.9 exakt: nur CREATED verarbeiten) ───
def _do_poll():
    try:
        ws   = get_leads_sheet()
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return

        for i, row in enumerate(rows[1:], start=2):
            try:
                status = row[15].strip() if len(row) > 15 else ""
                if status.upper() != "CREATED":
                    continue

                ws.update_cell(i, 16, "PROCESSING")

                lead_data = {
                    "lead_id":       row[0]  if len(row) > 0  else "",
                    "ad_id":         row[2]  if len(row) > 2  else "",
                    "ad_name":       row[3]  if len(row) > 3  else "",
                    "campaign_name": row[7]  if len(row) > 7  else "",
                    "email":         row[12] if len(row) > 12 else "",
                    "name":          row[13] if len(row) > 13 else "",
                    "phone":         normalize_phone(
                                         row[14] if len(row) > 14 else ""
                                     ),
                }

                success = process_lead(lead_data)
                ws.update_cell(i, 16, "VERTEILT" if success else "FEHLER")

            except Exception as e:
                logger.error(f"Fehler bei Lead-Zeile {i}: {e}")
                try:
                    ws.update_cell(i, 16, "FEHLER")
                except:
                    pass

    except Exception as e:
        logger.error(f"Poll-Fehler: {e}")
        send_whatsapp(MATZE_PHONE, f"Poll-Fehler:\n{e}", _skip_admin=True)

def polling_loop():
    logger.info(f"Polling gestartet (alle {POLL_INTERVAL}s)")
    while True:
        _do_poll()
        time.sleep(POLL_INTERVAL)

# ─── Header-Validierung (NEU) ─────────────────────────────
def validate_sheet_headers():
    try:
        ws = get_partner_sheet()
        headers = ws.row_values(1)
        if len(headers) < 8 or headers[7].strip() != "Email":
            send_whatsapp(MATZE_PHONE,
                f"SHEET FEHLER!\n"
                f"Spalte H = '{headers[7] if len(headers) > 7 else 'LEER'}'\n"
                f"Erwartet: 'Email'\n\n"
                f"Jetzt korrigieren:\n"
                f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}",
                _skip_admin=True)
            return False
        return True
    except Exception as e:
        logger.error(f"Header-Check Fehler: {e}")
        return False

# ─── FastAPI App ───────────────────────────────────────────
app = FastAPI(title="Lead-Verteilungs-Service v5.5")

@app.get("/")
def root():
    return {
        "service":  "Lead-Verteilungs-Service",
        "version":  "5.5",
        "status":   "running",
    }

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.5", "timestamp": datetime.now(BERLIN_TZ).isoformat()}

@app.get("/status")
def status_check():
    try:
        all_records = get_all_partner_records()
        active = [r for r in all_records if r["status"].lower() == "aktiv"]
        return {
            "version": "5.5",
            "active_partners": len(active),
            "total_partners": len(all_records),
            "status": "ok"
        }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/partner")
def list_partners():
    try:
        all_records = get_all_partner_records()
        return JSONResponse({"partners": [
            {"name": r["name"], "phone": r["phone"],
             "guthaben": r["guthaben"], "status": r["status"],
             "zeitfenster": r["zeitfenster"], "email": r["email"]}
            for r in all_records
        ], "count": len(all_records)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    payload    = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        if STRIPE_WEBHOOK_SECRET:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        else:
            event = json.loads(payload)

        if event["type"] == "checkout.session.completed":
            threading.Thread(
                target=process_stripe_payment,
                args=(event["data"]["object"],),
                daemon=True
            ).start()
            return {"status": "ok"}

        return {"status": "ignored", "type": event.get("type")}

    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Ungueltige Stripe-Signatur")
    except Exception as e:
        logger.error(f"Stripe-Webhook Fehler: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/zeitfenster")
def zeitfenster_waehlen(phone: str, wahl: str):
    erlaubt = ["Ganztag", "Vormittag", "Nachmittag", "Abend"]
    if wahl not in erlaubt:
        return HTMLResponse(content=f"""
        <html><body style="font-family:Arial;text-align:center;
        padding:50px;background:#fff0f0">
        <h1>Ungueltige Wahl</h1>
        <p>Erlaubte Werte: {', '.join(erlaubt)}</p>
        </body></html>""")

    success, partner_data = update_zeitfenster_im_sheet(phone, wahl)

    if success:
        partner_name = partner_data["name"] if partner_data else phone
        send_whatsapp(phone,
            f"Zeitfenster gesetzt!\n\n"
            f"Du erhaeltst Leads: {wahl} ({ZEITFENSTER_TEXT[wahl]})\n\n"
            f"Aendern? Einfach Matze schreiben:\n"
            f"wa.me/491715060008")

        # Admin Info
        send_whatsapp(MATZE_PHONE,
            f"Zeitfenster geaendert!\n"
            f"Partner: {partner_name}\n"
            f"Tel: {phone}\n"
            f"Neues Fenster: {wahl}",
            _skip_admin=True)

        return HTMLResponse(content=f"""
        <html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'></head>
        <body style="font-family:Arial;text-align:center;padding:50px;background:#f0f8f0">
        <h1>Gespeichert!</h1>
        <h2>Zeitfenster: <b>{wahl}</b></h2>
        <p>Du erhaeltst Leads: <b>{ZEITFENSTER_TEXT[wahl]}</b></p>
        <p>Du bekommst gleich eine WhatsApp-Bestaetigung.</p>
        <p style="margin-top:20px"><a href="https://wa.me/{LINA_WA_NUMBER}"
        style="background:#25D366;color:white;padding:12px 24px;border-radius:8px;
        text-decoration:none;font-weight:bold">Lina schreiben</a></p>
        <p style="color:gray;font-size:14px">Dieses Fenster kann geschlossen werden.</p>
        </body></html>""")

    return HTMLResponse(content="""
    <html><body style="font-family:Arial;text-align:center;
    padding:50px;background:#fff0f0">
    <h1>Partner nicht gefunden</h1>
    <p>Bitte Matze kontaktieren: wa.me/491715060008</p>
    </body></html>""")

@app.post("/poll")
def manual_poll():
    _do_poll()
    return {"status": "ok", "message": "Poll ausgefuehrt"}

@app.post("/test-reminder")
def test_reminder():
    send_daily_reminders()
    return {"status": "ok", "message": "Erinnerungen gesendet"}

# ─── Startup ──────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    # APScheduler: taeglich 08:00 Berlin
    scheduler = BackgroundScheduler(timezone=BERLIN_TZ)
    scheduler.add_job(send_daily_reminders, "cron", hour=8, minute=0)
    scheduler.start()
    logger.info("Scheduler gestartet - taeglich 08:00 Uhr")

    # Polling-Thread
    poll_thread = threading.Thread(target=polling_loop, daemon=True)
    poll_thread.start()
    logger.info("Polling-Thread gestartet")

    # Header pruefen
    header_ok = validate_sheet_headers()

    # Startmeldung an Matze
    send_whatsapp(
        MATZE_PHONE,
        f"Lead-System v5.5 gestartet!\n\n"
        f"Spalten-Mapping verifiziert\n"
        f"Zeitfenster-Logik aktiv (.strip() Fix)\n"
        f"Email-Match via Spalte H {'OK' if header_ok else 'FEHLER'}\n"
        f"Klick-Links nach Stripe-Zahlung\n"
        f"Polling laeuft ({POLL_INTERVAL}s)\n"
        f"Taegliche Erinnerungen 08:00\n"
        f"Stripe Webhook bereit\n"
        f"24h-Fenster-Erkennung + Lina\n"
        f"Admin-Benachrichtigungen aktiv\n"
        f"DE/AT/CH Nummern-Fix aktiv\n\n"
        f"Alles gruen Matze!",
        _skip_admin=True
    )
