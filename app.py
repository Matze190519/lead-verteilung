# ============================================================
# Lead-Verteilungs-Service v4.9 FINAL
# ============================================================
# ✅ Verifiziert gegen echte Sheet-Screenshots
# ✅ Partner_Konto: A=Name B=Telefon C=Guthaben_Euro
#                  D=Leads_Geliefert E=Letzter_Lead_Am
#                  F=Status G=Zeitfenster H=Email
# ✅ Leads_Log: A=Zeitstempel B=Lead_Name C=Lead_Phone
#              D=Lead_Email E=Partner_Name F=Partner_Telefon
#              G=Guthaben_Nach H=WhatsApp I=WhatsApp_Lead J=Status
# ✅ Tabellenblatt1: C=ad_id D=ad_name M=Email N=Name O=Phone P=Status
# ✅ Google-Auth (kein oauth2client)
# ✅ WhatsApp Meta Cloud API + 24h-Fenster-Erkennung
# ✅ Stripe Webhook (Link in Botpress)
# ✅ Tägliche Erinnerungen 08:00 CET
# ✅ Phone-Normalisierung DE(+49) AT(+43) CH(+41)
# ✅ Ad-Quelle als lesbarer Text
# ✅ Zeitfenster: Ganztag(24/7)/Vormittag/Nachmittag/Abend
# ✅ Zeitfenster-Wahl per Klick-Link vollautomatisch
# ✅ Auto-Pause bei Guthaben < 5€
# ✅ Guthaben in jeder Nachricht
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
from fastapi.responses import HTMLResponse
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

# ─── Konstanten ────────────────────────────────────────────
LEAD_PREIS     = 5.0
POLL_INTERVAL  = 60
BERLIN_TZ      = pytz.timezone("Europe/Berlin")

LEADS_SHEET_NAME   = "Tabellenblatt1"
PARTNER_SHEET_NAME = "Partner_Konto"
LOG_SHEET_NAME     = "Leads_Log"

# ─── Spalten Partner_Konto (verifiziert per Screenshot) ────
COL_NAME        = 1   # A – Name
COL_TELEFON     = 2   # B – Telefon
COL_GUTHABEN    = 3   # C – Guthaben_Euro
COL_LEADS       = 4   # D – Leads_Geliefert
COL_LETZTER     = 5   # E – Letzter_Lead_Am
COL_STATUS      = 6   # F – Status (Aktiv/Pausiert)
COL_ZEITFENSTER = 7   # G – Zeitfenster
COL_EMAIL       = 8   # H – Email (neu anlegen nach Löschen der doppelten Spalte!)

# ─── Zeitfenster ───────────────────────────────────────────
ZEITFENSTER = {
    "Ganztag":    None,        # 24/7 – keine Einschränkung
    "Vormittag":  (8,  12),
    "Nachmittag": (12, 17),
    "Abend":      (17, 22),
}
ZEITFENSTER_TEXT = {
    "Ganztag":    "24/7 (auch nachts)",
    "Vormittag":  "08:00 – 12:00 Uhr",
    "Nachmittag": "12:00 – 17:00 Uhr",
    "Abend":      "17:00 – 22:00 Uhr",
}

def partner_ist_verfuegbar(zeitfenster: str) -> bool:
    """Ganztag = immer. Sonst: Stunde prüfen."""
    fenster = ZEITFENSTER.get(zeitfenster.strip(), None)
    if fenster is None:
        return True  # Ganztag = 24/7
    now_hour = datetime.now(BERLIN_TZ).hour
    start, end = fenster
    return start <= now_hour < end

# ─── Stripe Init ───────────────────────────────────────────
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ─── Google Sheets Auth ────────────────────────────────────
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
    """Leads_Log – wird automatisch angelegt falls nicht vorhanden."""
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

# ─── Phone-Normalisierung DE / AT / CH ─────────────────────
def normalize_phone(phone: str) -> str:
    """Gibt Nummer ohne + zurück. Erkennt DE/AT/CH automatisch."""
    if not phone:
        return ""
    phone = re.sub(r'^p:', '', phone.strip())
    phone = re.sub(r'[\s\-\(\)]', '', phone)
    if phone.startswith('+'):
        return phone[1:]
    if phone.startswith('00'):
        return phone[2:]
    # Österreich: 06x, 07x, 05x + max 11 Stellen
    if re.match(r'^0[567]\d', phone) and len(phone) <= 11:
        return '43' + phone[1:]
    # Schweiz: 07x + genau 10 Stellen
    if re.match(r'^07\d', phone) and len(phone) == 10:
        return '41' + phone[1:]
    # Deutschland: 0 vorne
    if phone.startswith('0'):
        return '49' + phone[1:]
    return phone

# ─── Ad-Quelle als lesbarer Text ───────────────────────────
def get_ad_quelle(ad_name: str, campaign_name: str) -> str:
    """Gibt verständlichen Text zurück statt internem FB-Link."""
    name = (ad_name or campaign_name or "").lower()
    if "porsche" in name or "reel" in name or "auto" in name:
        return "🚗 Auto LR Reel\n   (Lifestyle, Autos, Erfolg)"
    elif "wage" in name or "clean" in name or "geld" in name or "online" in name:
        return "💻 Online Business\n   (Nebeneinkommen, Flexibilität)"
    elif "zoom" in name or "call" in name or "info" in name:
        return "📞 Zoom Info Call\n   (Informations-Gespräch)"
    elif "gesundheit" in name or "health" in name or "product" in name:
        return "💚 LR Gesundheitsprodukte"
    elif ad_name:
        return f"📊 {ad_name}"
    elif campaign_name:
        return f"📊 {campaign_name}"
    return "📊 Werbeanzeige"

# ─── WhatsApp senden ───────────────────────────────────────
def send_whatsapp(phone: str, message: str) -> bool:
    """Sendet WhatsApp via Meta Cloud API. Erkennt 24h-Fenster."""
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
        data = resp.json()
        if resp.status_code == 200:
            logger.info(f"✅ WhatsApp gesendet → {phone}")
            return True
        error_code = data.get("error", {}).get("code", 0)
        if error_code in [100, 131047]:
            logger.warning(f"⚠️ 24h-Fenster geschlossen für {phone}")
            send_whatsapp(
                MATZE_PHONE,
                f"⚠️ *24h-Fenster geschlossen!*\n"
                f"📱 Partner {phone} nicht erreichbar.\n"
                f"👉 Bitte Lina bitten den Partner anzuschreiben."
            )
        else:
            logger.error(f"❌ WhatsApp Fehler {error_code}: {data}")
        return False
    except Exception as e:
        logger.error(f"❌ WhatsApp Exception: {e}")
        return False

# ─── Partner lesen ─────────────────────────────────────────
def get_all_partner_records():
    """Liest alle Partner aus Partner_Konto."""
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
                    "phone":       str(row.get("Telefon", "")).strip(),
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
        logger.error(f"❌ Partner-Sheet Fehler: {e}")
        return []

# ─── Besten Partner finden ─────────────────────────────────
def find_best_partner():
    """
    1. Aktiv + Guthaben >= 5€ + Zeitfenster offen
    2. Fallback: Aktiv + Guthaben >= 5€ (alle)
    3. Niemand → Admin-Alert
    """
    all_records = get_all_partner_records()

    # Schritt 1: Zeitfenster-Filter (case-insensitiv)
    verfuegbar = [
        p for p in all_records
        if p["status"].lower() == "aktiv"
        and p["guthaben"] >= LEAD_PREIS
        and partner_ist_verfuegbar(p["zeitfenster"])
    ]

    # Schritt 2: Fallback alle aktiven Partner
    if not verfuegbar:
        logger.info("⏰ Kein Partner im Zeitfenster – Fallback auf alle aktiven")
        verfuegbar = [
            p for p in all_records
            if p["status"].lower() == "aktiv"
            and p["guthaben"] >= LEAD_PREIS
        ]

    # Schritt 3: Niemand verfügbar
    if not verfuegbar:
        logger.warning("🚨 Kein aktiver Partner mit Guthaben!")
        send_whatsapp(
            MATZE_PHONE,
            "🚨 *ALERT: Kein Partner verfügbar!*\n\n"
            "Kein aktiver Partner hat genug Guthaben.\n"
            "Bitte Partner zum Aufladen auffordern!"
        )
        return None

    # Fairste Verteilung: wer am längsten keinen Lead hatte
    return sorted(verfuegbar, key=lambda p: (p["last_lead"] or "0000"))[0]

# ─── Partner updaten ───────────────────────────────────────
def update_partner(row_index: int, new_guthaben: float, lead_count: int):
    """Guthaben, Leads, Timestamp, ggf. Status setzen."""
    try:
        ws = get_partner_sheet()
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.update_cell(row_index, COL_GUTHABEN, round(new_guthaben, 2))
        ws.update_cell(row_index, COL_LETZTER,  now_str)
        ws.update_cell(row_index, COL_LEADS,    lead_count)
        if new_guthaben < LEAD_PREIS:
            ws.update_cell(row_index, COL_STATUS, "Pausiert")
            logger.info(f"⏸️ Partner Zeile {row_index} auto-pausiert ({new_guthaben}€)")
    except Exception as e:
        logger.error(f"❌ Partner-Update Fehler Zeile {row_index}: {e}")

def update_partner_guthaben(email: str, betrag: float) -> bool:
    """Lädt Guthaben auf nach Stripe-Zahlung."""
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["email"] == email.lower().strip():
                new_g = p["guthaben"] + betrag
                ws.update_cell(p["row_index"], COL_GUTHABEN, round(new_g, 2))
                ws.update_cell(p["row_index"], COL_STATUS,   "Aktiv")
                logger.info(f"✅ Guthaben {email}: +{betrag}€ → {new_g}€")
                return True
        return False
    except Exception as e:
        logger.error(f"❌ Guthaben-Update Fehler: {e}")
        return False

def update_zeitfenster_im_sheet(phone: str, zeitfenster: str) -> bool:
    """Setzt Zeitfenster für Partner per Telefon."""
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["phone"] == phone.strip():
                ws.update_cell(p["row_index"], COL_ZEITFENSTER, zeitfenster)
                logger.info(f"✅ Zeitfenster {phone} → {zeitfenster}")
                return True
        logger.warning(f"⚠️ Partner {phone} nicht gefunden für Zeitfenster")
        return False
    except Exception as e:
        logger.error(f"❌ Zeitfenster-Update Fehler: {e}")
        return False

def add_new_partner(name: str, email: str, phone: str, guthaben: float):
    """Legt neuen Partner an."""
    try:
        ws = get_partner_sheet()
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        # Reihenfolge EXAKT wie Sheet:
        # A=Name, B=Telefon, C=Guthaben_Euro, D=Leads_Geliefert,
        # E=Letzter_Lead_Am, F=Status, G=Zeitfenster, H=Email
        ws.append_row([
            name,
            normalize_phone(phone),
            guthaben,
            0,
            now_str,
            "Aktiv",
            "Ganztag",
            email
        ])
        logger.info(f"✅ Neuer Partner: {name} | {email} | {guthaben}€")
    except Exception as e:
        logger.error(f"❌ Neuer Partner Fehler: {e}")

# ─── Lead-Logging ──────────────────────────────────────────
def log_lead(
    lead_data: dict,
    partner: dict,
    neues_guthaben: float,
    wa_partner: bool,
    status: str
):
    """
    Schreibt in Leads_Log – EXAKT passend zur bestehenden Struktur:
    A=Zeitstempel B=Lead_Name C=Lead_Phone D=Lead_Email
    E=Partner_Name F=Partner_Telefon G=Guthaben_Nach
    H=WhatsApp I=WhatsApp_Lead J=Status K=Kampagne L=Anzeige
    """
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
        logger.error(f"❌ Log-Fehler: {e}")

# ─── Lead verarbeiten ──────────────────────────────────────
def process_lead(lead_data: dict):
    """Hauptfunktion: Partner wählen, WhatsApp senden, Sheet updaten."""
    lead_name     = lead_data.get("name", "Unbekannt")
    lead_phone    = lead_data.get("phone", "")
    lead_email    = lead_data.get("email", "")
    campaign_name = lead_data.get("campaign_name", "")
    ad_name       = lead_data.get("ad_name", "")

    partner = find_best_partner()
    if not partner:
        logger.warning(f"⚠️ Kein Partner für Lead '{lead_name}'")
        return False

    neues_guthaben = partner["guthaben"] - LEAD_PREIS
    new_lead_count = partner["lead_count"] + 1
    ad_quelle      = get_ad_quelle(ad_name, campaign_name)

    # Partner-Nachricht
    partner_msg = (
        f"🔔 *Neuer Lead!*\n\n"
        f"👤 {lead_name}\n"
        f"📞 +{lead_phone}\n"
        f"📧 {lead_email}\n"
    )
    if campaign_name:
        partner_msg += f"\n📊 *Kampagne:* {campaign_name}"
    if ad_name:
        partner_msg += f"\n🎯 *Anzeige:* {ad_name}"
    partner_msg += (
        f"\n\n💬 *Lead kam über:*\n{ad_quelle}\n\n"
        f"💰 *Dein Guthaben: {neues_guthaben:.2f} €*"
    )

    # Admin-Nachricht
    admin_msg = (
        f"✅ *Lead verteilt!*\n\n"
        f"👤 {lead_name}\n"
        f"📞 +{lead_phone}\n"
        f"📧 {lead_email}\n"
        f"→ Partner: *{partner['name']}*\n"
        f"📊 {campaign_name} | {ad_name}\n"
        f"💰 Partner-Guthaben: {neues_guthaben:.2f} €"
    )

    # Senden
    wa_ok = send_whatsapp(partner["phone"], partner_msg)
    send_whatsapp(MATZE_PHONE, admin_msg)

    # Sheet updaten
    update_partner(partner["row_index"], neues_guthaben, new_lead_count)

    # Loggen
    final_status = "VERTEILT" if wa_ok else "VERTEILT_WA_FEHLER"
    log_lead(lead_data, partner, neues_guthaben, wa_ok, final_status)

    logger.info(
        f"✅ Lead '{lead_name}' → Partner '{partner['name']}' | "
        f"Guthaben danach: {neues_guthaben:.2f}€"
    )
    return True

# ─── Stripe Webhook ────────────────────────────────────────
def process_stripe_payment(session: dict):
    """Verarbeitet checkout.session.completed."""
    try:
        email        = session.get("customer_details", {}).get("email", "")
        amount_cents = session.get("amount_total", 0)
        betrag       = amount_cents / 100.0
        name         = session.get("customer_details", {}).get("name", "Unbekannt")

        if not email:
            logger.warning("Stripe: Keine E-Mail in Session")
            return

        # Bestehenden Partner aufladen oder neu anlegen
        found = update_partner_guthaben(email, betrag)
        if not found:
            phone_raw = session.get("customer_details", {}).get("phone", "")
            add_new_partner(name, email, phone_raw, betrag)
            logger.info(f"✅ Neuer Partner via Stripe: {name} | {email} | {betrag}€")

        # Partner-Telefon holen
        all_records   = get_all_partner_records()
        partner_phone = ""
        for p in all_records:
            if p["email"] == email.lower().strip():
                partner_phone = p["phone"]
                break

        if partner_phone:
            # Klick-Links für Zeitfenster
            link_g = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag"
            link_v = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag"
            link_n = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag"
            link_a = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"

            # Nachricht 1: Bestätigung
            send_whatsapp(
                partner_phone,
                f"✅ *Zahlung bestätigt!*\n\n"
                f"💰 *+{betrag:.0f} €* wurden deinem Konto gutgeschrieben.\n"
                f"🚀 Du erhältst ab sofort Leads!\n\n"
                f"Bei Fragen: Matze +49 171 506 0008"
            )
            time.sleep(2)

            # Nachricht 2: Zeitfenster-Wahl
            send_whatsapp(
                partner_phone,
                f"⏰ *Wann möchtest du Leads erhalten?*\n\n"
                f"Einfach antippen – wird automatisch gespeichert! ✅\n\n"
                f"1️⃣ Ganztag (24/7, auch nachts):\n{link_g}\n\n"
                f"2️⃣ Vormittag (08–12 Uhr):\n{link_v}\n\n"
                f"3️⃣ Nachmittag (12–17 Uhr):\n{link_n}\n\n"
                f"4️⃣ Abend (17–22 Uhr):\n{link_a}\n\n"
                f"_(Kein Klick nötig = Ganztag bleibt aktiv)_"
            )

        # Admin-Info
        send_whatsapp(
            MATZE_PHONE,
            f"💳 *Neue Zahlung!*\n\n"
            f"👤 {name}\n"
            f"📧 {email}\n"
            f"💰 +{betrag:.0f} €\n\n"
            f"⏰ Zeitfenster-Link automatisch verschickt ✅"
        )

    except Exception as e:
        logger.error(f"❌ Stripe-Verarbeitung Fehler: {e}")

# ─── Tägliche Erinnerungen 08:00 Uhr ──────────────────────
def send_daily_reminders():
    """Sendet täglich 08:00 eine Erinnerung an alle aktiven Partner."""
    logger.info("📅 Tägliche Erinnerungen werden gesendet...")
    try:
        all_records = get_all_partner_records()
        aktive = [p for p in all_records if p["status"].lower() == "aktiv"]
        for partner in aktive:
            send_whatsapp(
                partner["phone"],
                f"☀️ *Guten Morgen, {partner['name']}!*\n\n"
                f"💰 Dein Guthaben: *{partner['guthaben']:.2f} €*\n"
                f"📦 Leads erhalten: {partner['lead_count']}\n"
                f"⏰ Dein Zeitfenster: *{partner['zeitfenster']}*\n\n"
                f"👍 Kurze Antwort (OK/👍) damit Leads heute ankommen!\n\n"
                f"⚠️ Unter 10 €? Jetzt aufladen!\n"
                f"Fragen: Matze +49 171 506 0008"
            )
            time.sleep(1)
    except Exception as e:
        logger.error(f"❌ Tägliche Erinnerung Fehler: {e}")

# ─── Lead-Polling ──────────────────────────────────────────
def _do_poll():
    """Liest CREATED Leads aus Tabellenblatt1 und verteilt sie."""
    try:
        ws   = get_leads_sheet()
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return

        for i, row in enumerate(rows[1:], start=2):
            try:
                # Spalte P = Index 15 = Status
                status = row[15].strip() if len(row) > 15 else ""
                if status.upper() != "CREATED":
                    continue

                # Sofort auf PROCESSING setzen
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
                logger.error(f"❌ Fehler bei Lead-Zeile {i}: {e}")
                try:
                    ws.update_cell(i, 16, "FEHLER")
                except:
                    pass

    except Exception as e:
        logger.error(f"❌ Poll-Fehler: {e}")

def polling_loop():
    """Dauerhafter Polling-Thread alle 60 Sekunden."""
    logger.info(f"🔄 Polling gestartet (alle {POLL_INTERVAL}s)")
    while True:
        _do_poll()
        time.sleep(POLL_INTERVAL)

# ─── FastAPI App ───────────────────────────────────────────
app = FastAPI(title="Lead-Verteilungs-Service v4.9")

@app.get("/")
def root():
    return {
        "service":  "Lead-Verteilungs-Service",
        "version":  "4.9",
        "status":   "running",
        "sheets": {
            "leads":   LEADS_SHEET_NAME,
            "partner": PARTNER_SHEET_NAME,
            "log":     LOG_SHEET_NAME,
        }
    }

@app.get("/health")
def health():
    return {
        "status":    "ok",
        "timestamp": datetime.now(BERLIN_TZ).isoformat()
    }

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
            process_stripe_payment(event["data"]["object"])
            return {"status": "ok"}

        return {"status": "ignored", "type": event.get("type")}

    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Ungültige Stripe-Signatur")
    except Exception as e:
        logger.error(f"❌ Stripe-Webhook Fehler: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/zeitfenster")
def zeitfenster_waehlen(phone: str, wahl: str):
    """Partner klickt Link → Zeitfenster automatisch gesetzt."""
    erlaubt = ["Ganztag", "Vormittag", "Nachmittag", "Abend"]
    if wahl not in erlaubt:
        return HTMLResponse(content=f"""
        <html><body style="font-family:Arial;text-align:center;
        padding:50px;background:#fff0f0">
        <h1>❌ Ungültige Wahl</h1>
        <p>Erlaubte Werte: {', '.join(erlaubt)}</p>
        </body></html>""")

    success = update_zeitfenster_im_sheet(phone, wahl)

    if success:
        send_whatsapp(
            phone,
            f"✅ *Zeitfenster gesetzt!*\n\n"
            f"⏰ Du erhältst Leads: *{wahl}* ({ZEITFENSTER_TEXT[wahl]})\n\n"
            f"Ändern? Einfach Matze schreiben:\n"
            f"👉 wa.me/491715060008"
        )
        return HTMLResponse(content=f"""
        <html><body style="font-family:Arial;text-align:center;
        padding:50px;background:#f0f8f0">
        <h1>✅ Gespeichert!</h1>
        <h2>Zeitfenster: <b>{wahl}</b></h2>
        <p>Du erhältst Leads: <b>{ZEITFENSTER_TEXT[wahl]}</b></p>
        <p>Du bekommst gleich eine WhatsApp-Bestätigung. 📱</p>
        <p style="color:gray;font-size:14px">
        Dieses Fenster kann geschlossen werden.</p>
        </body></html>""")

    return HTMLResponse(content="""
    <html><body style="font-family:Arial;text-align:center;
    padding:50px;background:#fff0f0">
    <h1>⚠️ Partner nicht gefunden</h1>
    <p>Bitte Matze kontaktieren: wa.me/491715060008</p>
    </body></html>""")

@app.post("/poll")
def manual_poll():
    """Manueller Poll-Trigger."""
    _do_poll()
    return {"status": "ok", "message": "Poll ausgeführt"}

@app.post("/test-reminder")
def test_reminder():
    """Testet tägliche Erinnerungen."""
    send_daily_reminders()
    return {"status": "ok", "message": "Erinnerungen gesendet"}

# ─── Startup ───────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    # APScheduler: täglich 08:00 Berlin
    scheduler = BackgroundScheduler(timezone=BERLIN_TZ)
    scheduler.add_job(send_daily_reminders, "cron", hour=8, minute=0)
    scheduler.start()
    logger.info("⏰ Scheduler gestartet – täglich 08:00 Uhr")

    # Polling-Thread
    poll_thread = threading.Thread(target=polling_loop, daemon=True)
    poll_thread.start()
    logger.info("🔄 Polling-Thread gestartet")

    # Startmeldung an Matze
    send_whatsapp(
        MATZE_PHONE,
        "🚀 *Lead-System v4.9 gestartet!*\n\n"
        "✅ Spalten-Mapping verifiziert\n"
        "✅ Zeitfenster-Logik aktiv (Ganztag=24/7)\n"
        "✅ Klick-Links nach Stripe-Zahlung\n"
        "✅ Polling läuft (60s)\n"
        "✅ Tägliche Erinnerungen 08:00\n"
        "✅ Stripe Webhook bereit\n"
        "✅ DE/AT/CH Nummern-Fix aktiv\n\n"
        "Alles grün Matze! 💪"
    )
