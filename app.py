# ============================================================
# Lead-Verteilungs-Service v6.5 FINAL
# ============================================================
# Basis: v4.9 (stabil) + v6.3 + alle Fixes
# ============================================================
# FIXES in v6.5 (basiert auf v6.4):
# ✅ Auto-Erkennung vertauschter Felder (Email/Name/Phone)
# ✅ Kampagnen-Info in Partner-Nachricht
# ✅ Verständliche Nachrichten (kein Häkchen/X Kauderwelsch)
# ✅ Poll-Fehler NICHT mehr an Matze senden
# ✅ Admin-Nachricht zeigt was Partner bekommen hat
# ✅ 24h-Fenster verständlich erklärt
# ✅ Spalten verifiziert per CSV-Export:
#    A=id B=created_time C=ad_id D=ad_name E=adset_id
#    F=adset_name G=campaign_id H=campaign_name I=form_id
#    J=form_name K=is_organic L=platform
#    M=e-mail-adresse N=vollständiger_name O=telefonnummer
#    P=lead_status
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
LINA_WA_NUMBER        = os.getenv("LINA_WA_NUMBER", "4915170605019")
STRIPE_PAYMENT_LINK   = os.getenv("STRIPE_PAYMENT_LINK", "")

# ─── Konstanten ────────────────────────────────────────────
LEAD_PREIS     = 5.0
POLL_INTERVAL  = 60
BERLIN_TZ      = pytz.timezone("Europe/Berlin")

LEADS_SHEET_NAME   = "Tabellenblatt1"
PARTNER_SHEET_NAME = "Partner_Konto"
LOG_SHEET_NAME     = "Leads_Log"

# ─── Spalten Partner_Konto (1-basiert für update_cell) ─────
COL_NAME        = 1   # A – Name
COL_TELEFON     = 2   # B – Telefon
COL_GUTHABEN    = 3   # C – Guthaben_Euro
COL_LEADS       = 4   # D – Leads_Geliefert
COL_LETZTER     = 5   # E – Letzter_Lead_Am
COL_STATUS      = 6   # F – Status (Aktiv/Pausiert)
COL_ZEITFENSTER = 7   # G – Zeitfenster
COL_EMAIL       = 8   # H – Email

# ─── Spalten Tabellenblatt1 (0-basiert für row[index]) ─────
# Header: M=e-mail-adresse(12) N=vollständiger_name(13) O=telefonnummer(14) P=lead_status(15)
# ACHTUNG: Manche Facebook-Formulare schreiben die Daten vertauscht!
# Deswegen: Auto-Erkennung per Inhalt (@ = Email, p:+ = Phone)
LEAD_COL_FIELD1 = 12  # M – normalerweise Email, manchmal Name
LEAD_COL_FIELD2 = 13  # N – normalerweise Name, manchmal Phone
LEAD_COL_FIELD3 = 14  # O – normalerweise Phone, manchmal leer
LEAD_COL_STATUS = 15  # P – Status (CREATED/VERTEILT/etc.)

# Weitere Spalten für Kampagnen-Info
LEAD_COL_AD_NAME       = 3   # D – ad_name
LEAD_COL_CAMPAIGN_NAME = 7   # H – campaign_name
LEAD_COL_ADSET_NAME    = 5   # F – adset_name
LEAD_COL_FORM_NAME     = 9   # J – form_name

# ─── Zeitfenster ───────────────────────────────────────────
ZEITFENSTER = {
    "ganztag":    None,
    "vormittag":  (8,  12),
    "nachmittag": (12, 17),
    "abend":      (17, 22),
}
ZEITFENSTER_TEXT = {
    "ganztag":    "Ganztag (08–22 Uhr)",
    "vormittag":  "Vormittag (08–12 Uhr)",
    "nachmittag": "Nachmittag (12–17 Uhr)",
    "abend":      "Abend (17–22 Uhr)",
}

def partner_ist_verfuegbar(zeitfenster: str) -> bool:
    zf = zeitfenster.strip().lower() if zeitfenster else "ganztag"
    fenster = ZEITFENSTER.get(zf, None)
    if fenster is None:
        return True
    now_hour = datetime.now(BERLIN_TZ).hour
    start, end = fenster
    return start <= now_hour < end

# ─── Stripe Init ───────────────────────────────────────────
if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ─── Google Sheets Auth (v4.9 exakt) ──────────────────────
SCOPES = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]

def get_google_client():
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON ist nicht gesetzt!")
    creds_dict = json.loads(creds_json)
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

# ─── Phone-Normalisierung DE / AT / CH ────────────────────
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

# ─── Auto-Erkennung: Email / Name / Phone ─────────────────
def looks_like_email(val: str) -> bool:
    """Prüft ob ein Wert wie eine Email aussieht (enthält @)."""
    return '@' in val

def looks_like_phone(val: str) -> bool:
    """Prüft ob ein Wert wie eine Telefonnummer aussieht (p:+ oder nur Ziffern/+)."""
    v = val.strip()
    if v.startswith('p:'):
        return True
    digits = re.sub(r'[\s\-\(\)\+]', '', v)
    return len(digits) >= 8 and digits.isdigit()

def smart_extract_lead_fields(field1: str, field2: str, field3: str) -> dict:
    """
    Erkennt automatisch welches Feld Email, Name und Phone ist.
    Facebook schreibt die Daten manchmal in der falschen Reihenfolge.
    """
    fields = [field1.strip(), field2.strip(), field3.strip()]
    email = ""
    name = ""
    phone = ""

    # Schritt 1: Email finden (enthält @)
    for f in fields:
        if f and looks_like_email(f):
            email = f
            break

    # Schritt 2: Phone finden (p:+ oder nur Ziffern)
    for f in fields:
        if f and f != email and looks_like_phone(f):
            phone = f
            break

    # Schritt 3: Name = was übrig bleibt
    for f in fields:
        if f and f != email and f != phone:
            name = f
            break

    # Fallback: Wenn nichts erkannt, Reihenfolge wie Header
    if not email and not name and not phone:
        email = field1.strip()
        name = field2.strip()
        phone = field3.strip()

    return {
        "email": email,
        "name": name if name else "Unbekannt",
        "phone": normalize_phone(phone),
    }

# ─── Funnel-Mapping ───────────────────────────────────────
# Ordnet Kampagnen/Anzeigen einem verständlichen Funnel zu,
# damit Partner wissen was der Lead gesehen hat.
# ──────────────────────────────────────────────────────────

FUNNEL_MAPPING = [
    {
        "keywords": ["auto", "firmenwagen", "porsche", "bmw", "amg", "reel"],
        "label": "LR Auto & Firmenwagen",
        "emoji": "🚗",
        "beschreibung": (
            "Der Lead hat eine Anzeige zum LR Firmenwagen-Konzept gesehen. "
            "Er erwartet Infos, wie er mit LR starten kann, um sich einen "
            "Firmenwagen zu absoluten Sonderkonditionen zu sichern."
        ),
    },
    {
        "keywords": ["wage", "clean", "online", "garantie", "bonus"],
        "label": "Online-Business + Bonus",
        "emoji": "💻",
        "beschreibung": (
            "Der Lead hat eine Anzeige zum Online-Business mit garantiertem "
            "Bonus gesehen. Er erwartet ein klares System, wie er mit LR "
            "online starten und sich seinen garantierten Bonus sichern kann."
        ),
    },
    {
        "keywords": ["nebenverdienst", "hauptverdienst", "3 fragen", "info-paket"],
        "label": "Nebenverdienst → Hauptverdienst",
        "emoji": "💰",
        "beschreibung": (
            "Der Lead hat eine Anzeige gesehen mit der Frage: Was wäre, wenn "
            "dein Nebenverdienst dein Hauptverdienst wird? Er erwartet ein "
            "Konzept, wie er mit LR Schritt für Schritt ein Einkommen aufbauen kann."
        ),
    },
    {
        "keywords": ["lifestyle", "monatlicher bonus", "2000", "2.000"],
        "label": "LR Lifestyle – monatlicher Bonus",
        "emoji": "✨",
        "beschreibung": (
            "Der Lead hat eine Anzeige zu LR LIFESTYLE mit bis zu 2.000€ "
            "monatlichem Bonus gesehen. Er erwartet Infos, wie er mit LR "
            "ein stabiles monatliches Zusatz-Einkommen aufbauen kann."
        ),
    },
    {
        "keywords": ["lr business", "lead kam", "retargeting", "optimiert"],
        "label": "LR Business allgemein",
        "emoji": "🏢",
        "beschreibung": (
            "Der Lead hat eine allgemeine LR Business-Anzeige gesehen "
            "(inkl. Retargeting). Er erwartet Infos, wie er mit LR ein "
            "zusätzliches Einkommen aufbauen kann."
        ),
    },
    {
        "keywords": ["lina", "voice", "akquise"],
        "label": "LINA Voice Akquise",
        "emoji": "🎙️",
        "beschreibung": (
            "Der Lead wurde über LINA Voice (KI-Akquise) generiert."
        ),
    },
    {
        "keywords": ["gesundheit", "health", "produkt", "aloe"],
        "label": "LR Gesundheitsprodukte",
        "emoji": "💚",
        "beschreibung": (
            "Der Lead hat eine Anzeige zu LR Gesundheitsprodukten gesehen."
        ),
    },
]

def get_funnel_info(ad_name: str, campaign_name: str, adset_name: str = "") -> dict:
    """
    Ordnet einen Lead einem Funnel zu basierend auf ad_name, campaign_name, adset_name.
    Gibt dict zurück mit 'label', 'emoji', 'beschreibung'.
    """
    search_text = f"{ad_name} {campaign_name} {adset_name}".lower()

    for funnel in FUNNEL_MAPPING:
        for kw in funnel["keywords"]:
            if kw in search_text:
                return funnel

    # Fallback: Kampagnenname oder Anzeigenname als Label
    fallback_name = campaign_name or ad_name or "Werbeanzeige"
    return {
        "label": fallback_name,
        "emoji": "📊",
        "beschreibung": f"Der Lead hat eine Werbeanzeige gesehen ({fallback_name}).",
    }

# ─── WhatsApp senden ───────────────────────────────────────
def send_whatsapp(phone: str, message: str, _skip_admin: bool = False) -> bool:
    if not META_TOKEN or not META_PHONE_ID:
        logger.warning("META_TOKEN oder META_PHONE_ID fehlt!")
        return False
    if not phone:
        logger.warning("Keine Telefonnummer angegeben!")
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
            logger.info(f"✅ WhatsApp gesendet → {phone}")
            return True

        error_code = data.get("error", {}).get("code", 0)
        error_msg = data.get("error", {}).get("message", "Unbekannt")

        if error_code in [100, 131047]:
            logger.warning(f"⚠️ 24h-Fenster geschlossen für {phone}")
            if not _skip_admin:
                send_whatsapp(
                    MATZE_PHONE,
                    f"⚠️ *WhatsApp konnte nicht zugestellt werden*\n\n"
                    f"📱 An: {phone}\n"
                    f"📌 Grund: Der Empfänger hat in den letzten 24 Stunden "
                    f"nicht mit unserer Business-Nummer geschrieben.\n"
                    f"WhatsApp erlaubt erst dann Nachrichten, wenn der "
                    f"Empfänger uns zuerst schreibt.\n\n"
                    f"👉 *Lösung:* Empfänger soll einmal kurz Lina schreiben:\n"
                    f"https://wa.me/{LINA_WA_NUMBER}\n\n"
                    f"Danach können wir ihm Nachrichten senden.",
                    _skip_admin=True
                )
        else:
            logger.error(f"❌ WhatsApp Fehler {error_code}: {error_msg}")
            if not _skip_admin:
                send_whatsapp(
                    MATZE_PHONE,
                    f"🚨 *WhatsApp Fehler!*\n\n"
                    f"📱 Empfänger: {phone}\n"
                    f"❌ Fehler: {error_msg}\n"
                    f"Code: {error_code}",
                    _skip_admin=True
                )
        return False
    except Exception as e:
        logger.error(f"❌ WhatsApp Exception: {e}")
        return False

# ─── Partner lesen (mit Retry) ─────────────────────────────
def get_all_partner_records():
    for attempt in range(3):
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
                        "guthaben":    guthaben,
                        "email":       str(row.get("Email", "")).strip().lower(),
                        "status":      str(row.get("Status", "Aktiv")).strip(),
                        "lead_count":  int(
                            str(row.get("Leads_Geliefert", 0))
                            .replace(",", "").strip() or 0
                        ),
                        "zeitfenster": str(
                            row.get("Zeitfenster", "Ganztag")
                        ).strip() or "Ganztag",
                        "last_lead":   str(row.get("Letzter_Lead_Am", "")).strip(),
                    })
                except Exception as e:
                    logger.warning(f"Partner Zeile {i} Fehler: {e}")
            return result
        except Exception as e:
            logger.error(f"❌ Partner-Sheet Fehler (Versuch {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(5)
    return []

# ─── Partner per Email finden (für Stripe) ─────────────────
def find_partner_by_email(email: str):
    if not email:
        return None
    all_records = get_all_partner_records()
    for p in all_records:
        if p["email"] == email.lower().strip():
            return p
    return None

# ─── Besten Partner finden ─────────────────────────────────
def find_best_partner():
    all_records = get_all_partner_records()

    verfuegbar = [
        p for p in all_records
        if p["status"].strip().lower() == "aktiv"
        and p["guthaben"] >= LEAD_PREIS
        and partner_ist_verfuegbar(p["zeitfenster"])
    ]

    if not verfuegbar:
        logger.info("⏰ Kein Partner im Zeitfenster – Fallback auf alle aktiven")
        verfuegbar = [
            p for p in all_records
            if p["status"].strip().lower() == "aktiv"
            and p["guthaben"] >= LEAD_PREIS
        ]

    if not verfuegbar:
        logger.warning("🚨 Kein aktiver Partner mit Guthaben!")
        send_whatsapp(
            MATZE_PHONE,
            "🚨 *Kein Partner verfügbar!*\n\n"
            "Keiner deiner aktiven Partner hat genug Guthaben.\n"
            "Bitte Partner zum Aufladen auffordern!",
            _skip_admin=True
        )
        return None

    return sorted(verfuegbar, key=lambda p: (p["last_lead"] or "0000"))[0]

# ─── Partner updaten ───────────────────────────────────────
def update_partner(row_index: int, new_guthaben: float, lead_count: int):
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

def update_partner_guthaben(email: str, betrag: float) -> dict:
    """Lädt Guthaben auf. Gibt Partner-Dict zurück oder None."""
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["email"] == email.lower().strip():
                new_g = p["guthaben"] + betrag
                ws.update_cell(p["row_index"], COL_GUTHABEN, round(new_g, 2))
                ws.update_cell(p["row_index"], COL_STATUS,   "Aktiv")
                logger.info(f"✅ Guthaben {email}: +{betrag}€ → {new_g}€")
                p["guthaben"] = new_g
                return p
        return None
    except Exception as e:
        logger.error(f"❌ Guthaben-Update Fehler: {e}")
        return None

def update_zeitfenster_im_sheet(phone: str, zeitfenster: str) -> bool:
    try:
        all_records = get_all_partner_records()
        ws = get_partner_sheet()
        for p in all_records:
            if p["phone"] == phone.strip():
                ws.update_cell(p["row_index"], COL_ZEITFENSTER, zeitfenster)
                logger.info(f"✅ Zeitfenster {phone} → {zeitfenster}")
                # Admin informieren
                zf_text = ZEITFENSTER_TEXT.get(zeitfenster.strip().lower(), zeitfenster)
                send_whatsapp(
                    MATZE_PHONE,
                    f"⏰ *Zeitfenster geändert!*\n\n"
                    f"👤 {p['name']}\n"
                    f"📱 {p['phone']}\n"
                    f"⏰ Neues Zeitfenster: {zf_text}",
                    _skip_admin=True
                )
                return True
        return False
    except Exception as e:
        logger.error(f"❌ Zeitfenster-Update Fehler: {e}")
        return False

# ─── Neuen Partner anlegen ────────────────────────────────
def add_new_partner(name: str, email: str, phone_raw: str, guthaben: float):
    try:
        ws = get_partner_sheet()
        phone = normalize_phone(phone_raw)
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        ws.append_row([
            name,
            phone,
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
def log_lead(lead_data: dict, partner: dict, neues_guthaben: float, wa_partner: bool, wa_lead: bool, status: str):
    try:
        ws = get_log_sheet()
        ws.append_row([
            datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
            lead_data.get("name", ""),
            lead_data.get("phone", ""),
            lead_data.get("email", ""),
            partner.get("name", ""),
            partner.get("phone", ""),
            f"{neues_guthaben:.2f}",
            "OK" if wa_partner else "FEHLER",
            "OK" if wa_lead else "KEINE NR",
            status,
            lead_data.get("campaign_name", ""),
            lead_data.get("ad_name", ""),
        ])
    except Exception as e:
        logger.error(f"❌ Log-Fehler: {e}")

# ─── Lead verarbeiten ──────────────────────────────────────
def process_lead(lead_data: dict):
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
    # Funnel-Info ermitteln
    adset_name = lead_data.get("adset_name", "")
    funnel = get_funnel_info(ad_name, campaign_name, adset_name)

    # ── Partner-Nachricht (vollständig + verständlich!) ──
    partner_msg = (
        f"╔════════════════════════════════════╗\n"
        f"║     🎯 NEUER LEAD FÜR DICH!        ║\n"
        f"╚════════════════════════════════════╝\n\n"
        f"👤 *Name:* {lead_name}\n"
    )
    if lead_phone:
        partner_msg += f"📞 *Telefon:* +{lead_phone}\n"
    if lead_email:
        partner_msg += f"📧 *Email:* {lead_email}\n"

    partner_msg += (
        f"\n{funnel['emoji']} *Funnel:* {funnel['label']}\n"
        f"💬 *Was der Lead gesehen hat:*\n"
        f"{funnel['beschreibung']}\n"
    )

    partner_msg += (
        f"\n🧠 *Technische Infos:*\n"
    )
    if campaign_name:
        partner_msg += f"📊 Kampagne: {campaign_name}\n"
    if adset_name:
        partner_msg += f"📋 Anzeigengruppe: {adset_name}\n"
    if ad_name:
        partner_msg += f"🎯 Anzeige: {ad_name}\n"

    partner_msg += (
        f"\n💰 *Dein Guthaben danach:* {neues_guthaben:.2f} €\n"
        f"📦 *Deine Leads insgesamt:* {new_lead_count}\n"
    )

    if neues_guthaben < 15:
        aufladen_text = ""
        if STRIPE_PAYMENT_LINK:
            aufladen_text = f"💳 Jetzt aufladen: {STRIPE_PAYMENT_LINK}\n"
        aufladen_text += f"💬 Oder schreib Lina: https://wa.me/{LINA_WA_NUMBER}"
        partner_msg += (
            f"\n⚠️ *Guthaben wird knapp!*\n"
            f"Lade jetzt auf, damit du weiter Leads bekommst:\n"
            f"{aufladen_text}\n"
        )

    partner_msg += (
        f"\n⚡ *Tipp: Ruf den Lead am besten sofort an!*\n"
        f"Je schneller du dich meldest, desto höher die Chance."
    )

    # ── Senden an Partner ──
    wa_partner_ok = send_whatsapp(partner["phone"], partner_msg)

    # ── Lead auch anschreiben wenn Nummer vorhanden ──
    wa_lead_ok = False
    lead_wa_info = "Keine Nummer vorhanden"
    if lead_phone:
        lead_msg = (
            f"Hallo {lead_name}! 👋\n\n"
            f"Vielen Dank für dein Interesse!\n"
            f"Dein persönlicher Ansprechpartner *{partner['name']}* "
            f"wird sich in Kürze bei dir melden.\n\n"
            f"Beste Grüße,\nDein LR Lifestyle Team 🚀"
        )
        wa_lead_ok = send_whatsapp(lead_phone, lead_msg)
        if wa_lead_ok:
            lead_wa_info = "Zugestellt"
        else:
            lead_wa_info = "Nicht zugestellt (Lead muss erst Lina schreiben)"

    # ── Admin-Nachricht (detailliert + verständlich!) ──
    admin_msg = (
        f"✅ *Neuer Lead verteilt!*\n\n"
        f"━━━ LEAD-DATEN ━━━\n"
        f"👤 Name: {lead_name}\n"
    )
    if lead_phone:
        admin_msg += f"📞 Telefon: +{lead_phone}\n"
    if lead_email:
        admin_msg += f"📧 Email: {lead_email}\n"
    admin_msg += (
        f"{funnel['emoji']} Funnel: {funnel['label']}\n"
    )
    if campaign_name:
        admin_msg += f"📊 Kampagne: {campaign_name}\n"
    if adset_name:
        admin_msg += f"📋 Anzeigengruppe: {adset_name}\n"
    if ad_name:
        admin_msg += f"🎯 Anzeige: {ad_name}\n"

    admin_msg += (
        f"\n━━━ ZUGEWIESEN AN ━━━\n"
        f"👤 Partner: {partner['name']}\n"
        f"📱 Tel: {partner['phone']}\n"
        f"💰 Guthaben: {partner['guthaben']:.2f}€ → {neues_guthaben:.2f}€\n"
        f"📦 Leads gesamt: {new_lead_count}\n"
    )

    admin_msg += (
        f"\n━━━ STATUS ━━━\n"
        f"📲 Partner-Nachricht: {'Zugestellt' if wa_partner_ok else 'NICHT zugestellt!'}\n"
        f"📲 Lead-Nachricht: {lead_wa_info}\n"
    )

    send_whatsapp(MATZE_PHONE, admin_msg, _skip_admin=True)

    # Sheet updaten
    update_partner(partner["row_index"], neues_guthaben, new_lead_count)

    # Loggen
    final_status = "VERTEILT" if wa_partner_ok else "VERTEILT_WA_FEHLER"
    log_lead(lead_data, partner, neues_guthaben, wa_partner_ok, wa_lead_ok, final_status)

    logger.info(
        f"✅ Lead '{lead_name}' → Partner '{partner['name']}' | "
        f"Guthaben: {neues_guthaben:.2f}€"
    )
    return True

# ─── Stripe Webhook Verarbeitung ──────────────────────────
def process_stripe_payment(session: dict):
    try:
        email        = session.get("customer_details", {}).get("email", "")
        amount_cents = session.get("amount_total", 0)
        betrag       = amount_cents / 100.0
        name         = session.get("customer_details", {}).get("name", "Unbekannt")
        phone_raw    = session.get("customer_details", {}).get("phone", "")

        if not email:
            logger.warning("Stripe: Keine E-Mail in Session")
            send_whatsapp(
                MATZE_PHONE,
                f"⚠️ *Stripe Zahlung ohne Email!*\n\n"
                f"💰 Betrag: {betrag:.2f}€\n"
                f"👤 Name: {name}\n"
                f"Konnte keinem Partner zugeordnet werden.",
                _skip_admin=True
            )
            return

        # Partner per Email suchen
        existing = find_partner_by_email(email)

        if existing:
            # ── Bestehender Partner: Guthaben aufladen ──
            updated = update_partner_guthaben(email, betrag)
            new_guthaben = updated["guthaben"] if updated else existing["guthaben"] + betrag
            partner_phone = existing["phone"]

            if partner_phone:
                # Zeitfenster-Links
                link_g = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag"
                link_v = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag"
                link_n = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag"
                link_a = f"{APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend"

                wa_ok = send_whatsapp(
                    partner_phone,
                    f"💰 *Guthaben aufgeladen!*\n\n"
                    f"Hallo {existing['name']},\n\n"
                    f"✅ *+{betrag:.0f} €* wurden deinem Konto gutgeschrieben.\n"
                    f"💳 *Neues Guthaben:* {new_guthaben:.2f} €\n"
                    f"⏰ *Dein Zeitfenster:* {existing['zeitfenster']}\n\n"
                    f"Zeitfenster ändern? Einfach antippen:\n"
                    f"1️⃣ Ganztag (08–22h): {link_g}\n"
                    f"2️⃣ Vormittag (08–12h): {link_v}\n"
                    f"3️⃣ Nachmittag (12–17h): {link_n}\n"
                    f"4️⃣ Abend (17–22h): {link_a}\n\n"
                    f"Du erhältst ab sofort wieder Leads! 🚀"
                )
            else:
                wa_ok = False

            # Admin-Info
            send_whatsapp(
                MATZE_PHONE,
                f"💳 *Guthaben aufgeladen!*\n\n"
                f"👤 Partner: {existing['name']}\n"
                f"📧 Email: {email}\n"
                f"💰 Betrag: +{betrag:.0f}€\n"
                f"💳 Neues Guthaben: {new_guthaben:.2f}€\n"
                f"📲 Partner informiert: {'Ja' if wa_ok else 'Nein (24h-Fenster)'}",
                _skip_admin=True
            )

        else:
            # ── Neuer Partner ──
            add_new_partner(name, email, phone_raw, betrag)

            partner_phone = normalize_phone(phone_raw)

            if partner_phone:
                # Nachricht 1: Willkommen
                wa_ok = send_whatsapp(
                    partner_phone,
                    f"🎉 *Willkommen im LR Lifestyle Team!*\n\n"
                    f"Hallo {name},\n\n"
                    f"✅ Deine Registrierung ist bestätigt!\n"
                    f"💳 *Startguthaben:* {betrag:.2f} €\n\n"
                    f"📌 *So geht's weiter:*\n\n"
                    f"*Schritt 1:* Schreib kurz Lina (unsere KI):\n"
                    f"👉 https://wa.me/{LINA_WA_NUMBER}\n"
                    f"(Damit du Leads per WhatsApp empfangen kannst)\n\n"
                    f"*Schritt 2:* Wähle dein Zeitfenster für Leads:\n"
                    f"1️⃣ Ganztag (08–22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"2️⃣ Vormittag (08–12h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"3️⃣ Nachmittag (12–17h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"4️⃣ Abend (17–22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend\n\n"
                    f"Fragen? Matze: wa.me/491715060008\n\n"
                    f"🚀 Viel Erfolg!"
                )
            else:
                wa_ok = False

            # Admin-Info
            send_whatsapp(
                MATZE_PHONE,
                f"🆕 *Neuer Partner registriert!*\n\n"
                f"👤 Name: {name}\n"
                f"📧 Email: {email}\n"
                f"📱 Telefon: {partner_phone or 'Keine Nummer'}\n"
                f"💰 Startguthaben: {betrag:.2f}€\n"
                f"📲 Willkommen-Nachricht: {'Zugestellt' if wa_ok else 'Nicht zugestellt'}",
                _skip_admin=True
            )

    except Exception as e:
        logger.error(f"❌ Stripe-Verarbeitung Fehler: {e}")
        send_whatsapp(
            MATZE_PHONE,
            f"🚨 *Stripe Fehler!*\n\n{str(e)[:300]}",
            _skip_admin=True
        )

# ─── Tägliche Erinnerungen 08:00 Uhr ──────────────────────
def send_daily_reminders():
    logger.info("📅 Tägliche Erinnerungen werden gesendet...")
    try:
        all_records = get_all_partner_records()
        aktive = [p for p in all_records if p["status"].strip().lower() == "aktiv"]

        gesendet = []
        fehlgeschlagen = []

        for p in aktive:
            phone = p["phone"]
            name = p["name"]
            guthaben = p["guthaben"]
            zeitfenster = p.get("zeitfenster", "Ganztag")
            zf_text = ZEITFENSTER_TEXT.get(zeitfenster.strip().lower(), zeitfenster)

            # Zeitfenster-Links
            link_g = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Ganztag"
            link_v = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Vormittag"
            link_n = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Nachmittag"
            link_a = f"{APP_URL}/zeitfenster?phone={phone}&wahl=Abend"

            # Guthaben-Warnung
            if guthaben < 5:
                guthaben_info = f"🔴 *ACHTUNG: Guthaben aufgebraucht!* ({guthaben:.2f}€)\nDu bekommst KEINE Leads mehr!"
            elif guthaben < 15:
                guthaben_info = f"🟡 *Guthaben wird knapp:* {guthaben:.2f}€\nNoch {int(guthaben / LEAD_PREIS)} Leads möglich."
            else:
                guthaben_info = f"🟢 *Guthaben:* {guthaben:.2f}€\nNoch {int(guthaben / LEAD_PREIS)} Leads möglich."

            aufladen_text = ""
            if guthaben < 30:
                if STRIPE_PAYMENT_LINK:
                    aufladen_text = f"\n💳 Jetzt aufladen: {STRIPE_PAYMENT_LINK}"
                aufladen_text += f"\n💬 Oder schreib Lina: https://wa.me/{LINA_WA_NUMBER}"

            msg = (
                f"🚨🚨🚨 *GUTEN MORGEN {name.upper()}!* 🚨🚨🚨\n\n"
                f"Hier ist dein tägliches Update:\n\n"
                f"{guthaben_info}\n"
                f"⏰ *Zeitfenster:* {zf_text}\n"
                f"{aufladen_text}\n\n"
                f"Zeitfenster ändern? Einfach antippen:\n"
                f"1️⃣ Ganztag (08–22h): {link_g}\n"
                f"2️⃣ Vormittag (08–12h): {link_v}\n"
                f"3️⃣ Nachmittag (12–17h): {link_n}\n"
                f"4️⃣ Abend (17–22h): {link_a}\n\n"
                f"Einen erfolgreichen Tag! 💪🚀"
            )

            ok = send_whatsapp(phone, msg)
            if ok:
                gesendet.append(f"  ✅ {name} ({guthaben:.0f}€)")
            else:
                fehlgeschlagen.append(f"  ❌ {name} ({phone})")

            time.sleep(2)  # Rate-Limit Schutz

        # Admin-Summary
        summary = (
            f"📊 *Tages-Report {datetime.now(BERLIN_TZ).strftime('%d.%m.%Y')}*\n\n"
        )

        if gesendet:
            summary += f"*Morgen-Nachricht zugestellt ({len(gesendet)}):*\n"
            summary += "\n".join(gesendet)
            summary += "\n\n"

        if fehlgeschlagen:
            summary += f"*Nicht zugestellt ({len(fehlgeschlagen)}):*\n"
            summary += "\n".join(fehlgeschlagen)
            summary += f"\n→ Diese Partner müssen erst Lina schreiben!\n\n"

        pausierte = [p for p in all_records if p["status"].strip().lower() != "aktiv"]
        if pausierte:
            summary += f"*Pausiert ({len(pausierte)}):*\n"
            for p in pausierte:
                summary += f"  ⏸️ {p['name']} ({p['guthaben']:.0f}€)\n"
            summary += "\n"

        summary += f"*Gesamt:* {len(all_records)} Partner | {len(aktive)} aktiv | {len(pausierte)} pausiert"

        send_whatsapp(MATZE_PHONE, summary, _skip_admin=True)

    except Exception as e:
        logger.error(f"❌ Tägliche Erinnerung Fehler: {e}")
        send_whatsapp(
            MATZE_PHONE,
            f"🚨 *Reminder-Fehler!*\n\n{str(e)[:300]}",
            _skip_admin=True
        )

# ─── Lead-Polling ──────────────────────────────────────────
def _do_poll():
    try:
        ws   = get_leads_sheet()
        rows = ws.get_all_values()
        if not rows or len(rows) < 2:
            return

        for i, row in enumerate(rows[1:], start=2):
            try:
                status = row[LEAD_COL_STATUS].strip() if len(row) > LEAD_COL_STATUS else ""
                if status.upper() != "CREATED":
                    continue

                # Sofort auf PROCESSING setzen
                ws.update_cell(i, LEAD_COL_STATUS + 1, "PROCESSING")

                # Rohdaten aus den 3 Feldern lesen
                raw_field1 = row[LEAD_COL_FIELD1] if len(row) > LEAD_COL_FIELD1 else ""
                raw_field2 = row[LEAD_COL_FIELD2] if len(row) > LEAD_COL_FIELD2 else ""
                raw_field3 = row[LEAD_COL_FIELD3] if len(row) > LEAD_COL_FIELD3 else ""

                # Auto-Erkennung: Email / Name / Phone
                extracted = smart_extract_lead_fields(raw_field1, raw_field2, raw_field3)

                # Kampagnen-Daten
                ad_name       = row[LEAD_COL_AD_NAME].strip() if len(row) > LEAD_COL_AD_NAME else ""
                campaign_name = row[LEAD_COL_CAMPAIGN_NAME].strip() if len(row) > LEAD_COL_CAMPAIGN_NAME else ""

                adset_name    = row[LEAD_COL_ADSET_NAME].strip() if len(row) > LEAD_COL_ADSET_NAME else ""

                lead_data = {
                    "lead_id":       row[0]  if len(row) > 0  else "",
                    "ad_id":         row[2]  if len(row) > 2  else "",
                    "ad_name":       ad_name,
                    "campaign_name": campaign_name,
                    "adset_name":    adset_name,
                    "email":         extracted["email"],
                    "name":          extracted["name"],
                    "phone":         extracted["phone"],
                }

                logger.info(
                    f"📋 Lead Zeile {i}: "
                    f"Raw=['{raw_field1}', '{raw_field2}', '{raw_field3}'] → "
                    f"Name='{lead_data['name']}', "
                    f"Email='{lead_data['email']}', "
                    f"Phone='{lead_data['phone']}'"
                )

                success = process_lead(lead_data)
                ws.update_cell(i, LEAD_COL_STATUS + 1, "VERTEILT" if success else "FEHLER")

            except Exception as e:
                logger.error(f"❌ Fehler bei Lead-Zeile {i}: {e}")
                try:
                    ws.update_cell(i, LEAD_COL_STATUS + 1, "FEHLER")
                except Exception:
                    pass

    except json.JSONDecodeError as e:
        # Google API Rate Limit – NUR loggen, NICHT an Matze senden
        logger.warning(f"⚠️ Google API Rate Limit (nächster Poll versucht es erneut): {e}")
    except Exception as e:
        logger.error(f"❌ Poll-Fehler: {e}")

def polling_loop():
    logger.info(f"🔄 Polling gestartet (alle {POLL_INTERVAL}s)")
    consecutive_errors = 0
    while True:
        try:
            _do_poll()
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"❌ Polling-Loop Fehler #{consecutive_errors}: {e}")
            if consecutive_errors >= 5:
                send_whatsapp(
                    MATZE_PHONE,
                    f"🚨 *Polling-Problem!*\n\n"
                    f"5x hintereinander fehlgeschlagen.\n"
                    f"Letzter Fehler: {str(e)[:200]}\n\n"
                    f"System versucht weiter automatisch.",
                    _skip_admin=True
                )
                consecutive_errors = 0
        time.sleep(POLL_INTERVAL)

# ─── Sheet-Header Validierung ─────────────────────────────
def validate_sheet_headers():
    try:
        ws = get_partner_sheet()
        headers = ws.row_values(1)
        if len(headers) >= 8:
            if headers[7].strip().lower() != "email":
                logger.warning(f"⚠️ Spalte H Header ist '{headers[7]}' statt 'Email'!")
                send_whatsapp(
                    MATZE_PHONE,
                    f"⚠️ *Sheet-Header Warnung!*\n\n"
                    f"Spalte H im Partner_Konto heißt '{headers[7]}' statt 'Email'.\n"
                    f"Bitte korrigieren!",
                    _skip_admin=True
                )
                return False
        logger.info("✅ Sheet-Header validiert")
        return True
    except Exception as e:
        logger.error(f"❌ Header-Validierung Fehler: {e}")
        return False

# ─── FastAPI App ───────────────────────────────────────────
app = FastAPI(title="Lead-Verteilungs-Service v6.5")

@app.get("/")
def root():
    return {
        "service":  "Lead-Verteilungs-Service",
        "version":  "6.5",
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
        "version":   "6.5",
        "timestamp": datetime.now(BERLIN_TZ).isoformat()
    }

@app.get("/status")
def status_check():
    try:
        all_records = get_all_partner_records()
        aktive = [p for p in all_records if p["status"].strip().lower() == "aktiv"]
        return {
            "status": "ok",
            "version": "6.4",
            "partner_gesamt": len(all_records),
            "partner_aktiv": len(aktive),
            "timestamp": datetime.now(BERLIN_TZ).isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/partner")
def list_partners():
    try:
        all_records = get_all_partner_records()
        return {
            "partner": [
                {
                    "name": p["name"],
                    "status": p["status"],
                    "guthaben": p["guthaben"],
                    "leads": p["lead_count"],
                    "zeitfenster": p["zeitfenster"],
                    "letzter_lead": p["last_lead"],
                }
                for p in all_records
            ],
            "gesamt": len(all_records),
            "aktiv": len([p for p in all_records if p["status"].strip().lower() == "aktiv"]),
        }
    except Exception as e:
        return {"error": str(e)}

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
            session = event["data"]["object"]
            t = threading.Thread(
                target=process_stripe_payment,
                args=(session,),
                daemon=True
            )
            t.start()
            return {"status": "ok"}

        return {"status": "ignored", "type": event.get("type")}

    except stripe.error.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Ungültige Stripe-Signatur")
    except Exception as e:
        logger.error(f"❌ Stripe-Webhook Fehler: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/zeitfenster")
def zeitfenster_waehlen(phone: str, wahl: str):
    erlaubt = ["Ganztag", "Vormittag", "Nachmittag", "Abend"]
    if wahl not in erlaubt:
        return HTMLResponse(content=f"""
        <html><body style="font-family:Arial;text-align:center;
        padding:50px;background:#fff0f0">
        <h1>❌ Ungültige Wahl</h1>
        <p>Erlaubte Werte: {', '.join(erlaubt)}</p>
        </body></html>""")

    success = update_zeitfenster_im_sheet(phone, wahl)
    zf_text = ZEITFENSTER_TEXT.get(wahl.strip().lower(), wahl)

    if success:
        send_whatsapp(
            phone,
            f"✅ *Zeitfenster gespeichert!*\n\n"
            f"⏰ Du erhältst Leads: *{zf_text}*\n\n"
            f"Ändern? Einfach erneut einen Link antippen."
        )
        return HTMLResponse(content=f"""
        <html><body style="font-family:Arial;text-align:center;
        padding:50px;background:#f0f8f0">
        <h1>✅ Gespeichert!</h1>
        <h2>Zeitfenster: <b>{wahl}</b></h2>
        <p>Du erhältst Leads: <b>{zf_text}</b></p>
        <p>Du bekommst gleich eine WhatsApp-Bestätigung. 📱</p>
        <br>
        <a href="https://wa.me/{LINA_WA_NUMBER}" style="
        display:inline-block;padding:15px 30px;background:#25D366;
        color:white;text-decoration:none;border-radius:10px;
        font-size:18px">💬 Lina schreiben</a>
        <p style="color:gray;font-size:14px;margin-top:20px">
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
    _do_poll()
    return {"status": "ok", "message": "Poll ausgeführt"}

@app.post("/test-reminder")
def test_reminder():
    t = threading.Thread(target=send_daily_reminders, daemon=True)
    t.start()
    return {"status": "ok", "message": "Erinnerungen werden gesendet"}

# ─── Startup ───────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    # Header validieren
    header_ok = validate_sheet_headers()

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
        f"🚀 *Lead-System v6.5 gestartet!*\n\n"
        f"✅ Polling aktiv (alle {POLL_INTERVAL}s)\n"
        f"✅ Tages-Erinnerung aktiv (08:00 Berlin)\n"
        f"✅ Stripe Webhook aktiv\n"
        f"✅ Zeitfenster-Links aktiv\n"
        f"✅ Admin-Infos bei jeder Aktion\n"
        f"✅ Auto-Erkennung Email/Name/Phone\n"
        f"✅ Funnel-Mapping (Partner sieht was Lead gesehen hat)\n"
        f"{'✅' if header_ok else '❌'} Sheet-Header geprüft\n"
        f"✅ Lina: {LINA_WA_NUMBER}\n\n"
        f"Alles läuft! 💪",
        _skip_admin=True
    )
