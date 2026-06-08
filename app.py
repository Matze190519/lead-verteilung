
# ============================================================
# Lead-Verteilungs-Service v8.0
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
import secrets
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

# ─── Persönliche Outreach-Nummern (35 Leads, 18.05.2026) ──────────────────────
# Diese 35 Leads haben heute eine persönliche Nachricht von Lina erhalten,
# in der Mathias sich als Absender vorgestellt hat.
# Eingehende Antworten dieser Nummern werden NICHT an Botpress weitergeleitet,
# sondern direkt an Mathias (+491715060008) weitergeleitet.
# Nummern sind normalisiert (ohne +, ohne Leerzeichen).
# Liste kann jederzeit geleert werden wenn die Aktion abgeschlossen ist.
PERSONAL_OUTREACH_NUMBERS = {
    "4915229525402",   # Heiko
    "34641689043",     # Ator
    "436641100575",    # Marco S.
    "4915757078693",   # Salman
    "4915217523018",   # El Mitevski
    "34685578351",     # Iulia
    "436502007572",    # Rupert F.
    "905452511212",    # Conwaynen
    "4915204796954",   # Ingolf
    "41791548025",     # Kamel
    "4917630745947",   # Marco Sch.
    "4915511558064",   # Gabryel
    "436763223082",    # Anton
    "436765998767",    # Adrian
    "34638434151",     # Alejandro
    "4917654125929",   # Frank R.
    "491629331646",    # Frank H.
    "41787568841",     # Gi Van Alin
    "34652035095",     # Giuseppe
    "491759979085",    # Lydia
    "436767609001",    # Rupert P.
    "41765415688",     # Mario
    "436765326122",    # Christoph
    "491791375028",    # Michael
    "436766738315",    # Manuel
    "4915151868158",   # Tanja
    "491733060912",    # Manfred
    "4366499294121",   # Jürgen
    "436764407041",    # Phillip
    "436602037576",    # David
    "4916098645201",   # Bast
    "436503630029",    # Joachim
    "491799798794",    # Burkhardt
    "4917622834125",   # Sasha
    "4917623101086",   # Agostino
}

BOTPRESS_WEBHOOK_URL = "https://webhook.botpress.cloud/30208abb-37b2-462b-90b9-0478eb3ff498"

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
    "ganztag":    "Ganztag (rund um die Uhr)",
    "vormittag":  "Vormittag (08–12 Uhr)",
    "nachmittag": "Nachmittag (12–17 Uhr)",
    "abend":      "Abend (17–22 Uhr)",
}

def partner_ist_verfuegbar(zeitfenster: str) -> bool:
    """Prüft ob ein Partner gerade Leads empfangen soll.
    Nachts (22-08 Uhr): ALLE Partner bekommen Leads, egal welches Zeitfenster.
    Tagsueber (08-22 Uhr): Zeitfenster-Logik greift normal.
    """
    now_hour = datetime.now(BERLIN_TZ).hour
    # Nachts (22-08): Alle Partner verfuegbar - Leads morgens auf dem Handy
    if now_hour >= 22 or now_hour < 8:
        return True
    zf = zeitfenster.strip().lower() if zeitfenster else "ganztag"
    fenster = ZEITFENSTER.get(zf, None)
    if fenster is None:
        return True
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
        # Kampagne: Matze NEU 05.10.2025
        # Verschiedene Bildanzeigen: Online Geld verdienen, garantierter Bonus,
        # Teamwork, 3 Fragen, Nebenverdienst wird Hauptverdienst
        # Formular: 500-3.000€/Monat neben Hauptjob, flexible Zeiten, von überall
        "keywords": ["wage", "clean", "online", "garantie", "bonus",
                     "matze neu", "von zu hause", "nebeneinkommen",
                     "3 fragen", "nebenverdienst", "hauptverdienst"],
        "label": "📸 Bild-Kampagne (Online Geld verdienen)",
        "emoji": "📸",
        "beschreibung": (
            "Bildanzeige: Online Geld verdienen, garantierter Bonus, Teamwork.\n"
            "Der Interessent sucht einen Nebenverdienst von 500-3.000€/Monat."
        ),
        "tipps": [
            "💡 Frag, was ihn am meisten angesprochen hat",
            "💡 Frag, was er sich von einem Nebenverdienst erhofft",
            "💡 Erkläre kurz, wie er mit LR 500-3.000€ nebenbei verdienen kann",
        ],
    },
    {
        # Kampagne: LR Business Lead Kampagne - OPTIMIERT mit Retargeting
        # LR Auto Reels: Videos mit Autos auf denen LR drauf steht
        # Autos 70-80% finanziert, ohne Anzahlung, ohne Schlussrate,
        # ab der ersten Stufe bestellbar, viele machen das am ersten Tag
        "keywords": ["lr business", "retargeting", "optimiert", "lead kam",
                     "auto", "reel", "firmenwagen", "porsche", "bmw", "amg"],
        "label": "🚗 LR Auto Reels (Firmenwagen-Programm)",
        "emoji": "🚗",
        "beschreibung": (
            "Video-Reel: Autos mit LR-Branding.\n"
            "LR Autos sind 70-80% günstiger dank Großhandelskonditionen –\n"
            "ohne Anzahlung, ohne Schlussrate, ab der ersten Stufe bestellbar."
        ),
        "tipps": [
            "💡 Frag, welches Auto ihm im Reel am besten gefallen hat",
            "💡 Erkläre: 70-80% günstiger dank Großhandel, keine Anzahlung, keine Schlussrate",
            "💡 Viele bestellen ihr Auto schon am ersten Tag – erwähne das!",
        ],
    },
]

def get_funnel_info(ad_name: str, campaign_name: str, adset_name: str = "") -> dict:
    """
    Ordnet einen Interessenten einem Funnel zu basierend auf ad_name, campaign_name, adset_name.
    Gibt dict zurück mit 'label', 'emoji', 'beschreibung', 'tipps'.
    
    Erkenntnisse aus Sheet-Analyse (1125 Leads):
    - ad_name="1002025_Reel_Mathias Porsche" + campaign="Matze NEU" → Auto Reels ✅
    - ad_name="1100225_neue Wage-Clean – Kopie 2" + campaign="Matze NEU" → Bild ✅
    - ad_name="Neue Anzeige für Leads" + campaign="LR Business Lead Kampagne - OPTIMIERT mit Retargeting" → Auto Reels!
    - ad_name="Neue Anzeige für Leads" + campaign="Neue Kampagne für Leads" → Bild (Standard)
    
    ACHTUNG: Meta ordnet bei Retargeting manchmal Leads der falschen Kampagne zu
    (First-Touch Attribution). Dagegen können wir nichts tun.
    """
    ad_lower = ad_name.lower().strip() if ad_name else ""
    camp_lower = campaign_name.lower().strip() if campaign_name else ""
    adset_lower = adset_name.lower().strip() if adset_name else ""

    # SCHRITT 1: ad_name enthält eindeutige Auto-Keywords → Auto Reels
    auto_ad_keywords = ["reel", "porsche", "bmw", "amg", "firmenwagen"]
    if any(kw in ad_lower for kw in auto_ad_keywords):
        return FUNNEL_MAPPING[1]  # Auto Reels

    # SCHRITT 2: ad_name enthält eindeutige Bild-Keywords → Bild-Kampagne
    bild_ad_keywords = ["wage", "clean", "bonus", "3 fragen", "schritt",
                        "nebenverdienst", "hauptverdienst"]
    if any(kw in ad_lower for kw in bild_ad_keywords):
        return FUNNEL_MAPPING[0]  # Bild-Kampagne

    # SCHRITT 3: ad_name ist generisch ("Neue Anzeige für Leads" etc.)
    # → Kampagne entscheidet!
    # "LR Business Lead Kampagne - OPTIMIERT mit Retargeting" = Auto Reels Kampagne
    if "retargeting" in camp_lower or "lr business" in camp_lower:
        return FUNNEL_MAPPING[1]  # Auto Reels (diese Kampagne IST die Auto-Reel-Kampagne)

    # SCHRITT 4: Kampagne "Matze NEU" mit generischem ad_name → Bild-Kampagne
    # (Die meisten Matze NEU Leads sind Bild-Kampagne)
    if "matze neu" in camp_lower:
        return FUNNEL_MAPPING[0]  # Bild-Kampagne

    # SCHRITT 5: Andere bekannte Kampagnen
    if "outcome_leads" in camp_lower or "neue kampagne" in camp_lower:
        return FUNNEL_MAPPING[0]  # Bild-Kampagne (Standard)

    if "lina voice" in camp_lower:
        return FUNNEL_MAPPING[0]  # Bild-Kampagne

    # SCHRITT 6: Fallback – wenn gar nichts passt
    fallback_name = campaign_name or ad_name or "Werbeanzeige"
    return {
        "label": fallback_name,
        "emoji": "📊",
        "beschreibung": f"Werbeanzeige ({fallback_name}).",
        "tipps": ["💡 Frag einfach, was sein Interesse geweckt hat"],
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

def send_whatsapp_buttons(phone: str, body_text: str, btn_anruf_id: str, btn_selbst_id: str) -> bool:
    """Sendet eine WhatsApp mit 2 Auswahl-Buttons (Lina anrufen / selbst melden)."""
    if not META_TOKEN or not META_PHONE_ID or not phone:
        return False
    # Lead-Daten fuer ButtonBridge merken (Botpress 'Lina anrufen' schlaegt sie nach)
    try:
        _p = btn_anruf_id.split("|")
        requests.post(
            "https://hook.eu2.make.com/nxg2korn8ogmhki0y7h4uf9xqg2razhw",
            json={"mode": "store",
                  "partnerPhone": re.sub(r"[^0-9]", "", phone),
                  "leadPhone": _p[1] if len(_p) > 1 else "",
                  "leadName": _p[2] if len(_p) > 2 else "Interessent",
                  "partnerName": _p[3] if len(_p) > 3 else ""},
            timeout=6,
        )
    except Exception as _e:
        logger.warning(f"ButtonBridge store fehlgeschlagen: {_e}")
    url = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"
    headers = {"Authorization": f"Bearer {META_TOKEN}", "Content-Type": "application/json"}
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text[:1024]},
            "action": {"buttons": [
                {"type": "reply", "reply": {"id": btn_anruf_id[:256], "title": "☎️ Lina anrufen"}},
                {"type": "reply", "reply": {"id": btn_selbst_id[:256], "title": "✋ Selbst melden"}},
            ]},
        },
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        if resp.status_code == 200:
            logger.info(f"✅ Button-WhatsApp gesendet → {phone}")
            return True
        logger.warning(f"⚠️ Button-WA fehlgeschlagen {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as e:
        logger.error(f"❌ Button-WA Exception: {e}")
        return False


# ─── Partner lesen (mit Retry) ─────────────────────────────
def get_all_partner_records():
    for attempt in range(3):
        try:
            ws = get_partner_sheet()
            records = ws.get_all_records(value_render_option='UNFORMATTED_VALUE')
            result = []
            for i, raw_row in enumerate(records, start=2):
                # Strip header keys to avoid trailing space issues
                row = {k.strip(): v for k, v in raw_row.items()}
                try:
                    guthaben_raw = row.get("Guthaben_Euro", 0)
                    # UNFORMATTED_VALUE gibt Zahlen direkt als float/int zurück
                    try:
                        guthaben = float(guthaben_raw) if guthaben_raw != '' else 0.0
                    except (ValueError, TypeError):
                        # Fallback für Text-Werte
                        guthaben = float(str(guthaben_raw).replace(",", ".").replace("€", "").strip() or 0)
                    result.append({
                        "row_index":   i,
                        "name":        str(row.get("Name", "")).strip(),
                        "phone":       str(int(row.get("Telefon", 0))) if isinstance(row.get("Telefon", ""), (int, float)) else str(row.get("Telefon", "")).strip(),
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
        # Prüfe ob es aktive Partner MIT Guthaben gibt, die nur nicht im Zeitfenster sind
        aktive_mit_guthaben = [
            p for p in all_records
            if p["status"].strip().lower() == "aktiv"
            and p["guthaben"] >= LEAD_PREIS
        ]
        if aktive_mit_guthaben:
            logger.info("⏰ Partner vorhanden, aber keiner im aktuellen Zeitfenster – Lead wird NICHT verteilt")
            namen = ', '.join([p['name'] for p in aktive_mit_guthaben])
            send_whatsapp(
                MATZE_PHONE,
                f"⏰ *Lead nicht verteilt – Zeitfenster!*\n\n"
                f"Es gibt aktive Partner ({namen}), aber keiner ist gerade im Zeitfenster.\n"
                f"Der Lead wird beim nächsten Polling erneut geprüft.",
                _skip_admin=True
            )
            return None
        else:
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
        # Zahl direkt als Float schreiben (kein String-Konvertierung)
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
            if p["phone"] == phone.strip() and p["status"].strip().lower() == "aktiv":
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
    """Legt neuen Partner an + generiert Token für Lead-Board. Returnt Token (oder '')."""
    try:
        ws = get_partner_sheet()
        phone = normalize_phone(phone_raw)
        now_str = datetime.now(BERLIN_TZ).strftime("%Y-%m-%d %H:%M:%S")
        # Token deterministisch ueber md5(name + SALT) — konsistent mit Make-Scenario 9296372
        import hashlib
        token = hashlib.md5((name + "lina-board-2026-x7k2").encode()).hexdigest()
        ws.append_row([
            name,
            phone,
            round(guthaben, 2),
            0,
            now_str,
            "Aktiv",
            "Ganztag",
            email,
            token,  # Spalte 9 = Token für Lead-Board
        ])
        logger.info(f"✅ Neuer Partner: {name} | {email} | {guthaben}€ | Token: {token[:8]}…")
        return token
    except Exception as e:
        logger.error(f"❌ Neuer Partner Fehler: {e}")
        return ""

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

# ─── Interessent verarbeiten ──────────────────────────────
def process_lead(lead_data: dict):
    lead_name     = lead_data.get("name", "Unbekannt")
    lead_phone    = lead_data.get("phone", "")
    lead_email    = lead_data.get("email", "")
    campaign_name = lead_data.get("campaign_name", "")
    ad_name       = lead_data.get("ad_name", "")

    partner = find_best_partner()
    if not partner:
        logger.warning(f"⚠️ Kein Partner für Interessent '{lead_name}'")
        return False

    neues_guthaben = partner["guthaben"] - LEAD_PREIS
    new_lead_count = partner["lead_count"] + 1
    adset_name = lead_data.get("adset_name", "")
    funnel = get_funnel_info(ad_name, campaign_name, adset_name)

    # ── Partner-Nachricht ──
    partner_msg = (
        f"╔════════════════════════════════════════╗\n"
        f"║   🎯 NEUER INTERESSENT FÜR DICH!       ║\n"
        f"╚════════════════════════════════════════╝\n\n"
        f"👤 *Name:* {lead_name}\n"
    )
    if lead_phone:
        partner_msg += f"📞 *Telefon:* +{lead_phone}\n"
    if lead_email:
        partner_msg += f"📧 *Email:* {lead_email}\n"

    partner_msg += (
        f"\n{funnel['emoji']} *Woher kommt der Interessent:*\n"
        f"{funnel['beschreibung']}\n"
    )

    # Tipps zum Ansprechen
    tipps = funnel.get('tipps', [])
    if tipps:
        partner_msg += f"\n📋 *So sprichst du ihn am besten an:*\n"
        for tipp in tipps:
            partner_msg += f"{tipp}\n"

    partner_msg += (
        f"\n💰 *Dein Guthaben danach:* {neues_guthaben:.2f} €\n"
        f"📦 *Deine Interessenten insgesamt:* {new_lead_count}\n"
    )

    if neues_guthaben < 15:
        aufladen_text = ""
        if STRIPE_PAYMENT_LINK:
            aufladen_text = f"💳 Jetzt aufladen: {STRIPE_PAYMENT_LINK}\n"
        aufladen_text += f"💬 Oder schreib Lina: https://wa.me/{LINA_WA_NUMBER}"
        partner_msg += (
            f"\n⚠️ *Guthaben wird knapp!*\n"
            f"Lade jetzt auf, damit du weiter Interessenten bekommst:\n"
            f"{aufladen_text}\n"
        )

    partner_msg += (
        f"\n⚡ *Ruf ihn am besten SOFORT an!*\n"
        f"Je schneller du dich meldest, desto höher die Chance."
    )

    # ── Senden an Partner (mit Auswahl-Buttons) ──
    _btn_body = (
        f"🎯 NEUER LEAD für dich!\n"
        f"👤 {lead_name}\n"
        f"📞 +{lead_phone}\n"
        f"{funnel['emoji']} {funnel['label']}\n"
        f"💰 Guthaben danach: {neues_guthaben:.0f} €\n\n"
        f"Wie möchtest du weitermachen?"
    )
    _anruf_id = f"anruf|{lead_phone}|{lead_name}|{partner['name']}"
    _selbst_id = f"selbst|{lead_phone}"
    wa_partner_ok = send_whatsapp_buttons(partner["phone"], _btn_body, _anruf_id, _selbst_id)
    if not wa_partner_ok:
        wa_partner_ok = send_whatsapp(partner["phone"], partner_msg)

    # ── Interessent-Nachricht DEAKTIVIERT ──
    # Interessent schreibt Matze direkt auf WhatsApp (01715060008).
    # Lina hat kein 24h-Fenster zum Interessenten offen.
    # → Nachricht an Interessent bringt nichts und riskiert Meta-Spam-Warnung.
    wa_lead_ok = False
    lead_wa_info = "Deaktiviert (Interessent kontaktiert Matze direkt)"

    # ── Admin-Nachricht an Matze (kompakt) ──
    phone_str = f"+{lead_phone}" if lead_phone else "–"
    admin_msg = (
        f"✅ *{lead_name}* → {partner['name']}\n"
        f"📞 {phone_str} | {funnel['emoji']} {funnel['label']}\n"
        f"💰 {partner['guthaben']:.0f}€ → {neues_guthaben:.0f}€ | "
        f"{'✅' if wa_partner_ok else '❌'} Partner-WA"
    )
    send_whatsapp(MATZE_PHONE, admin_msg, _skip_admin=True)

    # Sheet updaten
    update_partner(partner["row_index"], neues_guthaben, new_lead_count)

    # Loggen
    final_status = "VERTEILT" if wa_partner_ok else "VERTEILT_WA_FEHLER"
    log_lead(lead_data, partner, neues_guthaben, wa_partner_ok, wa_lead_ok, final_status)

    logger.info(
        f"✅ Interessent '{lead_name}' → Partner '{partner['name']}' | "
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
                    f"1️⃣ Ganztag (rund um die Uhr): {link_g}\n"
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
            new_token = add_new_partner(name, email, phone_raw, betrag)

            partner_phone = normalize_phone(phone_raw)

            # Board-Link nur einbauen wenn Token erfolgreich generiert
            board_link_block = ""
            if new_token:
                board_url = f"https://lina-partner-board-lr.netlify.app/?p={new_token}"
                board_link_block = (
                    f"📱 *Dein persönliches Lead-Board:*\n"
                    f"👉 {board_url}\n"
                    f"(Hier siehst du alle Leads, machst Notizen, setzt Status — speichere den Link!)\n\n"
                )

            if partner_phone:
                # Nachricht 1: Willkommen mit Board-Link
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
                    f"1️⃣ Ganztag (rund um die Uhr): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Ganztag\n"
                    f"2️⃣ Vormittag (08–12h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Vormittag\n"
                    f"3️⃣ Nachmittag (12–17h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Nachmittag\n"
                    f"4️⃣ Abend (17–22h): {APP_URL}/zeitfenster?phone={partner_phone}&wahl=Abend\n\n"
                    f"{board_link_block}"
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

# ─── Termin-Reminder 90 Min vor Termin ───────────────────
def check_termin_reminders():
    """Pruefe Termine_Lina auf bevorstehende Termine + sende Erinnerungen.

    Trigger: Termin in 75-105 Minuten, Notification_Sent_Partner leer/nicht REMINDER_.
    Sendet WA an Partner (aus Partner_Konto) und Info an Mathias.
    Markiert Spalte M (Notification_Sent_Partner) mit REMINDER_<timestamp>.
    """
    try:
        sh = get_spreadsheet()
        ws = sh.worksheet("Termine_Lina")
        rows = ws.get_all_values()
        if len(rows) < 2:
            return
        header = rows[0]
        # Spalten-Indices (0-based)
        IDX_NAME, IDX_PHONE, IDX_PARTNER, IDX_TERMIN, IDX_ZWECK, IDX_NOTIF = 1, 2, 4, 6, 8, 12

        now_berlin = datetime.now(BERLIN_TZ)
        partners = {}
        try:
            for p in get_all_partner_records():
                partners[p["name"].strip().lower()] = p
        except Exception as pe:
            logger.warning(f"Reminder: partner-fetch failed: {pe}")

        for i, row in enumerate(rows[1:], start=2):  # 1-based Sheet-Zeile = i+1; rows[1:] start=2
            if len(row) <= IDX_TERMIN:
                continue
            termin_str = (row[IDX_TERMIN] or "").strip()
            if not termin_str:
                continue
            notif_old = (row[IDX_NOTIF] if len(row) > IDX_NOTIF else "").strip().upper()
            if notif_old.startswith("REMINDER_"):
                continue  # schon gesendet

            # Parse ISO mit Zeitzone
            try:
                # Beispiel: "2026-06-08T15:30:00+02:00"
                from datetime import datetime as _dt
                tdt = _dt.fromisoformat(termin_str)
                if tdt.tzinfo is None:
                    tdt = BERLIN_TZ.localize(tdt)
            except Exception as pe:
                logger.warning(f"Reminder: cannot parse termin {termin_str}: {pe}")
                continue

            delta_min = (tdt - now_berlin).total_seconds() / 60.0
            # Fenster: 75-105 Min vor Termin
            if not (75 <= delta_min <= 105):
                continue

            lead_name = row[IDX_NAME] if len(row) > IDX_NAME else ""
            lead_phone = row[IDX_PHONE] if len(row) > IDX_PHONE else ""
            partner_name = (row[IDX_PARTNER] if len(row) > IDX_PARTNER else "").strip()
            zweck = row[IDX_ZWECK] if len(row) > IDX_ZWECK else ""

            # Termin als HH:MM formatieren (Berlin-Zeit)
            tdt_b = tdt.astimezone(BERLIN_TZ)
            zeit_str = tdt_b.strftime("%H:%M")
            datum_str = tdt_b.strftime("%a, %d.%m.%Y")
            wd_map = {"Mon":"Mo","Tue":"Di","Wed":"Mi","Thu":"Do","Fri":"Fr","Sat":"Sa","Sun":"So"}
            for en,de in wd_map.items():
                datum_str = datum_str.replace(en, de)

            # Partner-Phone aus Partner_Konto
            partner_phone = ""
            p_rec = partners.get(partner_name.lower())
            if p_rec:
                partner_phone = p_rec.get("phone", "")

            reminder_msg = (
                f"⏰ *Erinnerung in 90 Min:*\n"
                f"📅 {datum_str} um *{zeit_str} Uhr*\n\n"
                f"👤 Lead: *{lead_name}*\n"
                f"📞 +{lead_phone}\n"
                f"📝 {zweck[:200]}\n\n"
                f"Viel Erfolg! 💪"
            )

            sent_p = False
            if partner_phone:
                try:
                    sent_p = send_whatsapp(partner_phone, reminder_msg, _skip_admin=True)
                except Exception as we:
                    logger.warning(f"Reminder Partner-WA Fehler: {we}")

            # Info an Mathias
            try:
                admin_msg = f"⏰ Reminder gesendet → {partner_name} (Termin {zeit_str} mit {lead_name})"
                send_whatsapp(MATZE_PHONE, admin_msg, _skip_admin=True)
            except Exception as ae:
                logger.warning(f"Reminder Admin-WA Fehler: {ae}")

            # Markiere in Spalte M (1-based Spalte 13)
            try:
                marker = f"REMINDER_{now_berlin.strftime('%Y-%m-%d %H:%M')}"
                if not sent_p:
                    marker = f"REMINDER_NO_PARTNER_PHONE_{now_berlin.strftime('%H:%M')}"
                ws.update_cell(i, 13, marker)
                logger.info(f"⏰ Reminder gesendet: Z{i} {lead_name} → {partner_name} ({zeit_str})")
            except Exception as ue:
                logger.warning(f"Reminder cell-update Fehler: {ue}")

    except Exception as e:
        logger.error(f"❌ check_termin_reminders Fehler: {e}")



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
            if guthaben < LEAD_PREIS:
                guthaben_info = f"🔴 *ACHTUNG: Guthaben aufgebraucht!* ({guthaben:.2f}€)\nDu bekommst KEINE Leads mehr!"
            elif guthaben < 15:
                guthaben_info = f"🟡 *Guthaben wird knapp:* {guthaben:.2f}€\nNoch {int(guthaben / LEAD_PREIS)} Leads möglich."
            else:
                guthaben_info = f"🟢 *Guthaben:* {guthaben:.2f}€\nNoch {int(guthaben / LEAD_PREIS)} Leads möglich."

            aufladen_text = ""
            if guthaben < LEAD_PREIS * 10:
                if STRIPE_PAYMENT_LINK:
                    aufladen_text = f"\n💳 Jetzt aufladen: {STRIPE_PAYMENT_LINK}"
                aufladen_text += f"\n💬 Oder schreib Lina: https://wa.me/{LINA_WA_NUMBER}"

            msg = (
                f"*Guten Morgen {name}!*\n\n"
                f"👉 *WICHTIG: Schreib jetzt kurz Lina, damit du heute Leads bekommst:*\n"
                f"https://wa.me/{LINA_WA_NUMBER}\n"
                f"(Einfach kurz \"Hi\" schreiben reicht!)\n\n"
                f"{guthaben_info}\n"
                f"⏰ *Zeitfenster:* {zf_text}"
                f"{aufladen_text}\n\n"
                f"Zeitfenster aendern? Einfach antippen:\n"
                f"1️⃣ Ganztag: {link_g}\n"
                f"2️⃣ Vormittag (08-12h): {link_v}\n"
                f"3️⃣ Nachmittag (12-17h): {link_n}\n"
                f"4️⃣ Abend (17-22h): {link_a}\n\n"
                f"Viel Erfolg heute! 💪"
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

# ─── Facebook-Tab Sync ────────────────────────────────────
# Facebook erstellt bei neuen Formular-Verbindungen immer einen neuen Tab.
# Diese Funktion prüft alle Tabs (außer Tabellenblatt1, Partner_Konto, Leads_Log,
# Tabellenblatt4) und überträgt neue CREATED-Leads nach Tabellenblatt1.
# Neue Facebook-Tabs haben folgende Spalten:
# id, created_time, ad_id, ad_name, adset_id, adset_name, campaign_id,
# campaign_name, form_id, form_name, is_organic, platform,
# employment_status, firmenwagen_interest, first_name, last_name,
# phone_number, email, lead_status
# ──────────────────────────────────────────────────────────

SYNC_SKIP_SHEETS = {"Tabellenblatt1", "Partner_Konto", "Leads_Log", "Tabellenblatt4", "SMS_Replies", "Termine_Lina", "Lina_Anruf_Status", "Partner_Notizen", "Partner_Lead_Status", "Partner_Nachfass_Log", "Recall_Queue", "Nachfass_Library"}

# Formular-IDs und Kampagnen-Schlüsselwörter die NICHT ins LR-System dürfen
SYNC_EXCLUDED_FORM_IDS = {"f:1401075551783502", "f:2860546967626542"}  # GTS Firmengründung Spanien
SYNC_EXCLUDED_CAMPAIGN_KEYWORDS = ["gts", "firmengr", "spanien", "firmengruendung", "dubai"]

def _sync_facebook_tabs():
    """Überträgt neue CREATED-Leads aus Facebook-Tabs nach Tabellenblatt1."""
    try:
        sh = get_spreadsheet()
        all_worksheets = sh.worksheets()
        target_ws = sh.worksheet(LEADS_SHEET_NAME)

        # Bestehende IDs in Tabellenblatt1 laden (Duplikat-Schutz)
        existing_rows = target_ws.get_all_values()
        existing_ids = set(r[0] for r in existing_rows[1:] if r)

        for ws in all_worksheets:
            if ws.title in SYNC_SKIP_SHEETS:
                continue

            try:
                rows = ws.get_all_values()
                if not rows or len(rows) < 2:
                    continue

                header = [h.strip().lower() for h in rows[0]]

                # Prüfen ob es ein Facebook-Lead-Tab ist (muss 'id' und 'lead_status' haben)
                if 'id' not in header or 'lead_status' not in header:
                    continue

                # Spalten-Indizes ermitteln
                def col(name):
                    return header.index(name) if name in header else -1

                idx_id            = col('id')
                idx_status        = col('lead_status')
                idx_created       = col('created_time')
                idx_ad_id         = col('ad_id')
                idx_ad_name       = col('ad_name')
                idx_adset_id      = col('adset_id')
                idx_adset_name    = col('adset_name')
                idx_campaign_id   = col('campaign_id')
                idx_campaign_name = col('campaign_name')
                idx_form_id       = col('form_id')
                idx_form_name     = col('form_name')
                idx_is_organic    = col('is_organic')
                idx_platform      = col('platform')
                idx_email         = col('email')
                # Unterstütze beide Spaltennamen: 'phone_number' und 'phone'
                idx_phone         = col('phone_number') if col('phone_number') >= 0 else col('phone')
                # Unterstütze beide Spaltennamen: 'first_name'/'last_name' und 'full_name'
                idx_first_name    = col('first_name')
                idx_last_name     = col('last_name')
                idx_full_name     = col('full_name')

                transferred = 0
                for i, row in enumerate(rows[1:], start=2):
                    if len(row) <= idx_status or idx_status < 0:
                        continue

                    status = row[idx_status].strip().upper() if idx_status >= 0 else ""
                    # EXCLUDED_GTS / VERTEILT / ERROR überspringen
                    # ACHTUNG: SYNCED hier NICHT überspringen!
                    # GTS-Leads kommen von Facebook mit Status SYNCED – die müssen noch geprüft werden.
                    if status in ("EXCLUDED_GTS", "VERTEILT", "ERROR"):
                        continue
                    lead_id = row[idx_id].strip() if idx_id >= 0 and len(row) > idx_id else ""
                    # Felder zusammenbauen
                    def get(idx):
                        return row[idx].strip() if idx >= 0 and len(row) > idx else ""
                    # GTS / Nicht-LR Kampagnen ausschließen – VOR dem CREATED-Check!
                    # (GTS-Leads kommen von Facebook mit Status SYNCED, nicht CREATED)
                    lead_form_id = get(idx_form_id)
                    lead_campaign = get(idx_campaign_name).lower()
                    lead_adset = get(idx_adset_name).lower()
                    if lead_form_id in SYNC_EXCLUDED_FORM_IDS or \
                       any(kw in lead_campaign for kw in SYNC_EXCLUDED_CAMPAIGN_KEYWORDS) or \
                       any(kw in lead_adset for kw in SYNC_EXCLUDED_CAMPAIGN_KEYWORDS):
                        logger.warning(
                            f"⛔ Sync übersprungen (GTS/Nicht-LR): "
                            f"form={lead_form_id} campaign={lead_campaign}"
                        )
                        try:
                            ws.update_cell(i, idx_status + 1, "EXCLUDED_GTS")
                        except Exception:
                            pass
                        # ✅ Matze per WhatsApp benachrichtigen (SYNC)
                        try:
                            _gts_first = get(idx_first_name)
                            _gts_last  = get(idx_last_name)
                            _gts_name  = (_gts_first + " " + _gts_last).strip()
                            if not _gts_name and idx_full_name >= 0:
                                _gts_name = get(idx_full_name)
                            _gts_name = _gts_name or "Unbekannt"
                            _gts_phone = normalize_phone(get(idx_phone))
                            _gts_email = get(idx_email)
                            send_whatsapp(
                                MATZE_PHONE,
                                f"🇪🇸 *Neuer GTS-Lead (Firmengründung Spanien)!*\n\n"
                                f"👤 Name: {_gts_name}\n"
                                f"📱 Telefon: {_gts_phone}\n"
                                f"📧 Email: {_gts_email}\n"
                                f"📌 Kampagne: {lead_campaign}\n\n"
                                f"⛔ Nicht an LR-Partner verteilt.",
                                _skip_admin=True
                            )
                        except Exception as _gts_sync_err:
                            logger.error(f"GTS Sync WhatsApp Fehler: {_gts_sync_err}")
                        continue
                    # Nur CREATED-Leads weiterverarbeiten (nach GTS-Check!)
                    if status != "CREATED":
                        continue
                    # Duplikat-Schutz: bereits in Tabellenblatt1?
                    if lead_id and lead_id in existing_ids:
                        try:
                            ws.update_cell(i, idx_status + 1, "SYNCED")
                        except Exception:
                            pass
                        continue
                    # Name aus first_name + last_name zusammensetzen (oder full_name)
                    first = get(idx_first_name)
                    last  = get(idx_last_name)
                    full_name = (first + " " + last).strip()
                    if not full_name and idx_full_name >= 0:
                        full_name = get(idx_full_name)
                    full_name = full_name or "Unbekannt"

                    email = get(idx_email)
                    phone = get(idx_phone)

                    # Neue Zeile für Tabellenblatt1 bauen (16 Spalten)
                    new_row = [
                        get(idx_id),             # A – id
                        get(idx_created),         # B – created_time
                        get(idx_ad_id),           # C – ad_id
                        get(idx_ad_name),         # D – ad_name
                        get(idx_adset_id),        # E – adset_id
                        get(idx_adset_name),      # F – adset_name
                        get(idx_campaign_id),     # G – campaign_id
                        get(idx_campaign_name),   # H – campaign_name
                        get(idx_form_id),         # I – form_id
                        get(idx_form_name),       # J – form_name
                        get(idx_is_organic),      # K – is_organic
                        get(idx_platform),        # L – platform
                        email,                    # M – e-mail-adresse
                        full_name,                # N – vollständiger_name
                        phone,                    # O – telefonnummer
                        "CREATED",               # P – lead_status
                    ]

                    target_ws.append_row(new_row, value_input_option="RAW")
                    existing_ids.add(lead_id)
                    transferred += 1

                    # Status im Quell-Tab auf SYNCED setzen
                    try:
                        ws.update_cell(i, idx_status + 1, "SYNCED")
                    except Exception:
                        pass

                    logger.info(
                        f"🔄 Sync: '{ws.title}' → Tabellenblatt1: "
                        f"{full_name} | {email} | {phone}"
                    )

                if transferred > 0:
                    logger.info(f"✅ Sync '{ws.title}': {transferred} Leads übertragen")
                    # Sync-Notification deaktiviert – Admin-Nachricht kommt bereits bei Verteilung

            except Exception as e:
                logger.error(f"❌ Sync-Fehler Tab '{ws.title}': {e}")

    except Exception as e:
        logger.error(f"❌ Facebook-Tab-Sync Fehler: {e}")


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

                # ⛔ GTS / Nicht-LR Kampagnen NICHT an Partner verteilen
                form_id_val = row[8].strip() if len(row) > 8 else ""  # I – form_id
                camp_lower  = campaign_name.lower()
                adset_lower = adset_name.lower()
                if form_id_val in SYNC_EXCLUDED_FORM_IDS or \
                   any(kw in camp_lower  for kw in SYNC_EXCLUDED_CAMPAIGN_KEYWORDS) or \
                   any(kw in adset_lower for kw in SYNC_EXCLUDED_CAMPAIGN_KEYWORDS):
                    logger.warning(
                        f"⛔ Poll: GTS-Lead übersprungen (nicht an Partner): "
                        f"form={form_id_val} campaign={campaign_name}"
                    )
                    ws.update_cell(i, LEAD_COL_STATUS + 1, "EXCLUDED_GTS")
                    # WhatsApp wird nur im SYNC-Block gesendet, nicht hier im POLL
                    # (verhindert Doppel-/Dreifach-Benachrichtigung)
                    continue

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
            # Zuerst Facebook-Tabs synchronisieren, dann verteilen
            _sync_facebook_tabs()
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

# ─── Self-Ping Keep-Alive (verhindert Render Sleep) ──────
def keep_alive_loop():
    """Pingt den eigenen /health Endpoint alle 10 Minuten,
    damit Render den Service nicht schlafen legt.
    So feuert der APScheduler zuverlässig um 08:00."""
    logger.info("💓 Keep-Alive gestartet (alle 600s)")
    while True:
        try:
            resp = requests.get(f"{APP_URL}/health", timeout=10)
            logger.debug(f"💓 Keep-Alive Ping: {resp.status_code}")
        except Exception as e:
            logger.warning(f"💓 Keep-Alive Fehler: {e}")
        time.sleep(600)  # alle 10 Minuten

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
app = FastAPI(title="Lead-Verteilungs-Service v8.0")

@app.get("/")
def root():
    return {
        "service":  "Lead-Verteilungs-Service",
        "version":  "8.0",
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
        "version":   "8.0",
        "timestamp": datetime.now(BERLIN_TZ).isoformat()
    }

@app.get("/status")
def status_check():
    try:
        all_records = get_all_partner_records()
        aktive = [p for p in all_records if p["status"].strip().lower() == "aktiv"]
        return {
            "status": "ok",
            "version": "8.0",
            "partner_gesamt": len(all_records),
            "partner_aktiv": len(aktive),
            "timestamp": datetime.now(BERLIN_TZ).isoformat()
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/debug-headers")
def debug_headers():
    try:
        ws = get_partner_sheet()
        headers = ws.row_values(1)
        # Get raw row 20 values
        row9 = ws.row_values(9)
        row20 = ws.row_values(20)
        # Also get via get_all_records
        records = ws.get_all_records(value_render_option='UNFORMATTED_VALUE')
        beyer_records = [r for r in records if 'Beyer' in str(r.get('Name', ''))]
        return {
            "headers": headers,
            "headers_repr": [repr(h) for h in headers],
            "row9_raw": row9,
            "row20_raw": row20,
            "beyer_records": beyer_records,
        }
    except Exception as e:
        return {"error": str(e)}

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

# ─── Inbound-Relay: Eingehende Nachrichten an Linas Nummer ──────────────────
# Dieser Endpoint empfängt alle eingehenden WhatsApp-Nachrichten
# an Linas Nummer (Phone ID 623007617563961).
#
# Logik:
# 1. Ist der Absender in PERSONAL_OUTREACH_NUMBERS?
#    JA  → Lina antwortet automatisch + Mathias erhält eine Benachrichtigung
#    NEIN → Nachricht wird 1:1 an Botpress weitergeleitet (normaler Bot-Flow)
#
# WICHTIG: Dieser Endpoint verändert KEINE bestehende Logik.
# Er ist rein additiv und greift nur bei eingehenden Nachrichten.
# ──────────────────────────────────────────────────────────────────────────────

# Namenszuordnung für die 35 Outreach-Leads (für die Benachrichtigung an Mathias)
OUTREACH_NAMES = {
    "4915229525402": "Heiko",
    "34641689043":   "Ator",
    "436641100575":  "Marco S.",
    "4915757078693": "Salman",
    "4915217523018": "El Mitevski",
    "34685578351":   "Iulia",
    "436502007572":  "Rupert F.",
    "905452511212":  "Conwaynen",
    "4915204796954": "Ingolf",
    "41791548025":   "Kamel",
    "4917630745947": "Marco Sch.",
    "4915511558064": "Gabryel",
    "436763223082":  "Anton",
    "436765998767":  "Adrian",
    "34638434151":   "Alejandro",
    "4917654125929": "Frank R.",
    "491629331646":  "Frank H.",
    "41787568841":   "Gi Van Alin",
    "34652035095":   "Giuseppe",
    "491759979085":  "Lydia",
    "436767609001":  "Rupert P.",
    "41765415688":   "Mario",
    "436765326122":  "Christoph",
    "491791375028":  "Michael",
    "436766738315":  "Manuel",
    "4915151868158": "Tanja",
    "491733060912":  "Manfred",
    "4366499294121": "Jürgen",
    "436764407041":  "Phillip",
    "436602037576":  "David",
    "4916098645201": "Bast",
    "436503630029":  "Joachim",
    "491799798794":  "Burkhardt",
    "4917622834125": "Sasha",
    "4917623101086": "Agostino",
}

@app.get("/anruf")
async def anruf_link(request: Request):
    """Link-Button 'Lina anrufen' -> loest den Anruf via Call-Trigger aus."""
    p = dict(request.query_params)
    phone = (p.get("phone") or "").strip()
    name = (p.get("name") or "Interessent").strip()
    partner = (p.get("partner") or "").strip()
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    ok = False
    if phone:
        try:
            r = requests.post(
                "https://hook.eu2.make.com/fkifa17zbg7fek20amdd2qdv35634ltk",
                json={"phoneNumber": phone, "leadName": name, "partnerName": partner, "leadSource": "LR-ButtonCall"},
                timeout=8,
            )
            ok = (r.status_code == 200)
        except Exception as e:
            logger.error(f"/anruf Trigger Fehler: {e}")
    msg = (f"✅ Lina ruft {name} gleich an und bucht den Termin fuer dich."
           if ok else
           "⚠️ Konnte den Anruf gerade nicht starten - bitte nochmal tippen oder den Lead selbst kontaktieren.")
    return HTMLResponse(content=f"""<!doctype html><html lang=de><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>Lina</title></head><body style="font-family:sans-serif;text-align:center;padding:48px 20px;background:#f7f7f7"><div style="max-width:420px;margin:0 auto;background:#fff;border-radius:16px;padding:32px;box-shadow:0 2px 12px rgba(0,0,0,.08)"><h2 style="margin:0 0 12px">{msg}</h2><p style="color:#666">Du kannst dieses Fenster jetzt schliessen.</p></div></body></html>""")


@app.get("/inbound")
async def inbound_verify(request: Request):
    """Meta Webhook-Verifizierung (GET-Request beim ersten Setup)."""
    params = dict(request.query_params)
    mode      = params.get("hub.mode", "")
    token     = params.get("hub.verify_token", "")
    challenge = params.get("hub.challenge", "")
    verify_token = os.getenv("WEBHOOK_VERIFY_TOKEN", "matze_inbound_2026")
    if mode == "subscribe" and token == verify_token:
        logger.info("✅ Meta Webhook-Verifizierung erfolgreich")
        return int(challenge)
    logger.warning(f"⚠️ Webhook-Verifizierung fehlgeschlagen: mode={mode} token={token}")
    raise HTTPException(status_code=403, detail="Verifizierung fehlgeschlagen")

@app.post("/inbound")
async def inbound_relay(request: Request):
    """
    Empfängt eingehende WhatsApp-Nachrichten an Linas Nummer.
    - Ist der Absender in PERSONAL_OUTREACH_NUMBERS: Lina antwortet + Matze wird benachrichtigt.
    - Sonst: Nachricht wird 1:1 an Botpress weitergeleitet.
    Gibt immer 200 OK zurück (Meta erwartet das, sonst Retry-Schleife).
    """
    try:
        body = await request.json()
    except Exception:
        # Kein gültiges JSON – trotzdem 200 zurückgeben
        return {"status": "ok"}

    try:
        # Meta-Webhook-Payload parsen
        # Struktur: body.entry[].changes[].value.messages[]
        entries = body.get("entry", [])
        for entry in entries:
            for change in entry.get("changes", []):
                value = change.get("value", {})
                messages = value.get("messages", [])
                contacts = value.get("contacts", [])

                for msg in messages:
                    msg_type = msg.get("type", "")
                    # Nur Text-Nachrichten verarbeiten (keine Status-Updates etc.)
                    if msg_type not in ("text", "audio", "image", "video", "document", "sticker", "reaction", "button", "interactive"):
                        continue

                    sender_raw = msg.get("from", "")
                    # Normalisieren: + und Leerzeichen entfernen
                    sender = re.sub(r'[\s\+]', '', sender_raw.strip())

                    # Nachrichtentext extrahieren (je nach Typ)
                    if msg_type == "text":
                        msg_text = msg.get("text", {}).get("body", "")
                    elif msg_type == "button":
                        msg_text = msg.get("button", {}).get("text", "[Button-Antwort]")
                    elif msg_type == "interactive":
                        interactive = msg.get("interactive", {})
                        if interactive.get("type") == "button_reply":
                            msg_text = interactive.get("button_reply", {}).get("title", "[Button]")
                        elif interactive.get("type") == "list_reply":
                            msg_text = interactive.get("list_reply", {}).get("title", "[Liste]")
                        else:
                            msg_text = "[Interaktive Nachricht]"
                    else:
                        msg_text = f"[{msg_type.upper()}]"

                    # Absendername aus contacts (falls vorhanden)
                    sender_name = ""
                    for contact in contacts:
                        if contact.get("wa_id", "") == sender_raw or \
                           re.sub(r'[\s\+]', '', contact.get("wa_id", "")) == sender:
                            sender_name = contact.get("profile", {}).get("name", "")
                            break

                    # ── Button-Klick vom Partner (Lead-Auswahl) ──
                    btn_id = ""
                    if msg_type == "interactive":
                        btn_id = msg.get("interactive", {}).get("button_reply", {}).get("id", "")
                    if btn_id.startswith("anruf|") or btn_id.startswith("selbst|"):
                        parts = btn_id.split("|")
                        aktion = parts[0]
                        lead_tel = parts[1] if len(parts) > 1 else ""
                        lead_nm = parts[2] if len(parts) > 2 else "Interessent"
                        partner_nm = parts[3] if len(parts) > 3 else ""
                        if aktion == "anruf":
                            try:
                                requests.post(
                                    "https://hook.eu2.make.com/fkifa17zbg7fek20amdd2qdv35634ltk",
                                    json={"phoneNumber": lead_tel if lead_tel.startswith("+") else ("+" + lead_tel),
                                          "leadName": lead_nm, "partnerName": partner_nm, "leadSource": "LR-ButtonCall"},
                                    timeout=8,
                                )
                                send_whatsapp(sender, f"✅ Top! Lina ruft {lead_nm} gleich an und bucht den Termin für dich.", _skip_admin=True)
                            except Exception as be:
                                logger.error(f"❌ Button-Anruf-Trigger Fehler: {be}")
                                send_whatsapp(sender, "⚠️ Konnte den Anruf gerade nicht starten, bitte nochmal tippen.", _skip_admin=True)
                        else:
                            send_whatsapp(sender, f"👍 Alles klar, du meldest dich selbst bei {lead_nm}.", _skip_admin=True)
                        continue

                    logger.info(f"📩 Eingehende Nachricht von +{sender}: {msg_text[:50]}")

                    if sender in PERSONAL_OUTREACH_NUMBERS:
                        # ─── OUTREACH-LEAD: Persönliche Weiterleitung an Mathias ───
                        lead_name = OUTREACH_NAMES.get(sender, sender_name or f"+{sender}")
                        logger.info(f"🟡 Outreach-Lead erkannt: {lead_name} (+{sender})")

                        # 1. Lina antwortet automatisch an den Lead
                        send_whatsapp(
                            sender,
                            "Mathias meldet sich gleich persönlich bei dir 👋",
                            _skip_admin=True
                        )

                        # 2. Mathias erhält eine Benachrichtigung mit vollem Kontext
                        matze_msg = (
                            f"📩 *Antwort von Outreach-Lead!*\n\n"
                            f"👤 *Name:* {lead_name}\n"
                            f"📱 *Nummer:* +{sender}\n\n"
                            f"💬 *Nachricht:*\n{msg_text}\n\n"
                            f"👉 Direkt antworten: https://wa.me/{sender}"
                        )
                        send_whatsapp(MATZE_PHONE, matze_msg, _skip_admin=True)

                    else:
                        # ─── NORMALER LEAD: An Botpress weiterleiten ───
                        logger.info(f"🤖 Nicht in Outreach-Liste → Botpress: +{sender}")
                        try:
                            requests.post(
                                BOTPRESS_WEBHOOK_URL,
                                json=body,
                                timeout=5
                            )
                        except Exception as bp_err:
                            logger.warning(f"⚠️ Botpress-Weiterleitung fehlgeschlagen: {bp_err}")

    except Exception as e:
        logger.error(f"❌ Inbound-Relay Fehler: {e}")
        # Trotzdem 200 zurückgeben – Meta darf keine Fehler sehen

    return {"status": "ok"}

# ─── Startup ───────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    # Header validieren
    header_ok = validate_sheet_headers()

    # APScheduler: täglich 08:00 Berlin
    scheduler = BackgroundScheduler(timezone=BERLIN_TZ)
    scheduler.add_job(send_daily_reminders, "cron", hour=8, minute=0)
    # Termin-Reminder: alle 15 Min pruefen
    scheduler.add_job(check_termin_reminders, "interval", minutes=15)
    scheduler.start()
    logger.info("⏰ Scheduler gestartet – täglich 08:00 Uhr")

    # Polling-Thread
    poll_thread = threading.Thread(target=polling_loop, daemon=True)
    poll_thread.start()
    logger.info("🔄 Polling-Thread gestartet")

    # Keep-Alive-Thread (verhindert Render Sleep)
    alive_thread = threading.Thread(target=keep_alive_loop, daemon=True)
    alive_thread.start()
    logger.info("💓 Keep-Alive-Thread gestartet")

    # Startmeldung an Matze
    header_emoji = "✅" if header_ok else "❌"
    send_whatsapp(
        MATZE_PHONE,
        f"🚀 *Lead-System v8.0 gestartet!*\n\n"
        f"✅ Polling aktiv (alle {POLL_INTERVAL}s)\n"
        f"✅ Facebook-Tab Sync aktiv (alle neuen Tabs werden erkannt)\n"
        f"✅ Tages-Erinnerung aktiv (08:00 Berlin)\n"
        f"✅ Stripe Webhook aktiv\n"
        f"✅ Zeitfenster-Links aktiv\n"
        f"✅ Admin-Infos bei jeder Aktion\n"
        f"✅ Auto-Erkennung Email/Name/Phone\n"
        f"✅ Funnel-Mapping (Partner sieht was Lead gesehen hat)\n"
        f"✅ GTS-Leads → WhatsApp an Mathias (nicht an LR-Partner)\n"
        f"✅ Inbound-Relay aktiv (35 Outreach-Leads → Mathias direkt)\n"
        f"{header_emoji} Sheet-Header geprüft\n"
        f"✅ Lina: {LINA_WA_NUMBER}\n\n"
        f"Alles läuft! 💪",
        _skip_admin=True
    )
