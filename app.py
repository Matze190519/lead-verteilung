"""
Lead-Verteilungs-Service v3.2 (SAFE MODE)
=========================================
Empfängt Facebook Lead Ads via Webhook ODER liest neue Leads aus dem
Google Sheet "Tabellenblatt1", verteilt Leads FAIR an aktive Partner
aus "Partner_Konto", zieht Guthaben ab und sendet WhatsApp-
Benachrichtigungen via Whapi API.

v3.2 Fixes:
- Threading-Lock: Polling kann nie doppelt laufen (Race Condition behoben)
- Lead-Status wird SOFORT auf "PROCESSING" gesetzt bevor Verteilung startet
- Doppelte Verteilung damit unmöglich
- Polling-Intervall auf 60 Sekunden reduziert (schnellere Lead-Zustellung)
- SAFE MODE: Keine Nachrichten an Leads (nur Partner/Admin)

v3.1: Intelligente Spalten-Erkennung (Name/Email/Telefon)
v3.0: Sheet-Polling, Spalte P lead_status

Autor: Manus für Matze
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

# Environment-Variablen
WHAPI_TOKEN = os.getenv("WHAPI_TOKEN", "")
WHAPI_URL = os.getenv("WHAPI_URL", "https://gate.whapi.cloud/messages/text")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY")
FB_VERIFY_TOKEN = os.getenv("FB_VERIFY_TOKEN", "mein_geheimer_token_2024")
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5"))
PAKET_PREIS = float(os.getenv("PAKET_PREIS", "50"))

# Stripe
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")

# Matze's WhatsApp-Nummer
MATZE_PHONE = os.getenv("MATZE_PHONE", "")

# Google Credentials
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

# Facebook
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")

# Polling-Intervall (Sekunden) - 60s für schnellere Lead-Zustellung
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))

# ─── Threading Lock (verhindert doppeltes Polling) ──────────────────────────
poll_lock = threading.Lock()

# ─── FastAPI App ─────────────────────────────────────────────────────────────
app = FastAPI(
    title="Lead-Verteilungs-Service",
    description="Verteilt Leads fair an Partner. Liest aus Google Sheet + Facebook Webhook + Stripe.",
    version="3.6.0",
)


# ─── Google Sheets Client ────────────────────────────────────────────────────
def get_google_client() -> gspread.Client:
    if GOOGLE_CREDENTIALS_JSON:
        creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
        gc = gspread.service_account_from_dict(creds_dict)
    else:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    return gc


def get_spreadsheet() -> gspread.Spreadsheet:
    gc = get_google_client()
    return gc.open_by_key(GOOGLE_SHEET_ID)


def get_sheet() -> gspread.Worksheet:
    """Öffnet Partner_Konto Tab."""
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet("Partner_Konto")
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("Tab 'Partner_Konto' nicht gefunden, verwende erstes Sheet")
        return spreadsheet.sheet1


def get_leads_sheet() -> gspread.Worksheet:
    """Öffnet Tabellenblatt1 (wo die Facebook-Leads landen)."""
    spreadsheet = get_spreadsheet()
    return spreadsheet.worksheet("Tabellenblatt1")


def get_leads_log_sheet() -> gspread.Worksheet:
    """Öffnet oder erstellt den Leads_Log Tab."""
    spreadsheet = get_spreadsheet()
    try:
        return spreadsheet.worksheet("Leads_Log")
    except gspread.exceptions.WorksheetNotFound:
        logger.info("Leads_Log Tab wird erstellt...")
        ws = spreadsheet.add_worksheet(title="Leads_Log", rows=1000, cols=10)
        headers = [
            "Zeitstempel", "Lead_Name", "Lead_Telefon", "Lead_Email",
            "Partner_Name", "Partner_Telefon", "Guthaben_Nachher",
            "WhatsApp_Partner", "WhatsApp_Lead", "Status"
        ]
        ws.append_row(headers, value_input_option="USER_ENTERED")
        return ws


def log_lead(lead_name: str, lead_phone: str, lead_email: str,
             partner_name: str, partner_phone: str, guthaben_nachher: float,
             wa_partner_ok: bool, wa_lead_ok: bool, status: str):
    """Schreibt einen Lead-Eintrag in den Leads_Log Tab."""
    try:
        log_sheet = get_leads_log_sheet()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = [
            now, lead_name, lead_phone, lead_email,
            partner_name, partner_phone, guthaben_nachher,
            "OK" if wa_partner_ok else "FEHLER",
            "OK" if wa_lead_ok else "FEHLER/KEINE NR",
            status,
        ]
        log_sheet.append_row(row, value_input_option="USER_ENTERED")
        logger.info(f"Lead geloggt: {lead_name} → {partner_name} ({status})")
    except Exception as e:
        logger.error(f"Fehler beim Lead-Logging: {e}")


# ─── Whapi WhatsApp ──────────────────────────────────────────────────────────
def send_whatsapp(phone: str, message: str) -> dict:
    if not WHAPI_TOKEN:
        logger.error("WHAPI_TOKEN nicht gesetzt!")
        return {"error": "WHAPI_TOKEN nicht konfiguriert"}

    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Telefonnummer: '{phone}'")
        return {"error": f"Ungültige Telefonnummer: {phone}"}

    to = f"{phone}@s.whatsapp.net"
    headers = {
        "Authorization": f"Bearer {WHAPI_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"to": to, "body": message, "typing_time": 2}

    try:
        logger.info(f"WhatsApp senden an {phone} (to={to})...")
        response = requests.post(WHAPI_URL, json=payload, headers=headers, timeout=30)
        logger.info(f"WhatsApp Response Status: {response.status_code}")
        logger.info(f"WhatsApp Response Body: {response.text[:500]}")
        response.raise_for_status()
        result = response.json()
        logger.info(f"WhatsApp gesendet an {phone}: OK")
        return result
    except requests.exceptions.RequestException as e:
        # Versuche den Response-Body zu loggen für bessere Fehleranalyse
        error_body = ""
        if hasattr(e, 'response') and e.response is not None:
            error_body = e.response.text[:500]
        logger.error(f"WhatsApp-Fehler an {phone}: {e} | Response: {error_body}")
        return {"error": str(e), "response_body": error_body}


# ─── Telefonnummer normalisieren ─────────────────────────────────────────────
def normalize_phone(phone: str) -> str:
    """
    Normalisiert Telefonnummer auf Format 49... (ohne +, ohne Leerzeichen).
    Erkennt auch das Format "p:+4915..." aus dem Google Sheet.
    """
    if not phone:
        return ""
    # Prefix "p:" entfernen (Facebook/Sheet-Format)
    if phone.startswith("p:"):
        phone = phone[2:]
    # Nur Ziffern behalten
    phone = "".join(c for c in phone if c.isdigit())
    if phone.startswith("0"):
        phone = "49" + phone[1:]
    if not phone.startswith("49") and len(phone) <= 11:
        phone = "49" + phone
    return phone


# ─── Partner-Suche und Update ────────────────────────────────────────────────
def get_all_partner_records(sheet: gspread.Worksheet) -> list:
    """Liest alle Partner-Daten aus Spalten A-F."""
    headers = ["Name", "Telefon", "Guthaben_Euro", "Leads_Geliefert", "Letzter_Lead_Am", "Status"]
    all_values = sheet.get_all_values()
    if len(all_values) <= 1:
        return []

    records = []
    for row in all_values[1:]:
        if len(row) >= 6:
            record = {headers[i]: row[i] for i in range(6)}
        else:
            record = {headers[i]: (row[i] if i < len(row) else "") for i in range(6)}
        if record.get("Name", "").strip():
            records.append(record)
    return records


def find_best_partner(sheet: gspread.Worksheet) -> Optional[dict]:
    """
    FAIRE VERTEILUNG (Round-Robin / Zeitbasiert):
    - Status = 'Aktiv'
    - Guthaben_Euro >= Lead-Preis
    - Sortiert nach Letzter_Lead_Am ASC (wer am längsten wartet, ist dran)
    - Neue Partner (leeres Datum) kommen ZUERST dran
    - Bei Gleichstand: Wenigste Leads zuerst
    """
    try:
        all_records = get_all_partner_records(sheet)
    except Exception as e:
        logger.error(f"Fehler beim Lesen des Sheets: {e}")
        return None

    if not all_records:
        logger.warning("Keine Partner im Sheet gefunden!")
        return None

    aktive_partner = []
    for idx, record in enumerate(all_records):
        status = str(record.get("Status", "")).strip()
        try:
            guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
        except (ValueError, TypeError):
            guthaben = 0
        try:
            leads = int(record.get("Leads_Geliefert", 0))
        except (ValueError, TypeError):
            leads = 0

        letzter_lead = str(record.get("Letzter_Lead_Am", "")).strip()

        if status == "Aktiv" and guthaben > LEAD_PREIS - 0.01:
            aktive_partner.append({
                "row": idx + 2,
                "name": str(record.get("Name", "Unbekannt")),
                "telefon": str(record.get("Telefon", "")),
                "guthaben": guthaben,
                "leads_geliefert": leads,
                "letzter_lead": letzter_lead,
                "status": status,
            })

    if not aktive_partner:
        logger.warning("Kein aktiver Partner mit ausreichend Guthaben gefunden!")
        return None

    def sort_key(p):
        datum = p["letzter_lead"]
        if not datum:
            return ("0000-00-00 00:00:00", p["leads_geliefert"])
        return (datum, p["leads_geliefert"])

    aktive_partner.sort(key=sort_key)
    best = aktive_partner[0]
    logger.info(
        f"Bester Partner (fair): {best['name']} (Zeile {best['row']}, "
        f"Letzter Lead: {best['letzter_lead'] or 'NIE'}, "
        f"Leads: {best['leads_geliefert']}, Guthaben: {best['guthaben']}€)"
    )
    return best


def update_partner(sheet: gspread.Worksheet, partner: dict) -> bool:
    """Aktualisiert den Partner: Guthaben -LEAD_PREIS, Leads +1, Datum = jetzt."""
    row = partner["row"]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        neue_leads = partner["leads_geliefert"] + 1
        sheet.update_cell(row, 4, neue_leads)
        sheet.update_cell(row, 5, now)
        logger.info(
            f"Partner {partner['name']} aktualisiert: "
            f"Guthaben {partner['guthaben']}€ → {neues_guthaben}€, "
    
...(truncated)...
