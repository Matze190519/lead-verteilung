"""
Lead-Verteilungs-Service v3.2
==============================
Empfängt Facebook Lead Ads via Webhook ODER liest neue Leads aus dem
Google Sheet "Tabellenblatt1", verteilt Leads FAIR an aktive Partner
aus "Partner_Konto", zieht Guthaben ab und sendet WhatsApp-
Benachrichtigungen via Whapi API.

v3.2 Fixes:
- Threading-Lock: Polling kann nie doppelt laufen (Race Condition behoben)
- Lead-Status wird SOFORT auf "PROCESSING" gesetzt bevor Verteilung startet
- Doppelte Verteilung damit unmöglich
- Polling-Intervall auf 60 Sekunden reduziert (schnellere Lead-Zustellung)

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
    version="3.2.0",
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
            f"Leads {partner['leads_geliefert']} → {neue_leads}"
        )
        if neues_guthaben < LEAD_PREIS:
            sheet.update_cell(row, 6, "Pausiert")
            logger.info(f"Partner {partner['name']} pausiert (Guthaben < {LEAD_PREIS}€)")
            if MATZE_PHONE:
                send_whatsapp(MATZE_PHONE,
                    f"⚠️ *Partner pausiert!*\n\n"
                    f"👤 {partner['name']} hat nur noch {neues_guthaben}€ Guthaben.\n"
                    f"Nächstes Lead-Paket nötig!"
                )
        return True
    except Exception as e:
        logger.error(f"Fehler beim Partner-Update: {e}")
        return False


def find_partner_by_phone(sheet: gspread.Worksheet, phone: str) -> Optional[dict]:
    normalized = normalize_phone(phone)
    if not normalized:
        return None
    records = get_all_partner_records(sheet)
    for idx, record in enumerate(records):
        partner_phone = normalize_phone(str(record.get("Telefon", "")))
        if partner_phone and partner_phone == normalized:
            try:
                guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
            except (ValueError, TypeError):
                guthaben = 0
            return {
                "row": idx + 2,
                "name": str(record.get("Name", "")),
                "telefon": partner_phone,
                "guthaben": guthaben,
            }
    return None


def find_partner_by_name(sheet: gspread.Worksheet, name: str) -> Optional[dict]:
    if not name:
        return None
    records = get_all_partner_records(sheet)
    name_lower = name.lower().strip()
    for idx, record in enumerate(records):
        record_name = str(record.get("Name", "")).lower().strip()
        if record_name and (record_name in name_lower or name_lower in record_name):
            try:
                guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
            except (ValueError, TypeError):
                guthaben = 0
            return {
                "row": idx + 2,
                "name": str(record.get("Name", "")),
                "telefon": normalize_phone(str(record.get("Telefon", ""))),
                "guthaben": guthaben,
            }
    return None


def add_new_partner(sheet: gspread.Worksheet, name: str, phone: str, guthaben: float) -> bool:
    try:
        normalized_phone = normalize_phone(phone)
        now = ""
        new_row = [name, normalized_phone, guthaben, 0, now, "Aktiv"]
        sheet.append_row(new_row, value_input_option="USER_ENTERED")
        logger.info(f"Neuer Partner hinzugefügt: {name}, Tel: {phone}, Guthaben: {guthaben}€")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Hinzufügen von Partner {name}: {e}")
        return False


def update_partner_guthaben(sheet: gspread.Worksheet, partner: dict, betrag: float) -> bool:
    row = partner["row"]
    try:
        neues_guthaben = round(partner["guthaben"] + betrag, 2)
        sheet.update_cell(row, 3, neues_guthaben)
        sheet.update_cell(row, 6, "Aktiv")
        logger.info(f"Partner {partner['name']} Guthaben erhöht: {partner['guthaben']}€ → {neues_guthaben}€")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Guthaben-Update für {partner['name']}: {e}")
        return False


def process_stripe_payment(customer_name: str, customer_phone: str, customer_email: str, amount: float):
    logger.info(f"=== Stripe-Zahlung: {customer_name} | {amount}€ ===")
    try:
        sheet = get_sheet()
    except Exception as e:
        logger.error(f"Google Sheet konnte nicht geöffnet werden: {e}")
        return

    partner = None
    if customer_phone:
        partner = find_partner_by_phone(sheet, customer_phone)
    if not partner and customer_name:
        partner = find_partner_by_name(sheet, customer_name)

    if partner:
        update_partner_guthaben(sheet, partner, amount)
        action = "GUTHABEN ERHÖHT"
        neues_guthaben = round(partner["guthaben"] + amount, 2)
    else:
        add_new_partner(sheet, customer_name, customer_phone, amount)
        action = "NEUER PARTNER ANGELEGT"
        neues_guthaben = amount

    if MATZE_PHONE:
        matze_msg = (
            f"💰 *Stripe-Zahlung eingegangen!*\n\n"
            f"👤 *Partner:* {customer_name}\n"
            f"📞 *Telefon:* {customer_phone}\n"
            f"📧 *Email:* {customer_email}\n"
            f"💶 *Betrag:* {amount}€\n\n"
            f"✅ *Aktion:* {action}\n"
            f"📊 *Neues Guthaben:* {neues_guthaben}€\n\n"
            f"🔔 Bitte Ad-Budget prüfen!"
        )
        send_whatsapp(MATZE_PHONE, matze_msg)

    logger.info(f"=== Stripe-Zahlung verarbeitet: {action} ===")


# ─── Sheet-Polling: Neue Leads aus Tabellenblatt1 lesen und verteilen ────────
def poll_new_leads():
    """
    Liest Tabellenblatt1 und verteilt alle Leads mit lead_status = "CREATED".
    
    WICHTIG v3.2: Verwendet Threading-Lock um doppelte Verteilung zu verhindern.
    Jeder Lead wird SOFORT auf "PROCESSING" gesetzt bevor die Verteilung startet.
    
    Sheet-Struktur Tabellenblatt1:
    - Spalte M (12): e-mail-adresse / vollständiger_name / telefonnummer
    - Spalte N (13): vollständiger_name / e-mail-adresse / telefonnummer
    - Spalte O (14): telefonnummer / vollständiger_name / e-mail-adresse
    - Spalte P (15): lead_status ("CREATED" = neu, "PROCESSING" = wird verteilt, 
                      "VERTEILT" = zugewiesen, "ARCHIV" = alt)
    """
    # Lock: Nur ein Polling-Durchlauf gleichzeitig!
    acquired = poll_lock.acquire(blocking=False)
    if not acquired:
        logger.warning("⚠️ Polling bereits aktiv - überspringe (Lock belegt)")
        return {"processed": 0, "errors": 0, "message": "Polling bereits aktiv"}
    
    try:
        return _do_poll_new_leads()
    finally:
        poll_lock.release()


def _do_poll_new_leads():
    """Interne Polling-Funktion (wird nur unter Lock ausgeführt)."""
    logger.info("=== Sheet-Polling gestartet ===")
    
    try:
        leads_sheet = get_leads_sheet()
        partner_sheet = get_sheet()
    except Exception as e:
        logger.error(f"Sheet konnte nicht geöffnet werden: {e}")
        return {"processed": 0, "errors": 0, "message": str(e)}

    # Alle Zeilen lesen
    all_values = leads_sheet.get_all_values()
    if len(all_values) <= 1:
        logger.info("Keine Leads im Sheet")
        return {"processed": 0, "errors": 0}

    # ─── SCHRITT 1: Neue Leads finden UND SOFORT als PROCESSING markieren ───
    # Das verhindert, dass ein zweiter Durchlauf die gleichen Leads findet
    new_leads = []
    for row_idx, row in enumerate(all_values[1:], start=2):
        # Spalte P (Index 15) = lead_status
        lead_status = row[15] if len(row) > 15 else ""
        if lead_status == "CREATED":
            # SOFORT auf PROCESSING setzen (Spalte P = Index 16 in 1-based)
            try:
                leads_sheet.update_cell(row_idx, 16, "PROCESSING")
                logger.info(f"Zeile {row_idx}: Status CREATED → PROCESSING")
            except Exception as e:
                logger.error(f"Fehler beim Setzen von PROCESSING für Zeile {row_idx}: {e}")
                continue  # Diesen Lead überspringen wenn Status nicht gesetzt werden kann
            
            # Spalten M(12), N(13), O(14) - Intelligente Erkennung
            col_m = row[12] if len(row) > 12 else ""
            col_n = row[13] if len(row) > 13 else ""
            col_o = row[14] if len(row) > 14 else ""
            
            # Alle 3 Werte sammeln und intelligent zuordnen
            raw_values = [col_m, col_n, col_o]
            name = "Unbekannt"
            email = ""
            phone_raw = ""
            
            for val in raw_values:
                val_stripped = val.strip()
                if not val_stripped:
                    continue
                # Telefonnummer erkennen
                if (val_stripped.startswith("p:") or 
                    val_stripped.startswith("+49") or
                    val_stripped.startswith("+4") or
                    val_stripped.startswith("49") or
                    (val_stripped.startswith("0") and len(val_stripped) > 8 and 
                     val_stripped.replace("+","").replace(" ","").replace("-","").isdigit())):
                    phone_raw = val_stripped
                # Email erkennen: enthält @
                elif "@" in val_stripped:
                    email = val_stripped
                # Alles andere = Name
                else:
                    name = val_stripped
            
            new_leads.append({
                "row": row_idx,
                "name": name,
                "email": email,
                "phone_raw": phone_raw,
                "phone": normalize_phone(phone_raw),
            })

    if not new_leads:
        logger.info("Keine neuen Leads (CREATED) gefunden")
        return {"processed": 0, "errors": 0}

    logger.info(f"🔥 {len(new_leads)} neue Leads gefunden und als PROCESSING markiert!")

    # ─── SCHRITT 2: Leads verteilen ───
    processed = 0
    errors = 0

    for lead in new_leads:
        try:
            # Partner suchen (jedes Mal neu lesen für aktuelle Daten)
            partner_sheet = get_sheet()
            partner = find_best_partner(partner_sheet)

            if not partner:
                logger.error(f"KEIN PARTNER für Lead {lead['name']}!")
                # Matze benachrichtigen
                if MATZE_PHONE:
                    send_whatsapp(MATZE_PHONE,
                        f"⚠️ *ACHTUNG: Lead ohne Partner!*\n\n"
                        f"👤 {lead['name']}\n📞 {lead['phone']}\n📧 {lead['email']}\n\n"
                        f"Kein aktiver Partner mit Guthaben verfügbar!"
                    )
                # Lead als KEIN_PARTNER markieren
                leads_sheet.update_cell(lead["row"], 16, "KEIN_PARTNER")
                log_lead(
                    lead_name=lead["name"], lead_phone=lead["phone"],
                    lead_email=lead["email"], partner_name="KEIN PARTNER",
                    partner_phone="", guthaben_nachher=0,
                    wa_partner_ok=False, wa_lead_ok=False, status="KEIN_PARTNER",
                )
                errors += 1
                continue

            # Partner aktualisieren (Guthaben -5€, Leads +1)
            if not update_partner(partner_sheet, partner):
                # Bei Fehler: Lead zurück auf CREATED setzen
                leads_sheet.update_cell(lead["row"], 16, "CREATED")
                errors += 1
                continue

            # WhatsApp an Partner
            partner_msg = (
                f"🔔 *Neuer Lead für dich!*\n\n"
                f"👤 *Name:* {lead['name']}\n"
                f"📞 *Telefon:* {lead['phone']}\n"
                f"📧 *Email:* {lead['email']}\n\n"
                f"💰 Guthaben: {round(partner['guthaben'] - LEAD_PREIS, 2)}€ verbleibend\n"
                f"📊 Lead Nr. {partner['leads_geliefert'] + 1}\n\n"
                f"Bitte kontaktiere den Lead so schnell wie möglich! 🚀"
            )
            partner_wa_result = send_whatsapp(partner["telefon"], partner_msg)

            # WhatsApp an Lead
            lead_wa_result = {"skipped": True}
            if lead["phone"]:
                lead_msg = (
                    f"Hallo {lead['name']}! 👋\n\n"
                    f"Vielen Dank für dein Interesse! Dein persönlicher Ansprechpartner "
                    f"*{partner['name']}* wird sich in Kürze bei dir melden.\n\n"
                    f"Wir freuen uns auf das Gespräch! 😊"
                )
                lead_wa_result = send_whatsapp(lead["phone"], lead_msg)

            # Lead-Status in Tabellenblatt1 auf "VERTEILT" setzen
            leads_sheet.update_cell(lead["row"], 16, "VERTEILT")

            # Im Leads_Log dokumentieren
            neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
            wa_partner_ok = "error" not in partner_wa_result
            wa_lead_ok = "error" not in lead_wa_result and "skipped" not in lead_wa_result
            log_lead(
                lead_name=lead["name"], lead_phone=lead["phone"],
                lead_email=lead["email"], partner_name=partner["name"],
                partner_phone=partner["telefon"], guthaben_nachher=neues_guthaben,
                wa_partner_ok=wa_partner_ok, wa_lead_ok=wa_lead_ok, status="VERTEILT",
            )

            logger.info(f"✅ Lead {lead['name']} → {partner['name']} verteilt")
            processed += 1

            # Matze benachrichtigen über erfolgreiche Verteilung
            if MATZE_PHONE:
                matze_info = (
                    f"🔔 *LEAD VERTEILT (Facebook Formular)*\n\n"
                    f"👤 *Lead:* {lead['name']}\n"
                    f"📞 *Telefon:* {lead['phone']}\n"
                    f"📧 *Email:* {lead['email']}\n\n"
                    f"➡️ *Zugewiesen an:* {partner['name']}\n"
                    f"💰 *Partner-Guthaben:* {neues_guthaben}€\n\n"
                    f"✅ Dieser Lead wurde automatisch über das Facebook-Formular verteilt."
                )
                send_whatsapp(MATZE_PHONE, matze_info)

            # Kurze Pause zwischen Leads (API Rate Limits)
            time.sleep(2)

        except Exception as e:
            logger.error(f"Fehler bei Lead {lead['name']}: {e}")
            # Bei unbekanntem Fehler: Lead zurück auf CREATED setzen
            try:
                leads_sheet.update_cell(lead["row"], 16, "CREATED")
            except:
                pass
            errors += 1

    logger.info(f"=== Polling fertig: {processed} verteilt, {errors} Fehler ===")
    return {"processed": processed, "errors": errors, "total_new": len(new_leads)}


# ─── Hintergrund-Polling-Thread ──────────────────────────────────────────────
polling_active = True

def polling_loop():
    """Hintergrund-Thread der alle POLL_INTERVAL Sekunden neue Leads prüft."""
    logger.info(f"📡 Polling-Thread gestartet (Intervall: {POLL_INTERVAL}s)")
    while polling_active:
        try:
            result = poll_new_leads()
            if result.get("processed", 0) > 0:
                logger.info(f"Polling-Ergebnis: {result}")
        except Exception as e:
            logger.error(f"Polling-Fehler: {e}")
        
        # Warten bis zum nächsten Durchlauf
        time.sleep(POLL_INTERVAL)


@app.on_event("startup")
async def startup_event():
    """Startet den Polling-Thread beim Server-Start."""
    thread = threading.Thread(target=polling_loop, daemon=True)
    thread.start()
    logger.info("🚀 Lead-Verteilungs-Service v3.2 gestartet (mit Lock-Schutz)")


# ─── Lead-Daten aus Facebook Webhook extrahieren ─────────────────────────────
def extract_lead_data_from_facebook(payload: dict) -> Optional[dict]:
    if "name" in payload or "full_name" in payload:
        return {
            "name": payload.get("name") or payload.get("full_name", "Unbekannt"),
            "email": payload.get("email", ""),
            "phone": payload.get("phone") or payload.get("phone_number", ""),
        }
    if "field_data" in payload:
        data = {"name": "Unbekannt", "email": "", "phone": ""}
        for field in payload["field_data"]:
            field_name = field.get("name", "").lower()
            values = field.get("values", [])
            value = values[0] if values else ""
            if "name" in field_name:
                data["name"] = value
            elif "email" in field_name:
                data["email"] = value
            elif "phone" in field_name:
                data["phone"] = value
        return data
    leadgen_id = None
    try:
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                leadgen_id = change.get("value", {}).get("leadgen_id")
                if leadgen_id:
                    break
            if leadgen_id:
                break
    except (KeyError, TypeError, IndexError):
        pass
    if leadgen_id and FB_ACCESS_TOKEN:
        return fetch_lead_from_facebook(leadgen_id)
    elif leadgen_id:
        logger.warning(f"leadgen_id {leadgen_id} erhalten, aber FB_ACCESS_TOKEN nicht gesetzt!")
        return None
    name = (payload.get("name") or payload.get("full_name") or
            payload.get("vorname", "") + " " + payload.get("nachname", "")).strip()
    phone = payload.get("phone") or payload.get("phone_number") or payload.get("telefon", "")
    email = payload.get("email") or payload.get("e_mail", "")
    if name or phone or email:
        return {"name": name or "Unbekannt", "email": email, "phone": phone}
    return None


def fetch_lead_from_facebook(leadgen_id: str) -> Optional[dict]:
    url = f"https://graph.facebook.com/v19.0/{leadgen_id}"
    params = {"access_token": FB_ACCESS_TOKEN}
    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        fb_data = response.json()
        data = {"name": "Unbekannt", "email": "", "phone": ""}
        for field in fb_data.get("field_data", []):
            field_name = field.get("name", "").lower()
            values = field.get("values", [])
            value = values[0] if values else ""
            if "name" in field_name:
                data["name"] = value
            elif "email" in field_name:
                data["email"] = value
            elif "phone" in field_name:
                data["phone"] = value
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Facebook API Fehler: {e}")
        return None


# ─── Hauptprozess: Lead verarbeiten (für Webhook) ───────────────────────────
def process_lead(lead_data: dict) -> dict:
    """Hauptprozess der Lead-Verteilung (für Webhook-Eingang)."""
    lead_name = lead_data.get("name", "Unbekannt")
    lead_phone = normalize_phone(lead_data.get("phone", ""))
    lead_email = lead_data.get("email", "")

    logger.info(f"=== Neuer Lead (Webhook): {lead_name} | {lead_phone} | {lead_email} ===")

    try:
        sheet = get_sheet()
    except Exception as e:
        logger.error(f"Sheet-Fehler: {e}")
        return {"status": "error", "message": str(e)}

    partner = find_best_partner(sheet)
    if not partner:
        if MATZE_PHONE:
            send_whatsapp(MATZE_PHONE,
                f"⚠️ *Lead ohne Partner!*\n\n👤 {lead_name}\n📞 {lead_phone}\n📧 {lead_email}")
        log_lead(lead_name, lead_phone, lead_email, "KEIN PARTNER", "", 0, False, False, "KEIN_PARTNER")
        return {"status": "error", "message": "Kein Partner verfügbar"}

    if not update_partner(sheet, partner):
        return {"status": "error", "message": "Partner-Update fehlgeschlagen"}

    partner_msg = (
        f"🔔 *Neuer Lead für dich!*\n\n"
        f"👤 *Name:* {lead_name}\n📞 *Telefon:* {lead_phone}\n📧 *Email:* {lead_email}\n\n"
        f"💰 Guthaben: {round(partner['guthaben'] - LEAD_PREIS, 2)}€ verbleibend\n"
        f"📊 Lead Nr. {partner['leads_geliefert'] + 1}\n\n"
        f"Bitte kontaktiere den Lead so schnell wie möglich! 🚀"
    )
    partner_wa_result = send_whatsapp(partner["telefon"], partner_msg)

    lead_wa_result = {"skipped": True}
    if lead_phone:
        lead_msg = (
            f"Hallo {lead_name}! 👋\n\n"
            f"Vielen Dank für dein Interesse! Dein persönlicher Ansprechpartner "
            f"*{partner['name']}* wird sich in Kürze bei dir melden.\n\n"
            f"Wir freuen uns auf das Gespräch! 😊"
        )
        lead_wa_result = send_whatsapp(lead_phone, lead_msg)

    neues_guthaben = round(partner["guthaben"] - LEAD_PREIS, 2)
    wa_partner_ok = "error" not in partner_wa_result
    wa_lead_ok = "error" not in lead_wa_result and "skipped" not in lead_wa_result
    log_lead(lead_name, lead_phone, lead_email, partner["name"], partner["telefon"],
             neues_guthaben, wa_partner_ok, wa_lead_ok, "VERTEILT")

    return {
        "status": "success",
        "lead": {"name": lead_name, "phone": lead_phone, "email": lead_email},
        "partner": {"name": partner["name"], "neues_guthaben": neues_guthaben},
    }


# ─── API Endpoints ───────────────────────────────────────────────────────────

@app.get("/")
async def root():
    return {
        "service": "Lead-Verteilungs-Service",
        "version": "3.2.0",
        "status": "running",
        "features": [
            "Sheet-Polling mit Lock-Schutz (keine doppelte Verteilung)",
            "PROCESSING-Status verhindert Race Conditions",
            "Faire Verteilung (zeitbasiert/Round-Robin)",
            "Stripe-Webhook (automatische Partner-Registrierung)",
            "WhatsApp-Benachrichtigungen (Partner + Lead)",
            "Leads_Log (komplette Dokumentation)",
        ],
        "endpoints": {
            "webhook": "/webhook (Facebook Lead Ads)",
            "stripe": "/stripe-webhook (Stripe Zahlungen)",
            "poll": "/poll (Manuelles Polling auslösen)",
            "health": "/health",
            "test": "/test",
            "partner": "/partner",
        },
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat(), "version": "3.2.0"}


# ─── Facebook Webhook ────────────────────────────────────────────────────────

@app.get("/webhook")
async def webhook_verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")
    if mode == "subscribe" and token == FB_VERIFY_TOKEN:
        logger.info("Facebook Webhook verifiziert!")
        return int(challenge)
    raise HTTPException(status_code=403, detail="Verification failed")


@app.post("/webhook")
async def webhook_receive(request: Request, background_tasks: BackgroundTasks):
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    logger.info(f"Webhook empfangen: {json.dumps(payload, ensure_ascii=False)[:500]}")
    lead_data = extract_lead_data_from_facebook(payload)
    if not lead_data:
        return JSONResponse(status_code=200, content={"status": "error", "message": "Keine Lead-Daten"})
    background_tasks.add_task(process_lead, lead_data)
    return {"status": "received", "message": "Lead wird verarbeitet"}


# ─── Stripe Webhook ──────────────────────────────────────────────────────────

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request, background_tasks: BackgroundTasks):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    if STRIPE_WEBHOOK_SECRET and sig_header:
        try:
            event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
        except stripe.error.SignatureVerificationError as e:
            logger.error(f"Stripe Signatur ungültig: {e}")
            raise HTTPException(status_code=400, detail="Invalid signature")
        except Exception as e:
            logger.error(f"Stripe Webhook Fehler: {e}")
            raise HTTPException(status_code=400, detail=str(e))
    else:
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid JSON")

    event_type = event.get("type", "") if isinstance(event, dict) else event.type
    logger.info(f"Stripe Event: {event_type}")

    if event_type == "checkout.session.completed":
        if isinstance(event, dict):
            session = event.get("data", {}).get("object", {})
        else:
            session = event.data.object

        customer_name = ""
        customer_phone = ""
        customer_email = ""
        amount = 0.0

        if isinstance(session, dict):
            cd = session.get("customer_details", {}) or {}
            customer_name = cd.get("name", "") or session.get("customer_name", "") or ""
            customer_email = cd.get("email", "") or session.get("customer_email", "") or ""
            customer_phone = cd.get("phone", "") or ""
            amount = (session.get("amount_total", 0) or 0) / 100.0
            metadata = session.get("metadata", {}) or {}
            if metadata.get("partner_name"):
                customer_name = metadata["partner_name"]
            if metadata.get("partner_phone"):
                customer_phone = metadata["partner_phone"]
        else:
            cd = getattr(session, "customer_details", None)
            if cd:
                customer_name = getattr(cd, "name", "") or ""
                customer_email = getattr(cd, "email", "") or ""
                customer_phone = getattr(cd, "phone", "") or ""
            amount = (getattr(session, "amount_total", 0) or 0) / 100.0

        if not customer_name:
            customer_name = customer_email.split("@")[0] if customer_email else "Unbekannt"

        background_tasks.add_task(process_stripe_payment, customer_name, customer_phone, customer_email, amount)
        return {"status": "received", "message": "Zahlung wird verarbeitet"}

    return {"status": "ignored", "event_type": event_type}


# ─── Manuelles Polling und Test Endpoints ────────────────────────────────────

@app.post("/poll")
async def manual_poll():
    """Manuell das Sheet-Polling auslösen (für Tests oder sofortige Verteilung)."""
    result = poll_new_leads()
    return {"status": "ok", "result": result}


@app.post("/test")
async def test_lead():
    """Test-Endpoint: Zeigt welcher Partner den nächsten Lead bekommen würde."""
    try:
        sheet = get_sheet()
        partner = find_best_partner(sheet)
        
        # Auch neue Leads zählen
        leads_sheet = get_leads_sheet()
        all_values = leads_sheet.get_all_values()
        new_leads_count = sum(1 for row in all_values[1:] if len(row) > 15 and row[15] == "CREATED")

        if partner:
            return {
                "status": "test_ok",
                "message": "System funktioniert! (v3.2 mit Lock-Schutz)",
                "neue_leads_im_sheet": new_leads_count,
                "naechster_partner": partner["name"],
                "partner_guthaben": partner["guthaben"],
                "partner_leads": partner["leads_geliefert"],
                "partner_letzter_lead": partner["letzter_lead"] or "NIE (neuer Partner)",
                "polling_intervall": f"{POLL_INTERVAL}s",
            }
        else:
            return {
                "status": "test_warning",
                "message": "Kein aktiver Partner gefunden!",
                "neue_leads_im_sheet": new_leads_count,
            }
    except Exception as e:
        return {"status": "test_error", "message": str(e)}


@app.get("/partner")
async def list_partners():
    try:
        sheet = get_sheet()
        records = get_all_partner_records(sheet)
        partners = []
        for record in records:
            partners.append({
                "name": record.get("Name", ""),
                "status": record.get("Status", ""),
                "guthaben": record.get("Guthaben_Euro", 0),
                "leads": record.get("Leads_Geliefert", 0),
                "letzter_lead": record.get("Letzter_Lead_Am", ""),
            })
        return {"partners": partners, "total": len(partners)}
    except Exception as e:
        return {"error": str(e)}


@app.post("/webhook/manual")
async def manual_lead(request: Request):
    """Manueller Lead-Eingang."""
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    lead_data = {
        "name": payload.get("name", "Unbekannt"),
        "phone": normalize_phone(payload.get("phone", "")),
        "email": payload.get("email", ""),
    }
    result = process_lead(lead_data)
    return result


# ─── Korrektur-Endpoint (einmalig für Duplikat-Fix) ─────────────────────────
@app.get("/fix-accounts")
async def fix_accounts():
    """Einmaliger Fix: Korrigiert Partner-Konten nach doppelter Verteilung."""
    try:
        sheet = get_sheet()
        results = []
        
        # Alle Partner lesen
        all_records = get_all_partner_records(sheet)
        for idx, record in enumerate(all_records):
            name = record.get("Name", "")
            row = idx + 2
            
            if "Michael" in name:
                # Michael: Guthaben +5€ (35→40), Leads -1 (3→2)
                current_guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
                current_leads = int(record.get("Leads_Geliefert", 0))
                new_guthaben = current_guthaben + 5
                new_leads = max(current_leads - 1, 0)
                sheet.update_cell(row, 3, new_guthaben)
                sheet.update_cell(row, 4, new_leads)
                results.append(f"Michael: {current_guthaben}€→{new_guthaben}€, {current_leads}→{new_leads} Leads")
            
            elif "Mathias" in name or "Matze" in name:
                # Mathias: Guthaben +5€ (490→495), Leads -1 (2→1)
                current_guthaben = float(str(record.get("Guthaben_Euro", 0)).replace(",", "."))
                current_leads = int(record.get("Leads_Geliefert", 0))
                new_guthaben = current_guthaben + 5
                new_leads = max(current_leads - 1, 0)
                sheet.update_cell(row, 3, new_guthaben)
                sheet.update_cell(row, 4, new_leads)
                results.append(f"Mathias: {current_guthaben}€→{new_guthaben}€, {current_leads}→{new_leads} Leads")
        
        # Leads_Log: Duplikate markieren
        log_sheet = get_leads_log_sheet()
        log_values = log_sheet.get_all_values()
        seen_leads = set()
        duplicates_fixed = 0
        for row_idx, row in enumerate(log_values[1:], start=2):
            lead_key = f"{row[1]}_{row[2]}"  # Name + Telefon
            if lead_key in seen_leads:
                log_sheet.update_cell(row_idx, 10, "DUPLIKAT_KORRIGIERT")
                duplicates_fixed += 1
            else:
                seen_leads.add(lead_key)
        
        results.append(f"{duplicates_fixed} Duplikate im Log markiert")
        
        return {"status": "ok", "fixes": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ─── Test WhatsApp Endpoint ──────────────────────────────────────────────────
@app.get("/test-whatsapp")
async def test_whatsapp():
    """Sendet eine Test-WhatsApp-Nachricht um die API zu prüfen."""
    test_phone = MATZE_PHONE  # Sende an Matze als Test
    if not test_phone:
        return {"error": "MATZE_PHONE nicht gesetzt"}
    
    test_msg = "Test-Nachricht vom Lead-Verteilungs-System. WhatsApp-Versand funktioniert!"
    result = send_whatsapp(test_phone, test_msg)
    return {
        "test_phone": test_phone,
        "result": result,
        "whapi_url": WHAPI_URL,
        "has_token": bool(WHAPI_TOKEN),
    }


@app.get("/test-whatsapp-lead/{phone}")
async def test_whatsapp_lead(phone: str):
    """Sendet eine Test-WhatsApp-Nachricht an eine beliebige Nummer."""
    normalized = normalize_phone(phone)
    if not normalized:
        return {"error": "Ungültige Telefonnummer"}
    
    test_msg = (
        f"Hallo! 👋\n\n"
        f"Dies ist eine Test-Nachricht vom Lead-Verteilungs-System.\n"
        f"Dein persönlicher Ansprechpartner wird sich in Kürze bei dir melden.\n\n"
        f"Wir freuen uns auf das Gespräch! 😊"
    )
    result = send_whatsapp(normalized, test_msg)
    return {
        "original_phone": phone,
        "normalized_phone": normalized,
        "result": result,
        "whapi_url": WHAPI_URL,
        "has_token": bool(WHAPI_TOKEN),
    }


# ─── Server starten ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
