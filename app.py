"""
Lead-Verteilungs-Service v3.6-META (WORKING)
=============================================
v3.6 Code mit Meta API statt Whapi (NUR WhatsApp Funktion geändert)
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
META_TOKEN = os.getenv("META_TOKEN", "")
META_PHONE_ID = os.getenv("META_PHONE_ID", "")
META_URL = f"https://graph.facebook.com/v18.0/{META_PHONE_ID}/messages"
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
    version="3.6-META",
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


# ─── META WHATSAPP (NUR DIESE FUNKTION GEÄNDERT) ─────────────────────────────
def send_whatsapp(phone: str, message: str) -> dict:
    if not META_TOKEN:
        logger.error("META_TOKEN nicht gesetzt!")
        return {"error": "META_TOKEN nicht konfiguriert"}

    if not phone or len(phone) < 10:
        logger.error(f"Ungültige Telefonnummer: '{phone}'")
        return {"error": f"Ungültige Telefonnummer: {phone}"}

    # Meta API Format: Nummer ohne + und ohne @s.whatsapp.net
    to = phone.replace("+", "").replace(" ", "").replace("@s.whatsapp.net", "")
    
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json",
    }
    
    # Meta API Payload
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to,
        "type": "text",
        "text": {
            "preview_url": False,
            "body": message
        }
    }

    try:
        logger.info(f"WhatsApp senden an {phone} (Meta API)...")
        response = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        logger.info(f"WhatsApp Response Status: {response.status_code}")
        logger.info(f"WhatsApp Response Body: {response.text[:500]}")
        
        if response.status_code >= 400:
            logger.error(f"Meta API Error: {response.text}")
            return {"error": response.text, "status_code": response.status_code}
            
        result = response.json()
        logger.info(f"WhatsApp gesendet an {phone}: OK")
        return result
        
    except requests.exceptions.RequestException as e:
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
    """Fügt einen neuen Partner hinzu."""
    try:
        normalized_phone = normalize_phone(phone)
        now = ""
        new_row = [name, normalized_phone, guthaben, 0, now, "Aktiv"]
        sheet.append_row(new_row, value_input_option="USER_ENTERED")
        logger.info(f"Neuer Partner hinzugefügt: {name}")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Hinzufügen von Partner {name}: {e}")
        return False


# ─── Lead-Verarbeitung ───────────────────────────────────────────────────────
def process_single_lead(lead_row_idx: int, lead_data: dict) -> bool:
    """
    Verarbeitet EINEN Lead:
    1. Sucht besten Partner (fair)
    2. Aktualisiert Partner (Guthaben -5€, Leads +1)
    3. Sendet WhatsApp an Partner
    4. Sendet WhatsApp an Lead (wenn Telefon vorhanden)
    5. Loggt alles
    """
    sheet = get_sheet()
    leads_sheet = get_leads_sheet()
    
    lead_name = lead_data.get("name", "")
    lead_phone = lead_data.get("phone", "")
    lead_email = lead_data.get("email", "")
    
    logger.info(f"Verarbeite Lead: {lead_name} ({lead_email})")
    
    # Status auf PROCESSING setzen (verhindert Doppelverarbeitung)
    try:
        leads_sheet.update_cell(lead_row_idx, 16, "PROCESSING")
    except Exception as e:
        logger.error(f"Konnte Status nicht auf PROCESSING setzen: {e}")
    
    # Besten Partner finden
    partner = find_best_partner(sheet)
    if not partner:
        logger.error("Kein Partner verfügbar!")
        try:
            leads_sheet.update_cell(lead_row_idx, 16, "KEIN_PARTNER")
        except:
            pass
        return False
    
    # Partner aktualisieren
    if not update_partner(sheet, partner):
        logger.error("Partner-Update fehlgeschlagen!")
        return False
    
    # WhatsApp an Partner
    partner_msg = (
        f"🎯 *Neuer Lead!*\n\n"
        f"👤 Name: {lead_name}\n"
        f"📧 Email: {lead_email}\n"
        f"📱 Telefon: {lead_phone or 'Nicht vorhanden'}\n\n"
        f"💰 Guthaben: {partner['guthaben'] - LEAD_PREIS}€"
    )
    wa_partner_result = send_whatsapp(partner["telefon"], partner_msg)
    wa_partner_ok = "error" not in wa_partner_result
    
    # WhatsApp an Lead (wenn Telefon vorhanden)
    wa_lead_ok = False
    if lead_phone:
        lead_msg = (
            f"Hallo {lead_name},\n\n"
            f"Danke für dein Interesse! {partner['name']} wird sich in Kürze bei dir melden.\n\n"
            f"Bei Fragen erreichst du uns jederzeit."
        )
        wa_lead_result = send_whatsapp(lead_phone, lead_msg)
        wa_lead_ok = "error" not in wa_lead_result
    
    # Logging
    log_lead(
        lead_name, lead_phone, lead_email,
        partner["name"], partner["telefon"], partner["guthaben"] - LEAD_PREIS,
        wa_partner_ok, wa_lead_ok, "VERTEILT"
    )
    
    # Status auf VERTEILT setzen
    try:
        leads_sheet.update_cell(lead_row_idx, 16, "VERTEILT")
    except Exception as e:
        logger.error(f"Konnte Status nicht auf VERTEILT setzen: {e}")
    
    return True


# ─── Polling für neue Leads ──────────────────────────────────────────────────
def poll_new_leads():
    """
    Pollt das Leads-Sheet nach neuen Leads (Status = NEU oder leer).
    """
    if not poll_lock.acquire(blocking=False):
        logger.info("Polling läuft bereits, überspringe...")
        return
    
    try:
        leads_sheet = get_leads_sheet()
        all_values = leads_sheet.get_all_values()
        
        if len(all_values) <= 1:
            logger.info("Keine Leads im Sheet")
            return
        
        headers = all_values[0]
        
        # Spalten-Indizes finden
        name_idx = None
        email_idx = None
        phone_idx = None
        status_idx = None
        
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if "name" in h_lower or "vollständiger name" in h_lower:
                name_idx = i
            elif "email" in h_lower or "e-mail" in h_lower:
                email_idx = i
            elif "phone" in h_lower or "telefon" in h_lower or "mobil" in h_lower:
                phone_idx = i
            elif "status" in h_lower or "lead_status" in h_lower:
                status_idx = i
        
        # Fallback: Standard-Positionen
        if name_idx is None:
            name_idx = 0
        if email_idx is None:
            email_idx = 1 if len(headers) > 1 else 0
        if phone_idx is None:
            phone_idx = 2 if len(headers) > 2 else 0
        if status_idx is None:
            status_idx = 15 if len(headers) > 15 else len(headers) - 1
        
        logger.info(f"Spalten-Indizes: Name={name_idx}, Email={email_idx}, Phone={phone_idx}, Status={status_idx}")
        
        new_leads_found = 0
        
        for row_idx, row in enumerate(all_values[1:], start=2):
            # Status prüfen (Spalte P = Index 15, oder gefundener Index)
            status = ""
            if len(row) > status_idx:
                status = row[status_idx].strip().upper()
            
            # Nur NEU oder leere Status verarbeiten
            if status not in ["", "NEU"]:
                continue
            
            # Lead-Daten extrahieren
            lead_data = {
                "name": row[name_idx] if len(row) > name_idx else "",
                "email": row[email_idx] if len(row) > email_idx else "",
                "phone": normalize_phone(row[phone_idx]) if len(row) > phone_idx else "",
            }
            
            if not lead_data["name"] and not lead_data["email"]:
                continue
            
            logger.info(f"Neuer Lead gefunden (Zeile {row_idx}): {lead_data['name']}")
            new_leads_found += 1
            
            # Lead verarbeiten
            success = process_single_lead(row_idx, lead_data)
            
            if success:
                logger.info(f"Lead {row_idx} erfolgreich verteilt")
            else:
                logger.error(f"Lead {row_idx} konnte nicht verteilt werden")
        
        if new_leads_found == 0:
            logger.info("Keine neuen Leads gefunden")
        else:
            logger.info(f"{new_leads_found} neue Leads verarbeitet")
            
    except Exception as e:
        logger.error(f"Fehler beim Polling: {e}")
    finally:
        poll_lock.release()


# ─── Hintergrund-Task für Polling ────────────────────────────────────────────
def start_polling():
    """Startet das Polling in einem Hintergrund-Thread."""
    def poll_loop():
        logger.info(f"Polling gestartet (Intervall: {POLL_INTERVAL}s)")
        while True:
            try:
                poll_new_leads()
            except Exception as e:
                logger.error(f"Fehler in Polling-Loop: {e}")
            time.sleep(POLL_INTERVAL)
    
    thread = threading.Thread(target=poll_loop, daemon=True)
    thread.start()
    logger.info("Polling-Thread gestartet")


# ─── API Endpoints ───────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "Lead-Verteilungs-Service",
        "version": "3.6-META",
        "features": ["sheet_polling", "facebook_webhook", "stripe_webhook", "meta_whatsapp"]
    }


@app.get("/health")
def health():
    return {"status": "healthy"}


# Facebook Webhook Verification
@app.get("/webhook/facebook")
def facebook_verify(request: Request):
    """Facebook Webhook Verification."""
    params = dict(request.query_params)
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")
    
    if mode == "subscribe" and token == FB_VERIFY_TOKEN:
        logger.info("Facebook Webhook verifiziert")
        return int(challenge) if challenge else "OK"
    
    raise HTTPException(status_code=403, detail="Verification failed")


# Facebook Lead Ads Webhook
@app.post("/webhook/facebook")
async def facebook_webhook(request: Request):
    """Empfängt Facebook Lead Ads."""
    try:
        data = await request.json()
        logger.info(f"Facebook Webhook erhalten: {json.dumps(data)[:500]}")
        
        # Lead-Daten extrahieren
        entry = data.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value = changes.get("value", {})
        
        lead_id = value.get("leadgen_id")
        form_id = value.get("form_id")
        
        if lead_id:
            logger.info(f"Neuer Lead von Facebook: {lead_id}")
            # Lead-Daten aus Facebook holen (wenn Access Token vorhanden)
            if FB_ACCESS_TOKEN:
                try:
                    fb_url = f"https://graph.facebook.com/v18.0/{lead_id}?access_token={FB_ACCESS_TOKEN}"
                    fb_response = requests.get(fb_url, timeout=30)
                    if fb_response.status_code == 200:
                        lead_data_fb = fb_response.json()
                        logger.info(f"Lead-Daten von Facebook: {json.dumps(lead_data_fb)[:500]}")
                        
                        # Felder extrahieren
                        field_data = lead_data_fb.get("field_data", [])
                        lead_info = {}
                        for field in field_data:
                            lead_info[field["name"]] = field["values"][0] if field["values"] else ""
                        
                        # Ins Sheet schreiben
                        leads_sheet = get_leads_sheet()
                        new_row = [
                            lead_info.get("full_name", ""),
                            lead_info.get("email", ""),
                            normalize_phone(lead_info.get("phone_number", "")),
                            "NEU",
                            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            form_id,
                            lead_id
                        ]
                        leads_sheet.append_row(new_row, value_input_option="USER_ENTERED")
                        logger.info(f"Lead {lead_id} ins Sheet geschrieben")
                        
                except Exception as e:
                    logger.error(f"Fehler beim Holen der Lead-Daten von Facebook: {e}")
        
        return {"status": "ok"}
        
    except Exception as e:
        logger.error(f"Fehler im Facebook Webhook: {e}")
        return {"status": "error", "message": str(e)}


# Stripe Webhook
@app.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    """Empfängt Stripe Payment Events."""
    try:
        payload = await request.body()
        sig_header = request.headers.get("stripe-signature")
        
        if STRIPE_WEBHOOK_SECRET and sig_header:
            try:
                event = stripe.Webhook.construct_event(
                    payload, sig_header, STRIPE_WEBHOOK_SECRET
                )
            except stripe.error.SignatureVerificationError:
                logger.error("Stripe Signature ungültig")
                raise HTTPException(status_code=400, detail="Invalid signature")
        else:
            data = await request.json()
            event = {"type": data.get("type"), "data": {"object": data.get("data", {}).get("object", {})}}
        
        event_type = event.get("type") if isinstance(event, dict) else event.type
        event_data = event.get("data", {}).get("object", {}) if isinstance(event, dict) else event.data.object
        
        logger.info(f"Stripe Event: {event_type}")
        
        if event_type == "checkout.session.completed":
            customer_email = event_data.get("customer_details", {}).get("email", "")
            amount = event_data.get("amount_total", 0) / 100  # Cent zu Euro
            
            logger.info(f"Zahlung erhalten: {customer_email} - {amount}€")
            
            # Partner finden oder erstellen
            sheet = get_sheet()
            partner = find_partner_by_name(sheet, customer_email)
            
            if partner:
                # Guthaben aufladen
                new_guthaben = partner["guthaben"] + amount
                sheet.update_cell(partner["row"], 3, new_guthaben)
                sheet.update_cell(partner["row"], 6, "Aktiv")
                logger.info(f"Guthaben aufgeladen: {partner['name']} - {partner['guthaben']}€ → {new_guthaben}€")
                
                # WhatsApp Benachrichtigung
                if partner["telefon"]:
                    msg = f"✅ *Zahlung erhalten!*\n\nGuthaben: {new_guthaben}€\nDu bist wieder aktiv."
                    send_whatsapp(partner["telefon"], msg)
            else:
                # Neuen Partner anlegen
                add_new_partner(sheet, customer_email, "", amount)
                logger.info(f"Neuer Partner angelegt: {customer_email} mit {amount}€")
        
        return {"status": "ok"}
        
    except Exception as e:
        logger.error(f"Fehler im Stripe Webhook: {e}")
        return {"status": "error", "message": str(e)}


# Manuelles Triggern der Lead-Verarbeitung
@app.post("/process-leads")
def process_leads_endpoint():
    """Manuelles Auslösen der Lead-Verarbeitung."""
    try:
        poll_new_leads()
        return {"status": "ok", "message": "Leads verarbeitet"}
    except Exception as e:
        logger.error(f"Fehler beim manuellen Verarbeiten: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Startup ─────────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup_event():
    logger.info("Lead-Verteilungs-Service gestartet")
    logger.info(f"Meta API URL: {META_URL}")
    logger.info(f"Google Sheet: {GOOGLE_SHEET_ID}")
    start_polling()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
