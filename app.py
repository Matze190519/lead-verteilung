"""
Lead-Verteilungs-Service v5.2 - Spalten-Fix + vollständiger UTM-Tracking
=========================================================================

CHANGELOG v5.2:
- ✅ Korrektes Spalten-Mapping (Name=N, Email=M, Phone=O, Status=P)
- ✅ Vollständige UTM-Integration (Kampagne, Anzeige, Facebook-Ads-Library-Link)
- ✅ Status-Filter: nur CREATED-Leads werden verarbeitet
- ✅ Partner-WhatsApp enthält Kampagnen-Info + Ad-Link
- ✅ Admin-Benachrichtigung mit Kampagnen-Daten
- ✅ Tägliche Erinnerungen um 08:00 Uhr
- ✅ 24h-Fenster-Erkennung (Error #100)

SPALTEN-MAPPING (Google Sheets):
A = id
B = created_time
C = ad_id
D = ad_name
E = adset_id
F = adset_name
G = campaign_id
H = campaign_name
I = form_id
J = form_name
K = is_organic
L = platform
M = e-mail-adresse
N = vollständiger_name
O = telefonnummer
P = lead_status
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import logging
import os
import time
from datetime import datetime, timezone
import stripe
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# ===========================
# LOGGING KONFIGURATION
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ===========================
# META WHATSAPP API CONFIG
# ===========================
META_TOKEN = "EAAVZBgrngqUwBO9W2lGDKGPAX7dT5tD4mpMgXiCZCW9MhOPfjILEqm5bZAJZAi0eWq9hSdHn3Aoy7ej72E7ZCnvxXp0ZBQRIyY9jqm1u5UcOQm90eqkIZCQkfhPGQZBgsBm4mjQCw93AKKoJXqLrvCYwZAFZBEG1uKsmMPDbmZBPDQ2e3vF2hhZBphYJWJU9e5IgZDZD"
META_PHONE_ID = "623007617563961"
META_URL = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"

# Admin-Nummer (Matze)
MATZE_PHONE = "491715060008"

# ===========================
# STRIPE KONFIGURATION
# ===========================
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "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")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_urcxW3DyRxhcAM25Q7wJvGdMzHFoTGAu")

# ===========================
# GOOGLE SHEETS KONFIGURATION
# ===========================
SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SHEET_ID = "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY"
CREDENTIALS_JSON = {
    "type": "service_account",
    "project_id": "leadverteilung",
    "private_key_id": "4bc0fa18ba3a9f7baa9dec4d5bc6f25cf0ae7fa0",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDlvHeCPY3xLI+P\nmS/0r6wqtxnlPEm5bQpZgPt4oIMAZDCBBl1W+zfwggvwZPGZO3jZGc8X8jkDPAEb\nJ8BsNWEqXL6bz2WVCK5dZIw/LSl1Gy3IVBf5lR7H/LqKcgBWYz3e7Hpe/6C5V4uS\nMqCVyOE8ZJ7K0JuDn+8oKxULhB7HzkGDKGWZcYHg7bL5nt9TXkW5aCDWx1Zb1LQX\nVmYnG9gFMZoB8ZX3HpI9C5qY0P5LqNKdVLz7Q8z7L5xBFqYQJ7k8HZz5L5xBFqYQ\nJ7k8HZz5L5xBFqYQJ7k8HZzP5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqY\nQJ7k8HZzAgMBAAECggEABYx7xL5Z8H3qY7z5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HQKBgQD5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7\nk8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7\nk8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7\nk8HZz5L5xBFqYQKBgQDrxBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZ\nz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZ\nz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZ\nz5L5xBFqYQKBgQD5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5\nxBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5\nxBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5\nxBFqYQKBgH5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqY\nQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqY\nQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqY\nQKBgH5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8\nHZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQJ7k8HZz5L5xBFqYQ\n-----END PRIVATE KEY-----\n",
    "client_email": "leadverteilung@leadverteilung.iam.gserviceaccount.com",
    "client_id": "111695653356748356050",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/leadverteilung%40leadverteilung.iam.gserviceaccount.com"
}

# ===========================
# GLOBALE VARIABLEN
# ===========================
LEAD_PREIS = 5.0
processed_lead_ids = set()
partner_responses_today = set()

# ===========================
# FASTAPI INIT
# ===========================
app = FastAPI(title="Lead-Verteilungs-Service v5.2")

# ===========================
# HELPER FUNCTIONS
# ===========================

def get_google_sheets_client():
    """Verbindung zu Google Sheets"""
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_dict(CREDENTIALS_JSON, SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"❌ Google Sheets Verbindung fehlgeschlagen: {e}")
        return None

def get_partner_sheet():
    """Partner-Sheet laden"""
    try:
        client = get_google_sheets_client()
        if not client:
            return None
        sheet = client.open_by_key(SHEET_ID).worksheet("Partner")
        return sheet
    except Exception as e:
        logger.error(f"❌ Partner-Sheet konnte nicht geladen werden: {e}")
        return None

def get_lead_sheet():
    """Lead-Sheet laden"""
    try:
        client = get_google_sheets_client()
        if not client:
            return None
        sheet = client.open_by_key(SHEET_ID).worksheet("Leads")
        return sheet
    except Exception as e:
        logger.error(f"❌ Lead-Sheet konnte nicht geladen werden: {e}")
        return None

def send_whatsapp(phone: str, message: str) -> dict:
    """WhatsApp-Nachricht senden via Meta Cloud API"""
    try:
        clean_phone = phone.replace("+", "").replace(" ", "").replace("-", "")
        if clean_phone.startswith("p:"):
            clean_phone = clean_phone[2:]
        
        payload = {
            "messaging_product": "whatsapp",
            "to": clean_phone,
            "type": "text",
            "text": {"body": message}
        }
        
        headers = {
            "Authorization": f"Bearer {META_TOKEN}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(META_URL, json=payload, headers=headers, timeout=10)
        result = response.json()
        
        if response.status_code == 200:
            logger.info(f"[META_RESPONSE] Status=200 | Phone={clean_phone}")
            return {"success": True, "response": result}
        else:
            error_msg = result.get("error", {}).get("message", "Unknown error")
            logger.error(f"[META_ERROR] Status={response.status_code} | Phone={clean_phone} | Error={error_msg}")
            return {"success": False, "error": error_msg, "status": response.status_code}
            
    except Exception as e:
        logger.error(f"❌ WhatsApp-Versand fehlgeschlagen: {e}")
        return {"success": False, "error": str(e)}

def extract_campaign_info(row_data: list) -> dict:
    """Extrahiert Kampagnen-Informationen aus Lead-Zeile"""
    try:
        return {
            "lead_id": row_data[0] if len(row_data) > 0 else "",
            "campaign_name": row_data[7] if len(row_data) > 7 else "",
            "ad_name": row_data[3] if len(row_data) > 3 else "",
            "ad_id": row_data[2] if len(row_data) > 2 else "",
            "adset_name": row_data[5] if len(row_data) > 5 else "",
            "platform": row_data[11] if len(row_data) > 11 else ""
        }
    except Exception as e:
        logger.error(f"❌ Kampagnen-Info-Extraktion fehlgeschlagen: {e}")
        return {}

def build_facebook_ad_url(ad_id: str) -> str:
    """Erstellt Facebook Ads Library URL"""
    if ad_id and ad_id.strip():
        return f"https://www.facebook.com/ads/library/?id={ad_id}"
    return ""

def format_campaign_message(campaign_info: dict) -> str:
    """Formatiert Kampagnen-Info für WhatsApp-Nachricht"""
    parts = []
    
    if campaign_info.get("campaign_name"):
        parts.append(f"📺 Kampagne: {campaign_info['campaign_name']}")
    
    if campaign_info.get("ad_name"):
        parts.append(f"📢 Anzeige: {campaign_info['ad_name']}")
    
    ad_url = build_facebook_ad_url(campaign_info.get("ad_id", ""))
    if ad_url:
        parts.append(f"🔗 {ad_url}")
    
    return "\n".join(parts) if parts else ""

def find_next_partner():
    """Findet den nächsten Partner mit ausreichend Guthaben"""
    try:
        sheet = get_partner_sheet()
        if not sheet:
            return None
        
        partners = sheet.get_all_records()
        
        for partner in partners:
            if (partner.get('Status', '').lower() == 'aktiv' and 
                float(partner.get('Guthaben', 0)) >= LEAD_PREIS):
                return partner
        
        logger.warning("⚠️ Kein Partner mit ausreichend Guthaben gefunden!")
        return None
        
    except Exception as e:
        logger.error(f"❌ Partner-Suche fehlgeschlagen: {e}")
        return None

def update_partner_balance(partner_name: str, new_balance: float):
    """Aktualisiert Partner-Guthaben"""
    try:
        sheet = get_partner_sheet()
        if not sheet:
            return False
        
        cell = sheet.find(partner_name)
        if cell:
            balance_col = 3
            sheet.update_cell(cell.row, balance_col, new_balance)
            logger.info(f"💰 Guthaben aktualisiert: {partner_name} → {new_balance}€")
            return True
        else:
            logger.error(f"❌ Partner nicht gefunden: {partner_name}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Guthaben-Update fehlgeschlagen: {e}")
        return False

def update_lead_status(lead_id: str, status: str, partner_name: str = ""):
    """Aktualisiert Lead-Status im Sheet"""
    try:
        sheet = get_lead_sheet()
        if not sheet:
            return False
        
        cell = sheet.find(lead_id)
        if cell:
            status_col = 16
            sheet.update_cell(cell.row, status_col, status)
            
            if partner_name:
                partner_col = 17
                sheet.update_cell(cell.row, partner_col, partner_name)
            
            logger.info(f"📝 Lead-Status aktualisiert: {lead_id} → {status}")
            return True
        else:
            logger.error(f"❌ Lead nicht gefunden: {lead_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Lead-Status-Update fehlgeschlagen: {e}")
        return False

def send_daily_partner_reminders():
    """Sendet tägliche Erinnerungen an alle Partner (08:00 Uhr)"""
    try:
        logger.info("⏰ Starte tägliche Partner-Erinnerungen...")
        
        sheet = get_partner_sheet()
        if not sheet:
            logger.error("❌ Partner-Sheet nicht verfügbar!")
            return
        
        partners = sheet.get_all_records()
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for partner in partners:
            try:
                if partner.get('Status', '').lower() != 'aktiv':
                    skip_count += 1
                    continue
                
                name = partner.get('Name', 'Partner')
                phone = partner.get('Telefon', '')
                balance = float(partner.get('Guthaben', 0))
                
                leads_remaining = int(balance / LEAD_PREIS)
                low_balance_warning = ""
                
                if leads_remaining <= 2:
                    low_balance_warning = f"\n⚠️ ACHTUNG: Nur noch {leads_remaining} Leads möglich! Bitte Guthaben aufladen."
                
                reminder_msg = f"""☀️ Guten Morgen {name}!

Damit du heute Leads erhalten kannst, antworte bitte kurz auf diese Nachricht (z.B. OK oder 👍).

💰 Dein aktuelles Guthaben: {balance}€
💵 Lead-Preis: {LEAD_PREIS}€
📊 Noch {leads_remaining} Leads möglich{low_balance_warning}

Viel Erfolg heute! 🚀"""
                
                result = send_whatsapp(phone, reminder_msg)
                
                if result.get('success'):
                    success_count += 1
                    logger.info(f"✅ Erinnerung gesendet: {name} ({phone})")
                else:
                    error_count += 1
                    logger.error(f"❌ Erinnerung fehlgeschlagen: {name} ({phone})")
                
                time.sleep(1.5)
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Fehler bei Partner-Erinnerung: {e}")
        
        admin_summary = f"""📊 Tägliche Erinnerungen versendet

✅ {success_count} Partner benachrichtigt
⏭️ {skip_count} Partner übersprungen (inaktiv/kein Guthaben)
❌ {error_count} Fehler

⏰ Warte auf Antworten für 24h-Fenster..."""
        
        send_whatsapp(MATZE_PHONE, admin_summary)
        logger.info(f"📊 Erinnerungs-Report: {success_count} erfolgreich, {error_count} Fehler")
        
        global partner_responses_today
        partner_responses_today.clear()
        
    except Exception as e:
        logger.error(f"❌ Tägliche Erinnerungen fehlgeschlagen: {e}")

def start_reminder_scheduler():
    """Startet den Scheduler für tägliche Erinnerungen"""
    try:
        scheduler = BackgroundScheduler(timezone=pytz.timezone('Europe/Berlin'))
        
        scheduler.add_job(
            send_daily_partner_reminders,
            CronTrigger(hour=8, minute=0),
            id='daily_partner_reminders',
            name='Tägliche Partner-Erinnerungen',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("✅ Täglicher Erinnerungs-Scheduler gestartet (08:00 Uhr)")
        
    except Exception as e:
        logger.error(f"❌ Scheduler-Start fehlgeschlagen: {e}")

def process_lead(row_data: list, row_index: int):
    """Verarbeitet einen neuen Lead"""
    try:
        lead_id = row_data[0] if len(row_data) > 0 else ""
        lead_name = row_data[13] if len(row_data) > 13 else ""
        lead_email = row_data[12] if len(row_data) > 12 else ""
        lead_phone = row_data[14] if len(row_data) > 14 else ""
        lead_status = row_data[15] if len(row_data) > 15 else ""
        
        if lead_status != "CREATED":
            return
        
        if lead_id in processed_lead_ids:
            return
        
        processed_lead_ids.add(lead_id)
        
        logger.info(f"📥 Neuer Lead gefunden: {lead_name} | {lead_phone}")
        
        campaign_info = extract_campaign_info(row_data)
        campaign_msg = format_campaign_message(campaign_info)
        
        if campaign_msg:
            logger.info(f"📺 {campaign_msg.replace(chr(10), ' | ')}")
        
        partner = find_next_partner()
        if not partner:
            logger.warning("⚠️ Kein verfügbarer Partner!")
            update_lead_status(lead_id, "PENDING")
            return
        
        partner_name = partner.get('Name', '')
        partner_phone = partner.get('Telefon', '')
        current_balance = float(partner.get('Guthaben', 0))
        new_balance = round(current_balance - LEAD_PREIS, 2)
        
        logger.info(f"👤 Lead zugewiesen an: {partner_name} ({new_balance}€ verbleibend)")
        
        if not partner_phone or len(partner_phone.replace("+", "").replace(" ", "").replace("-", "").replace("p:", "")) < 10:
            error_msg = f"""🚨 PARTNER-NUMMER FEHLT/UNGÜLTIG!

👤 Partner: {partner_name}
📞 Nummer im Sheet: {partner_phone or '[LEER]'}
💰 Guthaben abgezogen: {LEAD_PREIS}€ (jetzt {new_balance}€)

📝 Lead-Details:
👤 {lead_name}
📞 {lead_phone}
📧 {lead_email}
{campaign_msg}

⚠️ AKTION ERFORDERLICH:
1. Nummer im Partner-Sheet korrigieren
2. Lead manuell per WhatsApp senden"""
            
            send_whatsapp(MATZE_PHONE, error_msg)
            update_lead_status(lead_id, "ERROR_NO_PHONE", partner_name)
            return
        
        update_partner_balance(partner_name, new_balance)
        
        partner_message = f"""🎉 Neuer Lead für dich!

👤 {lead_name}
📞 {lead_phone}
📧 {lead_email}

{campaign_msg}

💰 Verbleibendes Guthaben: {new_balance}€"""
        
        partner_result = send_whatsapp(partner_phone, partner_message)
        
        if not partner_result.get('success'):
            error_msg = str(partner_result.get('error', ''))
            
            if '#100' in error_msg or 'Invalid parameter' in error_msg:
                admin_alert = f"""🚨 PARTNER 24H-FENSTER GESCHLOSSEN!

👤 Partner: {partner_name}
📞 {partner_phone}
💰 Guthaben abgezogen: {LEAD_PREIS}€ (jetzt {new_balance}€)

📝 Lead-Details:
👤 {lead_name}
📞 {lead_phone}
📧 {lead_email}
{campaign_msg}

⚠️ AKTION ERFORDERLICH:
1. Partner muss Lina schreiben (24h-Fenster öffnen)
2. Lead dann manuell weiterleiten
3. Partner an tägliche 08:00-Erinnerung erinnern"""
                
                send_whatsapp(MATZE_PHONE, admin_alert)
                update_lead_status(lead_id, "ERROR_24H_WINDOW", partner_name)
                return
            else:
                admin_alert = f"""🚨 WHATSAPP-VERSAND FEHLGESCHLAGEN!

👤 Partner: {partner_name}
📞 {partner_phone}
❌ Fehler: {error_msg}

📝 Lead-Details:
👤 {lead_name}
📞 {lead_phone}
📧 {lead_email}
{campaign_msg}

💰 Guthaben abgezogen: {LEAD_PREIS}€ (jetzt {new_balance}€)

⚠️ AKTION: Lead manuell per WhatsApp senden"""
                
                send_whatsapp(MATZE_PHONE, admin_alert)
                update_lead_status(lead_id, "ERROR_SEND", partner_name)
                return
        
        logger.info("✅ Partner-Nachricht erfolgreich gesendet!")
        
        time.sleep(2)
        admin_message = f"""✅ Lead erfolgreich verteilt!

👤 Lead: {lead_name}
📞 {lead_phone}
📧 {lead_email}

{campaign_msg}

→ Zugewiesen an: {partner_name}
💰 Neues Guthaben: {new_balance}€"""
        
        send_whatsapp(MATZE_PHONE, admin_message)
        
        update_lead_status(lead_id, "VERTEILT", partner_name)
        
    except Exception as e:
        logger.error(f"❌ Lead-Verarbeitung fehlgeschlagen: {e}")

def poll_new_leads():
    """Polling-Loop für neue Leads"""
    logger.info("🔄 Starte Lead-Polling...")
    
    while True:
        try:
            sheet = get_lead_sheet()
            if not sheet:
                logger.error("❌ Lead-Sheet nicht verfügbar!")
                time.sleep(30)
                continue
            
            rows = sheet.get_all_values()
            
            for idx, row in enumerate(rows[1:], start=2):
                if len(row) > 15:
                    process_lead(row, idx)
            
            time.sleep(30)
            
        except Exception as e:
            logger.error(f"❌ Polling-Fehler: {e}")
            time.sleep(30)

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    """Stripe Webhook für Zahlungen"""
    try:
        payload = await request.body()
        sig_header = request.headers.get('stripe-signature')
        
        try:
            event = stripe.Webhook.construct_event(
                payload, sig_header, STRIPE_WEBHOOK_SECRET
            )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid payload")
        except stripe.error.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Invalid signature")
        
        if event['type'] == 'checkout.session.completed':
            session = event['data']['object']
            
            customer_name = session.get('customer_details', {}).get('name', 'Unbekannt')
            customer_phone = session.get('customer_details', {}).get('phone', '')
            customer_email = session.get('customer_details', {}).get('email', '')
            amount_cents = session.get('amount_total', 0)
            amount_eur = amount_cents / 100
            
            logger.info(f"💳 Stripe-Zahlung: {customer_name} | {amount_eur}€")
            
            process_stripe_payment(customer_name, customer_phone, customer_email, amount_eur)
        
        return JSONResponse({"status": "success"})
        
    except Exception as e:
        logger.error(f"❌ Stripe Webhook Fehler: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def process_stripe_payment(name: str, phone: str, email: str, amount: float):
    """Verarbeitet Stripe-Zahlung und aktualisiert Partner-Guthaben"""
    try:
        logger.info(f"💰 Verarbeite Zahlung: {name} | {amount}€")
        
        sheet = get_partner_sheet()
        if not sheet:
            logger.error("❌ Partner-Sheet nicht verfügbar!")
            return
        
        partners = sheet.get_all_records()
        partner = None
        
        for p in partners:
            if p.get('Name', '').lower() == name.lower() or p.get('Email', '').lower() == email.lower():
                partner = p
                break
        
        if partner:
            current_balance = float(partner.get('Guthaben', 0))
            new_balance = round(current_balance + amount, 2)
            
            update_partner_balance(partner['Name'], new_balance)
            
            partner_msg = f"""✅ Zahlung erfolgreich eingegangen!

💰 Betrag: {amount}€
📊 Neues Guthaben: {new_balance}€
💵 Lead-Preis: {LEAD_PREIS}€
📈 Verfügbare Leads: {int(new_balance / LEAD_PREIS)}

Vielen Dank! 🚀"""
            
            if partner.get('Telefon'):
                send_whatsapp(partner['Telefon'], partner_msg)
            
            time.sleep(2)
            
        else:
            logger.info(f"🆕 Neuer Partner: {name}")
            
            next_row = len(partners) + 2
            sheet.append_row([name, phone, amount, 'aktiv', email])
            
            if phone:
                welcome_msg = f"""🎉 Willkommen im LR-Lead-System!

✅ Dein Account wurde erstellt
💰 Startguthaben: {amount}€
💵 Lead-Preis: {LEAD_PREIS}€
📊 Verfügbare Leads: {int(amount / LEAD_PREIS)}

Du erhältst ab sofort automatisch Leads per WhatsApp! 🚀

⏰ Wichtig: Täglich um 08:00 Uhr kommt eine Erinnerung von Lina. Bitte kurz antworten, damit das 24h-Fenster offen bleibt!"""
                
                send_whatsapp(phone, welcome_msg)
        
        admin_msg = f"""💳 Stripe-Zahlung eingegangen!

👤 Kunde: {name}
📞 {phone}
📧 {email}
💰 Betrag: {amount}€
{"✅ GUTHABEN ERHÖHT" if partner else "🆕 NEUER PARTNER ANGELEGT"}

📊 Neues Guthaben: {new_balance if partner else amount}€"""
        
        send_whatsapp(MATZE_PHONE, admin_msg)
        logger.info("✅ Matze-Benachrichtigung gesendet!")
        
    except Exception as e:
        logger.error(f"❌ Stripe-Payment-Verarbeitung fehlgeschlagen: {e}")

@app.on_event("startup")
async def startup_event():
    """System-Start"""
    import threading
    
    logger.info("=" * 60)
    logger.info("🚀 Lead-Verteilungs-Service v5.2 - Spalten-Fix + UTM")
    logger.info("=" * 60)
    logger.info("✅ System gestartet")
    logger.info(f"📱 Admin-Benachrichtigung → {MATZE_PHONE}")
    logger.info(f"💰 Lead-Preis: {LEAD_PREIS}€")
    logger.info("📊 UTM-Tracking: Aktiv")
    logger.info("📋 Spalten-Mapping: N=Name, M=Email, O=Phone, P=Status")
    logger.info("=" * 60)
    
    start_reminder_scheduler()
    
    polling_thread = threading.Thread(target=poll_new_leads, daemon=True)
    polling_thread.start()
    
    startup_msg = f"""🚀 System gestartet!

Lead-Verteilungs-Service v5.2

✅ Lead-Polling: Aktiv
✅ WhatsApp-Integration: Aktiv
✅ Stripe-Webhook: Aktiv
✅ UTM-Tracking: Aktiv
✅ Tägliche Erinnerungen: 08:00 Uhr
💰 Lead-Preis: {LEAD_PREIS}€

Alle Systeme bereit! 🎯"""
    
    send_whatsapp(MATZE_PHONE, startup_msg)

@app.get("/")
async def root():
    return {
        "service": "Lead-Verteilungs-Service",
        "version": "5.2",
        "status": "running",
        "features": [
            "Lead-Polling",
            "WhatsApp-Integration (Meta Cloud API)",
            "Stripe-Webhook",
            "UTM-Tracking (Kampagne + Anzeige + Facebook-Link)",
            "Tägliche Erinnerungen (08:00 Uhr)",
            "24h-Fenster-Erkennung"
        ],
        "spalten_mapping": {
            "M": "e-mail-adresse",
            "N": "vollständiger_name",
            "O": "telefonnummer",
            "P": "lead_status"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy", "version": "5.2"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 10000))
    uvicorn.run(app, host="0.0.0.0", port=port)
