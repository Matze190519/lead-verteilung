#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lead-Verteilungs-Service v5.1 - UTM-TRACKING + WERBE-QUELLEN
WhatsApp Business API via Meta Cloud API
Automatische Morgen-Erinnerungen um 08:00 Uhr
Werbe-Quelle (Kampagne, Anzeige, Creative-Link) an Partner
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
import pytz

# Web Framework
from fastapi import FastAPI, Request, HTTPException
import uvicorn

# Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# HTTP Requests
import requests

# Stripe
import stripe

# Scheduler für tägliche Erinnerungen
from apscheduler.schedulers.background import BackgroundScheduler

# ============================================================================
# KONFIGURATION
# ============================================================================

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# Meta WhatsApp API
META_TOKEN = os.getenv("META_WHATSAPP_TOKEN", "EAARgaZCn3eoYBO0Tr9nSqfmJYOcx3gx3NAzSdwekRpZB5rfmWH2poZAvKSXXVBdR0HDqiXAEbfESzfejzSYLTCkhZAxs0bVZCMufcy51ZBN16zkDlpy8bcaUL5Omu6FTLW37O30I9uO51HSgfZBZBYz6qPEQ49RVEMWNrJmnrvvmrwCgAlJaJB7eHk2GvDdU8pKYkwZDZD")
META_PHONE_ID = "623007617563961"
META_URL = f"https://graph.facebook.com/v22.0/{META_PHONE_ID}/messages"

# Admin-Nummer (Matze)
MATZE_PHONE = "491715060008"

# Google Sheet
GOOGLE_SHEET_ID = "1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY"

# Lead-Preis
LEAD_PREIS = float(os.getenv("LEAD_PREIS", "5.0"))

# Stripe
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "sk_test_PLACEHOLDER")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "whsec_PLACEHOLDER")

# Tracking für Partner-Antworten
partner_responses_today = {}

# ============================================================================
# GOOGLE SHEETS CLIENT
# ============================================================================

def get_google_sheets_client():
    """Google Sheets Client initialisieren"""
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Service Account aus Environment Variable
        creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
        if not creds_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON nicht gesetzt!")
        
        import json
        creds_dict = json.loads(creds_json)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        
        client = gspread.authorize(creds)
        logger.info("✅ Google Sheets Client initialisiert")
        return client
        
    except Exception as e:
        logger.error(f"❌ Google Sheets Init Fehler: {e}")
        raise

def get_partner_sheet():
    """Partner-Sheet öffnen"""
    client = get_google_sheets_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet("Partner")

def get_leads_sheet():
    """Leads-Sheet öffnen"""
    client = get_google_sheets_client()
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet("Tabellenblatt1")

# ============================================================================
# WHATSAPP FUNKTIONEN
# ============================================================================

def normalize_phone(phone: str) -> str:
    """Normalisiert Telefonnummer für WhatsApp API"""
    if not phone:
        return ""
    
    # Entferne p: prefix
    if phone.startswith("p:"):
        phone = phone[2:]
    
    # Nur Ziffern
    cleaned = ''.join(filter(str.isdigit, phone))
    
    # Deutsche Nummer mit 0 am Anfang
    if cleaned.startswith('0'):
        cleaned = '49' + cleaned[1:]
    
    # Wenn keine Ländervorwahl, annahme Deutschland
    if len(cleaned) < 12 and not cleaned.startswith('49'):
        cleaned = '49' + cleaned
    
    return cleaned

def send_whatsapp(phone: str, message: str) -> Dict:
    """
    Sendet WhatsApp-Nachricht via Meta Cloud API
    
    Returns:
        {"success": True/False, "phone": ..., "message_id": ..., "error": ...}
    """
    normalized = normalize_phone(phone)
    
    if not normalized or len(normalized) < 10:
        logger.error(f"❌ Ungültige Telefonnummer: '{phone}'")
        return {
            "success": False,
            "phone": phone,
            "error": "Ungültige Telefonnummer"
        }
    
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": normalized,
        "type": "text",
        "text": {"body": message}
    }
    
    headers = {
        "Authorization": f"Bearer {META_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(META_URL, json=payload, headers=headers, timeout=30)
        response_data = response.json()
        
        logger.info(f"[META_RESPONSE] Status={response.status_code} | Phone={normalized}")
        
        if response.status_code == 200:
            message_id = response_data.get("messages", [{}])[0].get("id", "unknown")
            return {
                "success": True,
                "phone": normalized,
                "message_id": message_id,
                "data": response_data
            }
        else:
            error_msg = response_data.get("error", {}).get("message", "Unknown error")
            error_code = response_data.get("error", {}).get("code", "")
            logger.error(f"❌ WhatsApp API Error: {error_msg} (Code: {error_code})")
            
            return {
                "success": False,
                "phone": normalized,
                "error": error_msg,
                "error_code": error_code,
                "data": response_data
            }
            
    except Exception as e:
        logger.error(f"❌ WhatsApp Send Exception: {e}")
        return {
            "success": False,
            "phone": normalized,
            "error": str(e)
        }

# ============================================================================
# UTM / WERBE-QUELLEN FUNKTIONEN
# ============================================================================

def extract_campaign_info(row_data: List[str]) -> Dict:
    """
    Extrahiert Kampagnen-/Werbe-Informationen aus Sheet-Zeile
    
    Erwartete Spalten (basierend auf deinen Daten):
    - Spalte L (Index 11): Lead-ID
    - Spalte M (Index 12): Kampagnen-Name
    - Spalte N (Index 13): Anzeigen-Set
    - Spalte O (Index 14): Anzeigen-Name
    - Spalte P (Index 15): Kampagnen-ID
    - Spalte Q (Index 16): Anzeigengruppe
    - Spalte R (Index 17): Formular-ID
    """
    campaign_info = {
        "lead_id": row_data[11] if len(row_data) > 11 else "",
        "campaign_name": row_data[12] if len(row_data) > 12 else "",
        "ad_set": row_data[13] if len(row_data) > 13 else "",
        "ad_name": row_data[14] if len(row_data) > 14 else "",
        "campaign_id": row_data[15] if len(row_data) > 15 else "",
        "ad_group": row_data[16] if len(row_data) > 16 else "",
        "form_id": row_data[17] if len(row_data) > 17 else "",
    }
    
    return campaign_info

def build_facebook_ad_url(campaign_id: str) -> Optional[str]:
    """
    Baut Facebook-Anzeigen-URL aus Campaign-ID
    Format: c:23853100940750736 → https://facebook.com/ads/...
    """
    if not campaign_id:
        return None
    
    # Extrahiere ID (entferne c: prefix)
    clean_id = campaign_id.replace("c:", "").strip()
    
    if not clean_id:
        return None
    
    # Facebook Ads Library URL
    return f"https://www.facebook.com/ads/library/?id={clean_id}"

def format_campaign_message(campaign_info: Dict) -> str:
    """
    Formatiert Kampagnen-Info für WhatsApp-Nachricht
    """
    parts = []
    
    # Kampagnen-Name (immer anzeigen wenn vorhanden)
    if campaign_info.get("campaign_name"):
        # Kürze lange Namen
        campaign_name = campaign_info["campaign_name"]
        if len(campaign_name) > 50:
            campaign_name = campaign_name[:47] + "..."
        parts.append(f"📺 *Kampagne:* {campaign_name}")
    
    # Anzeigen-Name
    if campaign_info.get("ad_name"):
        ad_name = campaign_info["ad_name"]
        if len(ad_name) > 50:
            ad_name = ad_name[:47] + "..."
        parts.append(f"📢 *Anzeige:* {ad_name}")
    
    # Facebook-Link (wenn Campaign-ID vorhanden)
    ad_url = build_facebook_ad_url(campaign_info.get("campaign_id", ""))
    if ad_url:
        parts.append(f"🔗 *Ansehen:* {ad_url}")
    
    if not parts:
        return ""
    
    return "\n\n" + "\n".join(parts)

# ============================================================================
# PARTNER MANAGEMENT
# ============================================================================

def get_all_partner_records(sheet) -> List[Dict]:
    """Holt alle Partner-Datensätze"""
    try:
        all_records = sheet.get_all_records()
        logger.info(f"📊 {len(all_records)} Partner-Datensätze geladen")
        return all_records
    except Exception as e:
        logger.error(f"❌ Fehler beim Laden der Partner: {e}")
        return []

def find_best_partner(sheet) -> Optional[Dict]:
    """Findet besten verfügbaren Partner"""
    records = get_all_partner_records(sheet)
    
    # Nur aktive Partner mit Guthaben >= Lead-Preis
    available = [
        {**r, "row_index": i+2}  # +2 wegen Header + 0-Index
        for i, r in enumerate(records)
        if r.get("Status", "").lower() == "aktiv" and float(r.get("Guthaben", 0)) >= LEAD_PREIS
    ]
    
    if not available:
        logger.warning("⚠️ Keine Partner mit ausreichend Guthaben verfügbar!")
        return None
    
    # Sortiere nach Guthaben (höchstes zuerst)
    available.sort(key=lambda x: float(x.get("Guthaben", 0)), reverse=True)
    
    best = available[0]
    logger.info(f"✅ Bester Partner: {best.get('Name')} (Guthaben: {best.get('Guthaben')}€)")
    
    return {
        "name": best.get("Name", "Unbekannt"),
        "phone": best.get("Telefon", ""),
        "email": best.get("E-Mail", ""),
        "guthaben": float(best.get("Guthaben", 0)),
        "row_index": best["row_index"]
    }

def update_partner_guthaben(sheet, partner: Dict, delta: float) -> float:
    """Aktualisiert Partner-Guthaben"""
    try:
        row_idx = partner["row_index"]
        current = partner["guthaben"]
        new_balance = current + delta
        
        # Guthaben-Spalte ist D (Index 4)
        sheet.update_cell(row_idx, 4, new_balance)
        
        logger.info(f"💰 {partner['name']} Guthaben: {current}€ → {new_balance}€")
        
        # Warnung wenn Guthaben aufgebraucht
        if new_balance < LEAD_PREIS:
            warning_msg = (
                f"⚠️ *Partner-Guthaben aufgebraucht!*\n\n"
                f"👤 {partner['name']}\n"
                f"💰 Neues Guthaben: {new_balance}€\n"
                f"📉 Zu wenig für nächsten Lead ({LEAD_PREIS}€)\n\n"
                f"Status wurde automatisch auf PAUSIERT gesetzt."
            )
            send_whatsapp(MATZE_PHONE, warning_msg)
            
            # Status auf pausiert setzen (Spalte C, Index 3)
            sheet.update_cell(row_idx, 3, "pausiert")
        
        return new_balance
        
    except Exception as e:
        logger.error(f"❌ Fehler beim Guthaben-Update: {e}")
        return partner["guthaben"]

def find_partner_by_phone(sheet, phone: str) -> Optional[Dict]:
    """Sucht Partner nach Telefonnummer"""
    normalized = normalize_phone(phone)
    records = get_all_partner_records(sheet)
    
    for i, r in enumerate(records):
        partner_phone = normalize_phone(r.get("Telefon", ""))
        if partner_phone == normalized:
            return {
                "name": r.get("Name", "Unbekannt"),
                "phone": r.get("Telefon", ""),
                "email": r.get("E-Mail", ""),
                "guthaben": float(r.get("Guthaben", 0)),
                "row_index": i + 2
            }
    
    return None

def find_partner_by_name(sheet, name: str) -> Optional[Dict]:
    """Sucht Partner nach Name"""
    records = get_all_partner_records(sheet)
    
    for i, r in enumerate(records):
        if r.get("Name", "").lower() == name.lower():
            return {
                "name": r.get("Name", "Unbekannt"),
                "phone": r.get("Telefon", ""),
                "email": r.get("E-Mail", ""),
                "guthaben": float(r.get("Guthaben", 0)),
                "row_index": i + 2
            }
    
    return None

def add_new_partner(sheet, name: str, phone: str, initial_balance: float):
    """Fügt neuen Partner hinzu"""
    try:
        new_row = [name, phone, "aktiv", initial_balance, ""]
        sheet.append_row(new_row)
        logger.info(f"✅ Neuer Partner angelegt: {name} | {initial_balance}€")
    except Exception as e:
        logger.error(f"❌ Fehler beim Anlegen des Partners: {e}")

# ============================================================================
# TÄGLICHE ERINNERUNGEN
# ============================================================================

def send_daily_partner_reminders():
    """Sendet jeden Morgen um 08:00 eine Erinnerung an alle aktiven Partner"""
    global partner_responses_today
    
    logger.info("📅 === TÄGLICHE PARTNER-ERINNERUNGEN ===")
    
    try:
        sheet = get_partner_sheet()
        all_records = sheet.get_all_records()
        
        # Tracking zurücksetzen
        partner_responses_today = {}
        
        reminder_count = 0
        failed_count = 0
        skipped_count = 0
        
        for row in all_records:
            # Nur aktive Partner
            if row.get("Status", "").lower() != "aktiv":
                continue
            
            # Nur Partner mit ausreichend Guthaben
            guthaben = float(row.get("Guthaben", 0))
            if guthaben < LEAD_PREIS:
                skipped_count += 1
                logger.info(f"⏭️ Überspringe {row.get('Name')} - kein Guthaben ({guthaben}€)")
                continue
            
            partner_phone = row.get("Telefon", "").strip()
            partner_name = row.get("Name", "Unbekannt")
            
            # Telefonnummer vorhanden?
            if not partner_phone or len(normalize_phone(partner_phone)) < 10:
                logger.warning(f"⚠️ {partner_name} hat keine gültige Telefonnummer")
                failed_count += 1
                continue
            
            # Erinnerungs-Nachricht
            remaining_leads = int(guthaben / LEAD_PREIS)
            
            reminder_msg = (
                f"☀️ *Guten Morgen {partner_name}!*\n\n"
                f"Antworte kurz auf diese Nachricht, "
                f"damit du heute Leads empfangen kannst 👍\n\n"
                f"📊 Dein Guthaben: *{guthaben}€*\n"
                f"💰 Lead-Preis: {LEAD_PREIS}€\n"
                f"📈 Noch *{remaining_leads} Leads* möglich\n\n"
            )
            
            # Warnung bei niedrigem Guthaben
            if remaining_leads <= 2:
                reminder_msg += f"⚠️ *Guthaben läuft aus!* Bitte aufladen.\n\n"
            
            reminder_msg += "Viel Erfolg heute! 🚀"
            
            # WhatsApp senden
            result = send_whatsapp(partner_phone, reminder_msg)
            
            if result.get("success"):
                logger.info(f"✅ Erinnerung gesendet: {partner_name}")
                reminder_count += 1
                partner_responses_today[normalize_phone(partner_phone)] = {
                    "name": partner_name,
                    "reminded": True,
                    "responded": False,
                    "time": datetime.now()
                }
            else:
                logger.error(f"❌ Erinnerung fehlgeschlagen: {partner_name} | {result.get('error')}")
                failed_count += 1
            
            # Rate-Limiting
            time.sleep(2)
        
        logger.info(f"✅ Erinnerungen abgeschlossen: {reminder_count} gesendet, {failed_count} fehlgeschlagen, {skipped_count} übersprungen")
        
        # Matze benachrichtigen
        matze_msg = (
            f"📅 *Morgen-Erinnerungen versendet*\n\n"
            f"✅ {reminder_count} Partner benachrichtigt\n"
            f"⏭️ {skipped_count} übersprungen (kein Guthaben)\n"
            f"❌ {failed_count} Fehler\n\n"
            f"Warte auf Antworten..."
        )
        send_whatsapp(MATZE_PHONE, matze_msg)
        
    except Exception as e:
        logger.error(f"❌ Fehler bei täglichen Erinnerungen: {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 *Fehler bei Morgen-Erinnerungen*\n\n{str(e)}")

# ============================================================================
# LEAD-VERARBEITUNG MIT UTM-TRACKING
# ============================================================================

def process_lead(lead_data: Dict, campaign_info: Optional[Dict] = None) -> Optional[Dict]:
    """Verarbeitet einen neuen Lead mit Kampagnen-Info"""
    logger.info(f"🎯 Verarbeite Lead: {lead_data.get('name', 'N/A')}")
    
    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"❌ Sheet-Fehler: {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 *FEHLER*\n\nSheet nicht erreichbar:\n{e}")
        return None
    
    partner = find_best_partner(sheet)
    
    if not partner:
        logger.error("❌ Kein Partner verfügbar!")
        no_partner_msg = (
            f"🚨 *KEIN PARTNER VERFÜGBAR*\n\n"
            f"Lead:\n"
            f"👤 {lead_data.get('name', 'N/A')}\n"
            f"📞 {lead_data.get('phone', 'N/A')}\n"
            f"📧 {lead_data.get('email', 'N/A')}\n\n"
            f"⚠️ Bitte manuell zuweisen!"
        )
        send_whatsapp(MATZE_PHONE, no_partner_msg)
        return None
    
    # GUTHABEN ABZIEHEN
    neues_guthaben = update_partner_guthaben(sheet, partner, -LEAD_PREIS)
    logger.info(f"💰 {partner['name']} Guthaben: {partner.get('guthaben', 0)}€ → {neues_guthaben}€")
    
    # PARTNER-NUMMER PRÜFEN
    partner_phone = partner.get("phone", "").strip()
    
    if not partner_phone or len(normalize_phone(partner_phone)) < 10:
        logger.error(f"❌ {partner['name']} HAT KEINE GÜLTIGE NUMMER")
        error_msg = (
            f"🚨 *PARTNER-NUMMER FEHLT!*\n\n"
            f"Partner: {partner['name']}\n"
            f"Nummer: '{partner_phone}'\n\n"
            f"Lead:\n"
            f"👤 {lead_data.get('name', 'N/A')}\n"
            f"📞 {lead_data.get('phone', 'N/A')}\n"
            f"📧 {lead_data.get('email', 'N/A')}\n\n"
            f"💰 Guthaben wurde abgezogen: {neues_guthaben}€\n\n"
            f"⚠️ Nummer eintragen und manuell senden!"
        )
        send_whatsapp(MATZE_PHONE, error_msg)
        return None
    
    # PARTNER-NACHRICHT SENDEN (MIT KAMPAGNEN-INFO!)
    partner_msg = (
        f"🎉 *Neuer Lead für dich!*\n\n"
        f"👤 {lead_data.get('name', 'N/A')}\n"
        f"📞 {lead_data.get('phone', 'N/A')}\n"
        f"📧 {lead_data.get('email', 'N/A')}"
    )
    
    # Kampagnen-Info hinzufügen (wenn vorhanden)
    if campaign_info:
        campaign_msg = format_campaign_message(campaign_info)
        if campaign_msg:
            partner_msg += campaign_msg
    
    partner_msg += f"\n\n💰 Verbleibendes Guthaben: {neues_guthaben}€"
    
    logger.info(f"📤 Sende Lead an {partner['name']} ({partner_phone})...")
    partner_result = send_whatsapp(partner_phone, partner_msg)
    
    # ERFOLGREICH?
    if partner_result.get("success"):
        logger.info(f"✅ Partner-Nachricht erfolgreich gesendet!")
    else:
        # FEHLGESCHLAGEN
        error_msg = partner_result.get("error", "")
        error_code = partner_result.get("error_code", "")
        
        logger.error(f"❌ Partner-Nachricht FEHLGESCHLAGEN: {partner_result}")
        
        # 24h-Fenster geschlossen?
        if "#100" in str(error_msg) or "100" in str(error_code) or "Invalid parameter" in str(error_msg):
            warning_msg = (
                f"🚨 *24H-FENSTER GESCHLOSSEN!*\n\n"
                f"Partner: {partner['name']}\n"
                f"Nummer: {partner_phone}\n\n"
                f"❌ Partner hat heute NICHT auf die "
                f"Morgen-Erinnerung geantwortet!\n\n"
                f"Lead konnte NICHT zugestellt werden:\n"
                f"👤 {lead_data.get('name', 'N/A')}\n"
                f"📞 {lead_data.get('phone', 'N/A')}\n"
                f"📧 {lead_data.get('email', 'N/A')}\n\n"
                f"💰 Guthaben wurde TROTZDEM abgezogen: {neues_guthaben}€\n\n"
                f"⚠️ AKTION ERFORDERLICH:\n"
                f"1) Lead manuell an {partner['name']} senden\n"
                f"2) Oder anderen Partner zuweisen\n"
                f"3) Partner erinnern: JEDEN TAG auf Erinnerung antworten!"
            )
        else:
            # Anderer Fehler
            warning_msg = (
                f"🚨 *WHATSAPP-VERSAND FEHLGESCHLAGEN!*\n\n"
                f"Partner: {partner['name']}\n"
                f"Nummer: {partner_phone}\n"
                f"Fehler: {error_msg}\n"
                f"Code: {error_code}\n\n"
                f"Lead:\n"
                f"👤 {lead_data.get('name', 'N/A')}\n"
                f"📞 {lead_data.get('phone', 'N/A')}\n"
                f"📧 {lead_data.get('email', 'N/A')}\n\n"
                f"⚠️ BITTE MANUELL SENDEN!"
            )
        
        send_whatsapp(MATZE_PHONE, warning_msg)
        
        return {
            "lead_id": lead_data.get("id"),
            "partner": partner["name"],
            "status": "FAILED",
            "guthaben": neues_guthaben,
            "error": str(error_msg)
        }
    
    time.sleep(2)
    
    # ADMIN-BENACHRICHTIGUNG (MATZE) - OHNE Kampagnen-Details (nur Übersicht)
    admin_msg = (
        f"✅ *Lead verteilt*\n\n"
        f"👤 {lead_data.get('name', 'N/A')}\n"
        f"📞 {lead_data.get('phone', 'N/A')}\n"
        f"📧 {lead_data.get('email', 'N/A')}\n\n"
        f"➡️ {partner['name']}\n"
        f"💰 Rest: {neues_guthaben}€"
    )
    
    logger.info("📤 Sende Admin-Benachrichtigung...")
    send_whatsapp(MATZE_PHONE, admin_msg)
    
    logger.info(f"✅ Lead-Verteilung abgeschlossen!")
    
    return {
        "lead_id": lead_data.get("id"),
        "partner": partner["name"],
        "partner_phone": partner_phone,
        "guthaben": neues_guthaben,
        "timestamp": datetime.now(pytz.timezone("Europe/Berlin")).isoformat()
    }

# ============================================================================
# SHEET POLLING MIT KAMPAGNEN-INFO
# ============================================================================

def poll_new_leads():
    """Pollt neue Leads aus Sheet und verarbeitet sie"""
    logger.info("🔄 Polling neue Leads...")
    
    try:
        leads_sheet = get_leads_sheet()
        all_values = leads_sheet.get_all_values()
        
        if len(all_values) <= 1:
            logger.info("📭 Keine neuen Leads")
            return {"processed": 0}
        
        processed_count = 0
        
        for row_idx, row_data in enumerate(all_values[1:], start=2):
            # Status-Spalte prüfen (Spalte P, Index 15 in deinem Sheet)
            # ANPASSEN je nach tatsächlicher Sheet-Struktur!
            status_col_index = 15  # Spalte P (0-indexed)
            
            if len(row_data) <= status_col_index:
                continue
            
            status = row_data[status_col_index].strip().upper()
            
            # Nur neue Leads verarbeiten
            if status != "CREATED":
                continue
            
            # Status auf PROCESSING setzen
            try:
                leads_sheet.update_cell(row_idx, status_col_index + 1, "PROCESSING")
            except:
                continue
            
            # Lead-Daten extrahieren
            # WICHTIG: Spalten-Indizes anpassen basierend auf deinem Sheet!
            # Beispiel aus deinen Daten:
            # Spalte J: Email
            # Spalte K: Name  
            # Spalte L: Telefon (p:+...)
            
            lead_data = {
                "name": row_data[10] if len(row_data) > 10 else "Unbekannt",  # Spalte K
                "email": row_data[9] if len(row_data) > 9 else "",  # Spalte J
                "phone": row_data[11] if len(row_data) > 11 else "",  # Spalte L
            }
            
            # Kampagnen-Info extrahieren
            campaign_info = extract_campaign_info(row_data)
            
            logger.info(f"📥 Neuer Lead: {lead_data['name']} | Kampagne: {campaign_info.get('campaign_name', 'N/A')}")
            
            # Lead verarbeiten
            result = process_lead(lead_data, campaign_info)
            
            # Status updaten
            if result and "error" not in result:
                leads_sheet.update_cell(row_idx, status_col_index + 1, "VERTEILT")
                processed_count += 1
            else:
                leads_sheet.update_cell(row_idx, status_col_index + 1, "FEHLER")
            
            time.sleep(2)
        
        logger.info(f"✅ Polling abgeschlossen: {processed_count} Leads verteilt")
        return {"processed": processed_count}
        
    except Exception as e:
        logger.error(f"❌ Polling-Fehler: {e}")
        return {"error": str(e)}

# ============================================================================
# STRIPE PAYMENT
# ============================================================================

def process_stripe_payment(customer_name: str, customer_phone: str, customer_email: str, amount: float):
    """Verarbeitet Stripe-Zahlung"""
    logger.info(f"=== Stripe: {customer_name} | {amount}€ ===")
    
    try:
        sheet = get_partner_sheet()
    except Exception as e:
        logger.error(f"❌ Sheet-Fehler: {e}")
        send_whatsapp(MATZE_PHONE, f"🚨 *Stripe-Fehler*\n\nSheet nicht erreichbar:\n{e}")
        return
    
    # Partner suchen
    partner = None
    if customer_phone:
        partner = find_partner_by_phone(sheet, customer_phone)
    if not partner and customer_name:
        partner = find_partner_by_name(sheet, customer_name)
    
    if partner:
        neues_guthaben = update_partner_guthaben(sheet, partner, amount)
        action = "GUTHABEN ERHÖHT"
        
        # Partner benachrichtigen
        partner_msg = (
            f"✅ *Zahlung erhalten!*\n\n"
            f"💵 {amount}€ wurde deinem Konto gutgeschrieben.\n"
            f"📊 Neues Guthaben: {neues_guthaben}€\n\n"
            f"Vielen Dank! 🙏"
        )
        send_whatsapp(customer_phone, partner_msg)
        logger.info(f"✅ Partner-Benachrichtigung gesendet an {customer_phone}")
        
    else:
        add_new_partner(sheet, customer_name, customer_phone, amount)
        neues_guthaben = amount
        action = "NEUER PARTNER"
        
        # Neuen Partner begrüßen
        if customer_phone:
            welcome_msg = (
                f"🎉 *Willkommen im Team!*\n\n"
                f"Deine Zahlung von {amount}€ ist eingegangen.\n"
                f"📊 Dein Guthaben: {neues_guthaben}€\n\n"
                f"Du bekommst ab jetzt jeden Morgen um 08:00 Uhr "
                f"eine Erinnerung. Einfach kurz antworten und "
                f"du erhältst den ganzen Tag Leads!\n\n"
                f"Let's go! 🚀"
            )
            send_whatsapp(customer_phone, welcome_msg)
    
    time.sleep(2)
    
    # MATZE BENACHRICHTIGEN
    matze_msg = (
        f"💰 *Stripe-Zahlung eingegangen!*\n\n"
        f"👤 {customer_name}\n"
        f"📞 {customer_phone}\n"
        f"📧 {customer_email}\n"
        f"💵 {amount}€\n\n"
        f"✅ {action}\n"
        f"📊 Neues Guthaben: {neues_guthaben}€"
    )
    
    logger.info("📤 Sende Stripe-Admin-Info an Matze...")
    result = send_whatsapp(MATZE_PHONE, matze_msg)
    
    if "error" in result:
        logger.error(f"❌ Matze-Benachrichtigung FEHLGESCHLAGEN: {result}")
    elif result.get("success"):
        logger.info("✅ Matze-Benachrichtigung gesendet!")
    else:
        logger.warning(f"⚠️ Matze-Benachrichtigung unklares Ergebnis: {result}")
    
    logger.info(f"✅ Stripe fertig: {action}")

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(title="Lead-Verteilungs-Service v5.1")

@app.get("/")
def root():
    """Health Check"""
    return {
        "status": "running",
        "version": "5.1-UTM-TRACKING",
        "timestamp": datetime.now(pytz.timezone("Europe/Berlin")).isoformat()
    }

@app.get("/webhook")
def facebook_webhook_verify(request: Request):
    """Facebook Webhook Verification"""
    mode = request.query_params.get("hub.mode")
    token = request.query_params.get("hub.verify_token")
    challenge = request.query_params.get("hub.challenge")
    
    if mode == "subscribe" and token == "VERIFY_TOKEN_123":
        logger.info("✅ Facebook Webhook verifiziert")
        return int(challenge)
    
    raise HTTPException(status_code=403, detail="Verification failed")

@app.post("/webhook")
async def facebook_webhook(request: Request):
    """Facebook Lead Webhook"""
    try:
        payload = await request.json()
        logger.info(f"📥 Facebook Webhook empfangen: {payload}")
        
        # Lead-Daten extrahieren (vereinfacht)
        lead_data = {
            "id": payload.get("entry", [{}])[0].get("id"),
            "name": "Test Lead",
            "phone": "+491234567890",
            "email": "test@example.com"
        }
        
        # Lead verarbeiten
        result = process_lead(lead_data)
        
        return {"status": "success", "result": result}
        
    except Exception as e:
        logger.error(f"❌ Webhook-Fehler: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    """Stripe Webhook"""
    payload = await request.body()
    sig_header = request.headers.get("Stripe-Signature")
    
    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, STRIPE_WEBHOOK_SECRET
        )
        
        if event["type"] == "checkout.session.completed":
            session = event["data"]["object"]
            
            customer_name = session.get("customer_details", {}).get("name", "Unbekannt")
            customer_phone = session.get("customer_details", {}).get("phone", "")
            customer_email = session.get("customer_details", {}).get("email", "")
            amount = session.get("amount_total", 0) / 100
            
            logger.info(f"💳 Stripe Checkout abgeschlossen: {customer_name} | {amount}€")
            
            process_stripe_payment(customer_name, customer_phone, customer_email, amount)
        
        return {"status": "success"}
        
    except Exception as e:
        logger.error(f"❌ Stripe Webhook Error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/manual-poll")
def manual_poll():
    """Manueller Polling-Trigger"""
    logger.info("🔄 Manueller Poll gestartet")
    result = poll_new_leads()
    return {"status": "ok", "result": result}

# ============================================================================
# SCHEDULER
# ============================================================================

def start_reminder_scheduler():
    """Startet den täglichen Erinnerungs-Scheduler"""
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Europe/Berlin"))
    
    # Täglich um 08:00 Uhr
    scheduler.add_job(
        send_daily_partner_reminders,
        'cron',
        hour=8,
        minute=0,
        id='daily_partner_reminder'
    )
    
    scheduler.start()
    logger.info("✅ Täglicher Erinnerungs-Scheduler gestartet (08:00 Uhr)")
    
    return scheduler

# ============================================================================
# STARTUP
# ============================================================================

@app.on_event("startup")
def startup_event():
    """Wird beim Start der App ausgeführt"""
    logger.info("=" * 60)
    logger.info("🚀 Lead-Verteilungs-Service v5.1 - UTM-TRACKING")
    logger.info("=" * 60)
    logger.info(f"✅ System gestartet")
    logger.info(f"📱 Admin-Benachrichtigungen → {MATZE_PHONE}")
    logger.info(f"💰 Lead-Preis: {LEAD_PREIS}€")
    logger.info(f"⏰ Tägliche Erinnerungen: 08:00 Uhr")
    logger.info(f"📊 UTM-Tracking: Aktiv")
    logger.info("=" * 60)
    
    # Scheduler starten
    app.state.scheduler = start_reminder_scheduler()
    
    # Test-Nachricht an Matze
    test_msg = (
        f"🚀 *System gestartet!*\n\n"
        f"Lead-Verteilungs-Service v5.1\n"
        f"✅ Tägliche Erinnerungen aktiv (08:00 Uhr)\n"
        f"📊 UTM-Tracking eingebaut\n"
        f"💰 Lead-Preis: {LEAD_PREIS}€\n\n"
        f"Partner bekommen jetzt Kampagnen-Info! 🎯"
    )
    send_whatsapp(MATZE_PHONE, test_msg)

@app.on_event("shutdown")
def shutdown_event():
    """Wird beim Herunterfahren ausgeführt"""
    logger.info("🛑 System wird heruntergefahren...")
    if hasattr(app.state, 'scheduler'):
        app.state.scheduler.shutdown()
        logger.info("✅ Scheduler gestoppt")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
