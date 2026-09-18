# -*- coding: utf-8 -*-
"""
Tests für die drei Sperren nach dem Vorfall vom 18.09.2026
(fremde Stripe-Zahlung über 1.815 € wurde 1:1 zu Lead-Guthaben):

  Punkt 3 – Weiße Liste: nur das Lead-Kauf-Produkt erzeugt Guthaben.
  Punkt 4 – Kontoanlage: ohne Telefon UND gültiger LR-Partnernummer kein Konto.
  Punkt 5 – Verteiler: Konto ohne Telefon bekommt nie einen Lead, wird nie belastet.

Alle Google-Sheet-/WhatsApp-/Stripe-API-Zugriffe sind gemockt.
Ausführen:  python -m pytest tests/ -v
"""
import logging
import sys
import os

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import app  # noqa: E402


# ─── Hilfsdaten: Sessions wie sie Stripe im checkout.session.completed schickt ──
LEAD_PLINK   = "plink_LEAD_KAUF_LR"
LEAD_PRICE   = "price_LEAD_50EUR"
LEAD_PRODUCT = "prod_LEAD_PAKET"

FIRMA_PRICE   = "price_FIRMENGRUENDUNG_ES"
FIRMA_PRODUCT = "prod_FIRMENGRUENDUNG_ES"


def session_firmengruendung_1815():
    """Der echte Vorfall: 1.815 € über fremden Payment-Link, kein Telefon."""
    return {
        "id": "cs_test_firma_1815",
        "amount_total": 181500,
        "payment_link": "plink_FIRMENGRUENDUNG",
        "customer_details": {
            "email": "info@adalgbgeothermie.com",
            "name": "Etienne Spruijt",
            "phone": None,
        },
        "line_items": {"data": [
            {"price": {"id": FIRMA_PRICE, "product": {"id": FIRMA_PRODUCT}}}
        ]},
        "custom_fields": [],
        "metadata": {},
    }


def session_lead_kauf(email="neu@partner.de", phone="+49 171 1234567", lr="1234567", betrag_cents=5000):
    """Ein echter Lead-Kauf über den LR-Payment-Link, mit Telefon + LR-Nummer."""
    s = {
        "id": "cs_test_lead",
        "amount_total": betrag_cents,
        "payment_link": LEAD_PLINK,
        "customer_details": {"email": email, "name": "Anna Partner", "phone": phone},
        "line_items": {"data": [
            {"price": {"id": LEAD_PRICE, "product": {"id": LEAD_PRODUCT}}}
        ]},
        "custom_fields": [],
        "metadata": {},
    }
    if lr is not None:
        s["custom_fields"] = [{
            "key": "lrpartnernummer",
            "label": {"type": "custom", "custom": "LR-Partnernummer"},
            "type": "text",
            "text": {"value": lr},
        }]
    return s


# ─── Fixtures: alles Externe wegmocken ───────────────────────────────────────
@pytest.fixture
def calls(monkeypatch):
    """Zeichnet alle Sheet-/WhatsApp-Aufrufe auf, statt sie auszuführen."""
    rec = {"append_row": [], "update_cell": [], "wa": [], "update_partner": [], "log_lead": []}

    class FakeWS:
        def append_row(self, row):
            rec["append_row"].append(row)
        def update_cell(self, r, c, v):
            rec["update_cell"].append((r, c, v))

    monkeypatch.setattr(app, "get_partner_sheet", lambda: FakeWS())
    monkeypatch.setattr(app, "send_whatsapp", lambda phone, msg, _skip_admin=False: rec["wa"].append((phone, msg)) or True)
    monkeypatch.setattr(app, "send_whatsapp_buttons", lambda *a, **k: True)
    monkeypatch.setattr(app, "update_partner", lambda *a: rec["update_partner"].append(a))
    monkeypatch.setattr(app, "log_lead", lambda *a: rec["log_lead"].append(a))
    monkeypatch.setattr(app, "partner_ist_verfuegbar", lambda zf: True)
    return rec


@pytest.fixture(autouse=True)
def _reset_cache(monkeypatch):
    monkeypatch.setattr(app, "_LEAD_PLINK_CACHE", "")
    monkeypatch.setattr(app, "STRIPE_PAYMENT_LINK", "")
    monkeypatch.setattr(app, "LR_PARTNERNUMMER_PFLICHT", False)


@pytest.fixture
def whitelist(monkeypatch):
    monkeypatch.setattr(app, "STRIPE_LEAD_PRICE_IDS", {LEAD_PRICE})
    monkeypatch.setattr(app, "STRIPE_LEAD_PRODUCT_IDS", {LEAD_PRODUCT})
    monkeypatch.setattr(app, "STRIPE_LEAD_PAYMENT_LINK_IDS", {LEAD_PLINK})


@pytest.fixture
def nur_lr_link_url(monkeypatch):
    """Der einfache Fall: Nur STRIPE_PAYMENT_LINK (URL des LR-Links) ist gesetzt.
    Stripe-API liefert zwei Links: LR und Spanien."""
    monkeypatch.setattr(app, "STRIPE_LEAD_PRICE_IDS", set())
    monkeypatch.setattr(app, "STRIPE_LEAD_PRODUCT_IDS", set())
    monkeypatch.setattr(app, "STRIPE_LEAD_PAYMENT_LINK_IDS", set())
    monkeypatch.setattr(app, "STRIPE_PAYMENT_LINK", "https://buy.stripe.com/LR_LEADS")
    monkeypatch.setattr(app.stripe, "api_key", "sk_test_dummy")
    monkeypatch.setattr(app.stripe.PaymentLink, "list", staticmethod(lambda **kw: {"data": [
        {"id": "plink_FIRMENGRUENDUNG", "url": "https://buy.stripe.com/ESPANA"},
        {"id": LEAD_PLINK,              "url": "https://buy.stripe.com/LR_LEADS"},
    ]}))


@pytest.fixture
def lr_pflicht(monkeypatch):
    monkeypatch.setattr(app, "LR_PARTNERNUMMER_PFLICHT", True)


@pytest.fixture
def keine_partner(monkeypatch):
    monkeypatch.setattr(app, "get_all_partner_records", lambda: [])


# ═════════════════════════════════════════════════════════════════════════════
# PUNKT 3 – Weiße Liste
# ═════════════════════════════════════════════════════════════════════════════
class TestPunkt3WeisseListe:

    # ── Der einfache Fall: nur STRIPE_PAYMENT_LINK (LR-Link-URL) ist gesetzt ──
    def test_einfach_spanien_link_wird_ignoriert(self, calls, nur_lr_link_url, keine_partner, caplog):
        """Zahlung über den Spanien-Link → kein Konto, kein Guthaben, nur Log."""
        caplog.set_level(logging.WARNING)
        s = session_firmengruendung_1815()
        s.pop("line_items")     # wie im echten Webhook: nur payment_link-ID drin
        app.process_stripe_payment(s)
        assert calls["append_row"] == []
        assert calls["update_cell"] == []
        assert "STRIPE-ZAHLUNG IGNORIERT" in caplog.text
        assert "1815.00" in caplog.text
        assert "info@adalgbgeothermie.com" in caplog.text

    def test_einfach_lr_link_erzeugt_konto(self, calls, nur_lr_link_url, keine_partner):
        """Zahlung über den LR-Link → Konto wird angelegt (Telefon reicht, keine LR-Nummer nötig)."""
        s = session_lead_kauf(lr=None)
        s.pop("line_items")
        app.process_stripe_payment(s)
        assert len(calls["append_row"]) == 1
        assert calls["append_row"][0][2] == 50.0

    def test_einfach_lr_link_wird_nur_einmal_aufgeloest(self, calls, nur_lr_link_url, keine_partner):
        s = session_lead_kauf(lr=None)
        app.process_stripe_payment(s)
        assert app._LEAD_PLINK_CACHE == LEAD_PLINK

    def test_einfach_link_nicht_gesetzt_alles_ignoriert(self, monkeypatch, calls, nur_lr_link_url, keine_partner, caplog):
        caplog.set_level(logging.WARNING)
        monkeypatch.setattr(app, "STRIPE_PAYMENT_LINK", "")
        app.process_stripe_payment(session_lead_kauf())
        assert calls["append_row"] == []
        assert "STRIPE_PAYMENT_LINK" in caplog.text

    # ── Erweiterte Fälle (IDs direkt hinterlegt) ──
    def test_vorfall_1815_firmengruendung_wird_ignoriert(self, calls, whitelist, keine_partner, caplog):
        """Der reale Vorfall darf nie wieder ein Konto/Guthaben erzeugen."""
        caplog.set_level(logging.WARNING)
        app.process_stripe_payment(session_firmengruendung_1815())

        assert calls["append_row"] == [], "Es darf KEIN Partnerkonto angelegt werden"
        assert calls["update_cell"] == [], "Es darf KEIN Guthaben gebucht werden"

        # Nur geloggt: Betrag, Produkt-ID, Zahler-Email
        log = caplog.text
        assert "STRIPE-ZAHLUNG IGNORIERT" in log
        assert "1815.00" in log
        assert FIRMA_PRODUCT in log
        assert "info@adalgbgeothermie.com" in log

        # Admin bekommt Hinweis, Zahler bekommt KEINE Willkommensnachricht
        assert len(calls["wa"]) == 1
        assert calls["wa"][0][0] == app.MATZE_PHONE
        assert "ignoriert" in calls["wa"][0][1].lower()

    @pytest.mark.parametrize("cents", [100, 121, 200])
    def test_testzahlungen_aug_1_bis_2_euro_werden_ignoriert(self, calls, whitelist, keine_partner, cents):
        """Die 4 Testzahlungen vom 10.–12.08. (1 €, 1,21 €, 2 €) ohne Lead-Produkt."""
        s = session_firmengruendung_1815()
        s["amount_total"] = cents
        s["customer_details"]["email"] = "test@example.com"
        app.process_stripe_payment(s)
        assert calls["append_row"] == []
        assert calls["update_cell"] == []

    def test_lead_kauf_ueber_payment_link_wird_akzeptiert(self, calls, whitelist, keine_partner):
        app.process_stripe_payment(session_lead_kauf())
        assert len(calls["append_row"]) == 1, "Echter Lead-Kauf muss Konto anlegen"
        assert calls["append_row"][0][2] == 50.0

    def test_lead_kauf_erkannt_ueber_price_id_ohne_payment_link(self, calls, whitelist, keine_partner):
        s = session_lead_kauf()
        s["payment_link"] = None
        app.process_stripe_payment(s)
        assert len(calls["append_row"]) == 1

    def test_lead_kauf_erkannt_ueber_product_id(self, monkeypatch, calls, keine_partner):
        monkeypatch.setattr(app, "STRIPE_LEAD_PRICE_IDS", set())
        monkeypatch.setattr(app, "STRIPE_LEAD_PAYMENT_LINK_IDS", set())
        monkeypatch.setattr(app, "STRIPE_LEAD_PRODUCT_IDS", {LEAD_PRODUCT})
        s = session_lead_kauf()
        s["payment_link"] = None
        app.process_stripe_payment(s)
        assert len(calls["append_row"]) == 1

    def test_gleicher_betrag_50_euro_aber_fremdes_produkt_wird_ignoriert(self, calls, whitelist, keine_partner):
        """Betrag allein reicht nicht – das Produkt muss stimmen."""
        s = session_firmengruendung_1815()
        s["amount_total"] = 5000
        app.process_stripe_payment(s)
        assert calls["append_row"] == []

    def test_line_items_werden_per_api_nachgeladen_wenn_nicht_im_event(self, monkeypatch, calls, whitelist, keine_partner):
        """Im echten Webhook-Event fehlen line_items → API-Call, dann Whitelist-Prüfung."""
        s = session_firmengruendung_1815()
        s.pop("line_items")
        s["payment_link"] = None
        monkeypatch.setattr(app.stripe, "api_key", "sk_test_dummy")
        monkeypatch.setattr(
            app.stripe.checkout.Session, "list_line_items",
            staticmethod(lambda sid, **kw: {"data": [{"price": {"id": FIRMA_PRICE, "product": {"id": FIRMA_PRODUCT}}}]}),
        )
        app.process_stripe_payment(s)
        assert calls["append_row"] == []

    def test_ohne_konfigurierte_whitelist_wird_alles_ignoriert(self, monkeypatch, calls, keine_partner, caplog):
        """Fail-closed: keine IDs konfiguriert → keine Zahlung erzeugt Guthaben."""
        caplog.set_level(logging.WARNING)
        monkeypatch.setattr(app, "STRIPE_LEAD_PRICE_IDS", set())
        monkeypatch.setattr(app, "STRIPE_LEAD_PRODUCT_IDS", set())
        monkeypatch.setattr(app, "STRIPE_LEAD_PAYMENT_LINK_IDS", set())
        app.process_stripe_payment(session_lead_kauf())
        assert calls["append_row"] == []
        assert "STRIPE_PAYMENT_LINK" in caplog.text

    def test_bestehender_partner_fremdes_produkt_kein_aufladen(self, monkeypatch, calls, whitelist):
        """Auch bestehende Partner bekommen aus fremden Produkten kein Guthaben."""
        monkeypatch.setattr(app, "get_all_partner_records", lambda: [{
            "row_index": 2, "name": "Anna", "phone": "491711234567", "guthaben": 10.0,
            "email": "info@adalgbgeothermie.com", "status": "Aktiv", "lead_count": 0,
            "zeitfenster": "Ganztag", "last_lead": "",
        }])
        app.process_stripe_payment(session_firmengruendung_1815())
        assert calls["update_cell"] == [], "Kein Guthaben-Update für fremdes Produkt"


# ═════════════════════════════════════════════════════════════════════════════
# PUNKT 4 – Kontoanlage nur mit Telefon UND gültiger LR-Partnernummer
# ═════════════════════════════════════════════════════════════════════════════
class TestPunkt4KontoAnlageSperre:

    # ── Standard: Telefon ist Pflicht, LR-Nummer optional ──
    def test_ohne_telefon_kein_konto(self, calls):
        token = app.add_new_partner("Etienne Spruijt", "info@adalgbgeothermie.com", "", 1815.0, "1234567")
        assert token == ""
        assert calls["append_row"] == []

    def test_ohne_beides_kein_konto(self, calls):
        token = app.add_new_partner("Etienne Spruijt", "info@adalgbgeothermie.com", "", 1815.0, "")
        assert token == ""
        assert calls["append_row"] == []

    def test_telefon_allein_reicht_standard(self, calls):
        token = app.add_new_partner("Anna", "anna@x.de", "+491711234567", 50.0, "")
        assert token != ""
        assert len(calls["append_row"]) == 1

    # ── Mit LR_PARTNERNUMMER_PFLICHT=1: beides Pflicht ──
    def test_pflicht_ohne_lr_nummer_kein_konto(self, calls, lr_pflicht):
        token = app.add_new_partner("Anna", "anna@x.de", "+491711234567", 50.0, "")
        assert token == ""
        assert calls["append_row"] == []

    @pytest.mark.parametrize("lr", ["abc", "12", "12345678901234", "LR-XYZ"])
    def test_pflicht_ungueltige_lr_nummer_kein_konto(self, calls, lr_pflicht, lr):
        token = app.add_new_partner("Anna", "anna@x.de", "+491711234567", 50.0, lr)
        assert token == ""
        assert calls["append_row"] == []

    def test_pflicht_stripe_flow_ohne_lr_nummer_kein_konto(self, calls, whitelist, keine_partner, lr_pflicht, caplog):
        caplog.set_level(logging.WARNING)
        app.process_stripe_payment(session_lead_kauf(lr=None))
        assert calls["append_row"] == []
        assert "LR-Partnernummer" in caplog.text

    def test_mit_telefon_und_lr_nummer_wird_konto_angelegt(self, calls, lr_pflicht):
        token = app.add_new_partner("Anna", "anna@x.de", "+49 171 1234567", 50.0, "12 345-67")
        assert token != ""
        assert len(calls["append_row"]) == 1
        row = calls["append_row"][0]
        assert row[1] == "491711234567"     # B Telefon normalisiert
        assert row[2] == 50.0               # C Guthaben
        assert row[7] == "anna@x.de"        # H Email
        assert row[9] == "1234567"          # J LR_Partnernummer (bereinigt)

    def test_stripe_flow_lead_kauf_ohne_telefon_kein_konto(self, calls, whitelist, keine_partner, caplog):
        """Selbst ein echter Lead-Kauf ohne Telefon legt kein Konto an."""
        caplog.set_level(logging.WARNING)
        app.process_stripe_payment(session_lead_kauf(phone=None))
        assert calls["append_row"] == []
        assert calls["update_cell"] == []
        assert "KONTO NICHT ANGELEGT" in caplog.text
        assert "Telefonnummer" in caplog.text
        # Nur Admin informiert, keine Willkommens-WA an den Zahler
        assert all(ph == app.MATZE_PHONE for ph, _ in calls["wa"])

    def test_lr_nummer_aus_metadata_wird_akzeptiert(self, calls, whitelist, keine_partner):
        s = session_lead_kauf(lr=None)
        s["metadata"] = {"lr_partnernummer": "7654321"}
        app.process_stripe_payment(s)
        assert len(calls["append_row"]) == 1
        assert calls["append_row"][0][9] == "7654321"


# ═════════════════════════════════════════════════════════════════════════════
# PUNKT 5 – Verteiler: Konto ohne Telefon bekommt nie einen Lead
# ═════════════════════════════════════════════════════════════════════════════
ETIENNE = {
    "row_index": 5, "name": "Etienne Spruijt", "phone": "", "guthaben": 1815.0,
    "email": "info@adalgbgeothermie.com", "status": "Aktiv", "lead_count": 0,
    "zeitfenster": "Ganztag", "last_lead": "",        # ältester last_lead → wäre sonst 1. Wahl
}
ANNA = {
    "row_index": 2, "name": "Anna", "phone": "491711234567", "guthaben": 50.0,
    "email": "anna@x.de", "status": "Aktiv", "lead_count": 3,
    "zeitfenster": "Ganztag", "last_lead": "2026-09-18 10:00:00",
}
LEAD = {"name": "Max Interessent", "phone": "491601111111", "email": "max@x.de",
        "campaign_name": "Test", "ad_name": "Test", "adset_name": ""}


class TestPunkt5VerteilerSperre:

    def test_konto_ohne_telefon_wird_nie_gewaehlt(self, monkeypatch, calls):
        """Etienne hat mehr Guthaben und den älteren last_lead – trotzdem Anna."""
        monkeypatch.setattr(app, "get_all_partner_records", lambda: [ETIENNE, ANNA])
        best = app.find_best_partner()
        assert best["name"] == "Anna"

    def test_nur_konto_ohne_telefon_vorhanden_kein_partner(self, monkeypatch, calls):
        monkeypatch.setattr(app, "get_all_partner_records", lambda: [ETIENNE])
        assert app.find_best_partner() is None

    def test_process_lead_belastet_konto_ohne_telefon_nicht(self, monkeypatch, calls):
        monkeypatch.setattr(app, "get_all_partner_records", lambda: [ETIENNE])
        ok = app.process_lead(dict(LEAD))
        assert ok is False
        assert calls["update_partner"] == [], "Keine Abbuchung"
        assert calls["log_lead"] == []

    def test_zweite_sperre_in_process_lead(self, monkeypatch, calls):
        """Selbst wenn find_best_partner ein Konto ohne Telefon liefern würde: keine Abbuchung."""
        monkeypatch.setattr(app, "find_best_partner", lambda: dict(ETIENNE))
        ok = app.process_lead(dict(LEAD))
        assert ok is False
        assert calls["update_partner"] == []
        assert calls["log_lead"] == []

    def test_partner_mit_telefon_wird_normal_beliefert(self, monkeypatch, calls):
        monkeypatch.setattr(app, "get_all_partner_records", lambda: [ETIENNE, ANNA])
        ok = app.process_lead(dict(LEAD))
        assert ok is True
        assert len(calls["update_partner"]) == 1
        row_index, neues_guthaben, lead_count = calls["update_partner"][0]
        assert row_index == ANNA["row_index"]
        assert neues_guthaben == 45.0

    @pytest.mark.parametrize("phone", ["", None, "0", "123", "   "])
    def test_partner_hat_telefon_erkennt_unbrauchbare_nummern(self, phone):
        assert app.partner_hat_telefon({"phone": phone}) is False

    def test_partner_hat_telefon_akzeptiert_echte_nummer(self):
        assert app.partner_hat_telefon({"phone": "491711234567"}) is True
