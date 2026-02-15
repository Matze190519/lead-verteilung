# Anleitung: Lead-Marktplatz mit Stripe & Lina (Botpress)

Matze, hier ist die Anleitung, um dein vollautomatisches Lead-System zu starten. Der Service läuft bereits auf Render.com und ist bereit.

## Schritt 1: Stripe Payment Link erstellen (5 Minuten)

Du brauchst einen Zahlungslink, den Lina an deine Partner schicken kann.

1.  **Gehe zu Stripe:** [https://dashboard.stripe.com/payment-links](https://dashboard.stripe.com/payment-links)
2.  **Klick auf "+ Neu"**
3.  **Produkt hinzufügen:**
    *   Name: `Lead-Paket (50€)`
    *   Preis: `50,00 €`
    *   Abrechnung: `Einmalig`
4.  **Optionen anpassen:**
    *   **Wichtig:** Unter "Erweiterte Optionen" → aktiviere **"Telefonnummer erfassen"**.
    *   Du kannst auch einstellen, dass Kunden ihre Rechnungen automatisch erhalten.
5.  **Klick auf "Link erstellen"**
6.  **Kopiere den Link.** Den brauchst du gleich für Lina.

## Schritt 2: Stripe Webhook einrichten (5 Minuten)

Damit Stripe deinen Service über neue Zahlungen informieren kann.

1.  **Gehe zu Stripe Webhooks:** [https://dashboard.stripe.com/webhooks](https://dashboard.stripe.com/webhooks)
2.  **Klick auf "Endpunkt hinzufügen"**
3.  **Konfiguration:**
    *   **Endpunkt-URL:** `https://lead-verteilung.onrender.com/stripe-webhook`
    *   **Version:** Neueste API-Version (so lassen)
    *   **Zu sendende Ereignisse auswählen:**
        *   Klick auf "Ereignisse auswählen"
        *   Suche nach `checkout.session.completed` und wähle es aus.
4.  **Klick auf "Endpunkt hinzufügen"**
5.  **Webhook Secret kopieren:**
    *   Nach der Erstellung siehst du eine Detailseite.
    *   Klick auf **"Anzeigen"** beim **Signaturgeheimnis** (Signing Secret).
    *   Kopiere den Wert, der mit `whsec_...` beginnt.

## Schritt 3: Render.com aktualisieren (2 Minuten)

Du musst Render noch deine Stripe-Geheimnisse verraten.

1.  **Gehe zu Render:** [https://dashboard.render.com/](https://dashboard.render.com/)
2.  Klick auf deinen `lead-verteilung` Service.
3.  Gehe zum Tab **"Environment"**.
4.  **Füge 3 neue Environment Variables hinzu:**

| Key | Value |
|---|---|
| `STRIPE_SECRET_KEY` | Dein Stripe Secret Key (beginnt mit `sk_live_...` oder `sk_test_...`) |
| `STRIPE_WEBHOOK_SECRET` | Das Webhook Secret aus Schritt 2 (beginnt mit `whsec_...`) |
| `MATZE_PHONE` | Deine WhatsApp-Nummer im Format `49...` (damit du Zahlungs-Benachrichtigungen bekommst) |

5.  **Klick auf "Save Changes"**. Der Service startet automatisch neu.

## Schritt 4: Lina (Botpress) konfigurieren (10 Minuten)

Jetzt bringst du Lina bei, wie sie die Leads verkauft.

1.  **Öffne deinen Bot in Botpress.**
2.  **Erstelle einen neuen Flow/Knoten:** Nenne ihn z.B. "Leads kaufen".
3.  **Füge eine "Raw Text"-Card hinzu:**
    *   Text: `Hallo! Hier kannst du ein Lead-Paket für 50€ kaufen. Klicke einfach auf den Link unten, um sicher via Stripe zu bezahlen. Dein Guthaben wird automatisch aufgeladen.`
4.  **Füge eine "URL Button"-Card hinzu:**
    *   Button-Text: `Lead-Paket kaufen (50€)`
    *   URL: Der Stripe Payment Link aus Schritt 1.
5.  **Verbinde diesen Flow mit deinem Hauptmenü:** Füge einen Button "Leads kaufen" in deinem Hauptmenü hinzu, der diesen neuen Flow startet.

## Schritt 5: Google Sheet aufräumen (WICHTIG)

Dein Google Sheet "Partner_Konto" hat noch kaputte Testdaten. **Du musst die Zeilen 3 bis 7 löschen**, sonst gibt es Fehler.

1.  Öffne dein Sheet: [https://docs.google.com/spreadsheets/d/1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY/edit](https://docs.google.com/spreadsheets/d/1wVevVuP1sm_2g7eg37rCYSVSoF_T6rjNj89Qkoh9DIY/edit)
2.  Markiere die Zeilen 3, 4, 5, 6 und 7.
3.  Rechtsklick → "Zeilen löschen".

---

**Das war's! Dein System ist jetzt vollautomatisch.**

- Partner kaufen über Lina.
- Guthaben wird automatisch aufgeladen.
- Leads werden fair verteilt.
- Du bekommst eine WhatsApp bei jeder Zahlung.

Melde dich, wenn du bei einem der Schritte Hilfe brauchst!
