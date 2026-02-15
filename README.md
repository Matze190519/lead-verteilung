# Lead-Verteilungs-Service – Bereit für den Start!

Hallo Matze, dein neuer, stabiler Lead-Verteilungs-Service ist fertig! Er ist als Python-Anwendung gebaut und kann mit wenigen Klicks auf einem Cloud-Dienst wie Render.com oder Railway.app deployed werden. Damit ist das Make.com-Drama Geschichte.

Der gesamte Code befindet sich in deinem neuen, privaten GitHub-Repository:
**https://github.com/Matze190519/lead-verteilung**

---

## 🚀 1-Click-Deployment auf Render.com

Render.com ist ein Cloud-Anbieter, der einen kostenlosen Plan anbietet, der für diesen Service ausreichen sollte. Das Deployment ist sehr einfach:

1.  **Account erstellen:** Erstelle einen Account auf [Render.com](https://render.com/) und verbinde ihn mit deinem GitHub-Account.

2.  **Neuen Service erstellen:**
    *   Gehe zum Dashboard und klicke auf **"New"** → **"Blueprint"**.
    *   Wähle dein neues Repository aus: `Matze190519/lead-verteilung`.
    *   Render liest automatisch die `render.yaml`-Datei und konfiguriert alles. Du musst nur einen Namen für den Service vergeben (z.B. `lead-verteilung`).

3.  **Geheimnisse (Secrets) hinzufügen:**
    *   Nachdem der Service erstellt wurde, gehe zum Tab **"Environment"**.
    *   Füge unter **"Secret Files"** eine neue Datei hinzu:
        *   **Filename:** `credentials.json`
        *   **Contents:** Füge hier den kompletten Inhalt deiner Google Service Account JSON-Datei ein.
    *   Füge unter **"Environment Variables"** eine neue Variable hinzu:
        *   **Key:** `WHAPI_TOKEN`
        *   **Value:** `HMivxIUhGo7K7qNVXHFtT25CEevclAaB`

4.  **Deployment abwarten:** Render wird den Service automatisch bauen und starten. Das kann ein paar Minuten dauern. Sobald der Status auf "Live" steht, ist der Service einsatzbereit.

---

## 🎯 Webhook bei Facebook einrichten

Sobald dein Service auf Render live ist, bekommst du eine öffentliche URL. Diese ist deine neue Webhook-URL.

1.  **Webhook-URL kopieren:**
    *   Die URL findest du oben auf der Service-Seite bei Render. Sie sieht so aus: `https://dein-service-name.onrender.com`.
    *   Deine vollständige Webhook-URL lautet: **`https://dein-service-name.onrender.com/webhook`**

2.  **Bei Facebook eintragen:**
    *   Gehe zu den Einstellungen deiner Facebook Lead Ad.
    *   Trage die kopierte Webhook-URL ein.
    *   Als **Verify Token** gibst du Folgendes ein: `mein_geheimer_token_2024`

Facebook wird eine Test-Anfrage an deinen Service senden. Wenn alles korrekt konfiguriert ist, wird der Webhook gespeichert und ist aktiv.

---

## ⚙️ Konfiguration im Überblick

Die wichtigsten Einstellungen werden über Environment-Variablen gesteuert. Du musst nur die Secrets (Whapi Token, Google Credentials) bei Render eintragen.

| Variable                  | Beschreibung                                                                                             | Wert                                     |
| ------------------------- | -------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `WHAPI_TOKEN`             | Dein API-Token von Whapi.cloud.                                                                          | **Musst du bei Render eintragen!**       |
| `GOOGLE_CREDENTIALS_JSON` | Der Inhalt deiner Google Service Account JSON-Datei.                                                     | **Musst du als Secret File eintragen!**  |
| `FB_VERIFY_TOKEN`         | Der Token zur Verifizierung des Webhooks bei Facebook.                                                   | `mein_geheimer_token_2024` (festgelegt)  |
| `GOOGLE_SHEET_NAME`       | Der Name deines Google Sheets.                                                                           | `Partner_Konto` (festgelegt)             |
| `LEAD_PREIS`              | Der Preis, der pro Lead vom Guthaben abgezogen wird.                                                     | `5` (festgelegt)                         |

---

Damit ist alles bereit. Sobald du den Service deployed und den Webhook bei Facebook eingetragen hast, läuft die Lead-Verteilung vollautomatisch und stabil. Melde dich, falls du Fragen hast!
