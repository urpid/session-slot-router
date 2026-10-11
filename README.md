# Lokaler Session-Slot-Router (Testversion)

- Start: `<PROJEKTORDNER>/start-router.sh`. Das Skript startet den Router mit `nohup` im Hintergrund, gibt seine PID aus und schreibt nach `<PROJEKTORDNER>/router.log` (`tail -f` zum Verfolgen). SQLite liegt unabhängig vom Startverzeichnis unter `<PROJEKTORDNER>/router.sqlite`. Nur eine Router-Instanz kann Port 8081 belegen; ein weiterer Start meldet einen Fehler in `router.log`. Der Log enthält Datum/Uhrzeit mit Zeitzone, HTTP-Methode/Route (ohne Query), `agent_name`, `agent_session_id`, `client_type`, vom Client angegebene `id_slot`, erkannte Slots, tatsächlicher Slot, Aktion (inklusive bisheriger Session bei Verdrängung), Antwortstatus und Ablehnungen. Nicht unterstützte Routen werden vor dem Einlesen ihres Bodys abgewiesen und zeigen daher nur Methode/Route. Keine Prompts, Tools, Request-Bodys oder API-Schlüssel.
- Router lauscht auf `0.0.0.0:8081` (lokal `http://127.0.0.1:8081/v1`); llama-server bleibt unter `http://127.0.0.1:8080/v1`.
- Pi-Test: `pi --provider llama-slot-test --model Qwen3.8-27B-S1`. Das Modell steht in `~/.pi/agent/models.json`; `~/.pi/agent/extensions/pi-session.ts` wird automatisch geladen und ergänzt nur die Session-ID.
- Hermes-Plugin: `plugin-hermes/` ist ein allgemeines Hermes-Plugin mit `llm_request`-Middleware, **kein** Modell-Provider. Eine Kopie liegt unter `~/.hermes/profiles/<PROFIL>/plugins/session-slot-router/` und ist in <PROFIL>s `plugins.enabled` eingetragen; es ist **nicht** global unter `~/.hermes/plugins/` installiert. Es ergänzt Haupt-Agent-Anfragen an eine Basis-URL mit Port `8081`; zusätzliche Hilfsmodell-Anfragen durchlaufen diesen Middleware-Hook nicht.
- Pi-Extension (`~/.pi/agent/extensions/pi-session.ts`, Kopie unter `plugin-pi/`) und Hermes-Middleware (`plugin-hermes/`) ergänzen die Session-ID und `client_type` nur, wenn die verwendete Basis-URL explizit Port `8081` hat. Pi sendet `ctx.mode` unverändert (`tui`, `print`, `rpc` oder `json`). Hermes sendet `cronjob` bei Cron-Plattform oder einer passenden Cron-Session-ID, sonst den Plattformnamen bzw. `unknown`.
- Der Router reicht Methoden, Route, Query, Header und Body ohne `agent_session_id` an llama-server auf Port 8080 weiter. Enthält ein JSON-Body `agent_session_id`, liest der Router `/slots`, wählt den zugeordneten Slot, entfernt `agent_name`, `agent_session_id` und `client_type`, überschreibt `id_slot` und leitet die Anfrage weiter. Nicht unterstützte llama-server-Routen liefern deshalb die unveränderte Upstream-Antwort, häufig HTTP 404.
- SQLite speichert Slot-Zuordnung, LRU-Zähler und Ereignisse/HTTP-Status samt `client_type`, **keine Prompts**. Der Typ ist reine Protokollinformation und beeinflusst die Slotwahl nicht; bei alten Clients bleibt er `NULL`. `cli` oder `print` beweist nicht, dass ein Mensch statt eines Skripts die Anfrage ausgelöst hat. Neue Sessions belegen freie Slots, sonst den inaktiven LRU-Slot. Laufende Router-Anfragen blockieren den jeweiligen Slot; falls alle belegt sind, wartet die neue Anfrage.
- Das Hermes-Plugin wandelt Session-IDs `cron_<12 hex Job-ID>_<YYYYMMDD_HHMMSS>` in `cron:<Job-ID>` um, damit Läufe desselben Jobs dieselbe Routing-ID senden. Alle anderen Session-IDs (auch rotierte Cron-Fortsetzungen ohne dieses Muster) bleiben unverändert. Der Router nimmt IDs unverändert entgegen und trennt nur die `pi:`-/`hermes:`-Namensräume.

## <PROFIL>: Installationsstand

- Das Plugin liegt unter `~/.hermes/profiles/<PROFIL>/plugins/session-slot-router/` (`__init__.py`, `plugin.yaml`); `~/.hermes/profiles/<PROFIL>/config.yaml` enthält `session-slot-router` zusätzlich zu `hermes-session-reset-policy` in `plugins.enabled`.
- <PROFIL> verwendet `model.provider: custom` mit `base_url: http://gmm:8081/v1`; das Plugin wurde in echten <PROFIL>-Anfragen erkannt und der Router hat deren Session wiederholt demselben Slot zugewiesen.

## Grenzen dieser ersten Fassung

- **Verdrängung ist ohne Nachfrage und ohne garantierte Sicherung.** Ein vorheriger langer Kontext kann erneut eingelesen werden. Auch direkte Anfragen an Port 8080 umgehen den Router vollständig.
- Der Router liest keine vorhandenen Kontexte aus den Slots; nach einem Neustart von Router oder llama-server beweist eine gespeicherte Zuordnung **keinen** erhaltenen KV-Zustand.
- **Keine Authentifizierung:** Bei `0.0.0.0:8081` ist der Router aus allen netzseitig erreichbaren Interfaces zugänglich, sofern die Firewall es zulässt. Für Netzzugriff die UFW-Freigabe auf vertrauenswürdige Quell-IP(s) beschränken. Keine garantierte Wiederaufnahme einer abgebrochenen Streaming-Anfrage und keine Beschränkung der Wartezeit, wenn alle Slots belegt sind.
- HTTP-Methoden GET, HEAD, POST, PUT, PATCH, DELETE und OPTIONS werden weitergeleitet; WebSockets und eingehende Bodies ohne `Content-Length` sind nicht implementiert.
- Pi und <PROFIL> wurden gegen den laufenden Router und llama-server verwendet. Pi `--print` benötigte in dieser Umgebung bei einem früheren Test ein Pseudoterminal (`script -qec 'pi ...' /dev/null`). Cron-End-to-End-Tests der aktuellen Hermes-Middleware stehen noch aus.

## Tests

Die Tests liegen unter `tests/` und werden aus dem Projektverzeichnis ausgeführt:

```bash
python3 -m unittest discover -s tests -v
```

Sie prüfen unter anderem Passthrough ohne Session-ID, Routing/LRU mit Session-ID, Streaming, HTTP-Methoden, SQLite-Migration, Terminal-Logging sowie die Pi- und Hermes-Plugin-Metadaten.
