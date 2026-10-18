# Slot-Router-Plugin für opencode

opencode-Plugin (v2) für den lokalen Session-Slot-Router. Es

1. registriert das vom Router bediente Modell im opencode-Katalog (Model-ID und
   Kontext-Limit live aus `GET {baseURL}/models` bzw. `meta.n_ctx`,
   Output-Limit aus `GET {origin}/slots` bzw. `params.max_tokens`),
2. stellt bei `settings.reasoning: true` jeden Eintrag von
   `settings.reasoning_efforts` als Thinking-Variante bereit und markiert das
   Modell mit `compatibility.reasoningField: "reasoning_content"`, damit
   opencode die Thinking-Blöcke der llama.cpp-Antworten parsest, und
3. versieht jeden Chat-Completion-Request über den `http.request`-Session-Hook
   mit den Routing-Feldern des Routers: `agent_session_id` (= opencode-
   Sessions-ID), `agent_name: "opencode"` und `client_type: "opencode"`.
   Der Hook ersetzt den ausgehenden `Request` durch eine Kopie mit erweitertem
   JSON-Body und korrigiertem `content-length`.

## Voraussetzungen

- opencode v2 (getestet mit 2.0.32)
- Session-Slot-Router unter `http://127.0.0.1:8081` (Projekt-README im
  Ordner `plugin-opencode/` bzw. im Router-Projekt), dessen `agent_name`-
  Whitelist `"opencode"` enthält — sonst lehnt der Router die gerouteten
  Requests mit HTTP 400 ab
- llama-server hinter dem Router, der `GET /v1/models` und `GET /slots` dient

## Installation

1. `index.ts` nach `~/.config/opencode/plugins/slot-router/index.ts` kopieren.
   opencode lädt lokale Plugin-Verzeichnisse automatisch; ein Eintrag in
   `plugins` in der Konfiguration ist **nicht** nötig.
2. Den Provider in `~/.config/opencode/opencode.jsonc` ergänzen (siehe unten).
3. Den opencode-Dienst neu starten. Alternativ genügt eine Änderung der
   Plugin-Datei: sie wird beobachtet, neu geladen und `setup` läuft erneut.

## Konfiguration

```jsonc
{
  "providers": {
    "slot-router": {
      "name": "Slot Router",
      "npm": "@ai-sdk/openai-compatible",
      "settings": {
        "baseURL": "http://127.0.0.1:8081/v1",
        "reasoning": true,
        "reasoning_efforts": ["low", "medium", "high", "xhigh"]
      }
    }
  }
}
```

| Feld | Bedeutung |
| --- | --- |
| `npm` | Provider-Package für den Session-Runner. **Pflicht**: ohne gültiges Package scheitert jede Generierung mit `UnsupportedPackageError` (leeres Package wird abgelehnt). |
| `settings.baseURL` | URL des Routers; aus ihr werden `/models` (am `{baseURL}`-End) und `/slots` (am `origin` der URL) abgeleitet. |
| `settings.reasoning` | `true` schaltet die Thinking-Varianten und `reasoningField: "reasoning_content"` ein. |
| `settings.reasoning_efforts` | Liste der Variant-IDs; jede wird im TUI im Thinking-Selector sichtbar und sendet `reasoning_effort` an llama-server. Änderungen wirken nach Config-/Plugin-Hot-Reload ohne Neustart. |

## Verhalten

- **Modellkatalog:** Ein Modell ohne `meta.n_ctx` (bzw. nicht Zahl / ≤ 0) wird
  übersprungen mit Warnung; fehlt `params.max_tokens` in `/slots` (oder ist
  `/slots` nicht erreichbar), gilt der Output-Fallback `min(n_ctx, 32768)`,
  ebenfalls mit Warnung. Ein unerreichbarer `/v1/models`-Endpunkt führt zu
  null registrierten Modellen (Provider bleibt im TUI sichtbar, ohne Modell).
- **Start- und Recovery-Verhalten:** Beim Start 20 × 500 ms Retry, solange kein
  Modell registriert ist. Danach wird alle 60 Sekunden erneut geprüft, **solange
  das Inventar leer ist** — ein Router, der später neu startet (z. B. bei
  Modellwechsel), wird ohne manuellen Eingriff wieder aufgenommen. Außerdem
  wird bei `config.updated`/`provider.updated` neu abgefragt.
- **Session-Affinität:** Jede opencode-Session bildet den Routing-Key
  `opencode:<sessions-ID>`. Folgeturns derselben Session erhalten denselben
  llama.cpp-Slot (KV-Cache-Affinität, `action=reuse`); neue Sessions belegen
  freie Slots oder verdrängen den LRU-Slot (`action=assign`/`evict`).
  Verifiziert gegen den laufenden Router.

## Verwendung im TUI

- Model-Selector: Provider `Slot Router` → das vom Router gemeldete Modell.
- Thinking-Selector: die konfigurierten `reasoning_efforts`.
- Die Thinking-Blöcke erscheinen als eigene Reasoning-Parts in der Antwort.

## Diagnose

- `~/.config/opencode/plugins/slot-router/debug.log` (neben der Plugin-Datei):
  - `registered 1 model(s)` / `refresh unchanged (1 model(s))` — Katalog OK
  - `injected agent_session_id for session ses_…` — Affinitäts-Injektion aktiv
  - Warnungen: `provider slot-router not found …` (Config fehlt/zu früh),
    `refresh failed: …` (Router down), `… falling back to 32768`
- `router.log` des Routers: `incoming … agent='opencode' session='ses_…'`,
  `request session='opencode:ses_…' action=assign|reuse|evict`,
  `finished … status=200`; bei Abweisung `rejected … reason=agent must be …`
- `~/.opencode/bin/opencode api GET /api/model` zeigt die registrierten
  Modelle inkl. Limits und Varianten; steht dort kein `slot-router`-Modell,
  zuerst `debug.log` prüfen.
