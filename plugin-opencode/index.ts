/**
 * slot-router models plugin (opencode v2)
 *
 * Registers the models served by the local OpenAI-compatible slot router
 * configured under `provider.slot-router.settings.baseURL` in the opencode
 * model catalog, using the limits the server itself reports:
 *
 *   GET {baseURL}/models -> data[].id, data[].meta.n_ctx   (context limit)
 *   GET {origin}/slots   -> params.max_tokens              (output limit)
 *
 * When `settings.reasoning` is true, every entry of
 * `settings.reasoning_efforts` becomes a thinking variant
 * (`settings: { reasoningEffort }`), and the model is tagged with
 * `compatibility.reasoningField: "reasoning_content"` so opencode parses
 * the thinking blocks of the llama.cpp response.
 *
 * Session-to-slot affinity: the router pins a conversation to a llama.cpp
 * slot based on `agent_session_id` in the request body (opencode does not
 * send it natively). So this plugin injects `agent_session_id` (the
 * opencode session ID), `agent_name: "opencode"` and
 * `client_type: "opencode"` into every chat-completion request via the
 * `http.request` session hook, which receives the outgoing Request and
 * allows replacing it.
 */

import { appendFileSync } from "node:fs"

const PROVIDER_ID = "slot-router"
const REQUEST_TIMEOUT_MS = 5000
const OUTPUT_FALLBACK_TOKENS = 32768
const LOG_FILE = "/home/worker/.config/opencode/plugins/slot-router/debug.log"

export default {
  id: "slot-router.models",
  setup: async (ctx: any) => {
    const write = (level: string, ...args: unknown[]) => {
      const line = `[${new Date().toISOString()}] ${level} ${args
        .map((a) => (typeof a === "string" ? a : JSON.stringify(a)))
        .join(" ")}\n`
      try {
        appendFileSync(LOG_FILE, line)
      } catch {}
      ;(level === "ERROR" ? console.error : console.log)("[slot-router.models]", ...args)
    }
    const log = (...args: unknown[]) => write("INFO", ...args)
    const warn = (...args: unknown[]) => write("WARN", ...args)

    let inventory: Record<string, any> = {}
    let signature = ""
    let refreshing: Promise<void> | undefined
    let providerName: string | undefined
    let providerPackage: string | undefined
    let providerSettings: Record<string, unknown> = {}

    const readSettings = async () => {
      const listed: any = await ctx.provider.list()
      log("provider.list() ->", Array.isArray(listed) ? listed.length : typeof listed, "entries")
      const entries = Array.isArray(listed)
        ? listed
        : Array.isArray(listed?.data)
          ? listed.data
          : []
      for (const entry of entries) {
        const info = entry?.provider && typeof entry.provider === "object" ? entry.provider : entry
        if (info?.id !== PROVIDER_ID) continue
        const settings = (info.settings ?? {}) as Record<string, unknown>
        providerName = typeof info.name === "string" ? info.name : undefined
        providerPackage = typeof info.package === "string" && info.package.length > 0 ? info.package : undefined
        providerSettings = settings
        return {
          baseURL: typeof settings.baseURL === "string" && settings.baseURL.length > 0 ? settings.baseURL : undefined,
          reasoning: settings.reasoning === true,
          efforts: Array.isArray(settings.reasoning_efforts)
            ? settings.reasoning_efforts.filter((effort: unknown) => typeof effort === "string" && effort.length > 0)
            : [],
        }
      }
      return undefined
    }

    const fetchJson = async (url: string) => {
      const res = await fetch(url, {
        headers: { accept: "application/json" },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      })
      if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`)
      return res.json()
    }

    const buildInventory = async (settings: { baseURL: string; reasoning: boolean; efforts: string[] }) => {
      const base = settings.baseURL.replace(/\/+$/, "")
      const [modelsPayload, slotsPayload] = await Promise.all([
        fetchJson(`${base}/models`),
        fetchJson(`${new URL(base).origin}/slots`).catch((err: any) => {
          warn(`slots fetch failed: ${err?.message ?? err}`)
          return undefined
        }),
      ])

      const data = Array.isArray(modelsPayload?.data) ? modelsPayload.data : []
      const slots = Array.isArray(slotsPayload) ? slotsPayload : []

      let maxTokens: number | undefined
      for (const slot of slots) {
        const value = slot?.params?.max_tokens
        if (typeof value === "number" && Number.isFinite(value) && value > 0) {
          maxTokens = Math.floor(value)
          break
        }
      }

      const next: Record<string, any> = {}
      for (const entry of data) {
        if (!entry || typeof entry.id !== "string" || entry.id.length === 0) continue
        const context =
          typeof entry.meta?.n_ctx === "number" && entry.meta.n_ctx > 0 ? Math.floor(entry.meta.n_ctx) : undefined
        if (context === undefined) {
          warn(`model ${entry.id} has no meta.n_ctx; skipped`)
          continue
        }
        const output = maxTokens ?? Math.min(context, OUTPUT_FALLBACK_TOKENS)
        if (maxTokens === undefined) {
          warn(`no params.max_tokens in /slots; falling back to ${output}`)
        }

        const model: any = {
          id: entry.id,
          modelID: entry.id,
          providerID: PROVIDER_ID,
          name: entry.id,
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          variants: settings.reasoning
            ? settings.efforts.map((effort) => ({ id: effort, settings: { reasoningEffort: effort } }))
            : [],
          time: { released: 0 },
          cost: [],
          status: "active",
          enabled: true,
          limit: { context, output },
        }
        if (settings.reasoning) {
          model.compatibility = { reasoningField: "reasoning_content" }
        }
        next[entry.id] = model
      }
      return next
    }

    const refresh = async () => {
      if (refreshing) return refreshing
      refreshing = (async () => {
        try {
          const settings = await readSettings()
          if (!settings?.baseURL) {
            warn(`provider ${PROVIDER_ID} not found or missing settings.baseURL`)
            return
          }
          const next = await buildInventory(settings)
          const nextSignature = JSON.stringify(next)
          if (nextSignature !== signature) {
            inventory = next
            signature = nextSignature
            await ctx.provider.reload()
            log(`registered ${Object.keys(next).length} model(s)`)
          } else {
            log(`refresh unchanged (${Object.keys(next).length} model(s))`)
          }
        } catch (err: any) {
          warn(`refresh failed: ${err?.stack ?? err?.message ?? err}`)
        } finally {
          refreshing = undefined
        }
      })()
      return refreshing
    }

    // Config-sourced providers are not visible in the transform editor; like
    // the discovery plugin, the provider must be added by the transform itself.
    const transform = (editor: any) => {
      const models = Object.values(inventory)
      const record = editor.get(PROVIDER_ID)
      if (!record) {
        editor.add({
          info: {
            id: PROVIDER_ID,
            name: providerName ?? PROVIDER_ID,
            activation: "enabled",
            // The session runner needs a resolvable AI provider package; the
            // endpoint is OpenAI-compatible by construction (it serves /v1/models).
            package: providerPackage ?? "@opencode/ai/providers/openai-compatible",
            settings: providerSettings,
            integrationID: PROVIDER_ID,
          },
          models,
        })
        log(`transform: provider added with ${models.length} model(s)`)
        return
      }
      const mine = new Set(Object.keys(inventory))
      const existing = [...record.models.values()].filter((model: any) => !mine.has(model.id))
      editor.models.set(PROVIDER_ID, [...existing, ...models])
      log(`transform: merged, total=`, existing.length + models.length)
    }

    // Session-to-slot affinity for the slot router (see file header).
    // The http.request hook receives the outgoing Request object and the
    // type marks `request` as mutable, so the routing fields are injected
    // by replacing the request with an equivalent one carrying the
    // augmented body (content-length adjusted accordingly).
    await ctx.session.hook(
      "http.request",
      async (req: any) => {
        const request = req?.request
        const url = typeof request?.url === "string" ? request.url : ""
        if (!url.includes("/chat/completions")) return
        try {
          const original = await request.clone().json()
          if (!original || typeof original !== "object" || Array.isArray(original)) return
          if (original.agent_session_id !== undefined) return
          original.agent_session_id = req.sessionID
          original.agent_name = "opencode"
          original.client_type = "opencode"
          const body = JSON.stringify(original)
          const headers = new Headers(request.headers)
          headers.set("content-length", String(new TextEncoder().encode(body).length))
          req.request = new Request(url, {
            method: request.method,
            headers,
            body,
          })
          log(`injected agent_session_id for session ${req.sessionID}`)
        } catch (err: any) {
          warn(`http.request injection failed: ${err?.message ?? err}`)
        }
      },
      { providerID: PROVIDER_ID },
    )
    log("setup: session-affinity hooks registered")

    const abort = new AbortController()
    log("setup: registering transform")
    await ctx.provider.transform(transform)
    await ctx.provider.reload()
    log("setup: initial reload done")

    // No polling for the config settings here: in opencode v2.2.0 every
    // provider.list() call re-arms the core's debounced settings merge for
    // config-sourced providers, so a startup retry loop starves the very
    // baseURL we wait for and setup() outlives the 10s ACP plugin-activation
    // window (session/new -> ServiceUnavailable "Internal service failure").
    // setup() therefore returns right away; the provider.updated event (fired
    // once the core applies the config settings, ~0.5s after the last
    // provider activity) triggers the first refresh and registers the models.
    // The recheck below covers a router that starts later.
    const recheck = setInterval(() => {
      if (Object.keys(inventory).length === 0) void refresh()
    }, 60000)

    log("setup: done, subscribing to events")

    void (async () => {
      try {
        for await (const event of ctx.event.subscribe({ signal: abort.signal })) {
          if (event?.type === "config.updated" || event?.type === "provider.updated") {
            try {
              await refresh()
            } catch {
              // keep the subscription alive
            }
          }
        }
      } catch {
        // subscription closed on abort
      }
    })()

    return () => {
      abort.abort()
      clearInterval(recheck)
    }
  },
}
