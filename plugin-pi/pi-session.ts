import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export default function (pi: ExtensionAPI) {
  pi.on("before_provider_request", (event, ctx) => {
    if (!ctx.model?.baseUrl || new URL(ctx.model.baseUrl).port !== "8081") return;
    if (!event.payload || typeof event.payload !== "object" || Array.isArray(event.payload)) return;
    const session = ctx.sessionManager.getSessionId();
    if (!session) throw new Error("Pi session ID missing");
    return { ...event.payload, agent_name: "pi", agent_session_id: session, client_type: ctx.mode };
  });
}
