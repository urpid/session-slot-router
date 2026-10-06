import importlib.util
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PI_FILES = [Path.home() / ".pi/agent/extensions/pi-session.ts", ROOT / "plugin-pi/pi-session.ts"]
HERMES_FILE = ROOT / "plugin-hermes/__init__.py"


class PluginTest(unittest.TestCase):
    def test_pi_port_not_provider_name(self):
        code = '''
const {default:ext} = await import(process.argv[1]);
let handler;
ext({on:(name,fn)=>{if(name==='before_provider_request')handler=fn;}});
const event={payload:{messages:['hello']}};
const ctx=(provider,baseUrl,session='sid',mode='tui')=>({model:{provider,baseUrl},mode,sessionManager:{getSessionId:()=>session}});
const on8081=handler(event,ctx('other','http://127.0.0.1:8081/v1'));
if(on8081?.agent_session_id!=='sid'||on8081.client_type!=='tui') throw Error('port 8081 or client type ignored');
if(handler(event,ctx('other','http://127.0.0.1:8081/v1','sid','print'))?.client_type!=='print') throw Error('print mode lost');
if(handler(event,ctx('llama-slot-test','http://127.0.0.1:8080/v1'))!==undefined) throw Error('other port tagged');
if(handler(event,ctx('llama-slot-test','http://127.0.0.1:18081/v1'))!==undefined) throw Error('substring port tagged');
let rejected=false;
try {handler(event,ctx('other','http://127.0.0.1:8081/v1',''));} catch {rejected=true;}
if(!rejected) throw Error('missing session accepted');
'''
        for path in PI_FILES:
            with self.subTest(path=path):
                subprocess.run(["node", "--input-type=module", "-e", code, str(path)], check=True, capture_output=True, text=True)

    def test_hermes_effective_port(self):
        spec = importlib.util.spec_from_file_location("test_hermes_router_plugin", HERMES_FILE)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        registered = []
        class Context:
            def register_middleware(self, kind, callback):
                registered.append((kind, callback))
        module.register(Context())
        self.assertEqual(len(registered), 1)
        kind, callback = registered[0]
        self.assertEqual(kind, "llm_request")
        request = {"model": "test", "extra_body": {"temperature_hint": 2, "agent_session_id": "stale"}}
        result = callback(request, base_url="http://example.org:8081/v1", session_id="sid", platform="cli")
        self.assertEqual(result["request"]["extra_body"], {"temperature_hint": 2, "agent_session_id": "sid", "agent_name": "hermes", "client_type": "cli"})
        self.assertEqual(request["extra_body"]["agent_session_id"], "stale")
        for cron_id in ("cron_abcdef123456_20261005_010101", "cron_abcdef123456_20261006_020202"):
            cron_result = callback(request, base_url="http://example.org:8081/v1", session_id=cron_id)
            self.assertEqual(cron_result["request"]["extra_body"]["agent_session_id"], "cron:abcdef123456")
            self.assertEqual(cron_result["request"]["extra_body"]["client_type"], "cronjob")
        other = callback(request, base_url="http://example.org:8081/v1",
                         session_id="cron_abcdef123457_20261006_020202")
        self.assertEqual(other["request"]["extra_body"]["agent_session_id"], "cron:abcdef123457")
        rotated = callback(request, base_url="http://example.org:8081/v1", session_id="continuation-1", platform="cron")
        self.assertEqual(rotated["request"]["extra_body"]["agent_session_id"], "continuation-1")
        self.assertEqual(rotated["request"]["extra_body"]["client_type"], "cronjob")
        for url in ("http://example.org:8080/v1", "http://example.org:18081/v1"):
            self.assertIsNone(callback(request, base_url=url, session_id="sid"))
        with self.assertRaises(ValueError):
            callback(request, base_url="http://example.org:8081/v1")


if __name__ == "__main__":
    unittest.main()
