"""llm_client.py - the ONLY place that talks to an LLM. The provider is chosen in config.yaml.

chat(prompt, schema=None) -> (text or parsed JSON, seconds)
With a JSON schema, the model is forced to return JSON matching it.
"""

import json
import os
import time
from pathlib import Path

import yaml
from openai import OpenAI

ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv(path=ROOT / ".env"):
    """Put KEY=value lines of the git-ignored .env into the environment (a value set in the shell wins)."""
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and not key.strip().startswith("#") and value.strip():
                os.environ.setdefault(key.strip(), value.strip().strip("'\""))


_load_dotenv()
_LLM = yaml.safe_load((ROOT / "python" / "config.yaml").read_text())["llm"]
PROVIDER = os.environ.get("LLM_PROVIDER", _LLM["provider"])  # LLM_PROVIDER=anthropic overrides for one run
CFG = {**_LLM, **_LLM["providers"][PROVIDER]}  # common settings + this provider's (model, ...)
MODEL = CFG["model"]
LAST_USAGE = {}  # tokens, cost and answering model of the latest call (read by analyze_llm.py)


def _ollama(prompt, schema):
    """Call Ollama through its OpenAI-compatible API; with a schema, force JSON output (structured outputs)."""
    client = OpenAI(base_url=CFG["base_url"], api_key=CFG["api_key"], timeout=CFG["timeout_s"])
    kwargs = {
        "model": MODEL,
        "temperature": CFG["temperature"],
        "seed": CFG["seed"],
        "messages": [{"role": "user", "content": prompt}],
    }
    if schema:
        kwargs["response_format"] = {
            "type": "json_schema",
            "json_schema": {"name": "result", "schema": schema, "strict": True},
        }
    response = client.chat.completions.create(**kwargs)
    LAST_USAGE.update(
        input_tokens=response.usage.prompt_tokens,
        output_tokens=response.usage.completion_tokens,
        cost_usd=0.0,  # runs locally
        answered_by=MODEL,
    )
    return response.choices[0].message.content


def _anthropic(prompt, schema):
    """Call Claude through the official SDK (llm_anthropic.py); record tokens and cost."""
    import llm_anthropic  # imported only when used, so Ollama runs need no Anthropic setup

    text, usage, model = llm_anthropic.call(prompt, schema, MODEL, CFG["max_tokens"])
    price = CFG["price_per_million"]
    LAST_USAGE.update(
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cost_usd=round(
            (usage.input_tokens * price["input"] + usage.output_tokens * price["output"]) / 1e6, 5
        ),
        answered_by=model,
    )
    return text


def chat(prompt, schema=None):
    """Send one prompt; return (parsed JSON if a schema is given, else text) and the seconds it took."""
    t0 = time.time()
    text = {"ollama": _ollama, "anthropic": _anthropic}[PROVIDER](prompt, schema)
    return (json.loads(text) if schema else text), round(time.time() - t0, 2)
