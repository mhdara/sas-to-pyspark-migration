"""llm_anthropic.py - the Claude (Anthropic API) provider used by llm_client.py.

Kept in its own module so the Ollama code (OpenAI-compatible API) and the Anthropic SDK code stay apart.
- Structured output: output_config.format = json_schema, so the answer is guaranteed to be valid JSON
  matching the schema (the same schema Ollama gets).
- Refusal fallback: fallbacks="default" (beta server-side-fallback-2026-07-01). If Claude's safety
  classifiers decline a request, the API re-runs it on Anthropic's recommended fallback model.
- Claude Opus 5 accepts no temperature/seed, so repeated runs can differ slightly.
The API key is read by the SDK from ANTHROPIC_API_KEY (set from the git-ignored .env by llm_client.py).
"""

import anthropic

_client = None


def call(prompt: str, schema: dict | None, model: str, max_tokens: int):
    """Send one prompt to Claude; return (response text, usage, model that answered)."""
    global _client
    _client = _client or anthropic.Anthropic()
    kwargs = {
        "model": model,
        "max_tokens": max_tokens,
        "betas": ["server-side-fallback-2026-07-01"],
        "fallbacks": "default",
        "messages": [{"role": "user", "content": prompt}],
    }
    if schema:
        kwargs["output_config"] = {"format": {"type": "json_schema", "schema": schema}}
    response = _client.beta.messages.create(**kwargs)
    if response.stop_reason == "refusal":  # the whole fallback chain declined: no usable content
        raise RuntimeError(f"Claude declined the request: {response.stop_details}")
    text = next(block.text for block in response.content if block.type == "text")
    return text, response.usage, response.model
