"""
LLM utilities for OpenAI integration and document processing.
"""

import json
from typing import Dict, Any, Optional

from ..config.settings import OPENAI_API_KEY, OPENAI_MODEL, ROUTE_LABELS


def strip_json_code_fences(s: str) -> str:
    """Remove JSON code fences from string."""
    s = s.strip()
    if s.startswith("```"):
        first_nl = s.find("\n")
        if first_nl != -1:
            s = s[first_nl+1:]
        if s.endswith("```"):
            s = s[:-3].strip()
    return s


def chat_json(model: str, system_text: str, user_payload: dict) -> dict:
    """
    Try strict JSON mode. If it fails or returns malformed JSON, fall back safely.
    Always return a dict (possibly empty) — never raise here.
    Supports both new SDK (OpenAI) and legacy openai.ChatCompletion.
    """
    system_msg = (
        system_text
        + "\n\nReturn a single JSON object only. Do not include any extra text."
    )
    user_msg = (
        "You MUST return a single JSON object only (JSON). No prose, no code fences.\n\n"
        "Payload follows as JSON:\n"
        + json.dumps(user_payload, ensure_ascii=False)
    )

    if not OPENAI_API_KEY:
        return {}

    # Preferred: new SDK
    try:
        from openai import OpenAI  # type: ignore
        client = OpenAI(api_key=OPENAI_API_KEY)
        try:
            resp = client.chat.completions.create(
                model=model,
                response_format={"type": "json_object"},
                temperature=0,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
            )
            content = resp.choices[0].message.content
            return json.loads(content)
        except Exception:
            # fallback without response_format
            try:
                resp = client.chat.completions.create(
                    model=model,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": system_msg + "\n(You must still return JSON.)"},
                        {"role": "user", "content": user_msg},
                    ],
                )
                raw = resp.choices[0].message.content
                raw = strip_json_code_fences(raw)
                return json.loads(raw)
            except Exception:
                return {}
    except Exception:
        # Legacy openai
        try:
            import openai  # type: ignore
            openai.api_key = OPENAI_API_KEY
            try:
                resp = openai.ChatCompletion.create(
                    model=model,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                )
                raw = resp["choices"][0]["message"]["content"]
                raw = strip_json_code_fences(raw)
                return json.loads(raw)
            except Exception:
                return {}
        except Exception:
            return {}


def remove_raw_text_fields(obj: Any) -> Any:
    """Remove raw_text fields from object."""
    if isinstance(obj, dict):
        return {k: remove_raw_text_fields(v) for k, v in obj.items() if k != "raw_text"}
    if isinstance(obj, list):
        return [remove_raw_text_fields(x) for x in obj]
    return obj


def classify_via_image(model: str, image_url: str) -> str:
    """Classify document type via image analysis."""
    system = (
        "You are a cautious document-type classifier for business and identity documents. "
        f"Choose exactly one label from {ROUTE_LABELS}. "
        "Return a single JSON object exactly in the form {\"doc_type\":\"<label>\"}. "
        "Rules: If not highly confident, return 'unknown'."
    )
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": [
                    {"type": "text", "text": "Classify this document. Return {\"doc_type\":\"<label>\"}."},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ]},
            ],
        )
        content = resp.choices[0].message.content
        out = json.loads(content)
        label = out.get("doc_type", "unknown")
        return label if label in ROUTE_LABELS else "unknown"
    except Exception:
        return "unknown"


def extract_via_image(model: str, doc_type: str, image_url: str, prompts_by_type: Dict[str, str]) -> Dict[str, Any]:
    """Extract data from document image."""
    system = (
        prompts_by_type.get(doc_type, prompts_by_type["unknown"]) +
        "\nReturn ONE JSON object only."
    )
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": [
                    {"type": "text", "text": "Extract all structured data from this document image per the rules."},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ]},
            ],
        )
        content = resp.choices[0].message.content
        try:
            return remove_raw_text_fields(json.loads(content))
        except Exception:
            return {}
    except Exception:
        return {}
