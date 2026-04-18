from __future__ import annotations

import json
import re
from typing import Any, Dict, Tuple
from urllib.parse import urlparse, urlunparse

import requests

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import (
    APPROVED_CANDIDATE_FAMILIES,
    APPROVED_DEDUP_RULES,
    APPROVED_FEATURES,
    APPROVED_LABEL_VARIANTS,
    APPROVED_SESSION_BUCKETS,
    APPROVED_SIDE_LOCKS,
    APPROVED_SPREAD_FILTERS,
    APPROVED_STOP_REASONS,
    APPROVED_THRESHOLD_PROFILES,
    APPROVED_TRAIN_VALIDATION_PLANS,
    coerce_supervisor_decision_payload,
)


DECISION_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "decision": {"type": "string", "enum": ["continue", "refine", "explore_new_family", "increase_slice", "split_by_pattern", "stop"]},
        "reason": {"type": "string"},
        "confidence_note": {"type": "string"},
        "stop_reason": {"type": ["string", "null"], "enum": [*sorted(APPROVED_STOP_REASONS), None]},
        "verdict": {"type": ["string", "null"]},
        "next_actions": {
            "type": "array",
            "maxItems": 8,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "action": {"type": "string", "enum": ["continue", "refine", "explore_new_family", "increase_slice", "split_by_pattern"]},
                    "reason": {"type": "string"},
                    "parameters": {
                        "type": ["object", "null"],
                        "additionalProperties": False,
                        "properties": {
                            "label_variant": {"type": "string", "enum": sorted(APPROVED_LABEL_VARIANTS.keys())},
                            "candidate_family": {"type": "string", "enum": list(APPROVED_CANDIDATE_FAMILIES)},
                            "threshold_profile": {"type": "string", "enum": sorted(APPROVED_THRESHOLD_PROFILES.keys())},
                            "feature_toggles": {
                                "type": "array",
                                "items": {"type": "string", "enum": list(APPROVED_FEATURES)},
                                "maxItems": 8,
                                "uniqueItems": True,
                            },
                            "session_filter": {
                                "type": "array",
                                "items": {"type": "string", "enum": list(APPROVED_SESSION_BUCKETS)},
                                "maxItems": 6,
                                "uniqueItems": True,
                            },
                            "spread_filter": {"type": "string", "enum": sorted(APPROVED_SPREAD_FILTERS.keys())},
                            "dedup_rule": {"type": "string", "enum": sorted(APPROVED_DEDUP_RULES.keys())},
                            "train_validation_plan": {"type": "string", "enum": list(APPROVED_TRAIN_VALIDATION_PLANS)},
                            "slice_rows": {"type": "integer", "minimum": 500, "maximum": 50000},
                            "slice_offset_rows": {"type": "integer", "minimum": 0, "maximum": 250000},
                            "warmup_rows": {"type": "integer", "minimum": 20, "maximum": 5000},
                            "side_lock": {"type": "string", "enum": list(APPROVED_SIDE_LOCKS)},
                        },
                    },
                },
                "required": ["action", "reason", "parameters"],
            },
        },
    },
    "required": ["decision", "reason", "confidence_note", "stop_reason", "verdict", "next_actions"],
}


class OpenAISupervisorClient:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings

    def is_enabled(self, *, model_override: str = "") -> bool:
        return bool(self._settings.supervisor_api_key and (model_override or self._settings.supervisor_model))

    def review(self, briefing: Dict[str, Any], *, model_override: str = "") -> Tuple[Dict[str, Any], str]:
        payload_text = self._truncate_briefing(briefing)
        system_prompt = self._system_prompt()
        raw_response = self._invoke_openai(
            system_prompt=system_prompt,
            user_prompt=payload_text,
            model_name=model_override or self._settings.supervisor_model,
        )
        decision_text = self._extract_text(raw_response)
        parsed = self._load_json_text(decision_text)
        return coerce_supervisor_decision_payload(parsed), decision_text

    def _truncate_briefing(self, briefing: Dict[str, Any]) -> str:
        text = json.dumps(briefing, sort_keys=True, default=str)
        if len(text) <= self._settings.supervisor_max_briefing_chars:
            return text
        trimmed = dict(briefing)
        trimmed["trimmed"] = True
        trimmed["trimmed_notice"] = f"Briefing exceeded {self._settings.supervisor_max_briefing_chars} chars and was truncated."
        text = json.dumps(trimmed, sort_keys=True, default=str)
        return text[: self._settings.supervisor_max_briefing_chars]

    def _system_prompt(self) -> str:
        return (
            "You are a calm research director for entry-only market research. Return JSON only. "
            "A weak run is not a stopping condition by itself. "
            "Use stop only when a strong stable regime exists, several distinct search directions have failed, the budget is exhausted, "
            "an explicit stop was requested, or a hard technical guardrail applies. "
            "Otherwise choose a bounded next strategy and populate next_actions from the supplied mutation proposals or a tighter variant of them. "
            "Never request live trading, hold/close logic, money management, unbounded scans, heavy ML, or broad brute force."
        )

    def _invoke_openai(self, *, system_prompt: str, user_prompt: str, model_name: str) -> Dict[str, Any]:
        endpoint = self._normalize_endpoint(self._settings.supervisor_endpoint, self._settings.supervisor_api_style)
        headers = {
            "Authorization": f"Bearer {self._settings.supervisor_api_key}",
            "Content-Type": "application/json",
        }
        if self._settings.supervisor_api_style == "chat_completions":
            payload = {
                "model": model_name,
                "temperature": self._settings.supervisor_temperature,
                "max_completion_tokens": self._settings.supervisor_max_output_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Review this compact JSON research briefing and answer with JSON only:\n{user_prompt}"},
                ],
                "response_format": {"type": "json_object"},
            }
        else:
            payload = {
                "model": model_name,
                "max_output_tokens": self._settings.supervisor_max_output_tokens,
                "temperature": self._settings.supervisor_temperature,
                "input": [
                    {"role": "developer", "content": [{"type": "input_text", "text": system_prompt}]},
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": f"Review this compact JSON research briefing and answer with JSON only:\n{user_prompt}"}],
                    },
                ],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "research_supervisor_decision",
                        "strict": True,
                        "schema": DECISION_JSON_SCHEMA,
                    }
                },
            }
        response = requests.post(
            endpoint,
            headers=headers,
            json=payload,
            timeout=self._settings.supervisor_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _normalize_endpoint(endpoint: str, api_style: str) -> str:
        raw = (endpoint or "").strip() or "https://api.openai.com/v1/responses"
        parsed = urlparse(raw)
        path = parsed.path.rstrip("/")
        expected = "/v1/chat/completions" if api_style == "chat_completions" else "/v1/responses"
        if path in {"", "/v1", "/v1/responses", "/v1/chat/completions"}:
            path = expected
        else:
            path = expected if path == "" else path
        return urlunparse((parsed.scheme or "https", parsed.netloc or "api.openai.com", path, "", "", ""))

    def _extract_text(self, payload: Dict[str, Any]) -> str:
        if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
            return payload["output_text"].strip()
        choices = payload.get("choices") or []
        if choices:
            message = (choices[0] or {}).get("message") or {}
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                return content.strip()
        output = payload.get("output") or []
        for item in output:
            for content in item.get("content") or []:
                text = content.get("text")
                if isinstance(text, str) and text.strip():
                    return text.strip()
        raise RuntimeError("OpenAI response did not contain extractable text.")

    @staticmethod
    def _load_json_text(text: str) -> Dict[str, Any]:
        candidate = (text or "").strip()
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate, flags=re.IGNORECASE | re.DOTALL).strip()
        try:
            return dict(json.loads(candidate))
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", candidate, flags=re.DOTALL)
            if match:
                return dict(json.loads(match.group(0)))
            raise
