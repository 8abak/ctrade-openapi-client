from __future__ import annotations

import json
from typing import Any, Dict, Tuple

import requests

from datavis.research.config import ResearchSettings
from datavis.research.guardrails import (
    APPROVED_CANDIDATE_FAMILIES,
    APPROVED_DEDUP_RULES,
    APPROVED_FEATURES,
    APPROVED_LABEL_VARIANTS,
    APPROVED_SESSION_BUCKETS,
    APPROVED_SPREAD_FILTERS,
    APPROVED_STOP_REASONS,
    APPROVED_THRESHOLD_PROFILES,
    APPROVED_TRAIN_VALIDATION_PLANS,
)


DECISION_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "decision": {"type": "string", "enum": ["continue", "refine", "stop"]},
        "reason": {"type": "string"},
        "confidence_note": {"type": "string"},
        "stop_reason": {"type": ["string", "null"], "enum": [*sorted(APPROVED_STOP_REASONS), None]},
        "verdict": {"type": ["string", "null"]},
        "next_action": {
            "type": ["object", "null"],
            "additionalProperties": False,
            "properties": {
                "type": {"type": "string", "enum": ["run_entry_research", "stop"]},
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
                    },
                },
            },
            "required": ["type", "parameters"],
        },
    },
    "required": ["decision", "reason", "confidence_note", "stop_reason", "verdict", "next_action"],
}


class OpenAISupervisorClient:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings

    def is_enabled(self) -> bool:
        return bool(self._settings.supervisor_api_key and self._settings.supervisor_model)

    def review(self, briefing: Dict[str, Any]) -> Tuple[Dict[str, Any], str]:
        payload_text = self._truncate_briefing(briefing)
        system_prompt = self._system_prompt()
        raw_response = self._invoke_openai(system_prompt=system_prompt, user_prompt=payload_text)
        decision_text = self._extract_text(raw_response)
        return json.loads(decision_text), decision_text

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
            "You are a calm research supervisor for entry-only market research. "
            "Return JSON only. Do not request raw ticks, code changes, broad searches, live trading changes, "
            "hold/close logic, or money management. Choose only from the allowed fields and enums. "
            "Prefer refine over continue when narrowing scope. Use stop when the evidence is already sufficient or weak."
        )

    def _invoke_openai(self, *, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._settings.supervisor_api_key}",
            "Content-Type": "application/json",
        }
        if self._settings.supervisor_api_style == "chat_completions":
            payload = {
                "model": self._settings.supervisor_model,
                "temperature": self._settings.supervisor_temperature,
                "max_tokens": self._settings.supervisor_max_output_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Review this compact JSON research briefing and answer with JSON only:\n{user_prompt}"},
                ],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": "research_supervisor_decision",
                        "strict": True,
                        "schema": DECISION_JSON_SCHEMA,
                    },
                },
            }
        else:
            payload = {
                "model": self._settings.supervisor_model,
                "max_output_tokens": self._settings.supervisor_max_output_tokens,
                "temperature": self._settings.supervisor_temperature,
                "input": [
                    {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
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
            self._settings.supervisor_endpoint,
            headers=headers,
            json=payload,
            timeout=self._settings.supervisor_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

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
