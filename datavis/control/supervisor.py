from __future__ import annotations

import json
import re
from typing import Any, Dict, Tuple
from urllib.parse import urlparse, urlunparse

import requests

from datavis.control.config import ControlSettings
from datavis.control.journal import EngineeringJournal
from datavis.control.models import EngineeringSupervisorDecision


DECISION_JSON_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "decision": {
            "type": "string",
            "enum": [
                "observe_only",
                "repair_config",
                "repair_permissions",
                "patch_known_issue",
                "restart_services",
                "reset_research_state",
                "requeue_job",
                "rollback_last_patch",
                "escalate_manual_review",
            ],
        },
        "reason": {"type": "string"},
        "confidence_note": {"type": "string"},
        "patch_template": {
            "type": ["string", "null"],
            "enum": [
                "fix_openai_endpoint_normalization",
                "fix_openai_request_payload",
                "fix_decision_defaults",
                "fix_feature_toggle_clamp",
                "fix_permission_safe_journal_write",
                None,
            ],
        },
        "config_changes": {"type": "object"},
        "restart_services": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
        "requeue_job_id": {"type": ["integer", "null"]},
        "reset_mode": {"type": ["string", "null"], "enum": ["soft", "hard", None]},
        "smoke_tests": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": [
                    "import_modules",
                    "control_api_boot",
                    "engineering_supervisor_schema",
                    "patch_roundtrip",
                    "db_select_1",
                    "research_status_query",
                    "service_manager_probe",
                ],
            },
            "maxItems": 8,
        },
        "pause_research": {"type": "boolean"},
    },
    "required": [
        "decision",
        "reason",
        "confidence_note",
        "patch_template",
        "config_changes",
        "restart_services",
        "requeue_job_id",
        "reset_mode",
        "smoke_tests",
        "pause_research",
    ],
}


class EngineeringSupervisor:
    def __init__(self, settings: ControlSettings) -> None:
        self._settings = settings
        self._journal = EngineeringJournal(settings, "engineering-supervisor")

    def is_enabled(self) -> bool:
        return bool(self._settings.openai_api_key and self._settings.openai_model)

    def review_incident(self, briefing: Dict[str, Any], *, force_fallback: bool = False) -> Tuple[EngineeringSupervisorDecision, str]:
        if force_fallback or not self.is_enabled():
            decision = self._heuristic_decision(briefing)
            self._journal.write(
                level="INFO",
                event_type="engineering-supervisor.decision",
                message=f"selected {decision.decision} using heuristic fallback",
                payload={"decision": decision.model_dump(), "incident": briefing.get("incident") or {}},
            )
            return decision, json.dumps(decision.model_dump(), separators=(",", ":"), sort_keys=True)
        raw_response = self._invoke_openai(system_prompt=self._system_prompt(), user_prompt=self._truncate_briefing(briefing))
        decision_text = self._extract_text(raw_response)
        parsed = self._load_json_text(decision_text)
        decision = EngineeringSupervisorDecision.model_validate(parsed)
        self._journal.write(
            level="INFO",
            event_type="engineering-supervisor.decision",
            message=f"selected {decision.decision} using OpenAI supervisor",
            payload={"decision": decision.model_dump(), "incident": briefing.get("incident") or {}},
        )
        return decision, decision_text

    def _heuristic_decision(self, briefing: Dict[str, Any]) -> EngineeringSupervisorDecision:
        incident = dict(briefing.get("incident") or {})
        incident_type = str(incident.get("type") or "code_bug")
        details = dict(incident.get("details") or {})
        affected_services = list((briefing.get("serviceStatus") or []) and [item.get("name") for item in briefing.get("serviceStatus") or [] if item.get("name")])
        suggested_patch = details.get("suggestedPatchTemplate")
        suggested_config = details.get("suggestedConfigChanges") or {}
        default_smokes = ["import_modules", "engineering_supervisor_schema", "control_api_boot", "patch_roundtrip"]
        if incident_type == "openai_api_request_issue":
            if suggested_config:
                return EngineeringSupervisorDecision(
                    decision="repair_config",
                    reason="Repair the OpenAI endpoint/style configuration to the bounded known-good path.",
                    confidence_note="Heuristic control decision for known endpoint/request-shape failures.",
                    config_changes=suggested_config,
                    restart_services=[self._settings.research_settings.supervisor_name],
                    smoke_tests=[*default_smokes, "db_select_1"],
                )
            return EngineeringSupervisorDecision(
                decision="patch_known_issue",
                reason="Apply the bounded OpenAI request payload patch for known parameter-shape failures.",
                confidence_note="Heuristic control decision for known request payload regressions.",
                patch_template="fix_openai_request_payload",
                restart_services=[self._settings.research_settings.supervisor_name],
                smoke_tests=[*default_smokes, "db_select_1"],
            )
        if incident_type == "permission_path_issue":
            return EngineeringSupervisorDecision(
                decision="repair_permissions",
                reason="Repair runtime directory permissions and verify journal writes before resuming the loop.",
                confidence_note="Heuristic control decision for permission and path failures.",
                restart_services=affected_services or [self._settings.research_settings.worker_name, self._settings.research_settings.supervisor_name],
                smoke_tests=[*default_smokes, "research_status_query"],
            )
        if incident_type == "service_runtime_issue":
            return EngineeringSupervisorDecision(
                decision="restart_services",
                reason="Reset failed units and restart the affected bounded service set.",
                confidence_note="Heuristic control decision for service/runtime failures.",
                restart_services=affected_services or list(self._settings.research_services),
                smoke_tests=[*default_smokes, "service_manager_probe", "research_status_query"],
            )
        if incident_type == "research_parameter_issue":
            return EngineeringSupervisorDecision(
                decision="patch_known_issue",
                reason="Apply the bounded feature-toggle clamp patch to keep the research loop within guardrails.",
                confidence_note="Heuristic control decision for research parameter overflow.",
                patch_template=suggested_patch or "fix_feature_toggle_clamp",
                requeue_job_id=details.get("jobId"),
                restart_services=[self._settings.research_settings.worker_name, self._settings.research_settings.orchestrator_name],
                smoke_tests=[*default_smokes, "research_status_query"],
            )
        if incident_type == "config_env_issue" and "api key" in str(details.get("errorText") or "").lower():
            return EngineeringSupervisorDecision(
                decision="escalate_manual_review",
                reason="Missing secrets cannot be fabricated safely by the control plane.",
                confidence_note="Manual review required because the failure depends on unavailable credentials.",
                smoke_tests=["import_modules"],
            )
        if incident_type == "stalled_loop":
            return EngineeringSupervisorDecision(
                decision="restart_services",
                reason="A stuck worker/run should be recovered by restarting the bounded research service set.",
                confidence_note="Heuristic control decision for stuck jobs or decisions.",
                restart_services=affected_services or list(self._settings.research_services),
                smoke_tests=[*default_smokes, "service_manager_probe", "research_status_query"],
            )
        if incident_type == "missing_artifact":
            return EngineeringSupervisorDecision(
                decision="restart_services",
                reason="Missing artifacts after a completed run suggests worker persistence drift; restart and re-verify.",
                confidence_note="Heuristic control decision for missing artifact persistence issues.",
                restart_services=[self._settings.research_settings.worker_name],
                smoke_tests=[*default_smokes, "research_status_query"],
            )
        return EngineeringSupervisorDecision(
            decision="patch_known_issue" if suggested_patch else "observe_only",
            reason="Use the smallest bounded intervention for an otherwise generic code-path failure.",
            confidence_note="Heuristic fallback because no stronger repair classification was available.",
            patch_template=suggested_patch,
            restart_services=affected_services[:1],
            smoke_tests=default_smokes,
        )

    def _truncate_briefing(self, briefing: Dict[str, Any]) -> str:
        text = json.dumps(briefing, sort_keys=True, default=str)
        if len(text) <= self._settings.supervisor_max_briefing_chars:
            return text
        return text[: self._settings.supervisor_max_briefing_chars]

    def _system_prompt(self) -> str:
        return (
            "You are a bounded engineering supervisor for a private research control plane. "
            "Return JSON only. Choose one safe action from the allowed enum. "
            "Prefer config repair, permission repair, small known patches, service restart, bounded reset, or explicit escalation. "
            "Never invent broad rewrites, arbitrary shell commands, trading logic changes, or public-facing changes. "
            "All repairs must remain within datavis/research, datavis/control, or the configured research env files."
        )

    def _invoke_openai(self, *, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        endpoint = self._normalize_endpoint(self._settings.openai_endpoint, self._settings.openai_api_style)
        headers = {
            "Authorization": f"Bearer {self._settings.openai_api_key}",
            "Content-Type": "application/json",
        }
        if self._settings.openai_api_style == "chat_completions":
            payload = {
                "model": self._settings.openai_model,
                "temperature": self._settings.openai_temperature,
                "max_completion_tokens": self._settings.openai_max_output_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Review this engineering incident briefing and answer with JSON only:\n{user_prompt}"},
                ],
                "response_format": {"type": "json_object"},
            }
        else:
            payload = {
                "model": self._settings.openai_model,
                "max_output_tokens": self._settings.openai_max_output_tokens,
                "temperature": self._settings.openai_temperature,
                "input": [
                    {"role": "developer", "content": [{"type": "input_text", "text": system_prompt}]},
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": f"Review this engineering incident briefing and answer with JSON only:\n{user_prompt}"}],
                    },
                ],
                "text": {
                    "format": {
                        "type": "json_schema",
                        "name": "engineering_supervisor_decision",
                        "strict": True,
                        "schema": DECISION_JSON_SCHEMA,
                    }
                },
            }
        response = requests.post(endpoint, headers=headers, json=payload, timeout=self._settings.openai_timeout_seconds)
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
