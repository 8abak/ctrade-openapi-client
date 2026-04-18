from __future__ import annotations

import difflib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

from datavis.control.config import BASE_DIR, ControlSettings, ensure_runtime_dirs
from datavis.control.panel_state import resolve_engineering_runtime
from datavis.control.journal import EngineeringJournal
from datavis.control.models import EngineeringSupervisorDecision
from datavis.control.research_manager import ResearchManager
from datavis.control.service_manager import ServiceManager
from datavis.control.store import EngineeringStore


class RepairExecutor:
    def __init__(
        self,
        settings: ControlSettings,
        *,
        store: EngineeringStore,
        research_manager: ResearchManager,
        service_manager: ServiceManager,
    ) -> None:
        self._settings = settings
        self._store = store
        self._research_manager = research_manager
        self._service_manager = service_manager
        self._journal = EngineeringJournal(settings, "repair-executor")

    def execute(
        self,
        conn: Any,
        *,
        incident: Dict[str, Any],
        decision: EngineeringSupervisorDecision,
        action_id: int,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {"decision": decision.model_dump(), "incidentId": int(incident["id"])}
        if decision.pause_research:
            self._research_manager.pause(conn, reason=decision.reason)
        if decision.decision == "repair_config":
            result["patch"] = self._apply_config_changes(
                conn,
                incident_id=int(incident["id"]),
                action_id=action_id,
                config_changes=decision.config_changes,
            )
        elif decision.decision == "repair_permissions":
            result["permissions"] = self._repair_permissions()
        elif decision.decision == "patch_known_issue":
            result["patch"] = self._apply_patch_template(
                conn,
                incident_id=int(incident["id"]),
                action_id=action_id,
                template_name=str(decision.patch_template),
            )
        elif decision.decision == "restart_services":
            result["services"] = self._restart_services(conn, decision.restart_services)
        elif decision.decision == "reset_research_state":
            result["reset"] = self._research_manager.reset(conn, mode=str(decision.reset_mode), reason=decision.reason)
        elif decision.decision == "requeue_job":
            result["requeue"] = self._research_manager.requeue(conn, job_id=decision.requeue_job_id, reason=decision.reason)
        elif decision.decision == "rollback_last_patch":
            result["rollback"] = self.rollback_last_patch(conn, incident_id=int(incident["id"]))
        elif decision.decision == "escalate_manual_review":
            result["escalated"] = True
        else:
            result["observed"] = True
        if decision.restart_services and decision.decision not in {"restart_services", "escalate_manual_review"}:
            result["services"] = self._restart_services(conn, decision.restart_services)
        if decision.requeue_job_id and decision.decision not in {"requeue_job", "escalate_manual_review"}:
            result["requeue"] = self._research_manager.requeue(conn, job_id=decision.requeue_job_id, reason=decision.reason)
        return result

    def rollback_last_patch(self, conn: Any, *, incident_id: int) -> Dict[str, Any]:
        patch_row = self._store.get_latest_patch(conn, incident_id=incident_id)
        if not patch_row:
            return {"rolledBack": False, "reason": "no patch history available"}
        metadata = dict(patch_row.get("metadata") or {})
        file_entries = list(metadata.get("files") or [])
        if not file_entries:
            return {"rolledBack": False, "reason": "patch metadata missing file backups"}
        rollback_dir = self._artifact_dir(incident_id, int(patch_row.get("action_id") or 0)) / "rollback"
        rollback_dir.mkdir(parents=True, exist_ok=True)
        restored = []
        for entry in file_entries:
            target_path = Path(str(entry["targetPath"]))
            backup_path = Path(str(entry["backupPath"]))
            if not backup_path.exists():
                raise FileNotFoundError(f"missing backup for rollback: {backup_path}")
            previous = backup_path.read_text(encoding="utf-8")
            current = target_path.read_text(encoding="utf-8") if target_path.exists() else ""
            rollback_diff = "".join(
                difflib.unified_diff(
                    current.splitlines(keepends=True),
                    previous.splitlines(keepends=True),
                    fromfile=str(target_path),
                    tofile=str(target_path),
                )
            )
            diff_path = rollback_dir / f"{target_path.name}.rollback.diff"
            diff_path.write_text(rollback_diff, encoding="utf-8")
            target_path.write_text(previous, encoding="utf-8")
            restored.append({"targetPath": str(target_path), "rollbackDiffPath": str(diff_path)})
        self._store.mark_patch_rolled_back(conn, patch_id=int(patch_row["id"]), rollback_path=str(rollback_dir), metadata={"rollbackFiles": restored})
        return {"rolledBack": True, "patchId": int(patch_row["id"]), "files": restored}

    def simulate_known_repair_path(self) -> Dict[str, Any]:
        temp_dir = Path(tempfile.mkdtemp(prefix="datavis-control-patch-"))
        sample = temp_dir / "guardrails.py"
        sample.write_text("def _validate_feature_list(values, *, max_count):\n    normalized = list(values)\n    if len(normalized) > max_count:\n        raise ValueError('too many feature toggles')\n    return normalized\n", encoding="utf-8")
        new_text = self._patch_feature_toggle_clamp(sample.read_text(encoding="utf-8"))
        sample.write_text(new_text, encoding="utf-8")
        return {
            "ok": "normalized = normalized[:max_count]" in new_text,
            "path": str(sample),
            "patchedText": new_text,
        }

    def _repair_permissions(self) -> Dict[str, Any]:
        ensure_runtime_dirs(self._settings)
        from datavis.research.config import ensure_runtime_dirs as ensure_research_runtime_dirs

        ensure_research_runtime_dirs(self._settings.research_settings)
        repaired_paths: List[str] = []
        for path in (
            self._settings.runtime_dir,
            self._settings.journal_dir,
            self._settings.artifact_dir,
            self._settings.log_dir,
            self._settings.research_settings.runtime_dir,
            self._settings.research_settings.journal_dir,
            self._settings.research_settings.artifact_dir,
            self._settings.research_settings.log_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(path, 0o775)
            except OSError:
                pass
            repaired_paths.append(str(path))
        for journal_file in (
            self._settings.journal_dir / "repair-executor.jsonl",
            self._settings.research_settings.journal_dir / "worker.jsonl",
        ):
            try:
                journal_file.parent.mkdir(parents=True, exist_ok=True)
                journal_file.touch(exist_ok=True)
                os.chmod(journal_file, 0o664)
            except OSError:
                pass
            repaired_paths.append(str(journal_file))
        return {"paths": repaired_paths}

    def _restart_services(self, conn: Any, service_names: List[str]) -> List[Dict[str, Any]]:
        runtime_policy = resolve_engineering_runtime(conn, self._settings, self._settings.research_settings)
        if self._store.restart_actions_last_hour(conn) >= int(runtime_policy["restartRateLimitPerHour"]):
            raise RuntimeError("restart budget exceeded for the last hour")
        snapshots = []
        for service_name in service_names:
            self._service_manager.reset_failed(service_name)
            snapshots.append(self._service_manager.restart(service_name).model_dump())
        return snapshots

    def _apply_config_changes(
        self,
        conn: Any,
        *,
        incident_id: int,
        action_id: int,
        config_changes: Dict[str, Any],
    ) -> Dict[str, Any]:
        grouped: Dict[Path, Dict[str, Any]] = {}
        for key, value in sorted(config_changes.items()):
            target = self._target_env_path_for_key(key)
            grouped.setdefault(target, {})[key] = value
        mutations: List[Tuple[Path, str]] = []
        for path, changes in grouped.items():
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            mutations.append((path, self._update_env_text(current, changes)))
        return self._apply_text_mutations(
            conn,
            incident_id=incident_id,
            action_id=action_id,
            patch_type="repair_config",
            mutations=mutations,
        )

    def _apply_patch_template(self, conn: Any, *, incident_id: int, action_id: int, template_name: str) -> Dict[str, Any]:
        templates = {
            "fix_openai_endpoint_normalization": self._template_openai_endpoint_normalization,
            "fix_openai_request_payload": self._template_openai_request_payload,
            "fix_decision_defaults": self._template_decision_defaults,
            "fix_feature_toggle_clamp": self._template_feature_toggle_clamp,
            "fix_permission_safe_journal_write": self._template_permission_safe_journal_write,
        }
        if template_name not in templates:
            raise ValueError(f"unsupported patch template: {template_name}")
        return templates[template_name](conn, incident_id=incident_id, action_id=action_id)

    def _template_openai_endpoint_normalization(self, conn: Any, *, incident_id: int, action_id: int) -> Dict[str, Any]:
        path = BASE_DIR / "datavis" / "research" / "supervisor_client.py"
        current = path.read_text(encoding="utf-8")
        replacement = """
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
"""
        mutated = self._replace_method(current, "_normalize_endpoint", replacement.strip("\n"))
        return self._apply_text_mutations(conn, incident_id=incident_id, action_id=action_id, patch_type="fix_openai_endpoint_normalization", mutations=[(path, mutated)])

    def _template_openai_request_payload(self, conn: Any, *, incident_id: int, action_id: int) -> Dict[str, Any]:
        path = BASE_DIR / "datavis" / "research" / "supervisor_client.py"
        current = path.read_text(encoding="utf-8")
        replacement = """
    def _invoke_openai(self, *, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        endpoint = self._normalize_endpoint(self._settings.supervisor_endpoint, self._settings.supervisor_api_style)
        headers = {
            "Authorization": f"Bearer {self._settings.supervisor_api_key}",
            "Content-Type": "application/json",
        }
        if self._settings.supervisor_api_style == "chat_completions":
            payload = {
                "model": self._settings.supervisor_model,
                "temperature": self._settings.supervisor_temperature,
                "max_completion_tokens": self._settings.supervisor_max_output_tokens,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Review this compact JSON research briefing and answer with JSON only:\\n{user_prompt}"},
                ],
                "response_format": {"type": "json_object"},
            }
        else:
            payload = {
                "model": self._settings.supervisor_model,
                "max_output_tokens": self._settings.supervisor_max_output_tokens,
                "temperature": self._settings.supervisor_temperature,
                "input": [
                    {"role": "developer", "content": [{"type": "input_text", "text": system_prompt}]},
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": f"Review this compact JSON research briefing and answer with JSON only:\\n{user_prompt}"}],
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
"""
        mutated = self._replace_method(current, "_invoke_openai", replacement.strip("\n"))
        return self._apply_text_mutations(conn, incident_id=incident_id, action_id=action_id, patch_type="fix_openai_request_payload", mutations=[(path, mutated)])

    def _template_decision_defaults(self, conn: Any, *, incident_id: int, action_id: int) -> Dict[str, Any]:
        path = BASE_DIR / "datavis" / "research" / "guardrails.py"
        current = path.read_text(encoding="utf-8")
        replacement = """
def coerce_supervisor_decision_payload(decision_payload: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(decision_payload or {})
    legacy_next = payload.pop("next_action", None)
    if not payload.get("decision"):
        payload["decision"] = "refine" if legacy_next or payload.get("next_actions") else "stop" if payload.get("stop_reason") else "continue"
    payload["reason"] = str(payload.get("reason") or "Supervisor response omitted a reason; control plane backfilled a bounded default.")
    payload["confidence_note"] = str(payload.get("confidence_note") or payload["reason"])
    payload["verdict"] = str(payload.get("verdict") or payload.get("decision") or "continue")
    next_actions = payload.get("next_actions")
    if next_actions is None and legacy_next is not None:
        action_name = str(payload.get("decision") or "refine")
        if action_name == "stop":
            action_name = "refine"
        next_actions = [
            {
                "action": action_name,
                "reason": str(payload["reason"]),
                "parameters": (legacy_next or {}).get("parameters"),
            }
        ]
    payload["next_actions"] = list(next_actions or [])
    if payload["decision"] == "stop":
        payload["next_actions"] = []
    return payload
"""
        mutated = self._replace_function(current, "coerce_supervisor_decision_payload", replacement.strip("\n"))
        return self._apply_text_mutations(conn, incident_id=incident_id, action_id=action_id, patch_type="fix_decision_defaults", mutations=[(path, mutated)])

    def _template_feature_toggle_clamp(self, conn: Any, *, incident_id: int, action_id: int) -> Dict[str, Any]:
        path = BASE_DIR / "datavis" / "research" / "guardrails.py"
        current = path.read_text(encoding="utf-8")
        mutated = self._patch_feature_toggle_clamp(current)
        return self._apply_text_mutations(conn, incident_id=incident_id, action_id=action_id, patch_type="fix_feature_toggle_clamp", mutations=[(path, mutated)])

    def _template_permission_safe_journal_write(self, conn: Any, *, incident_id: int, action_id: int) -> Dict[str, Any]:
        path = BASE_DIR / "datavis" / "research" / "journal.py"
        current = path.read_text(encoding="utf-8")
        replacement = """
    def _append_file_record(self, record: Dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), sort_keys=True)
        fallback = self._settings.runtime_dir / "_journal_fallback" / f"{self._component}.jsonl"
        for path in (self.component_path, fallback):
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(line)
                    handle.write("\\n")
                return
            except OSError:
                continue
"""
        mutated = self._replace_method(current, "_append_file_record", replacement.strip("\n"))
        return self._apply_text_mutations(conn, incident_id=incident_id, action_id=action_id, patch_type="fix_permission_safe_journal_write", mutations=[(path, mutated)])

    def _apply_text_mutations(
        self,
        conn: Any,
        *,
        incident_id: int,
        action_id: int,
        patch_type: str,
        mutations: List[Tuple[Path, str]],
    ) -> Dict[str, Any]:
        runtime_policy = resolve_engineering_runtime(conn, self._settings, self._settings.research_settings)
        changed = []
        total_lines = 0
        total_bytes = 0
        artifact_dir = self._artifact_dir(incident_id, action_id)
        artifact_dir.mkdir(parents=True, exist_ok=True)
        for path, new_text in mutations:
            if not self._settings.is_allowed_patch_path(path):
                raise ValueError(f"patch target is outside the allowlist: {path}")
            old_text = path.read_text(encoding="utf-8") if path.exists() else ""
            if old_text == new_text:
                continue
            diff = "".join(
                difflib.unified_diff(
                    old_text.splitlines(keepends=True),
                    new_text.splitlines(keepends=True),
                    fromfile=str(path),
                    tofile=str(path),
                )
            )
            line_changes = sum(
                1 for line in diff.splitlines()
                if line and line[0] in {"+", "-"} and not line.startswith("+++") and not line.startswith("---")
            )
            byte_changes = abs(len(new_text.encode("utf-8")) - len(old_text.encode("utf-8")))
            total_lines += line_changes
            total_bytes += byte_changes
            changed.append((path, old_text, new_text, diff, line_changes, byte_changes))
        if len(changed) > int(runtime_policy["maxPatchFiles"]):
            raise RuntimeError("patch exceeds allowed file count")
        if total_lines > int(runtime_policy["maxPatchLineChanges"]):
            raise RuntimeError("patch exceeds allowed line-change budget")
        if total_bytes > int(runtime_policy["maxPatchBytes"]):
            raise RuntimeError("patch exceeds allowed byte-change budget")
        if not changed:
            return {"applied": False, "reason": "already in known-good state"}
        file_metadata = []
        for index, (path, old_text, new_text, diff, line_changes, byte_changes) in enumerate(changed, start=1):
            safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(path.relative_to(BASE_DIR)))
            backup_path = artifact_dir / f"{index:02d}_{safe_name}.bak"
            diff_path = artifact_dir / f"{index:02d}_{safe_name}.diff"
            backup_path.write_text(old_text, encoding="utf-8")
            diff_path.write_text(diff, encoding="utf-8")
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(new_text, encoding="utf-8")
            file_metadata.append(
                {
                    "targetPath": str(path),
                    "backupPath": str(backup_path),
                    "diffPath": str(diff_path),
                    "linesChanged": line_changes,
                    "bytesChanged": byte_changes,
                }
            )
        patch_id = self._store.create_patch(
            conn,
            incident_id=incident_id,
            action_id=action_id,
            patch_type=patch_type,
            target_files=[item["targetPath"] for item in file_metadata],
            diff_path=str(artifact_dir),
            backup_path=str(artifact_dir),
            lines_changed=total_lines,
            bytes_changed=total_bytes,
            metadata={"files": file_metadata},
        )
        self._journal.write(
            level="INFO",
            event_type="repair.patch.applied",
            message=f"applied bounded patch {patch_type}",
            incident_id=incident_id,
            action_id=action_id,
            patch_id=patch_id,
            payload={"files": file_metadata},
            conn=conn,
        )
        return {"applied": True, "patchId": patch_id, "files": file_metadata}

    def _target_env_path_for_key(self, key: str) -> Path:
        if key.startswith("DATAVIS_CONTROL_"):
            return self._settings.control_env_file
        if key.startswith("DATAVIS_RESEARCH_"):
            return self._settings.research_env_file
        raise ValueError(f"unsupported config key for automatic repair: {key}")

    def _update_env_text(self, current_text: str, changes: Dict[str, Any]) -> str:
        lines = current_text.splitlines()
        updated = []
        seen = set()
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in line:
                updated.append(line)
                continue
            key, _ = line.split("=", 1)
            key = key.strip()
            if key in changes:
                updated.append(f"{key}={self._serialize_env_value(changes[key])}")
                seen.add(key)
            else:
                updated.append(line)
        for key, value in changes.items():
            if key in seen:
                continue
            updated.append(f"{key}={self._serialize_env_value(value)}")
        return "\n".join(updated).rstrip() + "\n"

    @staticmethod
    def _serialize_env_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value).strip().strip("'").strip('"')

    @staticmethod
    def _replace_method(text: str, method_name: str, replacement: str) -> str:
        pattern = re.compile(rf"(?ms)^(?:    @.*\n)*    def {re.escape(method_name)}\(.*?(?=^(?:    @|    def )|\Z)")
        if not pattern.search(text):
            raise RuntimeError(f"method not found for replacement: {method_name}")
        return pattern.sub(replacement + "\n\n", text, count=1)

    @staticmethod
    def _replace_function(text: str, function_name: str, replacement: str) -> str:
        pattern = re.compile(rf"(?ms)^def {re.escape(function_name)}\(.*?(?=^def |\Z)")
        if not pattern.search(text):
            raise RuntimeError(f"function not found for replacement: {function_name}")
        return pattern.sub(replacement + "\n\n", text, count=1)

    @staticmethod
    def _patch_feature_toggle_clamp(text: str) -> str:
        pattern = re.compile(r"(?ms)^def _validate_feature_list\(.*?(?=^def |\Z)")
        replacement = """
def _validate_feature_list(values: Iterable[str], *, max_count: int) -> List[str]:
    normalized: List[str] = []
    seen = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        if value not in APPROVED_FEATURES:
            raise ValueError(f"unsupported feature toggle: {value}")
        if value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    if not normalized:
        normalized = list(DEFAULT_FEATURES)
    if len(normalized) > max_count:
        normalized = normalized[:max_count]
    return normalized

"""
        if "normalized = normalized[:max_count]" in text:
            return text
        if not pattern.search(text):
            raise RuntimeError("feature toggle validator not found for replacement")
        return pattern.sub(replacement, text, count=1)

    def _artifact_dir(self, incident_id: int, action_id: int) -> Path:
        return self._settings.artifact_dir / f"incident-{incident_id:06d}" / f"action-{action_id:06d}"
