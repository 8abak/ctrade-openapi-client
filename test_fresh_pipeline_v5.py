from __future__ import annotations

import copy
import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from datavis.research.fresh_pipeline import (
    _research_state_binding,
    _research_state_binding_v5,
    run_registered_fresh_research,
)
from datavis.research.fresh_pipeline_cli import build_parser, main
from datavis.research.fresh_preregistration import (
    required_fresh_v5_implementation_files,
)
from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_search import StageRunResult


ROOT = Path(__file__).resolve().parent
ROLES = (
    "discovery",
    "walk_forward_1",
    "walk_forward_2",
    "walk_forward_3",
    "validation",
    "holdout",
)


def _split_manifest() -> dict:
    body = {
        "schemaVersion": "test",
        "windows": {
            role: {
                "role": role,
                "sessionAnchors": [f"2026-07-{ordinal + 1:02d}"],
            }
            for ordinal, role in enumerate(ROLES)
        },
    }
    return {**body, "manifestSha256": canonical_hash(body)}


def _v5_state_inputs(state_directory: Path) -> tuple[dict, dict, dict, str]:
    split = _split_manifest()
    base = _research_state_binding(state_directory, split)
    scientific_sha = "f" * 64
    predecessor_lineage_sha = "d" * 64
    predecessor_ledger_sha = "b" * 64
    predecessor_preregistration_sha = "a" * 64
    lineage = {
        "schema": "fresh-xauusd-study-lineage/v1",
        "studyId": "xauusd-fresh-causal-acceleration-v5",
        "predecessorStudyId": "xauusd-fresh-causal-acceleration-v4",
        "predecessorPreregistrationSha256": predecessor_preregistration_sha,
        "predecessorTerminalLedgerSha256": predecessor_ledger_sha,
        "splitManifestSha256": split["manifestSha256"],
        "researchWindowSetSha256": base["researchWindowSetSha256"],
        "scientificSpecificationSha256": scientific_sha,
    }
    v5_lineage_sha = canonical_hash(lineage)
    lineage_root = (
        Path(base["stateDirectory"])
        / "studies"
        / base["researchWindowSetSha256"]
        / "lineages"
    )
    predecessor_state = {
        "schema": "fresh-xauusd-durable-research-state/v3",
        "studyId": "xauusd-fresh-causal-acceleration-v4",
        "studyLineageSha256": predecessor_lineage_sha,
        "splitManifestSha256": base["splitManifestSha256"],
        "researchWindowSetSha256": base["researchWindowSetSha256"],
        "holdoutWindowSha256": base["holdoutWindowSha256"],
        "stateDirectory": base["stateDirectory"],
        "experimentLedgerPath": str(
            lineage_root
            / predecessor_lineage_sha
            / "fresh_experiment_ledger_v1.jsonl"
        ),
        "holdoutAuthorizationRegistryPath": base[
            "holdoutAuthorizationRegistryPath"
        ],
    }
    provenance = {
        "studyId": "xauusd-fresh-causal-acceleration-v5",
        "studyLineageSha256": v5_lineage_sha,
        "predecessorLedgerSha256": predecessor_ledger_sha,
        "predecessorPreregistrationSha256": predecessor_preregistration_sha,
        "predecessorLineageTerminal": True,
        "candidateOutcomeRecordCount": 0,
        "laterWindowOutcomeRecordCount": 0,
        "transientSpoolsRecovered": False,
        "transientCandidateComputationsRecovered": False,
        "partialCandidateResultsImported": False,
        "batchResultSealed": False,
        "restartPolicy": {
            "recomputeFromDiscoverySessionOrdinal": 1,
            "discardTransientSpools": True,
            "discardPartialCandidateComputations": True,
            "importCandidateResults": False,
        },
    }
    return split, predecessor_state, provenance, v5_lineage_sha


class FreshPipelineV5Tests(unittest.TestCase):
    def _directory(self, label: str) -> Path:
        selected = ROOT / f".fresh-pipeline-v5-{label}-{uuid.uuid4().hex}"
        selected.mkdir()
        self.addCleanup(shutil.rmtree, selected, ignore_errors=True)
        return selected

    def test_v5_state_uses_new_ledger_and_identical_global_holdout(self) -> None:
        state = self._directory("state-binding")
        split, predecessor_state, provenance, v5_lineage_sha = _v5_state_inputs(
            state
        )
        base = _research_state_binding(state, split)
        lineage = {
            "schema": "fresh-xauusd-study-lineage/v1",
            "studyId": "xauusd-fresh-causal-acceleration-v5",
            "predecessorStudyId": "xauusd-fresh-causal-acceleration-v4",
            "predecessorPreregistrationSha256": "a" * 64,
            "predecessorTerminalLedgerSha256": "b" * 64,
            "splitManifestSha256": split["manifestSha256"],
            "researchWindowSetSha256": base["researchWindowSetSha256"],
            "scientificSpecificationSha256": "f" * 64,
        }

        with (
            patch(
                "datavis.research.fresh_pipeline.RUN19_LEDGER_SHA256",
                "b" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_PREREGISTRATION_SHA256",
                "a" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_STUDY_LINEAGE_SHA256",
                "d" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_V5_STUDY_LINEAGE_SHA256",
                v5_lineage_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.canonical_fresh_v5_study_lineage",
                return_value=lineage,
            ),
            patch(
                "datavis.research.fresh_pipeline.fresh_v5_scientific_specification_sha256",
                return_value="f" * 64,
            ),
        ):
            restarted = _research_state_binding_v5(
                state,
                split,
                provenance,
                predecessor_state,
            )

        self.assertEqual(
            restarted["schema"],
            "fresh-xauusd-durable-research-state/v4",
        )
        self.assertEqual(restarted["studyLineage"], lineage)
        self.assertEqual(restarted["studyLineageSha256"], v5_lineage_sha)
        self.assertEqual(
            restarted["predecessorExperimentLedgerPath"],
            predecessor_state["experimentLedgerPath"],
        )
        self.assertNotEqual(
            restarted["experimentLedgerPath"],
            predecessor_state["experimentLedgerPath"],
        )
        self.assertIn(v5_lineage_sha, restarted["experimentLedgerPath"])
        self.assertEqual(
            restarted["holdoutAuthorizationRegistryPath"],
            base["holdoutAuthorizationRegistryPath"],
        )

    def test_v5_state_rejects_outcomes_resume_and_changed_v4_bindings(self) -> None:
        state = self._directory("state-rejections")
        split, predecessor_state, provenance, v5_lineage_sha = _v5_state_inputs(
            state
        )
        base = _research_state_binding(state, split)
        lineage = {
            "schema": "fresh-xauusd-study-lineage/v1",
            "studyId": "xauusd-fresh-causal-acceleration-v5",
            "predecessorStudyId": "xauusd-fresh-causal-acceleration-v4",
            "predecessorPreregistrationSha256": "a" * 64,
            "predecessorTerminalLedgerSha256": "b" * 64,
            "splitManifestSha256": split["manifestSha256"],
            "researchWindowSetSha256": base["researchWindowSetSha256"],
            "scientificSpecificationSha256": "f" * 64,
        }
        cases = []

        revealed = copy.deepcopy(provenance)
        revealed["candidateOutcomeRecordCount"] = 1
        cases.append(("candidate outcomes", revealed, predecessor_state, "outcome-free"))

        resumed = copy.deepcopy(provenance)
        resumed["restartPolicy"]["recomputeFromDiscoverySessionOrdinal"] = 38
        cases.append(("partial resume", resumed, predecessor_state, "outcome-free"))

        imported = copy.deepcopy(provenance)
        imported["restartPolicy"]["importCandidateResults"] = True
        cases.append(("candidate import", imported, predecessor_state, "outcome-free"))

        changed_root = copy.deepcopy(predecessor_state)
        changed_root["stateDirectory"] = str(state / "alternate")
        cases.append(
            ("state root", provenance, changed_root, "exact terminal v4 state root")
        )

        changed_ledger = copy.deepcopy(predecessor_state)
        changed_ledger["experimentLedgerPath"] = str(state / "other-ledger.jsonl")
        cases.append(
            (
                "ledger binding",
                provenance,
                changed_ledger,
                "predecessor ledger or global holdout",
            )
        )

        changed_holdout = copy.deepcopy(predecessor_state)
        changed_holdout["holdoutAuthorizationRegistryPath"] = str(
            state / "other-holdout.json"
        )
        cases.append(
            (
                "holdout binding",
                provenance,
                changed_holdout,
                "predecessor ledger or global holdout",
            )
        )

        with (
            patch(
                "datavis.research.fresh_pipeline.RUN19_LEDGER_SHA256",
                "b" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_PREREGISTRATION_SHA256",
                "a" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_STUDY_LINEAGE_SHA256",
                "d" * 64,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_V5_STUDY_LINEAGE_SHA256",
                v5_lineage_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.canonical_fresh_v5_study_lineage",
                return_value=lineage,
            ),
            patch(
                "datavis.research.fresh_pipeline.fresh_v5_scientific_specification_sha256",
                return_value="f" * 64,
            ),
        ):
            for label, selected_provenance, selected_state, message in cases:
                with self.subTest(case=label):
                    with self.assertRaisesRegex(PermissionError, message):
                        _research_state_binding_v5(
                            state,
                            split,
                            selected_provenance,
                            selected_state,
                        )

    def test_cli_forwards_v5_and_rejects_multiple_lineage_modes(self) -> None:
        with patch(
            "datavis.research.fresh_pipeline_cli.run_registered_fresh_research",
            return_value={"status": "complete", "holdoutOpened": False},
        ) as run:
            self.assertEqual(
                main(
                    [
                        "--output-dir",
                        "output",
                        "--scratch-dir",
                        "scratch",
                        "--restart-v5-artifact-dir",
                        "run19",
                        "--research-state-dir",
                        "state",
                        "--execute",
                    ]
                ),
                0,
            )
        self.assertEqual(
            run.call_args.kwargs[
                "infrastructure_restart_v5_artifact_directory"
            ],
            "run19",
        )
        self.assertEqual(run.call_args.kwargs["scratch_directory"], "scratch")

        with self.assertRaises(SystemExit):
            build_parser().parse_args(
                [
                    "--output-dir",
                    "output",
                    "--restart-v4-artifact-dir",
                    "run17",
                    "--restart-v5-artifact-dir",
                    "run19",
                ]
            )

    def test_v5_orchestration_recomputes_frozen_discovery_from_session_one(
        self,
    ) -> None:
        output = self._directory("output")
        state = self._directory("state")
        scratch = self._directory("scratch")
        predecessor_ledger = state / "v4" / "fresh_experiment_ledger_v1.jsonl"
        predecessor_ledger.parent.mkdir()
        predecessor_ledger.write_text("terminal-v4\n", encoding="utf-8")
        ledger = state / "v5" / "fresh_experiment_ledger_v1.jsonl"
        holdout = state / "holdout" / "fresh_holdout_authorization_v1.json"
        entry_bank_sha = "e" * 64
        predecessor_ledger_sha = "b" * 64
        discovery_window = {
            "role": "discovery",
            "sessionAnchors": ["2026-01-02"],
        }
        split = {
            "manifestSha256": "s" * 64,
            "windows": {"discovery": discovery_window},
        }
        provenance = {
            "predecessorRunId": 30042880650,
            "reusedOutcomeBlindInputs": {
                "fresh_entry_bank_v1.json": entry_bank_sha,
            },
        }
        bundle_paths = {
            name: ROOT / name
            for name in (
                "fresh_source_inventory_v1.json",
                "fresh_corpus_manifest_v1.json",
                "fresh_split_manifest_v2.json",
                "fresh_research_state_binding_v3.json",
                "fresh_experiment_ledger_v1.jsonl",
                "fresh_preregistration_v4.json",
                "fresh_implementation_manifest_v1.json",
                "fresh_quantile_bank_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            )
        }
        bundle = SimpleNamespace(
            inventory={"inventorySha256": "i" * 64},
            corpus={"corpusManifestSha256": "c" * 64},
            split=split,
            provenance=provenance,
            predecessor_state_binding={"stateDirectory": str(state)},
            quantile_bank={"bankSha256": "q" * 64},
            threshold_preflight={"allRegisteredThresholdDomainsResolved": True},
            paths=bundle_paths,
        )
        state_binding = {
            "studyId": "xauusd-fresh-causal-acceleration-v5",
            "studyLineageSha256": "5" * 64,
            "experimentLedgerPath": str(ledger),
            "predecessorExperimentLedgerPath": str(predecessor_ledger),
            "holdoutAuthorizationRegistryPath": str(holdout),
        }
        discovery_result = StageRunResult(
            stage="discovery",
            evaluated_ids=("candidate",),
            promoted_ids=(),
            ledger_record_numbers=(1,),
            study_failed=True,
        )
        frozen_discovery = MagicMock(return_value=discovery_result)
        ordinary_discovery = MagicMock(return_value=discovery_result)
        search = SimpleNamespace(
            run_frozen_discovery=frozen_discovery,
            run_discovery=ordinary_discovery,
            run_walk_forward_1=MagicMock(),
            run_walk_forward_2=MagicMock(),
            run_exit_search=MagicMock(),
            run_walk_forward_3=MagicMock(),
            run_validation=MagicMock(),
            audit_records=(),
        )
        build_entries = MagicMock(return_value=("entry-spec",))
        pipeline = SimpleNamespace(
            quantile_bank=None,
            threshold_preflight=None,
            stage_results=[],
            build_entry_candidates=build_entries,
            build_search=MagicMock(return_value=search),
        )
        implementation = {"manifestSha256": "m" * 64}
        preregistration = {"preregistrationSha256": "p" * 64}

        def file_sha(path: Path) -> str:
            selected = Path(path)
            if selected.resolve() == predecessor_ledger.resolve():
                return predecessor_ledger_sha
            if selected.name == "fresh_entry_bank_v1.json":
                return entry_bank_sha
            return "0" * 64

        with (
            patch(
                "datavis.research.fresh_pipeline.load_fresh_v5_restart_bundle",
                return_value=bundle,
            ) as load_bundle,
            patch(
                "datavis.research.fresh_pipeline._research_state_binding_v5",
                return_value=state_binding,
            ) as bind_state,
            patch(
                "datavis.research.fresh_pipeline._snapshot_new_file",
            ) as snapshot,
            patch(
                "datavis.research.fresh_pipeline._file_sha256",
                side_effect=file_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.RUN19_LEDGER_SHA256",
                predecessor_ledger_sha,
            ),
            patch(
                "datavis.research.fresh_pipeline.build_fresh_implementation_manifest",
                return_value=implementation,
            ) as build_manifest,
            patch(
                "datavis.research.fresh_pipeline.build_fresh_preregistration_v5",
                return_value=preregistration,
            ) as build_preregistration,
            patch(
                "datavis.research.fresh_pipeline.fresh_quantile_bank_from_payload",
                return_value="bound-quantile-bank",
            ) as bind_quantiles,
            patch(
                "datavis.research.fresh_pipeline.RegisteredFreshResearchPipeline",
                return_value=pipeline,
            ),
        ):
            summary = run_registered_fresh_research(
                lambda: None,
                repository_root=ROOT,
                output_directory=output,
                research_state_directory=state,
                scratch_directory=scratch,
                infrastructure_restart_v5_artifact_directory="run19",
            )

        load_bundle.assert_called_once_with("run19")
        bind_state.assert_called_once_with(
            state.resolve(),
            split,
            provenance,
            bundle.predecessor_state_binding,
        )
        self.assertEqual(
            build_manifest.call_args.kwargs["relative_paths"],
            required_fresh_v5_implementation_files(),
        )
        self.assertIs(
            build_preregistration.call_args.kwargs[
                "infrastructure_restart_provenance"
            ],
            provenance,
        )
        build_entries.assert_called_once()
        self.assertEqual(
            build_entries.call_args.args[1].windows[0].session_anchors,
            ("2026-01-02",),
        )
        bind_quantiles.assert_called_once_with(bundle.quantile_bank)
        frozen_discovery.assert_called_once_with(
            threshold_bank=bundle.quantile_bank,
            entry_specs=("entry-spec",),
        )
        ordinary_discovery.assert_not_called()
        copied_names = {
            call.args[0].name
            for call in snapshot.call_args_list
            if call.args
        }
        self.assertIn("fresh_quantile_bank_v1.json", copied_names)
        self.assertIn("fresh_threshold_domain_preflight_v1.json", copied_names)
        for operation in (
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        ):
            operation.assert_not_called()
        self.assertEqual(summary["infrastructureRestartVersion"], 5)
        self.assertEqual(summary["predecessorRunId"], 30042880650)
        self.assertEqual(
            summary["studyId"],
            "xauusd-fresh-causal-acceleration-v5",
        )
        self.assertFalse(summary["holdoutOpened"])


if __name__ == "__main__":
    unittest.main()
