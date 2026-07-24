"""Outcome-independent tests for the terminal-auditor source bootstrap."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parent
BOOTSTRAP_PATH = (
    ROOT / "datavis" / "research" / "fresh_terminal_audit_bootstrap.py"
)
AUDITOR_PATH = ROOT / "datavis" / "research" / "fresh_terminal_audit.py"
SPEC = importlib.util.spec_from_file_location(
    "_fresh_terminal_audit_bootstrap_test_target",
    BOOTSTRAP_PATH,
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("could not load bootstrap test target")
bootstrap = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bootstrap)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class FreshTerminalAuditBootstrapTests(unittest.TestCase):
    def test_exact_34_file_closure_and_aggregate_are_frozen(self) -> None:
        bootstrap._validate_static_contract()
        self.assertEqual(len(bootstrap.FROZEN_RUNTIME_CLOSURE_SHA256), 34)
        manifest = bootstrap._runtime_closure_manifest()
        self.assertEqual(
            bootstrap._sha256(bootstrap._canonical_bytes(manifest)),
            "86511918f8aad8eace17695c82223aa6264b36a9ad08eadf9fa419500a32ce88",
        )
        self.assertIn(
            "datavis/__init__.py",
            bootstrap.FROZEN_RUNTIME_CLOSURE_SHA256,
        )
        self.assertIn(
            "datavis/research/fresh_recovery.py",
            bootstrap.FROZEN_RUNTIME_CLOSURE_SHA256,
        )

    def test_current_closure_and_unsealed_auditor_can_be_cached_without_import(
        self,
    ) -> None:
        before = {
            name
            for name in sys.modules
            if name == "datavis" or name.startswith("datavis.")
        }
        modules, actual = bootstrap._read_verified_sources(
            ROOT,
            auditor_sha256=_sha256(AUDITOR_PATH),
        )
        self.assertEqual(len(modules), 33)
        self.assertEqual(
            set(actual),
            {
                *bootstrap.FROZEN_RUNTIME_CLOSURE_SHA256,
                bootstrap.AUDITOR_RELATIVE_PATH,
            },
        )
        self.assertIn(bootstrap.AUDITOR_MODULE, modules)
        self.assertEqual(
            {
                name
                for name in sys.modules
                if name == "datavis" or name.startswith("datavis.")
            },
            before,
        )

    def test_one_changed_closure_file_is_rejected_before_import(self) -> None:
        original = bootstrap._stable_read_source

        def changed(
            repository_root: Path,
            relative: str,
            *,
            maximum_bytes: int = bootstrap.MAX_SOURCE_BYTES,
        ) -> tuple[Path, bytes]:
            path, raw = original(
                repository_root,
                relative,
                maximum_bytes=maximum_bytes,
            )
            if relative == "datavis/research/fresh_protocol.py":
                raw += b"\n"
            return path, raw

        with (
            mock.patch.object(bootstrap, "_stable_read_source", side_effect=changed),
            self.assertRaisesRegex(
                bootstrap.FreshTerminalAuditBootstrapError,
                "frozen source bytes changed",
            ),
        ):
            bootstrap._read_verified_sources(
                ROOT,
                auditor_sha256=_sha256(AUDITOR_PATH),
            )

    def test_auditor_requires_one_explicit_external_seal(self) -> None:
        with self.assertRaisesRegex(
            bootstrap.FreshTerminalAuditBootstrapError,
            "unsealed",
        ):
            bootstrap._sealed_auditor_sha256(None)
        digest = _sha256(AUDITOR_PATH)
        self.assertEqual(bootstrap.FROZEN_AUDITOR_SHA256, digest)
        self.assertEqual(bootstrap._sealed_auditor_sha256(digest), digest)
        with self.assertRaisesRegex(
            bootstrap.FreshTerminalAuditBootstrapError,
            "invalid",
        ):
            bootstrap._sealed_auditor_sha256("not-a-digest")

    def test_auditor_and_bootstrap_runtime_closures_are_identical(self) -> None:
        saved = {
            name: module
            for name, module in tuple(sys.modules.items())
            if name == "datavis" or name.startswith("datavis.")
        }
        try:
            auditor = importlib.import_module(
                "datavis.research.fresh_terminal_audit"
            )
            self.assertEqual(
                bootstrap.FROZEN_RUNTIME_CLOSURE_SHA256,
                auditor.FROZEN_LOCAL_RUNTIME_CLOSURE_SHA256,
            )
            self.assertEqual(
                bootstrap.FROZEN_RUNTIME_CLOSURE_MANIFEST_SHA256,
                auditor.FROZEN_LOCAL_RUNTIME_CLOSURE_MANIFEST_SHA256,
            )
        finally:
            for name in tuple(sys.modules):
                if name == "datavis" or name.startswith("datavis."):
                    sys.modules.pop(name, None)
            sys.modules.update(saved)

    def test_cached_loader_ignores_changed_disk_bytes(self) -> None:
        saved = {
            name: module
            for name, module in tuple(sys.modules.items())
            if name == "datavis" or name.startswith("datavis.")
        }
        for name in saved:
            sys.modules.pop(name, None)
        package_path = ROOT / "datavis" / "__init__.py"
        # The origin below is the bootstrap's real path, whose disk bytes are
        # intentionally unrelated to the cached one-line probe source.
        probe_path = BOOTSTRAP_PATH
        modules = {
            "datavis": (
                package_path,
                b"PACKAGE = 'cached'\n",
                True,
            ),
            "datavis.probe": (
                probe_path,
                b"VALUE = 'cached-source'\n",
                False,
            ),
        }
        finder = bootstrap._VerifiedDatavisFinder(modules)
        sys.meta_path.insert(0, finder)
        try:
            probe = importlib.import_module("datavis.probe")
            self.assertEqual(probe.VALUE, "cached-source")
            self.assertIs(probe.__spec__.loader, finder.loaders["datavis.probe"])
        finally:
            if finder in sys.meta_path:
                sys.meta_path.remove(finder)
            for name in tuple(sys.modules):
                if name == "datavis" or name.startswith("datavis."):
                    sys.modules.pop(name, None)
            sys.modules.update(saved)

    def test_unexpected_and_preloaded_datavis_modules_are_rejected(self) -> None:
        finder = bootstrap._VerifiedDatavisFinder({})
        with self.assertRaisesRegex(
            bootstrap.FreshTerminalAuditBootstrapError,
            "outside frozen closure",
        ):
            finder.find_spec("datavis.shadow")

        sentinel = SimpleNamespace()
        previous = sys.modules.get("datavis.shadow")
        sys.modules["datavis.shadow"] = sentinel
        try:
            with self.assertRaisesRegex(
                bootstrap.FreshTerminalAuditBootstrapError,
                "loaded before verification",
            ):
                bootstrap._reject_preloaded_modules()
        finally:
            if previous is None:
                sys.modules.pop("datavis.shadow", None)
            else:
                sys.modules["datavis.shadow"] = previous

    def test_preloaded_dependency_submodule_is_rejected_without_root(self) -> None:
        name = "numpy._core.bootstrap_probe"
        previous = sys.modules.get(name)
        sys.modules[name] = SimpleNamespace()
        try:
            self.assertIn(name, bootstrap._preloaded_dependency_modules())
            with self.assertRaisesRegex(
                bootstrap.FreshTerminalAuditBootstrapError,
                name.replace(".", r"\."),
            ):
                bootstrap._reject_preloaded_modules()
        finally:
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous

    def test_runtime_flags_fail_closed(self) -> None:
        self.assertEqual(
            bootstrap._runtime_flag_failures(
                implementation="cpython",
                major_minor=(3, 11),
                isolated=1,
                safe_path=True,
                dont_write_bytecode=True,
                optimize=0,
            ),
            (),
        )
        failures = bootstrap._runtime_flag_failures(
            implementation="cpython",
            major_minor=(3, 13),
            isolated=0,
            safe_path=False,
            dont_write_bytecode=False,
            optimize=1,
        )
        self.assertIn("Python major/minor", failures)
        self.assertIn("-I isolated mode", failures)
        self.assertIn("-B bytecode suppression", failures)
        self.assertIn("optimization level", failures)

    def test_dependency_versions_and_origins_are_checked(self) -> None:
        prefix = Path(sys.prefix).resolve()
        origin = Path(sys.executable).resolve()

        class FakeDistribution:
            def __init__(self, version: str) -> None:
                self.version = version

            def locate_file(self, relative: str) -> Path:
                self.assert_empty(relative)
                return prefix

            @staticmethod
            def assert_empty(relative: str) -> None:
                if relative:
                    raise AssertionError(relative)

        versions = {
            "numpy": FakeDistribution("2.0.2"),
            "pandas": FakeDistribution("2.2.3"),
        }
        with (
            mock.patch.object(
                bootstrap.importlib.metadata,
                "distribution",
                side_effect=lambda name: versions[name],
            ),
            mock.patch.object(
                bootstrap.importlib.util,
                "find_spec",
                side_effect=lambda name: SimpleNamespace(origin=str(origin)),
            ),
        ):
            bindings = bootstrap._dependency_bindings()
        self.assertEqual(bindings["numpy"]["version"], "2.0.2")
        self.assertEqual(bindings["pandas"]["origin"], str(origin))

        versions["numpy"] = FakeDistribution("2.1.3")
        with (
            mock.patch.object(
                bootstrap.importlib.metadata,
                "distribution",
                side_effect=lambda name: versions[name],
            ),
            mock.patch.object(
                bootstrap.importlib.util,
                "find_spec",
                side_effect=lambda name: SimpleNamespace(origin=str(origin)),
            ),
            self.assertRaisesRegex(
                bootstrap.FreshTerminalAuditBootstrapError,
                "identity changed",
            ),
        ):
            bootstrap._dependency_bindings()

    def test_every_loaded_dependency_module_must_stay_in_distribution(
        self,
    ) -> None:
        distribution_root = Path(sys.prefix).resolve()
        valid_module = ModuleType("numpy._core.valid")
        valid_module.__spec__ = SimpleNamespace(
            origin=str(Path(sys.executable).resolve())
        )
        self.assertEqual(
            bootstrap._verified_dependency_module_origin(
                "numpy._core.valid",
                valid_module,
                distribution_root=distribution_root,
            ),
            Path(sys.executable).resolve(),
        )
        escaped_module = ModuleType("numpy._core.escaped")
        escaped_module.__spec__ = SimpleNamespace(origin=str(BOOTSTRAP_PATH))
        with self.assertRaisesRegex(
            bootstrap.FreshTerminalAuditBootstrapError,
            "escaped its distribution",
        ):
            bootstrap._verified_dependency_module_origin(
                "numpy._core.escaped",
                escaped_module,
                distribution_root=distribution_root,
            )

    def test_canonical_json_known_answer_is_checked(self) -> None:
        bootstrap._validate_canonical_runtime()
        with (
            mock.patch.object(
                bootstrap,
                "CANONICAL_JSON_VECTOR_SHA256",
                "0" * 64,
            ),
            self.assertRaisesRegex(
                bootstrap.FreshTerminalAuditBootstrapError,
                "canonical JSON",
            ),
        ):
            bootstrap._validate_canonical_runtime()


if __name__ == "__main__":
    unittest.main()
