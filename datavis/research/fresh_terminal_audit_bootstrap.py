"""Fail-closed source bootstrap for the detached-v5 terminal auditor.

Invoke this file by path from the canonical audit environment:

```
python -I -B datavis/research/fresh_terminal_audit_bootstrap.py BUNDLE_DIR
```

This module intentionally imports no ``datavis`` code.  It verifies and caches
the exact source bytes first, then installs a temporary importer which compiles
only those cached bytes.  That ordering prevents a valid working-tree hash
from attesting a shadowed module, a preloaded monkeypatch, a stale ``.pyc``, or
a file swapped between verification and import.

The reviewed auditor source is sealed below by its exact SHA-256.  This
bootstrap's raw blob and containing GitHub commit must also be pinned outside
this file before any audit output is treated as authoritative.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.abc
import importlib.metadata
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import sys
from types import ModuleType
from typing import Mapping, Sequence


BOOTSTRAP_SCHEMA = "fresh-xauusd-v5-terminal-audit-bootstrap/v1"
RUNTIME_CLOSURE_SCHEMA = "fresh-xauusd-v5-audit-runtime-closure/v1"
AUDITOR_MODULE = "datavis.research.fresh_terminal_audit"
AUDITOR_RELATIVE_PATH = "datavis/research/fresh_terminal_audit.py"
MAX_SOURCE_BYTES = 16 * 1024**2
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")

# This one explicit seal must be filled only after fresh_terminal_audit.py has
# completed independent review.  A CLI override would let the file attest
# itself and is deliberately not supported.
FROZEN_AUDITOR_SHA256: str | None = (
    "502be931a532fb0a22ba05d8251dd2553775f2f44dd565450836f8c26ed2344e"
)

FROZEN_RUNTIME_CLOSURE_SHA256 = {
    ".github/research-launch.txt": (
        "e357976d5879b31664059b96017bb56f72c2d74456873d4939b91e8011ff4727"
    ),
    ".github/workflows/fresh-xauusd-research.yml": (
        "592b06c6fafc7272ae1ea5fbcd348924d316a8591caa85ea59b3a91f206b5a59"
    ),
    "datavis/__init__.py": (
        "702648ad2cbc9a47b8e91b801c90855066e2a94de52fdaecb057bb8e02fc2e07"
    ),
    "datavis/db.py": (
        "e26524b82902441a2750311ad5ac5e6c31cb1e6140f2e9770470b058eebc3330"
    ),
    "datavis/research/__init__.py": (
        "0f729be715d82bf228511059f74fe074cbddab93f8e5d7794d2671a57b0c5fe3"
    ),
    "datavis/research/fresh_bootstrap.py": (
        "5048159a13fa30855570a0da2119be3db2a389d29ca1f7257503070b06669709"
    ),
    "datavis/research/fresh_candidate_grid.py": (
        "df7ac596e01c10dc1ffce2479459df76bf4b27ec92d439fa54936fd0cf376244"
    ),
    "datavis/research/fresh_data.py": (
        "e3aa81283e672faee932512310f746027f6eda653873fd606560124e252a5212"
    ),
    "datavis/research/fresh_db_source.py": (
        "f28e3c63f7f6a2e9d10fcde4b2860eadb1aca3bc51fcf585721f928dbc2c0acf"
    ),
    "datavis/research/fresh_decisions.py": (
        "f054fc7d7c24bf89bb7ea472025f836d69339ef0eeec2ab30d9e1733fd992795"
    ),
    "datavis/research/fresh_entry_diagnostics.py": (
        "4bd394e98e4770f7d63e699fb2f24aa5e255b943ffaf88a5ebf1fff81544bbb3"
    ),
    "datavis/research/fresh_event_filters.py": (
        "1ccab790e6864c280a21c2f6bf160ae34d9ad3298173b83aa3bc19e9e38da747"
    ),
    "datavis/research/fresh_exit_grid.py": (
        "18fc7fcba4f554d584465c4ba65756032f249cca4dd3e90fd27c041032dcc22b"
    ),
    "datavis/research/fresh_exits.py": (
        "5ce1a10a21d2fb19a5c8cb92d4ed8f445265c3edbe48443b2df2aae9d3328be0"
    ),
    "datavis/research/fresh_feature_bank.py": (
        "218fa40fbc8edfd1a22232612a6c6c57203270b671a45a89762e680d36c2a944"
    ),
    "datavis/research/fresh_features.py": (
        "9562cb71dc7e20c273b7ec9797144c217329bc2b3e7ea519d5459e851470c28c"
    ),
    "datavis/research/fresh_inventory.py": (
        "b84aa3fab3bf578066d275301de5096b79857b04d9eba5603c8015f160d5480c"
    ),
    "datavis/research/fresh_pipeline.py": (
        "7708e0cc74082b6b7e1a7db9ee1b3756aa8b62e7ba39056f63b9be5004d3f609"
    ),
    "datavis/research/fresh_pipeline_cli.py": (
        "2094381a25c58e1827d0b2656552b6932a3007e6a6c133aeb06fa0616ca00709"
    ),
    "datavis/research/fresh_preregistration.py": (
        "2d82a7f508957b9107fd860668958b4e2f6f0736d68f90187dd46ca0db28f029"
    ),
    "datavis/research/fresh_protocol.py": (
        "22b6642da2035fa7452b908b02d317ffeb99ad73dcedd99158fda746c7625009"
    ),
    "datavis/research/fresh_recovery.py": (
        "7e84e485157f671bf5df1b2514a3a98ce0b3242440756078a6d7a295863d757c"
    ),
    "datavis/research/fresh_replay.py": (
        "35d931f4a69ac8cf139638a9416ef41d5e8f24fb8ba816d997a0e1c02e05ef0f"
    ),
    "datavis/research/fresh_restart.py": (
        "d5a3c605f8a6be9f524cedaf22eaf291b0f2b7813b292e9aeb1f972123b65237"
    ),
    "datavis/research/fresh_restart_v4.py": (
        "2d19382490fed141de26efde6e6f2ed45456bd897b6ecb7ae0bc0c09b624a291"
    ),
    "datavis/research/fresh_restart_v5.py": (
        "e35d0ee7f161048a66f4fd448fd4143940f855823f576fd094f99b0d6d2d2b2f"
    ),
    "datavis/research/fresh_scoring.py": (
        "976d8c8090ad673090d99f02392f8a7f6a88fb6d0a0bb10f8ab5f0625d3c5424"
    ),
    "datavis/research/fresh_search.py": (
        "23614fb3c6e751e7f59b70b5c450e531a4f1a373e470e102867e0c471787653b"
    ),
    "datavis/research/fresh_session_eval.py": (
        "e7c6fe53fc953b9ba0b1f2361e65292a74807ef2db4d53b53a7383550df3f62c"
    ),
    "datavis/research/fresh_sessions.py": (
        "9bd40f1afeedba0dcbdf88a22717b1347c831479a5a836e508a2ca007915b03d"
    ),
    "datavis/research/fresh_signals.py": (
        "83fbf412c5566c01c5e22e10a32983ecc35f8781f2b69e83547ef4b8b313d95a"
    ),
    "datavis/research/fresh_spool.py": (
        "d88fad486740d2ad8b9608d84b6817e8099d36f2b5a348e65a52aa4f5e41060b"
    ),
    "datavis/research/fresh_thresholds.py": (
        "3daac1d1f6e7ea4affd913172584c344bb32f97ffef76d9ad6ad4a51a10ed48c"
    ),
    "datavis/research/ticks.py": (
        "1abbe6959bda9031e4ee4e67553c476f2337f1eb6cbfda79af2ef8a5be913bb8"
    ),
}
FROZEN_RUNTIME_CLOSURE_MANIFEST_SHA256 = (
    "86511918f8aad8eace17695c82223aa6264b36a9ad08eadf9fa419500a32ce88"
)

CANONICAL_PYTHON_IMPLEMENTATION = "cpython"
CANONICAL_PYTHON_MAJOR_MINOR = (3, 11)
REQUIRED_DISTRIBUTIONS = {
    "numpy": "2.0.2",
    "pandas": "2.2.3",
}
CANONICAL_JSON_VECTOR_SHA256 = (
    "3c40e0493f87be9d127c2e545f983c20e672def4f1760c239642b823be534703"
)


class FreshTerminalAuditBootstrapError(RuntimeError):
    """Raised before any scientific auditor code may execute."""


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        dict(payload),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _runtime_closure_manifest() -> dict[str, object]:
    return {
        "schema": RUNTIME_CLOSURE_SCHEMA,
        "files": [
            {"path": path, "sha256": FROZEN_RUNTIME_CLOSURE_SHA256[path]}
            for path in sorted(FROZEN_RUNTIME_CLOSURE_SHA256)
        ],
    }


def _validate_static_contract() -> None:
    paths = tuple(sorted(FROZEN_RUNTIME_CLOSURE_SHA256))
    if (
        len(paths) != 34
        or len(set(paths)) != len(paths)
        or any(
            PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or PurePosixPath(path).as_posix() != path
            for path in paths
        )
        or any(
            SHA256_PATTERN.fullmatch(value) is None
            for value in FROZEN_RUNTIME_CLOSURE_SHA256.values()
        )
        or _sha256(_canonical_bytes(_runtime_closure_manifest()))
        != FROZEN_RUNTIME_CLOSURE_MANIFEST_SHA256
    ):
        raise FreshTerminalAuditBootstrapError(
            "frozen audit-runtime closure contract is invalid"
        )


def _validate_canonical_runtime() -> None:
    vector = {
        "u": "Ω",
        "z": -0.0,
        "x": 0.1,
        "nested": [1, True, None],
    }
    if _sha256(_canonical_bytes(vector)) != CANONICAL_JSON_VECTOR_SHA256:
        raise FreshTerminalAuditBootstrapError(
            "canonical JSON implementation changed"
        )


def _runtime_flag_failures(
    *,
    implementation: str,
    major_minor: tuple[int, int],
    isolated: int,
    safe_path: bool,
    dont_write_bytecode: bool,
    optimize: int,
) -> tuple[str, ...]:
    failures: list[str] = []
    if implementation != CANONICAL_PYTHON_IMPLEMENTATION:
        failures.append("Python implementation")
    if major_minor != CANONICAL_PYTHON_MAJOR_MINOR:
        failures.append("Python major/minor")
    if isolated != 1:
        failures.append("-I isolated mode")
    if not safe_path:
        failures.append("safe import path")
    if not dont_write_bytecode:
        failures.append("-B bytecode suppression")
    if optimize != 0:
        failures.append("optimization level")
    return tuple(failures)


def _validate_runtime_flags() -> None:
    failures = _runtime_flag_failures(
        implementation=sys.implementation.name,
        major_minor=(sys.version_info.major, sys.version_info.minor),
        isolated=sys.flags.isolated,
        safe_path=bool(getattr(sys.flags, "safe_path", False)),
        dont_write_bytecode=bool(sys.dont_write_bytecode),
        optimize=sys.flags.optimize,
    )
    if failures:
        raise FreshTerminalAuditBootstrapError(
            "non-canonical audit interpreter: " + ", ".join(failures)
        )


def _stable_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        int(metadata.st_dev),
        int(metadata.st_ino),
        int(metadata.st_mode),
        int(metadata.st_size),
        int(metadata.st_mtime_ns),
        int(metadata.st_ctime_ns),
    )


def _stable_read_source(
    repository_root: Path,
    relative: str,
    *,
    maximum_bytes: int = MAX_SOURCE_BYTES,
) -> tuple[Path, bytes]:
    if maximum_bytes <= 0:
        raise ValueError("maximum_bytes must be positive")
    root = repository_root.resolve(strict=True)
    pure = PurePosixPath(relative)
    if (
        pure.is_absolute()
        or ".." in pure.parts
        or pure.as_posix() != relative
    ):
        raise FreshTerminalAuditBootstrapError(
            f"non-canonical source path: {relative}"
        )
    lexical = root.joinpath(*pure.parts)
    if lexical.is_symlink():
        raise FreshTerminalAuditBootstrapError(
            f"source path cannot be a symlink: {relative}"
        )
    try:
        resolved = lexical.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as error:
        raise FreshTerminalAuditBootstrapError(
            f"source path is unavailable or escaped the repository: {relative}"
        ) from error
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(resolved, flags)
    except OSError as error:
        raise FreshTerminalAuditBootstrapError(
            f"source file could not be opened: {relative}"
        ) from error
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size < 0
            or before.st_size > maximum_bytes
        ):
            raise FreshTerminalAuditBootstrapError(
                f"source file type or size is invalid: {relative}"
            )
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                raise FreshTerminalAuditBootstrapError(
                    f"source file ended while being read: {relative}"
                )
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise FreshTerminalAuditBootstrapError(
                f"source file grew while being read: {relative}"
            )
        after = os.fstat(descriptor)
        if _stable_identity(before) != _stable_identity(after):
            raise FreshTerminalAuditBootstrapError(
                f"source file changed while being read: {relative}"
            )
        return resolved, b"".join(chunks)
    finally:
        os.close(descriptor)


def _sealed_auditor_sha256(value: str | None) -> str:
    if value is None:
        raise FreshTerminalAuditBootstrapError(
            "FROZEN_AUDITOR_SHA256 is unsealed"
        )
    normalized = value.lower()
    if SHA256_PATTERN.fullmatch(normalized) is None:
        raise FreshTerminalAuditBootstrapError(
            "FROZEN_AUDITOR_SHA256 is invalid"
        )
    return normalized


def _module_name(relative: str) -> tuple[str, bool] | None:
    pure = PurePosixPath(relative)
    if pure.suffix != ".py":
        return None
    if pure.name == "__init__.py":
        components = pure.parts[:-1]
        is_package = True
    else:
        components = (*pure.parts[:-1], pure.stem)
        is_package = False
    if not components or components[0] != "datavis":
        return None
    return ".".join(components), is_package


def _read_verified_sources(
    repository_root: Path,
    *,
    auditor_sha256: str,
) -> tuple[dict[str, tuple[Path, bytes, bool]], dict[str, str]]:
    _validate_static_contract()
    expected_auditor_sha = _sealed_auditor_sha256(auditor_sha256)
    modules: dict[str, tuple[Path, bytes, bool]] = {}
    actual_hashes: dict[str, str] = {}
    for relative, expected_sha in FROZEN_RUNTIME_CLOSURE_SHA256.items():
        path, raw = _stable_read_source(repository_root, relative)
        actual_sha = _sha256(raw)
        if actual_sha != expected_sha:
            raise FreshTerminalAuditBootstrapError(
                f"frozen source bytes changed: {relative}"
            )
        actual_hashes[relative] = actual_sha
        identity = _module_name(relative)
        if identity is not None:
            name, is_package = identity
            if name in modules:
                raise FreshTerminalAuditBootstrapError(
                    f"duplicate frozen module name: {name}"
                )
            modules[name] = (path, raw, is_package)

    auditor_path, auditor_raw = _stable_read_source(
        repository_root,
        AUDITOR_RELATIVE_PATH,
    )
    if _sha256(auditor_raw) != expected_auditor_sha:
        raise FreshTerminalAuditBootstrapError(
            "terminal auditor differs from its external seal"
        )
    if AUDITOR_MODULE in modules:
        raise FreshTerminalAuditBootstrapError(
            "terminal auditor unexpectedly appears in the launch closure"
        )
    modules[AUDITOR_MODULE] = (auditor_path, auditor_raw, False)
    actual_hashes[AUDITOR_RELATIVE_PATH] = expected_auditor_sha
    return modules, actual_hashes


class _CachedSourceLoader(importlib.abc.Loader):
    def __init__(
        self,
        fullname: str,
        path: Path,
        source: bytes,
        *,
        is_package: bool,
    ) -> None:
        self.fullname = fullname
        self.path = path
        self.source = source
        self.is_package_value = is_package

    def create_module(self, spec: object) -> ModuleType | None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        code = compile(
            self.source,
            str(self.path),
            "exec",
            dont_inherit=True,
            optimize=0,
        )
        exec(code, module.__dict__)

    def is_package(self, fullname: str) -> bool:
        if fullname != self.fullname:
            raise ImportError(f"loader cannot serve {fullname}")
        return self.is_package_value


class _VerifiedDatavisFinder(importlib.abc.MetaPathFinder):
    def __init__(
        self,
        modules: Mapping[str, tuple[Path, bytes, bool]],
    ) -> None:
        self.modules = dict(modules)
        self.loaders = {
            name: _CachedSourceLoader(
                name,
                path,
                source,
                is_package=is_package,
            )
            for name, (path, source, is_package) in self.modules.items()
        }

    def find_spec(
        self,
        fullname: str,
        path: object = None,
        target: ModuleType | None = None,
    ) -> object:
        del path, target
        loader = self.loaders.get(fullname)
        if loader is not None:
            spec = importlib.util.spec_from_loader(
                fullname,
                loader,
                origin=str(loader.path),
                is_package=loader.is_package_value,
            )
            if spec is None:
                raise FreshTerminalAuditBootstrapError(
                    f"could not create frozen module spec: {fullname}"
                )
            spec.has_location = True
            if loader.is_package_value:
                spec.submodule_search_locations = [str(loader.path.parent)]
            return spec
        if fullname == "datavis" or fullname.startswith("datavis."):
            raise FreshTerminalAuditBootstrapError(
                f"unexpected datavis import outside frozen closure: {fullname}"
            )
        return None


def _preloaded_datavis_modules() -> tuple[str, ...]:
    return tuple(
        sorted(
            name
            for name in sys.modules
            if name == "datavis" or name.startswith("datavis.")
        )
    )


def _preloaded_dependency_modules() -> tuple[str, ...]:
    return tuple(
        sorted(
            name
            for name in sys.modules
            if any(
                name == dependency or name.startswith(dependency + ".")
                for dependency in REQUIRED_DISTRIBUTIONS
            )
        )
    )


def _reject_preloaded_modules() -> None:
    preloaded = _preloaded_datavis_modules()
    dependencies = _preloaded_dependency_modules()
    if preloaded or dependencies:
        details = ", ".join((*preloaded, *dependencies))
        raise FreshTerminalAuditBootstrapError(
            "audit modules were loaded before verification: " + details
        )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _dependency_bindings() -> dict[str, dict[str, str]]:
    """Bind package identity inside a separately trusted canonical environment.

    Version and origin checks do not cryptographically seal every third-party
    wheel byte.  The dedicated CPython 3.11 audit environment remains an
    explicit trust boundary and must be provisioned from the pinned packages.
    """

    bindings: dict[str, dict[str, str]] = {}
    prefix = Path(sys.prefix).resolve()
    for name, expected_version in REQUIRED_DISTRIBUTIONS.items():
        try:
            distribution = importlib.metadata.distribution(name)
            version = distribution.version
            distribution_root = Path(distribution.locate_file("")).resolve(
                strict=True
            )
            spec = importlib.util.find_spec(name)
        except (ImportError, OSError, ValueError) as error:
            raise FreshTerminalAuditBootstrapError(
                f"required audit dependency is unavailable: {name}"
            ) from error
        origin = getattr(spec, "origin", None) if spec is not None else None
        if (
            version != expected_version
            or not isinstance(origin, str)
            or not origin
        ):
            raise FreshTerminalAuditBootstrapError(
                f"audit dependency identity changed: {name}"
            )
        lexical_origin = Path(origin)
        try:
            origin_path = lexical_origin.resolve(strict=True)
        except OSError as error:
            raise FreshTerminalAuditBootstrapError(
                f"audit dependency origin is unavailable: {name}"
            ) from error
        if (
            lexical_origin.is_symlink()
            or not origin_path.is_file()
            or not _is_relative_to(origin_path, distribution_root)
            or not _is_relative_to(distribution_root, prefix)
        ):
            raise FreshTerminalAuditBootstrapError(
                f"audit dependency origin is not isolated: {name}"
            )
        bindings[name] = {
            "version": version,
            "origin": str(origin_path),
            "distributionRoot": str(distribution_root),
        }
    return bindings


def _verified_dependency_module_origin(
    qualified_name: str,
    module: object,
    *,
    distribution_root: Path,
) -> Path:
    if not isinstance(module, ModuleType):
        raise FreshTerminalAuditBootstrapError(
            f"loaded dependency module object is invalid: {qualified_name}"
        )
    specification = getattr(module, "__spec__", None)
    origin = getattr(specification, "origin", None)
    if not isinstance(origin, str) or not origin:
        raise FreshTerminalAuditBootstrapError(
            f"loaded dependency module has no origin: {qualified_name}"
        )
    lexical_origin = Path(origin)
    try:
        resolved_origin = lexical_origin.resolve(strict=True)
    except OSError as error:
        raise FreshTerminalAuditBootstrapError(
            f"loaded dependency module origin is unavailable: {qualified_name}"
        ) from error
    if (
        lexical_origin.is_symlink()
        or not resolved_origin.is_file()
        or not _is_relative_to(resolved_origin, distribution_root)
    ):
        raise FreshTerminalAuditBootstrapError(
            f"loaded dependency module escaped its distribution: {qualified_name}"
        )
    return resolved_origin


def _verify_loaded_modules(
    finder: _VerifiedDatavisFinder,
    dependency_bindings: Mapping[str, Mapping[str, str]],
) -> None:
    loaded = {
        name: module
        for name, module in sys.modules.items()
        if name == "datavis" or name.startswith("datavis.")
    }
    unexpected = set(loaded) - set(finder.modules)
    if unexpected:
        raise FreshTerminalAuditBootstrapError(
            "unexpected datavis modules loaded: " + ", ".join(sorted(unexpected))
        )
    if AUDITOR_MODULE not in loaded:
        raise FreshTerminalAuditBootstrapError("terminal auditor was not loaded")
    for name, module in loaded.items():
        if not isinstance(module, ModuleType):
            raise FreshTerminalAuditBootstrapError(
                f"loaded module object is invalid: {name}"
            )
        specification = getattr(module, "__spec__", None)
        loader = finder.loaders[name]
        if (
            specification is None
            or specification.loader is not loader
            or specification.origin != str(loader.path)
        ):
            raise FreshTerminalAuditBootstrapError(
                f"loaded module did not come from cached frozen bytes: {name}"
            )
    for name, expected in dependency_bindings.items():
        namespace = {
            qualified_name: module
            for qualified_name, module in sys.modules.items()
            if qualified_name == name or qualified_name.startswith(name + ".")
        }
        if name not in namespace:
            raise FreshTerminalAuditBootstrapError(
                f"required audit dependency was not loaded: {name}"
            )
        distribution_root = Path(expected["distributionRoot"]).resolve(
            strict=True
        )
        resolved_origins = {
            qualified_name: _verified_dependency_module_origin(
                qualified_name,
                module,
                distribution_root=distribution_root,
            )
            for qualified_name, module in namespace.items()
        }
        module = namespace[name]
        if (
            getattr(module, "__version__", None) != expected["version"]
            or str(resolved_origins[name]) != expected["origin"]
        ):
            raise FreshTerminalAuditBootstrapError(
                f"loaded audit dependency identity changed: {name}"
            )


def run_verified_auditor(
    bundle_directory: str | Path,
    *,
    repository_root: Path,
    auditor_sha256: str,
) -> int:
    """Load exact cached sources and run the auditor once."""

    _validate_canonical_runtime()
    _reject_preloaded_modules()
    dependency_bindings = _dependency_bindings()
    modules, _actual_hashes = _read_verified_sources(
        repository_root,
        auditor_sha256=auditor_sha256,
    )
    finder = _VerifiedDatavisFinder(modules)
    sys.meta_path.insert(0, finder)
    try:
        auditor = importlib.import_module(AUDITOR_MODULE)
        _verify_loaded_modules(finder, dependency_bindings)
        result = auditor.main([str(bundle_directory)])
        _verify_loaded_modules(finder, dependency_bindings)
    finally:
        if finder in sys.meta_path:
            sys.meta_path.remove(finder)
    if isinstance(result, bool) or not isinstance(result, int):
        raise FreshTerminalAuditBootstrapError(
            "terminal auditor returned a non-integer status"
        )
    return result


def main(arguments: Sequence[str] | None = None) -> int:
    """Validate the bootstrap environment and execute the sealed auditor."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle_directory")
    parsed = parser.parse_args(arguments)
    try:
        if __package__:
            raise FreshTerminalAuditBootstrapError(
                "bootstrap must be invoked by script path, not as a module"
            )
        _validate_runtime_flags()
        auditor_sha = _sealed_auditor_sha256(FROZEN_AUDITOR_SHA256)
        repository_root = Path(__file__).resolve().parents[2]
        return run_verified_auditor(
            parsed.bundle_directory,
            repository_root=repository_root,
            auditor_sha256=auditor_sha,
        )
    except FreshTerminalAuditBootstrapError as error:
        print(f"terminal audit bootstrap rejected execution: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
