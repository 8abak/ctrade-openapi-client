from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from datavis.control.config import ControlSettings, ensure_runtime_dirs, load_settings
from datavis.control.executor import RepairExecutor
from datavis.control.failure_detector import FailureDetector
from datavis.control.orchestrator import EngineeringOrchestrator
from datavis.control.research_manager import ResearchManager
from datavis.control.service_manager import ServiceManager
from datavis.control.smoke import SmokeRunner
from datavis.control.store import EngineeringStore
from datavis.control.supervisor import EngineeringSupervisor


@dataclass(frozen=True)
class ControlRuntime:
    settings: ControlSettings
    service_manager: ServiceManager
    store: EngineeringStore
    research_manager: ResearchManager
    supervisor: EngineeringSupervisor
    executor: RepairExecutor
    smoke_runner: SmokeRunner
    detector: FailureDetector
    orchestrator: EngineeringOrchestrator


@lru_cache(maxsize=1)
def get_control_runtime() -> ControlRuntime:
    settings = load_settings()
    ensure_runtime_dirs(settings)
    service_manager = ServiceManager(settings)
    store = EngineeringStore(settings)
    research_manager = ResearchManager(settings, service_manager)
    supervisor = EngineeringSupervisor(settings)
    executor = RepairExecutor(settings, store=store, research_manager=research_manager, service_manager=service_manager)
    smoke_runner = SmokeRunner(
        settings,
        store=store,
        research_manager=research_manager,
        service_manager=service_manager,
        supervisor=supervisor,
        executor=executor,
    )
    detector = FailureDetector(settings, store=store, research_manager=research_manager, service_manager=service_manager)
    orchestrator = EngineeringOrchestrator(
        settings,
        store=store,
        detector=detector,
        supervisor=supervisor,
        executor=executor,
        smoke_runner=smoke_runner,
        research_manager=research_manager,
    )
    return ControlRuntime(
        settings=settings,
        service_manager=service_manager,
        store=store,
        research_manager=research_manager,
        supervisor=supervisor,
        executor=executor,
        smoke_runner=smoke_runner,
        detector=detector,
        orchestrator=orchestrator,
    )
