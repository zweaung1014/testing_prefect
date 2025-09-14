"""
LLE Orchestration — Lite Skeleton (Demo)
- Adds time.sleep(2) inside each high-level workflow so you can see step-by-step execution.
- Returns stub outputs so the flow completes and writes results_<experiment_id>.json.
- No low-level/unit operations are implemented (just placeholders).
"""

from __future__ import annotations
from typing import Optional, Tuple, List, Dict, Any
from datetime import datetime
import uuid
import json
import time  # <-- for the 2-second sleeps

from prefect import flow, task, get_run_logger

try:
    from pydantic import BaseModel, Field, PositiveFloat, conint, confloat, ValidationError
except Exception:
    from pydantic.v1 import BaseModel, Field, PositiveFloat, conint, confloat, ValidationError


# -----------------------
# Run metadata
# -----------------------
class RunMeta(BaseModel):
    experiment_id: str
    operator: str
    notes: Optional[str] = None
    replicates: conint(ge=1, le=50) = 1
    randomize_order: bool = False


# -----------------------
# Workflow I/O (exactly your fields)
# -----------------------
class PrepareSolutionIn(BaseModel):
    amount_crude_solution_mL: PositiveFloat
    amount_dichloromethane_mL: PositiveFloat
    amount_aqueous_solution_mL: PositiveFloat
    amount_aqueous_to_dispense_mL: PositiveFloat
    pos_crude_solution: str
    pos_dichloromethane: str
    pos_aqueous_solution: str
    pos_extraction_vial: str

class PrepareSolutionOut(BaseModel):
    weight_crude_dispensed_g: Optional[PositiveFloat] = None
    weight_dcm_dispensed_g: Optional[PositiveFloat] = None
    weight_aqueous_dispensed_g: Optional[PositiveFloat] = None
    pH_measurement: Optional[float] = None

class InitiateExtractionIn(BaseModel):
    stirring_speed_rpm: conint(ge=0, le=5000)
    stirring_duration_s: conint(ge=0, le=24*3600)
    pos_extraction_vial: str
    easymax_location: str
    separation_time_s: conint(ge=0, le=24*3600)
    easymax_temperature_C: float

class InitiateExtractionOut(BaseModel):
    volume_aqueous_phase_mL: Optional[PositiveFloat] = None
    volume_organic_phase_mL: Optional[PositiveFloat] = None
    center_aq_layer_xy: Optional[Tuple[float, float]] = None
    center_or_layer_xy: Optional[Tuple[float, float]] = None

class PrepHPLCVialsIn(BaseModel):
    pos_extraction_vial_in_easymax: str
    pos_aqueous_vial: str
    pos_organic_vial: str
    amount_aqueous_phase_mL: PositiveFloat
    amount_methanol_in_aq_vial_mL: PositiveFloat
    amount_organic_phase_mL: PositiveFloat
    amount_methanol_in_or_vial_mL: PositiveFloat
    amount_TMB_in_aq_mL: PositiveFloat
    amount_TMB_in_or_mL: PositiveFloat
    pos_TMB: str

class PrepHPLCVialsOut(BaseModel):
    pH_aqueous: Optional[float] = None
    weight_hplc_vial_aq_empty_g: Optional[PositiveFloat] = None
    weight_hplc_vial_or_empty_g: Optional[PositiveFloat] = None
    weight_hplc_vial_with_aq_g: Optional[PositiveFloat] = None
    weight_hplc_vial_with_or_g: Optional[PositiveFloat] = None
    weight_hplc_vial_aq_with_IPA_TMB_g: Optional[PositiveFloat] = None
    weight_hplc_vial_or_with_IPA_TMB_g: Optional[PositiveFloat] = None

class StartHPLCAnalysisIn(BaseModel):
    pos_aq_vial_initial: str
    pos_or_vial_initial: str
    pos_aq_vial_hplc: str
    pos_or_vial_hplc: str
    hplc_run_length_min: PositiveFloat
    acetonitrile_percent: confloat(ge=0, le=100)
    sample_injection_volume_uL: PositiveFloat

class ChromMetrics(BaseModel):
    rt_amino_acid_min: Optional[PositiveFloat] = None
    rt_internal_std_min: Optional[PositiveFloat] = None
    area_amino_acid: Optional[PositiveFloat] = None
    area_internal_std: Optional[PositiveFloat] = None
    area_impurity: Optional[PositiveFloat] = None

class StartHPLCAnalysisOut(BaseModel):
    aqueous: ChromMetrics = ChromMetrics()
    organic: ChromMetrics = ChromMetrics()

class BOIn(BaseModel):
    composition_V_over_V: str
    stirring_speed_rpm: conint(ge=0, le=5000)
    stirring_duration_s: conint(ge=0, le=24*3600)
    temperature_C: float
    pH: float
    aqueous_solution_kind: str
    recovery_aq_pct: Optional[float] = None
    purity_aq_pct: Optional[float] = None
    recovery_or_pct: Optional[float] = None
    purity_or_pct: Optional[float] = None
    separation: Optional[bool] = None
    partition_coefficient_PC: Optional[PositiveFloat] = None

class BOOut(BaseModel):
    suggestion_composition_V_over_V: str
    suggestion_stirring_speed_rpm: int
    suggestion_stirring_duration_s: int
    suggestion_temperature_C: float
    suggestion_pH: float
    suggestion_aqueous_solution_kind: str


# -----------------------
# Plan shapes (simple)
# -----------------------
class SamplePlan(BaseModel):
    sample_id: str
    prepare_solution_in: PrepareSolutionIn
    initiate_extraction_in: InitiateExtractionIn
    prep_hplc_vials_in: PrepHPLCVialsIn
    start_hplc_analysis_in: StartHPLCAnalysisIn
    bo_in: Optional[BOIn] = None  # optional

class Plan(BaseModel):
    run: RunMeta
    samples: List[SamplePlan]


# -----------------------
# Workflows (demo: 2s sleeps + stub outputs)
# -----------------------
@task(name="Prepare solution")
def prepare_solution(inputs: PrepareSolutionIn) -> PrepareSolutionOut:
    logger = get_run_logger()
    logger.info("Prepare solution: starting")
    time.sleep(2)
    # Stub: use simple density guesses to generate non-null demo values
    rho_crude = 1.0   # g/mL (placeholder)
    rho_dcm   = 1.33  # g/mL (placeholder)
    rho_aq    = 1.0   # g/mL (placeholder water-like)
    out = PrepareSolutionOut(
        weight_crude_dispensed_g=inputs.amount_crude_solution_mL * rho_crude,
        weight_dcm_dispensed_g=inputs.amount_dichloromethane_mL * rho_dcm,
        weight_aqueous_dispensed_g=inputs.amount_aqueous_solution_mL * rho_aq,
        pH_measurement=None
    )
    logger.info("Prepare solution: done")
    return out

@task(name="Initiate extraction")
def initiate_extraction(inputs: InitiateExtractionIn) -> InitiateExtractionOut:
    logger = get_run_logger()
    logger.info("Initiate extraction: starting")
    time.sleep(2)
    # Stub: return placeholder volumes and dummy layer centers
    out = InitiateExtractionOut(
        volume_aqueous_phase_mL=None,
        volume_organic_phase_mL=None,
        center_aq_layer_xy=(100.0, 200.0),
        center_or_layer_xy=(100.0, 50.0),
    )
    logger.info("Initiate extraction: done")
    return out

@task(name="Prep HPLC vials")
def prep_hplc_vials(inputs: PrepHPLCVialsIn) -> PrepHPLCVialsOut:
    logger = get_run_logger()
    logger.info("Prep HPLC vials: starting")
    time.sleep(2)
    # Stub: pretend empty vial is 10 g, then add volumes as grams (placeholder)
    base_empty = 10.00
    aq_added = float(inputs.amount_aqueous_phase_mL)  # 1 g/mL placeholder
    or_added = float(inputs.amount_organic_phase_mL)  # 1 g/mL placeholder
    aq_meoh = float(inputs.amount_methanol_in_aq_vial_mL)
    or_meoh = float(inputs.amount_methanol_in_or_vial_mL)
    aq_tmb  = float(inputs.amount_TMB_in_aq_mL)
    or_tmb  = float(inputs.amount_TMB_in_or_mL)

    out = PrepHPLCVialsOut(
        pH_aqueous=None,
        weight_hplc_vial_aq_empty_g=base_empty,
        weight_hplc_vial_or_empty_g=base_empty,
        weight_hplc_vial_with_aq_g=base_empty + aq_added,
        weight_hplc_vial_with_or_g=base_empty + or_added,
        weight_hplc_vial_aq_with_IPA_TMB_g=base_empty + aq_added + aq_meoh + aq_tmb,
        weight_hplc_vial_or_with_IPA_TMB_g=base_empty + or_added + or_meoh + or_tmb,
    )
    logger.info("Prep HPLC vials: done")
    return out

@task(name="Start HPLC analysis")
def start_hplc_analysis(inputs: StartHPLCAnalysisIn) -> StartHPLCAnalysisOut:
    logger = get_run_logger()
    logger.info("Start HPLC analysis: starting")
    time.sleep(2)
    # Stub: fixed demo peaks/areas
    out = StartHPLCAnalysisOut(
        aqueous=ChromMetrics(
            rt_amino_acid_min=3.45,
            rt_internal_std_min=2.10,
            area_amino_acid=120000.0,
            area_internal_std=80000.0,
            area_impurity=5000.0
        ),
        organic=ChromMetrics(
            rt_amino_acid_min=3.42,
            rt_internal_std_min=2.08,
            area_amino_acid=95000.0,
            area_internal_std=78000.0,
            area_impurity=3000.0
        )
    )
    logger.info("Start HPLC analysis: done")
    return out

@task(name="Run Bayesian optimization model")
def run_bo_model(inputs: BOIn) -> BOOut:
    logger = get_run_logger()
    logger.info("Run BO model: starting")
    time.sleep(2)
    # Stub: echo back the same conditions as the "suggestion"
    out = BOOut(
        suggestion_composition_V_over_V=inputs.composition_V_over_V,
        suggestion_stirring_speed_rpm=int(inputs.stirring_speed_rpm),
        suggestion_stirring_duration_s=int(inputs.stirring_duration_s),
        suggestion_temperature_C=float(inputs.temperature_C),
        suggestion_pH=float(inputs.pH),
        suggestion_aqueous_solution_kind=inputs.aqueous_solution_kind,
    )
    logger.info("Run BO model: done")
    return out


# -----------------------
# Per-sample flow (simple chaining)
# -----------------------
@flow(name="process_sample")
def process_sample(plan: Plan, sample_id: str) -> Dict[str, Any]:
    logger = get_run_logger()
    s = next(sp for sp in plan.samples if sp.sample_id == sample_id)

    logger.info(f"[{sample_id}] 1) Prepare solution")
    ps_out = prepare_solution(s.prepare_solution_in)

    logger.info(f"[{sample_id}] 2) Initiate extraction")
    ie_out = initiate_extraction(s.initiate_extraction_in)

    logger.info(f"[{sample_id}] 3) Prep HPLC vials")
    pv_out = prep_hplc_vials(s.prep_hplc_vials_in)

    logger.info(f"[{sample_id}] 4) Start HPLC analysis")
    ha_out = start_hplc_analysis(s.start_hplc_analysis_in)

    bo_out = None
    if s.bo_in:
        logger.info(f"[{sample_id}] 5) Run BO model")
        bo_out = run_bo_model(s.bo_in)

    return {
        "run_id": str(uuid.uuid4()),
        "experiment_id": plan.run.experiment_id,
        "operator": plan.run.operator,
        "sample_id": sample_id,
        "notes": plan.run.notes,
        "outputs": {
            "prepare_solution": ps_out.model_dump(),
            "initiate_extraction": ie_out.model_dump(),
            "prep_hplc_vials": pv_out.model_dump(),
            "start_hplc_analysis": ha_out.model_dump(),
            "bo": (bo_out.model_dump() if bo_out else None),
        }
    }


# -----------------------
# Top-level flow
# -----------------------
@flow(name="lle_pipeline")
def lle_pipeline(plan: Plan) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Run {plan.run.experiment_id} — operator: {plan.run.operator}")
    sample_ids = [s.sample_id for s in plan.samples]

    results: List[Dict[str, Any]] = []
    for _ in range(plan.run.replicates):
        for sid in sample_ids:
            record = process_sample(plan, sid)
            results.append(record)

    out_path = f"results_{plan.run.experiment_id}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Saved results to {out_path}")
    return results


# -----------------------
# YAML loader and entry
# -----------------------
def load_plan_from_yaml(path: str) -> Plan:
    import yaml
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return Plan(**data)

if __name__ == "__main__":
    import sys
    plan_path = sys.argv[1] if len(sys.argv) > 1 else "plan.yml"
    plan = load_plan_from_yaml(plan_path)
    lle_pipeline(plan)
