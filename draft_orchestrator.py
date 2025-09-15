"""
LLE Orchestration — Prefect UI Demo
- Every high-level workflow is a @flow (so you see them in the UI).
- process_sample and lle_pipeline are also @flow.
- Each workflow sleeps for 2s and returns stub outputs.
"""
from operations import Operations
from __future__ import annotations
from typing import Optional, Tuple, List, Dict, Any
import uuid
import json
import time

from prefect import flow, get_run_logger

try:
    from pydantic import BaseModel, PositiveFloat, conint, confloat
except Exception:
    from pydantic.v1 import BaseModel, PositiveFloat, conint, confloat

# Initialize operations class
operations = Operations()

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
# Workflow I/O (your fields)
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
# Plan
# -----------------------
class SamplePlan(BaseModel):
    sample_id: str
    prepare_solution_in: PrepareSolutionIn
    initiate_extraction_in: InitiateExtractionIn
    prep_hplc_vials_in: PrepHPLCVialsIn
    start_hplc_analysis_in: StartHPLCAnalysisIn
    bo_in: Optional[BOIn] = None

class Plan(BaseModel):
    run: RunMeta
    samples: List[SamplePlan]


# -----------------------
# High-level workflows (each is a flow)
# -----------------------
@flow(name="Prepare solution")
def prepare_solution(inputs: PrepareSolutionIn) -> PrepareSolutionOut:
    logger = get_run_logger()
    logger.info("Prepare solution started")
    time.sleep(2)

    # Initialize deck
    operations.initialize_deck()
    operations.hold(1) # wait 1 min
    operations.heat() # specify heat

    # Move vial to scale
    operations.move_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_tray,
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale
    )

    # Weigh the empty vial
    operations.weigh_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_extraction_vial,
        "Empty vial"
    )

    # Move extraction vial back to its position
    operations.move_container(
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale,
        PrepareSolutionIn.pos_crude_solution,
        PrepareSolutionIn.type_extraction_tray
    )

    # Put the crude solution in extraction vial
    operations.charge_liquid(
        PrepareSolutionIn.amount_crude_solution_mL,
        PrepareSolutionIn.type_crude_solution_mL,
        PrepareSolutionIn.pos_crude_solution,
        PrepareSolutionIn.type_extraction_vial,
        PrepareSolutionIn.pos_extraction_vial
    )

    # Move vial to scale
    operations.move_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_tray,
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale
    )

    # Weigh vial with crude
    operations.weigh_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_extraction_vial,
        "Vial with crude"
    )

    # Move extraction vial back to its position
    operations.move_container(
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale,
        PrepareSolutionIn.pos_crude_solution,
        PrepareSolutionIn.type_extraction_tray
    )
    
    # Put dichloromethane in extraction vial
    operations.charge_liquid(
        PrepareSolutionIn.amount_dichloromethane_mL,
        PrepareSolutionIn.type_dichloromethane_mL,
        PrepareSolutionIn.pos_dichloromethane,
        PrepareSolutionIn.type_extraction_vial,
        PrepareSolutionIn.pos_extraction_vial
    )

    # Move vial to scale
    operations.move_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_tray,
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale
    )

    # Weight vial with crude + dichloromethane
    operations.weigh_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_extraction_vial,
        "Vial with crude+dichloromethane"
    )

    # Move extraction vial back to its position
    operations.move_container(
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale,
        PrepareSolutionIn.pos_crude_solution,
        PrepareSolutionIn.type_extraction_tray
    )

    # Put aqueous solution in extraction vial
    operations.charge_liquid(
        PrepareSolutionIn.amount_aqueous_solution_mL,
        PrepareSolutionIn.type_aqueous_solution_mL,
        PrepareSolutionIn.pos_aqueous_solution,
        PrepareSolutionIn.type_extraction_vial,
        PrepareSolutionIn.pos_extraction_vial
    )

    # Move vial to scale
    operations.move_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_tray,
        PrepareSolutionIn.weigh_scale,
        PrepareSolutionIn.type_weigh_scale
    )

    # Weigh vial with crude + dichloromethane + aqueous
    operations.weigh_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_extraction_vial,
        "Vial with crude+dichloromethane+aq"
    )

    """
    Insert the process for reading pH with the pH unit.
    put it back in original position and extract a sample for pH unit
    """

    # Move extraction vial to EasyMax
    operations.move_container(
        PrepareSolutionIn.pos_extraction_vial,
        PrepareSolutionIn.type_tray,
        PrepareSolutionIn.pos_reactor,
        PrepareSolutionIn.reactor_tray_type
    )

    return PrepareSolutionOut(
        weight_crude_dispensed_g=operations.trays[""][""],
        weight_dcm_dispensed_g=operations.trays[""][""],
        weight_aqueous_dispensed_g=operations.trays[""][""],
        pH_measurement=None,
    )

@flow(name="Initiate extraction")
def initiate_extraction(inputs: InitiateExtractionIn) -> InitiateExtractionOut:
    logger = get_run_logger()
    logger.info("Initiate extraction started")
    time.sleep(2)
    return InitiateExtractionOut(
        volume_aqueous_phase_mL=None,
        volume_organic_phase_mL=None,
        center_aq_layer_xy=(100.0, 200.0),
        center_or_layer_xy=(100.0, 50.0),
    )

@flow(name="Prep HPLC vials")
def prep_hplc_vials(inputs: PrepHPLCVialsIn) -> PrepHPLCVialsOut:
    logger = get_run_logger()
    logger.info("Prep HPLC vials started")
    time.sleep(2)
    base_empty = 10.0
    return PrepHPLCVialsOut(
        pH_aqueous=None,
        weight_hplc_vial_aq_empty_g=base_empty,
        weight_hplc_vial_or_empty_g=base_empty,
        weight_hplc_vial_with_aq_g=base_empty + float(inputs.amount_aqueous_phase_mL),
        weight_hplc_vial_with_or_g=base_empty + float(inputs.amount_organic_phase_mL),
        weight_hplc_vial_aq_with_IPA_TMB_g=base_empty + float(inputs.amount_aqueous_phase_mL) + float(inputs.amount_methanol_in_aq_vial_mL) + float(inputs.amount_TMB_in_aq_mL),
        weight_hplc_vial_or_with_IPA_TMB_g=base_empty + float(inputs.amount_organic_phase_mL) + float(inputs.amount_methanol_in_or_vial_mL) + float(inputs.amount_TMB_in_or_mL),
    )

@flow(name="Start HPLC analysis")
def start_hplc_analysis(inputs: StartHPLCAnalysisIn) -> StartHPLCAnalysisOut:
    logger = get_run_logger()
    logger.info("Start HPLC analysis started")
    time.sleep(2)
    return StartHPLCAnalysisOut(
        aqueous=ChromMetrics(
            rt_amino_acid_min=3.45,
            rt_internal_std_min=2.10,
            area_amino_acid=120000.0,
            area_internal_std=80000.0,
            area_impurity=5000.0,
        ),
        organic=ChromMetrics(
            rt_amino_acid_min=3.42,
            rt_internal_std_min=2.08,
            area_amino_acid=95000.0,
            area_internal_std=78000.0,
            area_impurity=3000.0,
        ),
    )

@flow(name="Run BO model")
def run_bo_model(inputs: BOIn) -> BOOut:
    logger = get_run_logger()
    logger.info("Run BO model started")
    time.sleep(2)
    return BOOut(
        suggestion_composition_V_over_V=inputs.composition_V_over_V,
        suggestion_stirring_speed_rpm=int(inputs.stirring_speed_rpm),
        suggestion_stirring_duration_s=int(inputs.stirring_duration_s),
        suggestion_temperature_C=float(inputs.temperature_C),
        suggestion_pH=float(inputs.pH),
        suggestion_aqueous_solution_kind=inputs.aqueous_solution_kind,
    )


# -----------------------
# Per-sample orchestration
# -----------------------
@flow(name="process_sample")
def process_sample(plan: Plan, sample_id: str) -> Dict[str, Any]:
    logger = get_run_logger()
    
    s = next(sp for sp in plan.samples if sp.sample_id == sample_id)

    ps_out = prepare_solution(s.prepare_solution_in)
    ie_out = initiate_extraction(s.initiate_extraction_in)
    pv_out = prep_hplc_vials(s.prep_hplc_vials_in)
    ha_out = start_hplc_analysis(s.start_hplc_analysis_in)
    bo_out = run_bo_model(s.bo_in) if s.bo_in else None

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
        },
    }


# -----------------------
# Top-level orchestration
# -----------------------
@flow(name="lle_pipeline")
def lle_pipeline(plan: Plan) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Run {plan.run.experiment_id} — operator: {plan.run.operator}")
    results: List[Dict[str, Any]] = []
    for _ in range(plan.run.replicates):
        for s in plan.samples:
            results.append(process_sample(plan, s.sample_id))
    out_path = f"results_{plan.run.experiment_id}.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    logger.info(f"Saved results to {out_path}")
    return results


# -----------------------
# YAML loader and entrypoint
# -----------------------
def load_plan_from_yaml(path: str) -> Plan:
    import yaml
    with open(path, "r") as f:
        data = yaml.safe_load(f)
    return Plan(**data)

if __name__ == "__main__":
    import sys
    plan_path = sys.argv[1] if len(sys.argv) > 1 else "plan.yaml"
    plan = load_plan_from_yaml(plan_path)
    lle_pipeline(plan)
