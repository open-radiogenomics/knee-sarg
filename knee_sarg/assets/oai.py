"""NIH Imaging Data Commons (OAI) dataset assets."""

from typing import List
from pathlib import Path

import polars as pl
import pandas as pd
from dagster import (
    asset,
    get_dagster_logger,
    DynamicPartitionsDefinition,
    AssetExecutionContext,
    Config,
)
from pydantic import Field

from ..resources import (
    OAISampler,
    OAI_SAMPLED_DIR,
    OaiPipeline,
    make_output_dir,
    OAI_COLLECTION_NAME,
)

log = get_dagster_logger()

patient_id_partitions_def = DynamicPartitionsDefinition(name="patient_id")
series_id_partitions_def = DynamicPartitionsDefinition(name="series_id")


@asset(
    partitions_def=patient_id_partitions_def,
    metadata={"partition_expr": "patient_id"},
)
def oai_samples(
    context: AssetExecutionContext, oai_sampler: OAISampler
) -> pl.DataFrame:
    """
    OAI Samples. Samples are placed in data/staged/oai/dagster/.
    """
    patient_id = context.partition_key
    series = oai_sampler.get_samples(patient_id)

    series_ids = series["series_id"].unique().to_list()
    context.instance.add_dynamic_partitions(series_id_partitions_def.name, series_ids)

    return series


class ThicknessImages(Config):
    required_output_files: List[str] = Field(
        default_factory=lambda: ["thickness_FC.png", "thickness_TC.png"],
        description="List of required output files",
    )


@asset(
    partitions_def=series_id_partitions_def,
    metadata={"partition_expr": "series_id"},
    op_tags={"gpu": ""},
)
def thickness_images(
    context: AssetExecutionContext, config: ThicknessImages, oai_pipeline: OaiPipeline
) -> pl.DataFrame:
    """
    Thickness Images. Generates thickness image for a series in data/collections/OAI_COLLECTION_NAME/patient_id/study_id/series_id.
    """
    series_id = context.partition_key
    # gather images we want to run the pipeline on
    staged_images_root: str = str(OAI_SAMPLED_DIR)
    patient_dirs = [d for d in Path(staged_images_root).iterdir() if d.is_dir()]
    study_dirs = [
        subdir
        for directory in patient_dirs
        for subdir in directory.iterdir()
        if subdir.is_dir()
    ]

    def get_first_file(series_dir: Path):
        return str(
            next((item for item in series_dir.iterdir() if not item.is_dir()), None)
        )

    series_dirs = [
        {
            "patient_id": study_dir.parent.name,
            "study_id": study_dir.name,
            "series_id": series_dir.name,
            "study_dir": study_dir,
            "series_dir": series_dir,
            "image_path": get_first_file(series_dir),
        }
        for study_dir in study_dirs
        for series_dir in (study_dir / "nifti").iterdir()
        if series_dir.is_dir()
    ]

    series = next(
        (item for item in series_dirs if item["series_id"] == series_id), None
    )
    if not series:
        raise Exception(
            f"Series {series_id} not found in staging dir: {staged_images_root}"
        )

    output_dir = make_output_dir(OAI_COLLECTION_NAME, series)
    computed_files_dir = output_dir / "cartilage_thickness"

    # todo surface errors in remote compute and check output
    oai_pipeline.run_pipeline(series["image_path"], str(computed_files_dir), series_id)

    # Check if specific files are in computed_files_dir
    expected_files = ["thickness_FC.png", "thickness_TC.png"]
    missing_files = []
    for file in expected_files:
        file_path = computed_files_dir / file
        if not file_path.exists():
            missing_files.append(file)

    if missing_files:
        raise Exception(
            f"The following files are missing in computed_files_dir: {missing_files}"
        )

    return pl.from_pandas(
        pd.DataFrame(
            [
                {
                    "patient_id": series["patient_id"],
                    "study_id": series["study_id"],
                    "series_id": series["series_id"],
                    "computed_files_dir": str(computed_files_dir),
                }
            ]
        ).astype(
            {
                "patient_id": "str",
                "study_id": "str",
                "series_id": "str",
                "computed_files_dir": "str",
            }
        )
    )
