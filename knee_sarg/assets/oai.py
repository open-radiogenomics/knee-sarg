"""NIH Imaging Data Commons (OAI) dataset assets."""

import os
from pathlib import Path

import pandas as pd
import polars as pl
from dagster import asset, get_dagster_logger

import itk

from ..resources import OAISampler, STAGED_DIR, OAI_COLLECTION_NAME

log = get_dagster_logger()

@asset()
def oai_samples(oai_sampler: OAISampler) -> pl.DataFrame:
    """
    OAI Samples. Samples are placed in data/staged/oai/dagster/.
    """
    return oai_sampler.get_samples()