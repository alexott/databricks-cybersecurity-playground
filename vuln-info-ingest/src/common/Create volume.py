# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Create the scratch Volume used for staging and checkpoints
import os, sys
sys.path.append(os.path.abspath(".."))
from common.params import COMMON_PARAMS, get_params, volume_name

p = get_params(dbutils, COMMON_PARAMS)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name(p)}")
print(f"Volume {volume_name(p)} ready")
