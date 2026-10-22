import os
import pathlib
import sys

# Source packages (common, cve_v5, ghsa, osv, kev) live under src/, as on Databricks.
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "src"))
# Spark's Python workers must run the same interpreter as the tests.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
