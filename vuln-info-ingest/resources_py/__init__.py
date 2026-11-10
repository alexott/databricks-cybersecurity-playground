"""Bundle resources in Python: the Genie space, only when `--var genie=true`.

Genie checks the tables of the space when it is created, and the tables exist only after the
jobs ran. So `databricks bundle deploy` leaves the space out by default (genie=false), and a
deploy with `--var genie=true --var warehouse_id=<id>` adds it. The space file uses the names
`fe_lakewatch_catalog.lakewatch_field`; the genie_agent_retarget mutator rewrites them to
${var.catalog}.${var.schema}.
"""
from __future__ import annotations

from databricks.bundles.core import Bundle, Resources, Variable, variables

from resources_py.genie import genie_space


@variables
class Variables:
    genie: Variable[bool]


def load_resources(bundle: Bundle) -> Resources:
    resources = Resources()
    if bundle.resolve_variable(Variables.genie):
        resources.add_genie_space("analysis_of_vulnerabilities", genie_space())
    return resources
