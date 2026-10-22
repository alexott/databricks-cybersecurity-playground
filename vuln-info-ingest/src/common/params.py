"""Job parameters: read every setting from notebook widgets (job parameters are
pushed into them) and derive table / Volume names. No defaults in code: the job
YAML is the single place where defaults live."""
from __future__ import annotations

COMMON_PARAMS: dict[str, type] = {"catalog": str, "schema": str, "volume": str}


def get_params(dbutils, spec: dict[str, type]) -> dict[str, object]:
    """Read the parameters in `spec` (name -> str/int/float), converting types.
    Raises ValueError naming the parameter when it is missing, empty or malformed."""
    out: dict[str, object] = {}
    for name, typ in spec.items():
        # Creating the widget lets the notebook also run interactively.
        dbutils.widgets.text(name, "")
        value = dbutils.widgets.get(name).strip()
        if not value:
            raise ValueError(f"missing required job parameter: {name}")
        try:
            out[name] = typ(value)
        except ValueError as e:
            raise ValueError(f"job parameter {name}={value!r} is not a valid {typ.__name__}") from e
    return out


def _quote(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def table_name(p: dict, name: str) -> str:
    """Fully qualified, quoted name of a table in the configured catalog/schema."""
    return ".".join(_quote(x) for x in (p["catalog"], p["schema"], name))


def volume_name(p: dict) -> str:
    """Fully qualified, quoted name of the scratch Volume."""
    return table_name(p, p["volume"])


def volume_path(p: dict, sub: str) -> str:
    """Path of a sub-directory on the scratch Volume."""
    return f"/Volumes/{p['catalog']}/{p['schema']}/{p['volume']}/{sub}"
