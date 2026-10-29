"""Run Databricks SQL notebook source files on a SQL warehouse, one cell at a time.

The cells are separated by `-- COMMAND ----------`. Cells that have only comments (`-- MAGIC %md`) are skipped.
A cell that fails (for example, a test cell that calls `raise_error`) stops the run with exit code 1.

Usage:
  python tests/run_sql.py --profile PROFILE --warehouse-id ID --catalog CATALOG --schema SCHEMA FILE.sql [FILE.sql ...]
  python tests/run_sql.py --dry-run FILE.sql [FILE.sql ...]
"""
import argparse
import sys
import time


def cells(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    for cell in text.split("-- COMMAND ----------"):
        code = [line for line in cell.splitlines() if line.strip() and not line.lstrip().startswith("--")]
        if code:
            yield cell.strip().rstrip(";").strip()


def first_line(statement):
    return next(line for line in statement.splitlines() if line.strip() and not line.lstrip().startswith("--")).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile")
    parser.add_argument("--warehouse-id")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    parser.add_argument("--dry-run", action="store_true", help="print the statements, don't run them")
    parser.add_argument("files", nargs="+")
    args = parser.parse_args()

    if args.dry_run:
        for path in args.files:
            for statement in cells(path):
                print(f"{path}: {first_line(statement)[:120]}")
        return 0

    missing = [name for name in ("profile", "warehouse_id", "catalog", "schema") if getattr(args, name) is None]
    if missing:
        parser.error("missing: " + ", ".join("--" + name.replace("_", "-") for name in missing))

    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient(profile=args.profile)
    for path in args.files:
        for statement in cells(path):
            resp = w.statement_execution.execute_statement(
                statement=statement,
                warehouse_id=args.warehouse_id,
                catalog=args.catalog,
                schema=args.schema,
                wait_timeout="50s",
            )
            while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
                time.sleep(2)
                resp = w.statement_execution.get_statement(resp.statement_id)
            if resp.status.state != StatementState.SUCCEEDED:
                error = resp.status.error.message if resp.status.error else resp.status.state
                print(f"FAILED {path}: {first_line(statement)[:120]}\n{error}")
                return 1
            rows = resp.result.data_array if resp.result and resp.result.data_array else []
            result = rows[0][0] if rows and rows[0] else ""
            print(f"ok {path}: {first_line(statement)[:80]} {result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
