# Cloud identifier UDFs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Pure-SQL Unity Catalog functions in `uc-udfs/` that parse AWS ARNs and IAM IDs, Azure resource IDs, GCP resource names, principals and service accounts, and cloud storage URIs, and map cloud identifiers to OCSF `cloud`/`resource_details` fields.

**Architecture:** Four Databricks notebook-source `.sql` files (one per cloud plus a cross-cloud file), each a set of `CREATE OR REPLACE FUNCTION ... RETURN <expr>` SQL UDFs. Public functions call helper functions to bind intermediate values (SQL UDFs have no local variables). Each file has a test notebook in `uc-udfs/tests/` that compares `to_json(<result>)` with expected JSON and calls `raise_error` on a mismatch; `uc-udfs/tests/run_sql.py` runs notebooks cell by cell on a SQL warehouse.

**Tech Stack:** Databricks SQL UDFs (Unity Catalog), Databricks SQL warehouse (ANSI mode), Python 3.10+ with `databricks-sdk` (Statement Execution API) for the test runner.

**Spec:** `docs/superpowers/specs/2026-10-29-cloud-identifier-udfs-design.md`

## Global Constraints

- Branch: `cloud-identifier-udfs` (already created; the spec is committed on it). Don't push.
- Every commit message ends with the line `Co-authored-by: Isaac <no-reply@databricks.com>`.
- Pure SQL only: no `LANGUAGE PYTHON`, no external packages. Every function is `DETERMINISTIC` and has a `COMMENT`.
- File format: the first line is `-- Databricks notebook source`, cells are separated by `-- COMMAND ----------`, the first cell is a `-- MAGIC %md` header (the same as `uc-udfs/ip_classification.sql`). One statement per cell.
- Invalid input and `NULL` give `NULL`; no function raises an error. `CASE` uses positive conditions (`WHEN <valid> THEN <struct> END`), never `WHEN <invalid> THEN NULL ELSE <struct>`.
- Function names, parameter names, struct field names and field order are exactly as in the code below: the tests compare JSON, so the field order matters.
- **No AWS key-shaped literals** (`AKIA`, `ASIA`, `AIDA`, `AROA`, `AGPA`, `AIPA`, `ANPA`, `ANVA`, `ABIA`, `ACCA`, `APKA`, `ASCA` followed by 12+ letters/digits) in any committed file, including this plan. Build them at run time with `concat('AK', 'IA', '...')`. The Databricks pre-commit secret-scan hook blocks them and must not be bypassed (`SKIP_SECRET_SCAN` is not allowed).
- **No values from the user's sample logs** in the repo: only placeholder subscription IDs (`aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e`), tenant IDs (`aaaabbbb-0000-cccc-1111-dddd2222eeee`), GUIDs, project IDs (`my-project`) and names.
- **Databricks connection:** never choose a Databricks CLI profile yourself. Before Task 2, ask the user for the profile, the SQL warehouse ID, and the catalog and schema to create the test functions in, then export them:
  ```bash
  export PROFILE=<profile> WAREHOUSE_ID=<warehouse id> CATALOG=<catalog> SCHEMA=<schema>
  ```
  All `run_sql.py` commands below use these variables and run from the repository root.
- Python: `databricks-sdk` must be importable (`python3 -c "import databricks.sdk"`).

## Review Focus

1. **Nested UDF cost on Databricks:** `cloud_resource_to_ocsf` calls helpers 4–5 levels deep. A person runs it on a large log table and expects it to take about as long as the other UDFs in `uc-udfs`. Task 5 Step 6 runs it on 1,000,000 generated IDs and checks the time.
2. **Spaces around values** (CSV ingests, hand-typed filters): the result is expected to be the same as for the trimmed value. Each task has trim test cases (`'  arn:aws:...  '`, `' user:alex@example.com '`, `' s3://my-bucket/k '`, …).
3. **Real Azure log shapes:** Entra ID `/tenants/<id>/providers/Microsoft.aadiam` in three casings, JSON-quoted `newValue` in Entra audit logs, lowercase Log Analytics `ResourceUri`, uppercase NSG/Key Vault IDs, and the application-ID GUID in sign-in logs (`NULL`). They are in the Task 3 tests.
4. **`NULL` input** gives a `NULL` result, not a struct of `NULL` fields (`to_json` gives `{}` for the latter). Every test cell has a `(NULL, NULL)` row and an empty-string row.
5. **An identifier from another cloud** given to a parser (an S3 URI to `gcp_resource_name_parse`, a quoted Azure ID to `cloud_resource_to_ocsf`, an Entra GUID to `azure_resource_id_parse`) is expected to give `NULL` or the correct cloud. These cases are in the Task 3, 4 and 5 tests.

## Files

| File | Responsibility |
|---|---|
| `uc-udfs/tests/run_sql.py` (create) | runs `.sql` notebook-source files cell by cell on a SQL warehouse; exits 1 on the first failed cell |
| `uc-udfs/aws_identifiers.sql` (create) | `aws_arn_parse`, `aws_role_name`, `aws_assumed_role_to_role_arn`, `aws_unique_id_type`, `aws_account_id_from_access_key` + helpers |
| `uc-udfs/azure_identifiers.sql` (create) | `azure_resource_id_parse`, `azure_resource_id_normalize`, `azure_resource_id_clean` + helpers |
| `uc-udfs/gcp_identifiers.sql` (create) | `gcp_resource_name_parse`, `gcp_principal_parse`, `gcp_service_account_parse` + helpers |
| `uc-udfs/cloud_resources.sql` (create) | `storage_uri_parse`, `cloud_resource_to_ocsf` + helpers; uses the three files above |
| `uc-udfs/tests/<file>_test.sql` (create, one per file) | test cells: `VALUES (input, expected)` → `raise_error` listing every failed case |
| `uc-udfs/README.md` (modify) | one bullet per new file, and a "Tests" paragraph |

The code in this plan was checked on a local Spark 3.5 in ANSI mode (with the SQL UDF bodies inlined by a scratch script). All tests passed there. Running them on Databricks is the real check.

---

### Task 1: Test runner

**Files:**
- Create: `uc-udfs/tests/run_sql.py`
- Modify: `uc-udfs/README.md` (append a "Tests" paragraph)

**Interfaces:**
- Produces: `python3 uc-udfs/tests/run_sql.py --profile P --warehouse-id W --catalog C --schema S FILE.sql [FILE.sql ...]`, which prints `ok <file>: <first line> <first value of the result>` per cell and returns exit code 1 with `FAILED <file>: <first line>` and the error message on the first failed cell; `--dry-run FILE.sql ...` prints `<file>: <first line>` per cell without connecting.

- [ ] **Step 1: Create `uc-udfs/tests/run_sql.py`**

```python
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
```

- [ ] **Step 2: Check the cell splitting with a dry run**

Run: `python3 uc-udfs/tests/run_sql.py --dry-run uc-udfs/ip_classification.sql | head -3`

Expected:
```
uc-udfs/ip_classification.sql: CREATE OR REPLACE FUNCTION ip_is_private_binary(ip BINARY)
uc-udfs/ip_classification.sql: CREATE OR REPLACE FUNCTION ip_is_private(ip STRING)
uc-udfs/ip_classification.sql: CREATE OR REPLACE FUNCTION ip_is_loopback_binary(ip BINARY)
```
(the `-- MAGIC %md` header cell is skipped).

Run: `python3 uc-udfs/tests/run_sql.py --profile x uc-udfs/ip_classification.sql; echo $?`

Expected: `run_sql.py: error: missing: --warehouse-id, --catalog, --schema` and `2`.

- [ ] **Step 3: Add the "Tests" paragraph to `uc-udfs/README.md`**

Append at the end of the file:

```markdown

Tests: `tests/` has a test notebook for each file that has them. Each test cell fails with `raise_error` and lists the failed cases.
Create the functions and run the tests on a SQL warehouse with `tests/run_sql.py` (it needs `databricks-sdk`), for example:

    python3 uc-udfs/tests/run_sql.py --profile <profile> --warehouse-id <id> --catalog <catalog> --schema <schema> \
      uc-udfs/aws_identifiers.sql uc-udfs/tests/aws_identifiers_test.sql
```

- [ ] **Step 4: Commit**

```bash
git add uc-udfs/tests/run_sql.py uc-udfs/README.md
git commit -m "uc-udfs: test runner for SQL notebook files

Co-authored-by: Isaac <no-reply@databricks.com>"
```

Expected: the secret-scan pre-commit hook passes. If it reports `aws-access-token`, a key-shaped literal is in the staged files: build it with `concat` (see Global Constraints). Never bypass the hook.

---

### Task 2: AWS identifiers

Before this task: ask the user for the Databricks profile, warehouse ID, catalog and schema, and export `PROFILE`, `WAREHOUSE_ID`, `CATALOG`, `SCHEMA` (Global Constraints).

**Files:**
- Create: `uc-udfs/aws_identifiers.sql`
- Test: `uc-udfs/tests/aws_identifiers_test.sql`
- Modify: `uc-udfs/README.md`

**Interfaces:**
- Consumes: `uc-udfs/tests/run_sql.py` (Task 1).
- Produces (used by Task 5):
  - ``aws_arn_parse(arn STRING) RETURNS STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>``
  - `aws_role_name(arn STRING) RETURNS STRING`, `aws_assumed_role_to_role_arn(arn STRING) RETURNS STRING`
  - `aws_unique_id_type(id STRING) RETURNS STRING`, `aws_account_id_from_access_key(key_id STRING) RETURNS STRING`
  - helpers: `aws_arn_parse_parts(p ARRAY<STRING>)`, `aws_role_name_parsed(a <aws_arn_parse struct>)`, `aws_assumed_role_to_role_arn_parsed(a <aws_arn_parse struct>)`

- [ ] **Step 1: Write the test notebook `uc-udfs/tests/aws_identifiers_test.sql`**

The key-shaped values are built with `concat` (Global Constraints). The access key vectors are from Aidan Steele's write-up and were checked against a Python implementation of the decoding.

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `aws_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../aws_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The values that look like AWS keys (`AKIA...`, `AIDA...`, ...) are built with `concat`, because the repository secret scanner blocks them.
-- MAGIC They are the examples from the AWS documentation and from the published write-ups about access key IDs, not real keys.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_arn_parse',
          raise_error(concat('aws_arn_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(aws_arn_parse(input)) AS actual
    FROM VALUES
      ('arn:aws:iam::123456789012:user/John',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/John","resource_type":"user","resource_id":"John"}'),
      ('arn:aws:iam::123456789012:user/division_abc/subdivision_xyz/Jane',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/division_abc/subdivision_xyz/Jane","resource_type":"user","resource_id":"division_abc/subdivision_xyz/Jane"}'),
      ('arn:aws:iam::123456789012:group/division_abc/subdivision_xyz/product_A/Developers',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"group/division_abc/subdivision_xyz/product_A/Developers","resource_type":"group","resource_id":"division_abc/subdivision_xyz/product_A/Developers"}'),
      ('arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer","resource_type":"role","resource_id":"aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer"}'),
      ('arn:aws:iam::123456789012:root',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws:sts::123456789012:self',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"self","resource_id":"self"}'),
      ('arn:aws:sts::123456789012:federated-user/Paulo',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"federated-user/Paulo","resource_type":"federated-user","resource_id":"Paulo"}'),
      ('arn:aws:sts::123456789012:assumed-role/Accounting-Role/Mary',
       '{"partition":"aws","service":"sts","account_id":"123456789012","resource":"assumed-role/Accounting-Role/Mary","resource_type":"assumed-role","resource_id":"Accounting-Role/Mary"}'),
      ('arn:aws:iam::123456789012:u2f/user/John/default',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"u2f/user/John/default","resource_type":"u2f","resource_id":"user/John/default"}'),
      ('arn:aws:iam::123456789012:oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"oidc-provider/oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111","resource_type":"oidc-provider","resource_id":"oidc.eks.us-west-2.amazonaws.com/id/a1b2c3d4567890abcdefEXAMPLE11111"}'),
      ('arn:aws:iam::aws:contextProvider/IdentityCenter',
       '{"partition":"aws","service":"iam","account_id":"aws","resource":"contextProvider/IdentityCenter","resource_type":"contextProvider","resource_id":"IdentityCenter"}'),
      ('arn:aws:sns:us-east-1:123456789012:example-sns-topic-name',
       '{"partition":"aws","service":"sns","region":"us-east-1","account_id":"123456789012","resource":"example-sns-topic-name","resource_id":"example-sns-topic-name"}'),
      ('arn:aws:ec2:us-east-1:123456789012:vpc/vpc-0e9801d129EXAMPLE',
       '{"partition":"aws","service":"ec2","region":"us-east-1","account_id":"123456789012","resource":"vpc/vpc-0e9801d129EXAMPLE","resource_type":"vpc","resource_id":"vpc-0e9801d129EXAMPLE"}'),
      ('arn:aws:ec2:*:123456789012:vpc/*',
       '{"partition":"aws","service":"ec2","region":"*","account_id":"123456789012","resource":"vpc/*","resource_type":"vpc","resource_id":"*"}'),
      ('arn:aws:qbusiness:*:123456789012:*',
       '{"partition":"aws","service":"qbusiness","region":"*","account_id":"123456789012","resource":"*","resource_id":"*"}'),
      ('arn:aws:iam::*:user/division_abc/subdivision_xyz/Richard',
       '{"partition":"aws","service":"iam","account_id":"*","resource":"user/division_abc/subdivision_xyz/Richard","resource_type":"user","resource_id":"division_abc/subdivision_xyz/Richard"}'),
      ('arn:aws:lambda:us-east-2:123456789012:function:my-function:prod',
       '{"partition":"aws","service":"lambda","region":"us-east-2","account_id":"123456789012","resource":"function:my-function:prod","resource_type":"function","resource_id":"my-function:prod"}'),
      ('arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/x:*',
       '{"partition":"aws","service":"logs","region":"us-east-1","account_id":"123456789012","resource":"log-group:/aws/lambda/x:*","resource_type":"log-group","resource_id":"/aws/lambda/x:*"}'),
      ('arn:aws:s3:::example_bucket',
       '{"partition":"aws","service":"s3","resource":"example_bucket","resource_type":"bucket","resource_id":"example_bucket"}'),
      ('arn:aws:s3:::example_bucket/marketing/newproductlaunch/widget/*',
       '{"partition":"aws","service":"s3","resource":"example_bucket/marketing/newproductlaunch/widget/*","resource_type":"object","resource_id":"example_bucket/marketing/newproductlaunch/widget/*"}'),
      ('arn:aws:s3:::amzn-s3-demo-bucket/????-test',
       '{"partition":"aws","service":"s3","resource":"amzn-s3-demo-bucket/????-test","resource_type":"object","resource_id":"amzn-s3-demo-bucket/????-test"}'),
      ('arn:aws:s3:us-west-2:123456789012:accesspoint/my-access-point',
       '{"partition":"aws","service":"s3","region":"us-west-2","account_id":"123456789012","resource":"accesspoint/my-access-point","resource_type":"accesspoint","resource_id":"my-access-point"}'),
      ('arn:aws-cn:iam::123456789012:root',
       '{"partition":"aws-cn","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws-us-gov:iam::123456789012:root',
       '{"partition":"aws-us-gov","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('  arn:aws:iam::123456789012:root  ',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"root","resource_id":"root"}'),
      ('arn:aws:iam::123456789012:user/',
       '{"partition":"aws","service":"iam","account_id":"123456789012","resource":"user/","resource_type":"user"}'),
      ('arn:aws:sqs', NULL),
      ('arn:aws:iam::123456789012:', NULL),
      ('arn:azure:iam::123456789012:root', NULL),
      ('AWS Internal', NULL),
      ('123456789012', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_role_name, aws_assumed_role_to_role_arn',
          raise_error(concat('aws_role_name, aws_assumed_role_to_role_arn: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(name, 'NULL'), ' / ', coalesce(role_arn, 'NULL'),
                ', expected ', coalesce(expected_name, 'NULL'), ' / ', coalesce(expected_role_arn, 'NULL')) AS failure
  FROM (
    SELECT input, expected_name, expected_role_arn, aws_role_name(input) AS name, aws_assumed_role_to_role_arn(input) AS role_arn
    FROM VALUES
      ('arn:aws:sts::123456789012:assumed-role/Accounting-Role/Mary', 'Accounting-Role', 'arn:aws:iam::123456789012:role/Accounting-Role'),
      ('arn:aws:sts::123456789012:assumed-role/AWSReservedSSO_Admin_0123456789abcdef/alice@example.com',
       'AWSReservedSSO_Admin_0123456789abcdef', 'arn:aws:iam::123456789012:role/AWSReservedSSO_Admin_0123456789abcdef'),
      ('arn:aws-us-gov:sts::123456789012:assumed-role/Ops/i-0abc', 'Ops', 'arn:aws-us-gov:iam::123456789012:role/Ops'),
      ('arn:aws:iam::123456789012:role/S3Access', 'S3Access', 'arn:aws:iam::123456789012:role/S3Access'),
      ('arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess', 'RDSAccess',
       'arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess'),
      ('arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer',
       'AWSServiceRoleForAccessAnalyzer', 'arn:aws:iam::123456789012:role/aws-service-role/access-analyzer.amazonaws.com/AWSServiceRoleForAccessAnalyzer'),
      ('arn:aws:iam::123456789012:role/service-role/QuickSightAction', 'QuickSightAction',
       'arn:aws:iam::123456789012:role/service-role/QuickSightAction'),
      ('arn:aws:sts::123456789012:federated-user/Paulo', NULL, NULL),
      ('arn:aws:iam::123456789012:user/John', NULL, NULL),
      ('arn:aws:iam::123456789012:root', NULL, NULL),
      ('AWS Internal', NULL, NULL),
      (NULL, NULL, NULL)
      AS t(input, expected_name, expected_role_arn)
  )
  WHERE NOT (name <=> expected_name AND role_arn <=> expected_role_arn)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_unique_id_type',
          raise_error(concat('aws_unique_id_type: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, aws_unique_id_type(input) AS actual
    FROM VALUES
      -- examples from the "Unique identifiers" section of the IAM identifiers page
      (concat('AI', 'DA', 'JQABLZS4A3QDU576Q'), 'user'),
      (concat('AI', 'DA', 'CKCEVSQ6C2EXAMPLE'), 'user'),
      (concat('AR', 'OA', 'DBQP57FF2AEXAMPLE'), 'role'),
      (concat('AR', 'OA', 'DBQP57FF2AEXAMPLE', ':role-session-name'), 'role'),
      (concat('AR', 'OA', '1234567890EXAMPLE'), 'role'),
      -- the AWS documentation example access key ID
      (concat('AK', 'IA', 'IOSFODNN7EXAMPLE'), 'access_key'),
      -- one ID for each prefix
      (concat('AB', 'IA', 'EXAMPLE123456789'), 'sts_bearer_token'),
      (concat('AC', 'CA', 'EXAMPLE123456789'), 'context_specific_credential'),
      (concat('AG', 'PA', 'EXAMPLE123456789'), 'group'),
      (concat('AI', 'PA', 'EXAMPLE123456789'), 'instance_profile'),
      (concat('AN', 'PA', 'EXAMPLE123456789'), 'managed_policy'),
      (concat('AN', 'VA', 'EXAMPLE123456789'), 'managed_policy_version'),
      (concat('AP', 'KA', 'EXAMPLE123456789'), 'public_key'),
      (concat('AS', 'CA', 'EXAMPLE123456789'), 'certificate'),
      (concat('AS', 'IA', 'EXAMPLE123456789'), 'temporary_access_key'),
      ('111122223333', NULL),
      ('XXXXJQABLZS4A3QDU576Q', NULL),
      (lower(concat('AI', 'DA', 'JQABLZS4A3QDU576Q')), NULL),
      (concat('AI', 'DA', 'SHORT'), NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: aws_account_id_from_access_key',
          raise_error(concat('aws_account_id_from_access_key: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, aws_account_id_from_access_key(input) AS actual
    FROM VALUES
      -- vectors from https://awsteele.com/blog/2020/09/26/aws-access-key-format.html
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV7A'), '609629065308'),
      (concat('AS', 'IA', 'Y34FZKBNKMUTVV7A'), '609629065306'),
      (concat('AS', 'IA', 'Y34FZKAOKMUTVV7A'), '609629065244'),
      (concat('AS', 'IA', 'QAAAAAAAAAAAAAAA'), '000000000000'),
      (concat('AS', 'IA', '6RVFFB77AAAAAAAA'), '999999999998'),
      (concat('AK', 'IA', 'Y34FZKBOKMUTVV7A'), '609629065308'),
      -- a key created before 2019-03-29 (the AWS documentation example) doesn't encode the account ID
      (concat('AK', 'IA', 'IOSFODNN7EXAMPLE'), NULL),
      (concat('AR', 'OA', 'Y34FZKBOKMUTVV7A'), NULL),
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV7'), NULL),
      (concat('AS', 'IA', 'Y34FZKBOKMUTVV71'), NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
```

- [ ] **Step 2: Run the tests to verify that they fail**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/tests/aws_identifiers_test.sql`

Expected: `FAILED uc-udfs/tests/aws_identifiers_test.sql: SELECT if(count(*) = 0, ...` with `[UNRESOLVED_ROUTINE] Cannot resolve routine `aws_arn_parse``, exit code 1.

- [ ] **Step 3: Create `uc-udfs/aws_identifiers.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse AWS identifiers
-- MAGIC
-- MAGIC These SQL UDFs parse [ARNs](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html)
-- MAGIC and [IAM unique IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html) in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `aws_arn_parse(arn)` | `STRUCT<partition, service, region, account_id, resource, resource_type, resource_id>` |
-- MAGIC | `aws_role_name(arn)` | the role name from an IAM role ARN or an STS assumed-role ARN |
-- MAGIC | `aws_assumed_role_to_role_arn(arn)` | the IAM role ARN for an STS assumed-role ARN (an IAM role ARN is returned unchanged) |
-- MAGIC | `aws_unique_id_type(id)` | the type of an IAM unique ID or access key ID from its prefix: `access_key`, `temporary_access_key`, `user`, `role`, ... |
-- MAGIC | `aws_account_id_from_access_key(key_id)` | the 12-digit account ID encoded in an `AKIA`/`ASIA` access key ID |
-- MAGIC | `aws_arn_parse_parts`, `aws_role_name_parsed`, `aws_assumed_role_to_role_arn_parsed` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - An invalid value or `NULL` gives `NULL`. The functions don't raise errors, so you can use them on log fields that sometimes don't have an ARN (`AWS Internal`, an account ID).
-- MAGIC - `aws_arn_parse` splits `resource` at the first `/` or `:`: `instance/i-0abc` gives `instance` and `i-0abc`, `function:fn:prod` gives `function` and `fn:prod`.
-- MAGIC   If there is no separator, `resource_type` is `NULL` (`arn:aws:sns:us-east-1:123456789012:topic`). The IAM path stays in `resource_id` (`role/division/Admin` gives `division/Admin`).
-- MAGIC - S3 bucket and object ARNs (`arn:aws:s3:::bucket/key`) don't have a type: `resource_type` is `bucket` or `object`, and `resource_id` is the full resource.
-- MAGIC - An empty region or account gives `NULL`. Wildcards (`*`) and the account `aws` (`arn:aws:iam::aws:policy/...`) are kept.
-- MAGIC - Incomplete ARNs (`arn:aws:sqs`, allowed in IAM policies) give `NULL`.
-- MAGIC - **Role paths:** an assumed-role ARN (`arn:aws:sts::123456789012:assumed-role/Admin/alice`) doesn't have the role path. So `aws_assumed_role_to_role_arn` doesn't give the
-- MAGIC   inventory ARN for roles with a path: IAM Identity Center roles (`aws-reserved/sso.amazonaws.com/`), service roles (`service-role/`), and service-linked roles (`aws-service-role/`).
-- MAGIC   For these roles, join on the account ID and `aws_role_name(arn)`.
-- MAGIC - `aws_unique_id_type` ignores a `:...` suffix (`AROA...:session-name`, the form of `aws:userId` and of CloudTrail `userIdentity.principalId`).
-- MAGIC - `aws_account_id_from_access_key` gives the same result as `aws sts get-access-key-info`, without an API call. It uses the
-- MAGIC   [decoding found by Aidan Steele and Tal Be'ery](https://awsteele.com/blog/2020/09/26/aws-access-key-format.html).
-- MAGIC   Keys created before 2019-03-29 (5th character before `Q`) don't encode the account ID, so they give `NULL`.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_arn_parse_parts(p ARRAY<STRING>)
  RETURNS STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for aws_arn_parse: builds the result from the ARN split on the first 5 colons'
  RETURN CASE
    WHEN size(p) = 6 AND get(p, 0) = 'arn' AND get(p, 1) RLIKE '^aws[a-z-]*$' AND get(p, 2) != '' AND get(p, 5) != '' THEN named_struct(
      'partition', get(p, 1),
      'service', get(p, 2),
      'region', nullif(get(p, 3), ''),
      'account_id', nullif(get(p, 4), ''),
      'resource', get(p, 5),
      'resource_type', CASE
        WHEN get(p, 2) = 's3' AND get(p, 4) = '' THEN if(get(p, 5) LIKE '%/%', 'object', 'bucket')
        ELSE nullif(regexp_extract(get(p, 5), '^([^/:]*)[/:]', 1), '')
      END,
      'resource_id', CASE
        WHEN get(p, 2) = 's3' AND get(p, 4) = '' THEN get(p, 5)
        WHEN get(p, 5) RLIKE '[/:]' THEN nullif(regexp_extract(get(p, 5), '^[^/:]*[/:](.*)$', 1), '')
        ELSE get(p, 5)
      END)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_arn_parse(arn STRING)
  RETURNS STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>
  DETERMINISTIC
  COMMENT 'Parses an AWS ARN into partition, service, region, account_id, resource, resource_type and resource_id. Returns NULL for an invalid ARN'
  RETURN aws_arn_parse_parts(split(trim(arn), ':', 6));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_role_name_parsed(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Helper for aws_role_name: gets the role name from the result of aws_arn_parse'
  RETURN CASE
    WHEN a.service = 'iam' AND a.resource_type = 'role' THEN nullif(element_at(split(a.resource_id, '/'), -1), '')
    WHEN a.service = 'sts' AND a.resource_type = 'assumed-role' THEN nullif(get(split(a.resource_id, '/'), 0), '')
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_role_name(arn STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the role name from an IAM role ARN (last path segment) or an STS assumed-role ARN. Returns NULL for other values'
  RETURN aws_role_name_parsed(aws_arn_parse(arn));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_assumed_role_to_role_arn_parsed(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Helper for aws_assumed_role_to_role_arn: builds the IAM role ARN from the result of aws_arn_parse'
  RETURN CASE
    WHEN a.service = 'iam' AND a.resource_type = 'role' THEN concat('arn:', a.`partition`, ':iam::', a.account_id, ':', a.resource)
    WHEN a.service = 'sts' AND a.resource_type = 'assumed-role' AND nullif(get(split(a.resource_id, '/'), 0), '') IS NOT NULL
      THEN concat('arn:', a.`partition`, ':iam::', a.account_id, ':role/', get(split(a.resource_id, '/'), 0))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_assumed_role_to_role_arn(arn STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Converts an STS assumed-role ARN to the IAM role ARN (without the role path, which the assumed-role ARN does not have). An IAM role ARN is returned unchanged. Returns NULL for other values'
  RETURN aws_assumed_role_to_role_arn_parsed(aws_arn_parse(arn));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_unique_id_type(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the type of an IAM unique ID or access key ID from its prefix (access_key, temporary_access_key, user, role, group, ...). A :suffix is ignored. Returns NULL for an unknown prefix or an invalid value'
  RETURN CASE
    WHEN id RLIKE '^[A-Z0-9]{16,128}(:.*)?$' THEN CASE substr(id, 1, 4)
      WHEN 'ABIA' THEN 'sts_bearer_token'
      WHEN 'ACCA' THEN 'context_specific_credential'
      WHEN 'AGPA' THEN 'group'
      WHEN 'AIDA' THEN 'user'
      WHEN 'AIPA' THEN 'instance_profile'
      WHEN 'AKIA' THEN 'access_key'
      WHEN 'ANPA' THEN 'managed_policy'
      WHEN 'ANVA' THEN 'managed_policy_version'
      WHEN 'APKA' THEN 'public_key'
      WHEN 'AROA' THEN 'role'
      WHEN 'ASCA' THEN 'certificate'
      WHEN 'ASIA' THEN 'temporary_access_key'
    END
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aws_account_id_from_access_key(key_id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the 12-digit AWS account ID encoded in an AKIA/ASIA access key ID (the same as aws sts get-access-key-info). Returns NULL for keys created before 2019-03-29 and for invalid values'
  RETURN CASE
    -- the 5th character must be Q-Z or 2-7: older keys do not encode the account ID
    WHEN key_id RLIKE '^(AKIA|ASIA)[Q-Z2-7][A-Z2-7]{15}$' THEN lpad(
      CAST(shiftright(
        CAST(conv(substr(array_join(transform(sequence(5, 14),
          -- base32: each character is 5 bits
          i -> lpad(conv(CAST(instr('ABCDEFGHIJKLMNOPQRSTUVWXYZ234567', substr(key_id, i, 1)) - 1 AS STRING), 10, 2), 5, '0')), ''),
          1, 48), 2, 10) AS BIGINT)
        -- 0x7fffffffff80: drop the top bit and the low 7 bits of the first 6 bytes
        & 140737488355200, 7) AS STRING),
      12, '0')
  END;
```

- [ ] **Step 4: Run the tests to verify that they pass**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/aws_identifiers.sql uc-udfs/tests/aws_identifiers_test.sql`

Expected: one `ok` line per `CREATE OR REPLACE FUNCTION` and per test cell (for example `ok uc-udfs/tests/aws_identifiers_test.sql: SELECT if(count(*) = 0, 'ok: ... ok: ...`), exit code 0.
If a test cell fails, its error lists every failed case as `input => actual, expected expected`. Fix the function, not the expected value, unless the expected value contradicts the spec (then stop and ask).

- [ ] **Step 5: Add the README entry**

In `uc-udfs/README.md`, add this bullet after the `tldextract.sql` bullet:

```markdown
- `aws_identifiers.sql` contains functions to parse AWS identifiers: `aws_arn_parse` (an ARN into partition, service, region, account ID and resource), `aws_role_name` and `aws_assumed_role_to_role_arn` (to join CloudTrail assumed-role sessions with IAM roles), `aws_unique_id_type` (the type of an IAM unique ID or access key ID from its prefix), and `aws_account_id_from_access_key` (the account ID encoded in an access key ID). They are pure SQL.
```

- [ ] **Step 6: Commit**

```bash
git add uc-udfs/aws_identifiers.sql uc-udfs/tests/aws_identifiers_test.sql uc-udfs/README.md
git commit -m "uc-udfs: AWS ARN and IAM identifier functions

Co-authored-by: Isaac <no-reply@databricks.com>"
```

Expected: the secret-scan pre-commit hook passes. If it reports `aws-access-token`, a key-shaped literal is in the staged files: build it with `concat` (see Global Constraints). Never bypass the hook.

---

### Task 3: Azure resource IDs

**Files:**
- Create: `uc-udfs/azure_identifiers.sql`
- Test: `uc-udfs/tests/azure_identifiers_test.sql`
- Modify: `uc-udfs/README.md`

**Interfaces:**
- Consumes: `uc-udfs/tests/run_sql.py` (Task 1).
- Produces (used by Task 5):
  - `azure_resource_id_parse(id STRING) RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING, provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>`
  - `azure_resource_id_clean(id STRING) RETURNS STRING`, `azure_resource_id_normalize(id STRING) RETURNS STRING`
  - helpers: `azure_resource_id_parse_parts(s STRING, prefix STRING, t ARRAY<STRING>)`, `azure_resource_id_parse_cleaned(s STRING)`

- [ ] **Step 1: Write the test notebook `uc-udfs/tests/azure_identifiers_test.sql`**

The cases are the formats from the ARM template functions documentation and the ID shapes found in the Azure sample logs, with placeholder IDs and names only.

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `azure_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../azure_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The IDs are the formats from the ARM template functions documentation and the ID shapes found in Azure logs
-- MAGIC (Entra ID audit, sign-in and diagnostic logs, NSG and Key Vault logs, Log Analytics exports), with placeholder subscription and tenant IDs and names.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: azure_resource_id_parse',
          raise_error(concat('azure_resource_id_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(azure_resource_id_parse(input)) AS actual
    FROM VALUES
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/otherResourceGroup/providers/Microsoft.Storage/storageAccounts/examplestorage',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"otherResourceGroup","provider_namespace":"Microsoft.Storage","resource_type":"Microsoft.Storage/storageAccounts","resource_name":"examplestorage","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/otherResourceGroup"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/examplegroup/providers/Microsoft.SQL/servers/serverName/databases/databaseName',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"examplegroup","provider_namespace":"Microsoft.SQL","resource_type":"Microsoft.SQL/servers/databases","resource_name":"databaseName","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/examplegroup/providers/Microsoft.SQL/servers/serverName"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/vnet1/subnets/subnet1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Network","resource_type":"Microsoft.Network/virtualNetworks/subnets","resource_name":"subnet1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Network/virtualNetworks/vnet1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleAssignments","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleAssignments","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleDefinitions","resource_name":"b24988ac-6180-42a0-ab88-20f7382dd24c","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/providers/Microsoft.Management/managementGroups/mg1',
       '{"management_group":"mg1","provider_namespace":"Microsoft.Management","resource_type":"Microsoft.Management/managementGroups","resource_name":"mg1"}'),
      ('/providers/Microsoft.Management/managementGroups/mg1/providers/Microsoft.Authorization/policyDefinitions/p1',
       '{"management_group":"mg1","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/policyDefinitions","resource_name":"p1","parent_id":"/providers/Microsoft.Management/managementGroups/mg1"}'),
      ('/providers/Microsoft.Management/managementGroups/cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a',
       '{"management_group":"cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a","provider_namespace":"Microsoft.Management","resource_type":"Microsoft.Management/managementGroups","resource_name":"cccc2c2c-dd3d-ee4e-ff5f-aaaaaa6a6a6a"}'),
      ('/providers/Microsoft.Authorization/policyDefinitions/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/policyDefinitions","resource_name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_type":"Microsoft.Resources/subscriptions","resource_name":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","resource_type":"Microsoft.Resources/resourceGroups","resource_name":"rg1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '{"tenant_id":"aaaabbbb-0000-cccc-1111-dddd2222eeee","provider_namespace":"Microsoft.aadiam","resource_type":"Microsoft.aadiam","parent_id":"/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee"}'),
      ('/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE/PROVIDERS/MICROSOFT.AADIAM',
       '{"tenant_id":"AAAABBBB-0000-CCCC-1111-DDDD2222EEEE","provider_namespace":"MICROSOFT.AADIAM","resource_type":"MICROSOFT.AADIAM","parent_id":"/TENANTS/AAAABBBB-0000-CCCC-1111-DDDD2222EEEE"}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee',
       '{"tenant_id":"aaaabbbb-0000-cccc-1111-dddd2222eeee"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Compute',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Compute","resource_type":"Microsoft.Compute","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.operationalinsights/workspaces/ws1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"microsoft.operationalinsights","resource_type":"microsoft.operationalinsights/workspaces","resource_name":"ws1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1"}'),
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1',
       '{"subscription_id":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","resource_group":"RG1","provider_namespace":"MICROSOFT.NETWORK","resource_type":"MICROSOFT.NETWORK/NETWORKSECURITYGROUPS","resource_name":"NSG1","parent_id":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1"}'),
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.KEYVAULT/VAULTS/KV1',
       '{"subscription_id":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","resource_group":"RG1","provider_namespace":"MICROSOFT.KEYVAULT","resource_type":"MICROSOFT.KEYVAULT/VAULTS","resource_name":"KV1","parent_id":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Compute","resource_type":"Microsoft.Compute/virtualMachines","resource_name":"vm1","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1"}'),
      ('"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/providers/Microsoft.Authorization/roleDefinitions/b24988ac-6180-42a0-ab88-20f7382dd24c"',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","provider_namespace":"Microsoft.Authorization","resource_type":"Microsoft.Authorization/roleDefinitions","resource_name":"b24988ac-6180-42a0-ab88-20f7382dd24c","parent_id":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"}'),
      ('/Subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/ResourceGroups/rg1/Providers/Microsoft.Web/sites/app1',
       '{"subscription_id":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","resource_group":"rg1","provider_namespace":"Microsoft.Web","resource_type":"Microsoft.Web/sites","resource_name":"app1","parent_id":"/Subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/ResourceGroups/rg1"}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines', NULL),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers', NULL),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute//vm1', NULL),
      ('/foo/bar/providers/Microsoft.Compute/virtualMachines/vm1', NULL),
      ('00000003-0000-0000-c000-000000000000', NULL),
      ('vm1', NULL),
      ('/', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: azure_resource_id_normalize, azure_resource_id_clean',
          raise_error(concat('azure_resource_id_normalize, azure_resource_id_clean: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(normalized, 'NULL'), ' / ', coalesce(cleaned, 'NULL'),
                ', expected ', coalesce(expected_normalized, 'NULL'), ' / ', coalesce(expected_cleaned, 'NULL')) AS failure
  FROM (
    SELECT input, expected_normalized, expected_cleaned,
           azure_resource_id_normalize(input) AS normalized, azure_resource_id_clean(input) AS cleaned
    FROM VALUES
      ('/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.COMPUTE/VIRTUALMACHINES/VM1',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1',
       '/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.COMPUTE/VIRTUALMACHINES/VM1'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1/',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourcegroups/rg1/providers/microsoft.compute/virtualmachines/vm1',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/microsoft.aadiam',
       '/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam'),
      ('  "/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e"  ',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e',
       '/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e'),
      ('/', NULL, NULL),
      ('', NULL, NULL),
      (NULL, NULL, NULL)
      AS t(input, expected_normalized, expected_cleaned)
  )
  WHERE NOT (normalized <=> expected_normalized AND cleaned <=> expected_cleaned)
);
```

- [ ] **Step 2: Run the tests to verify that they fail**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/tests/azure_identifiers_test.sql`

Expected: `FAILED uc-udfs/tests/azure_identifiers_test.sql: SELECT if(count(*) = 0, ...` with `[UNRESOLVED_ROUTINE] Cannot resolve routine `azure_resource_id_parse``, exit code 1.

- [ ] **Step 3: Create `uc-udfs/azure_identifiers.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse Azure resource IDs
-- MAGIC
-- MAGIC These SQL UDFs parse [Azure Resource Manager resource IDs](https://learn.microsoft.com/en-us/azure/azure-resource-manager/templates/template-functions-resource#resourceid)
-- MAGIC in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `azure_resource_id_parse(id)` | `STRUCT<tenant_id, subscription_id, resource_group, management_group, provider_namespace, resource_type, resource_name, parent_id>` |
-- MAGIC | `azure_resource_id_normalize(id)` | the ID in lower case, without surrounding quotes and a trailing `/`: a join key |
-- MAGIC | `azure_resource_id_clean(id)` | the ID without surrounding spaces and quotes and a trailing `/` (the original case is kept) |
-- MAGIC | `azure_resource_id_parse_cleaned`, `azure_resource_id_parse_parts` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - An invalid value or `NULL` gives `NULL`. The functions don't raise errors.
-- MAGIC - These ID forms are supported: resource group, subscription, management group and tenant level resources, child resources (`.../virtualNetworks/vnet1/subnets/subnet1`),
-- MAGIC   extension resources (`<resource ID>/providers/Microsoft.Authorization/roleAssignments/<guid>`), scopes (`/subscriptions/<id>`, `/subscriptions/<id>/resourceGroups/<name>`, `/tenants/<id>`),
-- MAGIC   and IDs that end with a provider namespace (`/tenants/<id>/providers/Microsoft.aadiam` in Entra ID diagnostic logs).
-- MAGIC - `resource_type` is the provider namespace and all types (`Microsoft.Network/virtualNetworks/subnets`), and `resource_name` is the last name (`subnet1`).
-- MAGIC   For an ID that ends with a provider namespace, `resource_type` is the namespace and `resource_name` is `NULL`.
-- MAGIC - `parent_id` is the resource that is extended (extension resources), the parent resource (child resources), or the resource group, subscription or tenant scope.
-- MAGIC   It is `NULL` for subscriptions, management groups and tenant level resources: the ID doesn't encode the management group.
-- MAGIC - Azure resource IDs are case-insensitive, and logs use different case for the same ID (`resourceGroups`, `resourcegroups`, `RESOURCEGROUPS`).
-- MAGIC   The keywords are matched without case, and the values keep the original case. Use `azure_resource_id_normalize` to join IDs from different sources.
-- MAGIC - One pair of surrounding double quotes is removed: Entra ID audit logs have JSON-encoded strings in `modifiedProperties[].newValue`.
-- MAGIC - In Entra ID sign-in logs, `properties.resourceId` is the application ID of the resource (a GUID), not a resource ID, so it gives `NULL`.
-- MAGIC   IDs in URL-encoded `requestUri` filters are not extracted: use `url_decode` and `regexp_extract` first.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_clean(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Removes surrounding spaces, one pair of surrounding double quotes and trailing slashes from an Azure resource ID. The case is not changed'
  RETURN nullif(regexp_replace(regexp_replace(trim(id), '^"(.*)"$', '$1'), '/+$', ''), '');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_normalize(id STRING)
  RETURNS STRING
  DETERMINISTIC
  COMMENT 'Returns the Azure resource ID in lower case, without surrounding spaces and quotes and trailing slashes. Use it to join IDs from different sources'
  RETURN lower(azure_resource_id_clean(id));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse_parts(s STRING, prefix STRING, t ARRAY<STRING>)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for azure_resource_id_parse: builds the result from the ID, the part before the last /providers/ and the segments after it'
  RETURN CASE
    WHEN (prefix = '' OR prefix RLIKE '(?i)^/(subscriptions|tenants|providers)/')
      AND size(t) % 2 = 1 AND NOT array_contains(t, '') THEN named_struct(
      'tenant_id', nullif(regexp_extract(s, '(?i)^/tenants/([^/]+)', 1), ''),
      'subscription_id', nullif(regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1), ''),
      'resource_group', nullif(regexp_extract(s, '(?i)^/subscriptions/[^/]+/resourceGroups/([^/]+)', 1), ''),
      'management_group', nullif(regexp_extract(s, r'(?i)^/providers/Microsoft\.Management/managementGroups/([^/]+)', 1), ''),
      'provider_namespace', get(t, 0),
      'resource_type', array_join(concat(array(get(t, 0)), filter(t, (x, i) -> i % 2 = 1)), '/'),
      'resource_name', if(size(t) = 1, NULL, element_at(t, -1)),
      'parent_id', if(size(t) >= 5, regexp_extract(s, '^(.*)/[^/]+/[^/]+$', 1), nullif(prefix, '')))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse_cleaned(s STRING)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Helper for azure_resource_id_parse: parses the result of azure_resource_id_clean'
  RETURN CASE
    WHEN s RLIKE '(?i)/providers/' THEN azure_resource_id_parse_parts(
      s,
      regexp_extract(s, '(?i)^(.*)/providers/(.*)$', 1),
      split(regexp_extract(s, '(?i)^(.*)/providers/(.*)$', 2), '/'))
    WHEN s RLIKE '(?i)^/subscriptions/[^/]+$' THEN named_struct(
      'tenant_id', CAST(NULL AS STRING),
      'subscription_id', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'resource_group', CAST(NULL AS STRING),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', 'Microsoft.Resources/subscriptions',
      'resource_name', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'parent_id', CAST(NULL AS STRING))
    WHEN s RLIKE '(?i)^/subscriptions/[^/]+/resourceGroups/[^/]+$' THEN named_struct(
      'tenant_id', CAST(NULL AS STRING),
      'subscription_id', regexp_extract(s, '(?i)^/subscriptions/([^/]+)', 1),
      'resource_group', regexp_extract(s, '(?i)/resourceGroups/([^/]+)$', 1),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', 'Microsoft.Resources/resourceGroups',
      'resource_name', regexp_extract(s, '(?i)/resourceGroups/([^/]+)$', 1),
      'parent_id', regexp_extract(s, '(?i)^(/subscriptions/[^/]+)', 1))
    WHEN s RLIKE '(?i)^/tenants/[^/]+$' THEN named_struct(
      'tenant_id', regexp_extract(s, '(?i)^/tenants/([^/]+)', 1),
      'subscription_id', CAST(NULL AS STRING),
      'resource_group', CAST(NULL AS STRING),
      'management_group', CAST(NULL AS STRING),
      'provider_namespace', CAST(NULL AS STRING),
      'resource_type', CAST(NULL AS STRING),
      'resource_name', CAST(NULL AS STRING),
      'parent_id', CAST(NULL AS STRING))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION azure_resource_id_parse(id STRING)
  RETURNS STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                 provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>
  DETERMINISTIC
  COMMENT 'Parses an Azure resource ID into tenant_id, subscription_id, resource_group, management_group, provider_namespace, resource_type, resource_name and parent_id. Returns NULL for an invalid ID'
  RETURN azure_resource_id_parse_cleaned(azure_resource_id_clean(id));
```

- [ ] **Step 4: Run the tests to verify that they pass**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/azure_identifiers.sql uc-udfs/tests/azure_identifiers_test.sql`

Expected: one `ok` line per `CREATE OR REPLACE FUNCTION` and per test cell (for example `ok uc-udfs/tests/azure_identifiers_test.sql: SELECT if(count(*) = 0, 'ok: ... ok: ...`), exit code 0.
If a test cell fails, its error lists every failed case as `input => actual, expected expected`. Fix the function, not the expected value, unless the expected value contradicts the spec (then stop and ask).

- [ ] **Step 5: Add the README entry**

In `uc-udfs/README.md`, add this bullet after the `aws_identifiers.sql` bullet:

```markdown
- `azure_identifiers.sql` contains `azure_resource_id_parse`, which parses an Azure resource ID into tenant, subscription, resource group, management group, provider namespace, resource type and name, and parent ID, and `azure_resource_id_normalize` and `azure_resource_id_clean`, to join IDs that have a different case in different logs. They are pure SQL.
```

- [ ] **Step 6: Commit**

```bash
git add uc-udfs/azure_identifiers.sql uc-udfs/tests/azure_identifiers_test.sql uc-udfs/README.md
git commit -m "uc-udfs: Azure resource ID functions

Co-authored-by: Isaac <no-reply@databricks.com>"
```

Expected: the secret-scan pre-commit hook passes. If it reports `aws-access-token`, a key-shaped literal is in the staged files: build it with `concat` (see Global Constraints). Never bypass the hook.

---

### Task 4: GCP identifiers

**Files:**
- Create: `uc-udfs/gcp_identifiers.sql`
- Test: `uc-udfs/tests/gcp_identifiers_test.sql`
- Modify: `uc-udfs/README.md`

**Interfaces:**
- Consumes: `uc-udfs/tests/run_sql.py` (Task 1).
- Produces (used by Task 5):
  - `gcp_resource_name_parse(name STRING) RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING, parent: STRING, segments: MAP<STRING, STRING>>`
  - `gcp_principal_parse(member STRING) RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>`
  - `gcp_service_account_parse(email STRING) RETURNS STRUCT<account_type: STRING, project_id: STRING, project_number: STRING, service: STRING>`
  - helpers: `gcp_resource_name_build`, `gcp_resource_name_parse_segments`, `gcp_resource_name_parse_relative`, `gcp_resource_name_parse_trimmed`, `gcp_principal_build`, `gcp_principal_parse_core`, `gcp_service_account_parse_email`

Two choices in the code differ from the first version of the spec; both are in the spec now: `service-PROJECT_NUMBER@X.iam.gserviceaccount.com` is a service agent for any `X` (Compute Engine's agent is `@compute-system`), and resource URIs accept `alpha`/`beta` versions (`/compute/beta/...`).

- [ ] **Step 1: Write the test notebook `uc-udfs/tests/gcp_identifiers_test.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `gcp_identifiers.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../gcp_identifiers.sql` in the same schema first.
-- MAGIC
-- MAGIC The names are the formats from AIP-122, the Cloud Asset Inventory resource name formats, the IAM principal identifiers page
-- MAGIC and the service account types page, with placeholder project IDs and numbers.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_resource_name_parse',
          raise_error(concat('gcp_resource_name_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_resource_name_parse(input)) AS actual
    FROM VALUES
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/disks/disk1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/disks/disk1","project":"my-project","location":"us-central1-a","resource_type":"disks","resource_name":"disk1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","disks":"disk1"}}'),
      ('//compute.googleapis.com/projects/my-project/global/networks/default',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/networks/default","project":"my-project","location":"global","resource_type":"networks","resource_name":"default","parent":"projects/my-project","segments":{"projects":"my-project","networks":"default"}}'),
      ('//compute.googleapis.com/projects/my-project/global/firewalls/allow-ssh',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/firewalls/allow-ssh","project":"my-project","location":"global","resource_type":"firewalls","resource_name":"allow-ssh","parent":"projects/my-project","segments":{"projects":"my-project","firewalls":"allow-ssh"}}'),
      ('//compute.googleapis.com/projects/my-project/regions/us-central1/subnetworks/subnet1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/regions/us-central1/subnetworks/subnet1","project":"my-project","location":"us-central1","resource_type":"subnetworks","resource_name":"subnet1","parent":"projects/my-project/regions/us-central1","segments":{"projects":"my-project","regions":"us-central1","subnetworks":"subnet1"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1',
       '{"service":"container.googleapis.com","relative_name":"projects/my-project/locations/us-central1/clusters/c1","project":"my-project","location":"us-central1","resource_type":"clusters","resource_name":"c1","parent":"projects/my-project/locations/us-central1","segments":{"projects":"my-project","locations":"us-central1","clusters":"c1"}}'),
      ('//container.googleapis.com/projects/my-project/locations/us-central1/clusters/c1/nodePools/pool1',
       '{"service":"container.googleapis.com","relative_name":"projects/my-project/locations/us-central1/clusters/c1/nodePools/pool1","project":"my-project","location":"us-central1","resource_type":"nodePools","resource_name":"pool1","parent":"projects/my-project/locations/us-central1/clusters/c1","segments":{"projects":"my-project","locations":"us-central1","clusters":"c1","nodePools":"pool1"}}'),
      ('//cloudkms.googleapis.com/projects/my-project/locations/global/keyRings/kr1',
       '{"service":"cloudkms.googleapis.com","relative_name":"projects/my-project/locations/global/keyRings/kr1","project":"my-project","location":"global","resource_type":"keyRings","resource_name":"kr1","parent":"projects/my-project/locations/global","segments":{"projects":"my-project","locations":"global","keyRings":"kr1"}}'),
      ('//cloudkms.googleapis.com/projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1',
       '{"service":"cloudkms.googleapis.com","relative_name":"projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1/cryptoKeyVersions/1","project":"my-project","location":"global","resource_type":"cryptoKeyVersions","resource_name":"1","parent":"projects/my-project/locations/global/keyRings/kr1/cryptoKeys/k1","segments":{"projects":"my-project","locations":"global","keyRings":"kr1","cryptoKeys":"k1","cryptoKeyVersions":"1"}}'),
      ('//bigquery.googleapis.com/projects/my-project/datasets/ds1',
       '{"service":"bigquery.googleapis.com","relative_name":"projects/my-project/datasets/ds1","project":"my-project","resource_type":"datasets","resource_name":"ds1","parent":"projects/my-project","segments":{"projects":"my-project","datasets":"ds1"}}'),
      ('//bigquery.googleapis.com/projects/my-project/datasets/ds1/tables/t1',
       '{"service":"bigquery.googleapis.com","relative_name":"projects/my-project/datasets/ds1/tables/t1","project":"my-project","resource_type":"tables","resource_name":"t1","parent":"projects/my-project/datasets/ds1","segments":{"projects":"my-project","datasets":"ds1","tables":"t1"}}'),
      ('//iam.googleapis.com/projects/-/serviceAccounts/sa1@my-project.iam.gserviceaccount.com',
       '{"service":"iam.googleapis.com","relative_name":"projects/-/serviceAccounts/sa1@my-project.iam.gserviceaccount.com","resource_type":"serviceAccounts","resource_name":"sa1@my-project.iam.gserviceaccount.com","parent":"projects/-","segments":{"projects":"-","serviceAccounts":"sa1@my-project.iam.gserviceaccount.com"}}'),
      ('//iam.googleapis.com/projects/my-project/serviceAccounts/sa1@my-project.iam.gserviceaccount.com',
       '{"service":"iam.googleapis.com","relative_name":"projects/my-project/serviceAccounts/sa1@my-project.iam.gserviceaccount.com","project":"my-project","resource_type":"serviceAccounts","resource_name":"sa1@my-project.iam.gserviceaccount.com","parent":"projects/my-project","segments":{"projects":"my-project","serviceAccounts":"sa1@my-project.iam.gserviceaccount.com"}}'),
      ('//iam.googleapis.com/projects/my-project/roles/customRole',
       '{"service":"iam.googleapis.com","relative_name":"projects/my-project/roles/customRole","project":"my-project","resource_type":"roles","resource_name":"customRole","parent":"projects/my-project","segments":{"projects":"my-project","roles":"customRole"}}'),
      ('//cloudresourcemanager.googleapis.com/projects/my-project',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"projects/my-project","project":"my-project","resource_type":"projects","resource_name":"my-project","segments":{"projects":"my-project"}}'),
      ('//cloudresourcemanager.googleapis.com/projects/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"projects/123456789012","project":"123456789012","resource_type":"projects","resource_name":"123456789012","segments":{"projects":"123456789012"}}'),
      ('//cloudresourcemanager.googleapis.com/folders/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"folders/123456789012","resource_type":"folders","resource_name":"123456789012","segments":{"folders":"123456789012"}}'),
      ('//cloudresourcemanager.googleapis.com/organizations/123456789012',
       '{"service":"cloudresourcemanager.googleapis.com","relative_name":"organizations/123456789012","resource_type":"organizations","resource_name":"123456789012","segments":{"organizations":"123456789012"}}'),
      ('//storage.googleapis.com/my-bucket',
       '{"service":"storage.googleapis.com","relative_name":"projects/_/buckets/my-bucket","resource_type":"buckets","resource_name":"my-bucket","parent":"projects/_","segments":{"projects":"_","buckets":"my-bucket"}}'),
      ('//storage.googleapis.com/projects/_/buckets/my-bucket/objects/a/b.txt',
       '{"service":"storage.googleapis.com","relative_name":"projects/_/buckets/my-bucket/objects/a/b.txt","resource_type":"objects","resource_name":"a/b.txt","parent":"projects/_/buckets/my-bucket","segments":{"projects":"_","buckets":"my-bucket","objects":"a/b.txt"}}'),
      ('//cloudsql.googleapis.com/projects/my-project/instances/db1',
       '{"service":"cloudsql.googleapis.com","relative_name":"projects/my-project/instances/db1","project":"my-project","resource_type":"instances","resource_name":"db1","parent":"projects/my-project","segments":{"projects":"my-project","instances":"db1"}}'),
      ('//pubsub.googleapis.com/projects/my-project/topics/t1',
       '{"service":"pubsub.googleapis.com","relative_name":"projects/my-project/topics/t1","project":"my-project","resource_type":"topics","resource_name":"t1","parent":"projects/my-project","segments":{"projects":"my-project","topics":"t1"}}'),
      ('//run.googleapis.com/projects/my-project/locations/us-central1/services/svc1',
       '{"service":"run.googleapis.com","relative_name":"projects/my-project/locations/us-central1/services/svc1","project":"my-project","location":"us-central1","resource_type":"services","resource_name":"svc1","parent":"projects/my-project/locations/us-central1","segments":{"projects":"my-project","locations":"us-central1","services":"svc1"}}'),
      ('//secretmanager.googleapis.com/projects/123456789012/secrets/s1',
       '{"service":"secretmanager.googleapis.com","relative_name":"projects/123456789012/secrets/s1","project":"123456789012","resource_type":"secrets","resource_name":"s1","parent":"projects/123456789012","segments":{"projects":"123456789012","secrets":"s1"}}'),
      -- audit log protoPayload.resourceName
      ('projects/_/buckets/my-bucket',
       '{"relative_name":"projects/_/buckets/my-bucket","resource_type":"buckets","resource_name":"my-bucket","parent":"projects/_","segments":{"projects":"_","buckets":"my-bucket"}}'),
      ('projects/_/buckets/my-bucket/objects/dir/file.txt',
       '{"relative_name":"projects/_/buckets/my-bucket/objects/dir/file.txt","resource_type":"objects","resource_name":"dir/file.txt","parent":"projects/_/buckets/my-bucket","segments":{"projects":"_","buckets":"my-bucket","objects":"dir/file.txt"}}'),
      -- AIP-122 examples in the three forms
      ('publishers/123/books/les-miserables',
       '{"relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      ('//library.googleapis.com/publishers/123/books/les-miserables',
       '{"service":"library.googleapis.com","relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      ('https://library.googleapis.com/v1/publishers/123/books/les-miserables',
       '{"service":"library.googleapis.com","relative_name":"publishers/123/books/les-miserables","resource_type":"books","resource_name":"les-miserables","parent":"publishers/123","segments":{"publishers":"123","books":"les-miserables"}}'),
      -- selfLinks
      ('https://www.googleapis.com/compute/v1/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://compute.googleapis.com/compute/v1/projects/my-project/global/networks/default',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/global/networks/default","project":"my-project","location":"global","resource_type":"networks","resource_name":"default","parent":"projects/my-project","segments":{"projects":"my-project","networks":"default"}}'),
      ('https://www.googleapis.com/compute/beta/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://www.googleapis.com/compute/projects/my-project/zones/us-central1-a/instances/vm1', NULL),
      ('https://www.googleapis.com/compute/v1beta1/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"service":"compute.googleapis.com","relative_name":"projects/my-project/zones/us-central1-a/instances/vm1","project":"my-project","location":"us-central1-a","resource_type":"instances","resource_name":"vm1","parent":"projects/my-project/zones/us-central1-a","segments":{"projects":"my-project","zones":"us-central1-a","instances":"vm1"}}'),
      ('https://www.googleapis.com/storage/v1/b/my-bucket/o/file.txt',
       '{"service":"storage.googleapis.com","relative_name":"b/my-bucket/o/file.txt","resource_type":"o","resource_name":"file.txt","parent":"b/my-bucket","segments":{"b":"my-bucket","o":"file.txt"}}'),
      ('  //pubsub.googleapis.com/projects/my-project/topics/t1 ',
       '{"service":"pubsub.googleapis.com","relative_name":"projects/my-project/topics/t1","project":"my-project","resource_type":"topics","resource_name":"t1","parent":"projects/my-project","segments":{"projects":"my-project","topics":"t1"}}'),
      ('projects/my-project/', NULL),
      ('projects/my-project/instances', NULL),
      ('projects/my-project/zones/z1/zones/z2', NULL),
      ('projects//instances/i1', NULL),
      ('/projects/my-project', NULL),
      ('https://www.googleapis.com/', NULL),
      ('s3://bucket/key', NULL),
      ('foo', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_principal_parse',
          raise_error(concat('gcp_principal_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_principal_parse(input)) AS actual
    FROM VALUES
      ('user:alex@example.com', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('principal://goog/subject/alex@example.com', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('deleted:user:alex@example.com?uid=123456789012345678901',
       '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principal://goog/subject/alex@example.com?uid=123456789012345678901',
       '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('serviceAccount:my-service-account@my-project.iam.gserviceaccount.com',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":false}'),
      ('principal://iam.googleapis.com/projects/-/serviceAccounts/my-service-account@my-project.iam.gserviceaccount.com',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":false}'),
      ('deleted:serviceAccount:my-service-account@my-project.iam.gserviceaccount.com?uid=123456789012345678901',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principal://iam.googleapis.com/projects/-/serviceAccounts/my-service-account@my-project.iam.gserviceaccount.com?uid=123456789012345678901',
       '{"principal_type":"service_account","identity":"my-service-account@my-project.iam.gserviceaccount.com","domain":"my-project.iam.gserviceaccount.com","deleted":true,"uid":"123456789012345678901"}'),
      ('group:my-group@example.com', '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":false}'),
      ('principalSet://goog/group/my-group@example.com', '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":false}'),
      ('deleted:group:my-group@example.com?uid=123456789012345678901',
       '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('deleted:principalSet://goog/group/my-group@example.com?uid=123456789012345678901',
       '{"principal_type":"group","identity":"my-group@example.com","domain":"example.com","deleted":true,"uid":"123456789012345678901"}'),
      ('domain:example.com', '{"principal_type":"domain","identity":"example.com","domain":"example.com","deleted":false}'),
      ('allUsers', '{"principal_type":"all_users","deleted":false}'),
      ('allAuthenticatedUsers', '{"principal_type":"all_authenticated_users","deleted":false}'),
      ('principalSet://goog/public:all', '{"principal_type":"all_principals","deleted":false}'),
      ('principalSet://goog/cloudIdentityCustomerId/C01Abc35', '{"principal_type":"cloud_identity_customer","identity":"C01Abc35","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/projects/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"projects/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/folders/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"folders/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/organizations/123456789012/type/ServiceAccount',
       '{"principal_type":"service_account_set","identity":"organizations/123456789012","deleted":false}'),
      ('principalSet://cloudresourcemanager.googleapis.com/projects/123456789012/type/ServiceAgent',
       '{"principal_type":"service_agent_set","identity":"projects/123456789012","deleted":false}'),
      ('principal://iam.googleapis.com/locations/global/workforcePools/my-pool/subject/alex@example.com',
       '{"principal_type":"workforce_identity","identity":"alex@example.com","pool":"my-pool","deleted":false}'),
      ('deleted:principal://iam.googleapis.com/locations/global/workforcePools/my-pool/subject/alex@example.com',
       '{"principal_type":"workforce_identity","identity":"alex@example.com","pool":"my-pool","deleted":true}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/group/my-group',
       '{"principal_type":"workforce_identity_set","identity":"group/my-group","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/attribute.department/sales',
       '{"principal_type":"workforce_identity_set","identity":"attribute.department/sales","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/locations/global/workforcePools/my-pool/*',
       '{"principal_type":"workforce_identity_set","identity":"*","pool":"my-pool","deleted":false}'),
      ('principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/subject/system:serviceaccount:ns1:sa1',
       '{"principal_type":"workload_identity","identity":"system:serviceaccount:ns1:sa1","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/group/admins',
       '{"principal_type":"workload_identity_set","identity":"group/admins","pool":"my-pool","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-pool/*',
       '{"principal_type":"workload_identity_set","identity":"*","pool":"my-pool","deleted":false}'),
      ('principal://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-project.svc.id.goog/subject/ns/default/sa/app',
       '{"principal_type":"gke_workload","identity":"ns/default/sa/app","pool":"my-project.svc.id.goog","deleted":false}'),
      ('principalSet://iam.googleapis.com/projects/123456789012/locations/global/workloadIdentityPools/my-project.svc.id.goog/namespace/default',
       '{"principal_type":"gke_workload","identity":"namespace/default","pool":"my-project.svc.id.goog","deleted":false}'),
      ('serviceAccount:my-project.svc.id.goog[default/app]',
       '{"principal_type":"gke_workload","identity":"default/app","pool":"my-project.svc.id.goog","deleted":false}'),
      ('principal://agents.global.org-123456789012.system.id.goog/resources/aiplatform/projects/123456789012/locations/us-central1/reasoningEngines/1',
       '{"principal_type":"agent_identity","identity":"aiplatform/projects/123456789012/locations/us-central1/reasoningEngines/1","deleted":false}'),
      ('principalSet://agents.global.org-123456789012.system.id.goog/*',
       '{"principal_type":"principal_set","identity":"agents.global.org-123456789012.system.id.goog/*","deleted":false}'),
      ('principal://example.com/foo', '{"principal_type":"principal","identity":"example.com/foo","deleted":false}'),
      (' user:alex@example.com ', '{"principal_type":"user","identity":"alex@example.com","domain":"example.com","deleted":false}'),
      ('alex@example.com', NULL),
      ('user:', NULL),
      ('projectOwner:my-project', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: gcp_service_account_parse',
          raise_error(concat('gcp_service_account_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(gcp_service_account_parse(input)) AS actual
    FROM VALUES
      ('my-service-account@my-project.iam.gserviceaccount.com', '{"account_type":"user_managed","project_id":"my-project"}'),
      ('serviceAccount:my-service-account@my-project.iam.gserviceaccount.com', '{"account_type":"user_managed","project_id":"my-project"}'),
      ('my-project@appspot.gserviceaccount.com', '{"account_type":"default_app_engine","project_id":"my-project"}'),
      ('123456789012-compute@developer.gserviceaccount.com', '{"account_type":"default_compute","project_number":"123456789012"}'),
      ('123456789012@cloudservices.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"cloudservices"}'),
      ('123456789012@cloudbuild.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"cloudbuild"}'),
      ('service-123456789012@gcp-sa-pubsub.iam.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"pubsub"}'),
      ('service-123456789012@compute-system.iam.gserviceaccount.com', '{"account_type":"service_agent","project_number":"123456789012","service":"compute-system"}'),
      ('bqcx-442188550395-jujw@gcp-sa-bigquery-condel.iam.gserviceaccount.com', '{"account_type":"service_agent","service":"bigquery-condel"}'),
      ('service-agent-manager@system.gserviceaccount.com', '{"account_type":"service_agent","service":"system"}'),
      ('alex@example.com', NULL),
      ('my-project.iam.gserviceaccount.com', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
```

- [ ] **Step 2: Run the tests to verify that they fail**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/tests/gcp_identifiers_test.sql`

Expected: `FAILED uc-udfs/tests/gcp_identifiers_test.sql: SELECT if(count(*) = 0, ...` with `[UNRESOLVED_ROUTINE] Cannot resolve routine `gcp_resource_name_parse``, exit code 1.

- [ ] **Step 3: Create `uc-udfs/gcp_identifiers.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined functions to parse GCP identifiers
-- MAGIC
-- MAGIC These SQL UDFs parse [GCP resource names](https://google.aip.dev/122), [IAM principal identifiers](https://cloud.google.com/iam/docs/principal-identifiers)
-- MAGIC and [service account emails](https://cloud.google.com/iam/docs/service-account-types) in pure SQL, without a Python sandbox.
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `gcp_resource_name_parse(name)` | `STRUCT<service, relative_name, project, location, resource_type, resource_name, parent, segments MAP<STRING,STRING>>` |
-- MAGIC | `gcp_principal_parse(member)` | `STRUCT<principal_type, identity, domain, pool, deleted BOOLEAN, uid>` |
-- MAGIC | `gcp_service_account_parse(email)` | `STRUCT<account_type, project_id, project_number, service>` |
-- MAGIC | `gcp_resource_name_parse_trimmed`, `gcp_resource_name_parse_relative`, `gcp_resource_name_parse_segments`, `gcp_resource_name_build`, `gcp_principal_parse_core`, `gcp_principal_build`, `gcp_service_account_parse_email` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior of `gcp_resource_name_parse`:
-- MAGIC
-- MAGIC - It takes full resource names (`//compute.googleapis.com/projects/p/zones/z/instances/i`, Cloud Asset Inventory),
-- MAGIC   resource URIs and selfLinks (`https://www.googleapis.com/compute/v1/projects/p/zones/z/instances/i`), and relative names (`projects/p/zones/z/instances/i`, audit log `protoPayload.resourceName`).
-- MAGIC   For a relative name, `service` is `NULL`: use `coalesce` with `protoPayload.serviceName`.
-- MAGIC - `segments` maps each collection to its ID (`{"projects":"p","zones":"z","instances":"i"}`). `resource_type` is the last collection, `resource_name` is the last ID,
-- MAGIC   and `parent` is the relative name without the last collection and ID. `location` is the `locations`, `zones` or `regions` value, or `global`.
-- MAGIC - `service` and `relative_name` are a join key between Cloud Asset Inventory, selfLinks and audit logs. Cloud Storage selfLinks (`/storage/v1/b/B/o/O`) use the collections `b` and `o`, so they don't match.
-- MAGIC - `global` without an ID (`projects/p/global/networks/n`) gives `location` `global`, and it is not in `segments` and `parent`. `relative_name` keeps it.
-- MAGIC - The projects `-` and `_` (`projects/-/serviceAccounts/...`, `projects/_/buckets/...`) give `project` `NULL`. `segments` keeps them.
-- MAGIC - The Cloud Storage bucket name `//storage.googleapis.com/BUCKET` becomes `projects/_/buckets/BUCKET`, the same as in audit logs. An object name can have `/` (`.../objects/dir/file.txt`).
-- MAGIC - A value with a collection and no ID, a duplicate collection, or an empty segment gives `NULL`.
-- MAGIC
-- MAGIC Behavior of `gcp_principal_parse`:
-- MAGIC
-- MAGIC - `principal_type` is one of `user`, `service_account`, `group`, `domain`, `all_users`, `all_authenticated_users`, `all_principals`, `cloud_identity_customer`,
-- MAGIC   `service_account_set`, `service_agent_set`, `workforce_identity`, `workforce_identity_set`, `workload_identity`, `workload_identity_set`, `gke_workload`, `agent_identity`,
-- MAGIC   `principal` and `principal_set` (other `principal://` and `principalSet://` identifiers). An unknown value gives `NULL`.
-- MAGIC - The allow policy form (`user:alex@example.com`) and the deny policy form (`principal://goog/subject/alex@example.com`) give the same result.
-- MAGIC - `identity` is the email, domain, subject or attribute. `domain` is the part after `@` for users, service accounts and groups. `pool` is the workforce or workload identity pool ID.
-- MAGIC - Deleted principals (`deleted:user:alex@example.com?uid=123`) give `deleted` `true` and `uid`.
-- MAGIC
-- MAGIC Behavior of `gcp_service_account_parse`:
-- MAGIC
-- MAGIC - `account_type` is `user_managed` (`NAME@PROJECT_ID.iam.gserviceaccount.com`), `default_app_engine` (`PROJECT_ID@appspot.gserviceaccount.com`),
-- MAGIC   `default_compute` (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`), or `service_agent` (`service-PROJECT_NUMBER@gcp-sa-SERVICE.iam.gserviceaccount.com`, `service-PROJECT_NUMBER@compute-system.iam.gserviceaccount.com`,
-- MAGIC   any `@gcp-sa-SERVICE.iam.gserviceaccount.com`, and other `*.gserviceaccount.com` domains).
-- MAGIC   Google says that these formats can change.
-- MAGIC - A `serviceAccount:` prefix is removed. A value that isn't a service account email gives `NULL`.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_build(service STRING, relative_name STRING, segs ARRAY<STRING>, has_global BOOLEAN, m MAP<STRING, STRING>)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: builds the result from the collection/ID segments and their map'
  RETURN named_struct(
    'service', service,
    'relative_name', relative_name,
    'project', CASE WHEN try_element_at(m, 'projects') NOT IN ('-', '_') THEN try_element_at(m, 'projects') END,
    'location', coalesce(try_element_at(m, 'locations'), try_element_at(m, 'zones'), try_element_at(m, 'regions'), if(has_global, 'global', NULL)),
    'resource_type', element_at(segs, -2),
    'resource_name', element_at(segs, -1),
    'parent', if(size(segs) > 2, array_join(slice(segs, 1, size(segs) - 2), '/'), NULL),
    'segments', m);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_segments(service STRING, relative_name STRING, segs ARRAY<STRING>, has_global BOOLEAN)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: checks the collection/ID segments'
  RETURN CASE
    WHEN size(segs) % 2 = 0 AND NOT array_contains(segs, '')
      AND size(array_distinct(filter(segs, (x, i) -> i % 2 = 0))) = size(segs) DIV 2
      AND get(segs, 0) RLIKE '^[a-zA-Z][a-zA-Z0-9]*$'
    THEN gcp_resource_name_build(service, relative_name, segs, has_global,
      map_from_arrays(filter(segs, (x, i) -> i % 2 = 0), filter(segs, (x, i) -> i % 2 = 1)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_relative(service STRING, relative_name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: splits the relative name into collection/ID segments'
  RETURN gcp_resource_name_parse_segments(
    service,
    relative_name,
    CASE
      -- a Cloud Storage object name can have /
      WHEN regexp_replace(relative_name, '^(projects/[^/]+)/global(/|$)', '$1$2') RLIKE '(^|/)buckets/[^/]+/objects/.'
        THEN concat(
          split(regexp_extract(relative_name, '^(.*?/objects)/(.*)$', 1), '/'),
          array(regexp_extract(relative_name, '^(.*?/objects)/(.*)$', 2)))
      ELSE split(regexp_replace(relative_name, '^(projects/[^/]+)/global(/|$)', '$1$2'), '/')
    END,
    relative_name RLIKE '^projects/[^/]+/global(/|$)');

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse_trimmed(name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Helper for gcp_resource_name_parse: gets the service and the relative name from the trimmed name'
  RETURN gcp_resource_name_parse_relative(
    CASE
      WHEN name RLIKE '^//[^/]+/.' THEN regexp_extract(name, '^//([^/]+)/', 1)
      WHEN name RLIKE r'^https://www\.googleapis\.com/' THEN
        concat(nullif(regexp_extract(name, '^https://[^/]+/(?:([^/]+)/)?(?:v[0-9]+[a-z0-9]*|alpha|beta)/(.*)$', 1), ''), '.googleapis.com')
      WHEN name RLIKE '^https://[^/]+/' THEN regexp_extract(name, '^https://([^/]+)/', 1)
    END,
    CASE
      -- a Cloud Storage bucket full name has no collection
      WHEN name RLIKE r'^//storage\.googleapis\.com/[^/]+$' THEN concat('projects/_/buckets/', regexp_extract(name, '^//[^/]+/(.*)$', 1))
      WHEN name RLIKE '^//[^/]+/.' THEN regexp_extract(name, '^//[^/]+/(.*)$', 1)
      WHEN name RLIKE '^https://' THEN nullif(regexp_extract(name, '^https://[^/]+/(?:[^/]+/)?(?:v[0-9]+[a-z0-9]*|alpha|beta)/(.*)$', 1), '')
      WHEN name RLIKE '^[a-zA-Z]' AND NOT name RLIKE '://' THEN name
    END);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_resource_name_parse(name STRING)
  RETURNS STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING, resource_name: STRING,
                 parent: STRING, segments: MAP<STRING, STRING>>
  DETERMINISTIC
  COMMENT 'Parses a GCP full resource name (//service/...), resource URI (https://...) or relative name into service, relative_name, project, location, resource_type, resource_name, parent and a map of collections to IDs. Returns NULL for an invalid name'
  RETURN gcp_resource_name_parse_trimmed(trim(name));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_build(r ARRAY<STRING>, deleted BOOLEAN, uid STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_principal_parse: builds the result from the type, identity and pool'
  RETURN CASE
    WHEN get(r, 0) IS NOT NULL THEN named_struct(
      'principal_type', get(r, 0),
      'identity', get(r, 1),
      'domain', CASE
        WHEN get(r, 0) = 'domain' THEN get(r, 1)
        WHEN get(r, 0) IN ('user', 'service_account', 'group') THEN nullif(regexp_extract(get(r, 1), '@([^@]+)$', 1), '')
      END,
      'pool', get(r, 2),
      'deleted', deleted,
      'uid', uid)
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_parse_core(c STRING, deleted BOOLEAN, uid STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_principal_parse: parses the principal identifier without deleted: and ?uid='
  RETURN gcp_principal_build(
    -- array(principal_type, identity, pool)
    CASE
      WHEN c RLIKE '^user:.' THEN array('user', substr(c, 6), NULL)
      WHEN c RLIKE r'^serviceAccount:[^\[]+\.svc\.id\.goog\[[^\]]+\]$' THEN array('gke_workload',
        regexp_extract(c, r'\[([^\]]+)\]$', 1), regexp_extract(c, r'^serviceAccount:([^\[]+)\[', 1))
      WHEN c RLIKE '^serviceAccount:.' THEN array('service_account', substr(c, 16), NULL)
      WHEN c RLIKE '^group:.' THEN array('group', substr(c, 7), NULL)
      WHEN c RLIKE '^domain:.' THEN array('domain', substr(c, 8), NULL)
      WHEN c = 'allUsers' THEN array('all_users', NULL, NULL)
      WHEN c = 'allAuthenticatedUsers' THEN array('all_authenticated_users', NULL, NULL)
      WHEN c RLIKE '^principal://goog/subject/.' THEN array('user', regexp_extract(c, '^principal://goog/subject/(.*)$', 1), NULL)
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/projects/[^/]+/serviceAccounts/.' THEN array('service_account',
        regexp_extract(c, '/serviceAccounts/(.*)$', 1), NULL)
      WHEN c RLIKE '^principalSet://goog/group/.' THEN array('group', regexp_extract(c, '^principalSet://goog/group/(.*)$', 1), NULL)
      WHEN c = 'principalSet://goog/public:all' THEN array('all_principals', NULL, NULL)
      WHEN c RLIKE '^principalSet://goog/cloudIdentityCustomerId/.' THEN array('cloud_identity_customer',
        regexp_extract(c, '^principalSet://goog/cloudIdentityCustomerId/(.*)$', 1), NULL)
      WHEN c RLIKE r'^principalSet://cloudresourcemanager\.googleapis\.com/(projects|folders|organizations)/[^/]+/type/ServiceAccount$' THEN array('service_account_set',
        regexp_extract(c, r'^principalSet://cloudresourcemanager\.googleapis\.com/([^/]+/[^/]+)/type/', 1), NULL)
      WHEN c RLIKE r'^principalSet://cloudresourcemanager\.googleapis\.com/(projects|folders|organizations)/[^/]+/type/ServiceAgent$' THEN array('service_agent_set',
        regexp_extract(c, r'^principalSet://cloudresourcemanager\.googleapis\.com/([^/]+/[^/]+)/type/', 1), NULL)
      WHEN c RLIKE r'^principal(Set)?://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+\.svc\.id\.goog/.' THEN array('gke_workload',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/(?:subject/)?(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/locations/[^/]+/workforcePools/[^/]+/subject/.' THEN array('workforce_identity',
        regexp_extract(c, '/workforcePools/[^/]+/subject/(.*)$', 1), regexp_extract(c, '/workforcePools/([^/]+)/', 1))
      WHEN c RLIKE r'^principalSet://iam\.googleapis\.com/locations/[^/]+/workforcePools/[^/]+/.' THEN array('workforce_identity_set',
        regexp_extract(c, '/workforcePools/[^/]+/(.*)$', 1), regexp_extract(c, '/workforcePools/([^/]+)/', 1))
      WHEN c RLIKE r'^principal://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+/subject/.' THEN array('workload_identity',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/subject/(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE r'^principalSet://iam\.googleapis\.com/projects/[^/]+/locations/[^/]+/workloadIdentityPools/[^/]+/.' THEN array('workload_identity_set',
        regexp_extract(c, '/workloadIdentityPools/[^/]+/(.*)$', 1), regexp_extract(c, '/workloadIdentityPools/([^/]+)/', 1))
      WHEN c RLIKE '^principal://[^/]+/resources/.' THEN array('agent_identity', regexp_extract(c, '^principal://[^/]+/resources/(.*)$', 1), NULL)
      WHEN c RLIKE '^principal://.' THEN array('principal', substr(c, 13), NULL)
      WHEN c RLIKE '^principalSet://.' THEN array('principal_set', substr(c, 16), NULL)
    END,
    deleted,
    uid);

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_principal_parse(member STRING)
  RETURNS STRUCT<principal_type: STRING, identity: STRING, domain: STRING, pool: STRING, deleted: BOOLEAN, uid: STRING>
  DETERMINISTIC
  COMMENT 'Parses a GCP IAM principal identifier (user:, serviceAccount:, group:, domain:, principal://, principalSet://, deleted:...) into principal_type, identity, domain, pool, deleted and uid. Returns NULL for an unknown value'
  RETURN gcp_principal_parse_core(
    regexp_replace(regexp_replace(trim(member), '^deleted:', ''), r'\?uid=[^?]*$', ''),
    trim(member) LIKE 'deleted:%',
    nullif(regexp_extract(trim(member), r'\?uid=([^?]*)$', 1), ''));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_service_account_parse_email(e STRING)
  RETURNS STRUCT<account_type: STRING, project_id: STRING, project_number: STRING, service: STRING>
  DETERMINISTIC
  COMMENT 'Helper for gcp_service_account_parse: parses the email without the serviceAccount: prefix'
  RETURN CASE
    -- service agents: service-PROJECT_NUMBER@gcp-sa-SERVICE and also older domains such as compute-system
    WHEN e RLIKE r'^service-[0-9]+@[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', regexp_extract(e, '^service-([0-9]+)@', 1),
      'service', regexp_extract(e, '@(?:gcp-sa-)?([^.@]+)[.]', 1))
    WHEN e RLIKE r'^[^@]+@gcp-sa-[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', CAST(NULL AS STRING),
      'service', regexp_extract(e, '@gcp-sa-([^.@]+)[.]', 1))
    WHEN e RLIKE r'^[^@]+@[^.@]+\.iam\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'user_managed',
      'project_id', regexp_extract(e, '@([^.@]+)[.]', 1),
      'project_number', CAST(NULL AS STRING),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[^@]+@appspot\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'default_app_engine',
      'project_id', regexp_extract(e, '^([^@]+)@', 1),
      'project_number', CAST(NULL AS STRING),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[0-9]+-compute@developer\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'default_compute',
      'project_id', CAST(NULL AS STRING),
      'project_number', regexp_extract(e, '^([0-9]+)-compute@', 1),
      'service', CAST(NULL AS STRING))
    WHEN e RLIKE r'^[^@]+@[^.@]+\.gserviceaccount\.com$' THEN named_struct(
      'account_type', 'service_agent',
      'project_id', CAST(NULL AS STRING),
      'project_number', nullif(regexp_extract(e, '^([0-9]+)@', 1), ''),
      'service', regexp_extract(e, '@([^.@]+)[.]', 1))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gcp_service_account_parse(email STRING)
  RETURNS STRUCT<account_type: STRING, project_id: STRING, project_number: STRING, service: STRING>
  DETERMINISTIC
  COMMENT 'Parses a GCP service account email into account_type (user_managed, default_app_engine, default_compute, service_agent), project_id, project_number and service. A serviceAccount: prefix is removed. Returns NULL for other values'
  RETURN gcp_service_account_parse_email(regexp_replace(trim(email), '^serviceAccount:', ''));
```

- [ ] **Step 4: Run the tests to verify that they pass**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/gcp_identifiers.sql uc-udfs/tests/gcp_identifiers_test.sql`

Expected: one `ok` line per `CREATE OR REPLACE FUNCTION` and per test cell (for example `ok uc-udfs/tests/gcp_identifiers_test.sql: SELECT if(count(*) = 0, 'ok: ... ok: ...`), exit code 0.
If a test cell fails, its error lists every failed case as `input => actual, expected expected`. Fix the function, not the expected value, unless the expected value contradicts the spec (then stop and ask).

- [ ] **Step 5: Add the README entry**

In `uc-udfs/README.md`, add this bullet after the `azure_identifiers.sql` bullet:

```markdown
- `gcp_identifiers.sql` contains `gcp_resource_name_parse` (full resource names, selfLinks and relative names into service, project, location, resource type and name, and a map of collections to IDs), `gcp_principal_parse` (IAM principal identifiers, including the deny policy, workforce and workload identity forms), and `gcp_service_account_parse` (user-managed, default and Google-managed service accounts). They are pure SQL.
```

- [ ] **Step 6: Commit**

```bash
git add uc-udfs/gcp_identifiers.sql uc-udfs/tests/gcp_identifiers_test.sql uc-udfs/README.md
git commit -m "uc-udfs: GCP resource name, principal and service account functions

Co-authored-by: Isaac <no-reply@databricks.com>"
```

Expected: the secret-scan pre-commit hook passes. If it reports `aws-access-token`, a key-shaped literal is in the staged files: build it with `concat` (see Global Constraints). Never bypass the hook.

---

### Task 5: Storage URIs and OCSF mapping

**Files:**
- Create: `uc-udfs/cloud_resources.sql`
- Test: `uc-udfs/tests/cloud_resources_test.sql`
- Modify: `uc-udfs/README.md`

**Interfaces:**
- Consumes: `aws_arn_parse` (Task 2), `azure_resource_id_clean`, `azure_resource_id_parse` (Task 3), `gcp_resource_name_parse` (Task 4), with the struct types listed in those tasks.
- Produces:
  - `storage_uri_parse(uri STRING) RETURNS STRUCT<cloud: STRING, scheme: STRING, account: STRING, container: STRING, path: STRING>`
  - ``cloud_resource_to_ocsf(id STRING) RETURNS STRUCT<cloud: STRUCT<provider, region, zone, cloud_partition, account: STRUCT<uid, type, type_id INT>, org: STRUCT<uid, ou_uid>>, resource: STRUCT<uid, name, type, region, zone, cloud_partition, `group`: STRUCT<name>>>`` (all leaf fields `STRING` except `type_id`)
  - helpers: `storage_uri_parse_trimmed`, `cloud_resource_ocsf_aws`, `cloud_resource_ocsf_azure`, `cloud_resource_ocsf_gcp`

- [ ] **Step 1: Write the test notebook `uc-udfs/tests/cloud_resources_test.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Tests for `cloud_resources.sql`
-- MAGIC
-- MAGIC Each cell fails with `raise_error` and lists the failed cases. Create the functions from `../aws_identifiers.sql`, `../azure_identifiers.sql`,
-- MAGIC `../gcp_identifiers.sql` and `../cloud_resources.sql` in the same schema first.

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: storage_uri_parse',
          raise_error(concat('storage_uri_parse: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(storage_uri_parse(input)) AS actual
    FROM VALUES
      ('s3://my-bucket/k/x.parquet', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k/x.parquet"}'),
      ('s3a://my-bucket/k', '{"cloud":"aws","scheme":"s3a","container":"my-bucket","path":"k"}'),
      ('s3n://my-bucket/k', '{"cloud":"aws","scheme":"s3n","container":"my-bucket","path":"k"}'),
      ('s3://my-bucket', '{"cloud":"aws","scheme":"s3","container":"my-bucket"}'),
      ('s3://my-bucket/', '{"cloud":"aws","scheme":"s3","container":"my-bucket"}'),
      ('S3://my-bucket/k', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k"}'),
      ('gs://my-bucket/o/p.json', '{"cloud":"gcp","scheme":"gs","container":"my-bucket","path":"o/p.json"}'),
      ('abfss://container1@account1.dfs.core.windows.net/p/q',
       '{"cloud":"azure","scheme":"abfss","account":"account1","container":"container1","path":"p/q"}'),
      ('abfs://container1@account1.dfs.core.windows.net/p',
       '{"cloud":"azure","scheme":"abfs","account":"account1","container":"container1","path":"p"}'),
      ('wasbs://container1@account1.blob.core.windows.net/p',
       '{"cloud":"azure","scheme":"wasbs","account":"account1","container":"container1","path":"p"}'),
      ('wasb://container1@account1.blob.core.windows.net',
       '{"cloud":"azure","scheme":"wasb","account":"account1","container":"container1"}'),
      ('abfss://container1@account1.dfs.core.chinacloudapi.cn/p',
       '{"cloud":"azure","scheme":"abfss","account":"account1","container":"container1","path":"p"}'),
      (' s3://my-bucket/k ', '{"cloud":"aws","scheme":"s3","container":"my-bucket","path":"k"}'),
      ('abfss://account1.dfs.core.windows.net/p', NULL),
      ('dbfs:/mnt/x', NULL),
      ('/Volumes/main/default/v1/f.csv', NULL),
      ('https://account1.blob.core.windows.net/container1/p', NULL),
      ('s3://', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);

-- COMMAND ----------

SELECT if(count(*) = 0, 'ok: cloud_resource_to_ocsf',
          raise_error(concat('cloud_resource_to_ocsf: ', string(count(*)), ' failed: ', array_join(collect_list(failure), ' | '))))
FROM (
  SELECT concat(coalesce(input, 'NULL'), ' => ', coalesce(actual, 'NULL'), ', expected ', coalesce(expected, 'NULL')) AS failure
  FROM (
    SELECT input, expected, to_json(cloud_resource_to_ocsf(input)) AS actual
    FROM VALUES
      ('arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:iam::123456789012:role/application_abc/component_xyz/RDSAccess","name":"RDSAccess","type":"iam:role","cloud_partition":"aws"}}'),
      ('arn:aws:s3:::example_bucket/marketing/report.csv',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws"},"resource":{"uid":"arn:aws:s3:::example_bucket/marketing/report.csv","name":"example_bucket/marketing/report.csv","type":"s3:object","cloud_partition":"aws"}}'),
      ('arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0',
       '{"cloud":{"provider":"AWS","region":"us-east-1","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0","name":"i-0123456789abcdef0","type":"ec2:instance","region":"us-east-1","cloud_partition":"aws"}}'),
      ('arn:aws:sns:us-east-1:123456789012:example-sns-topic-name',
       '{"cloud":{"provider":"AWS","region":"us-east-1","cloud_partition":"aws","account":{"uid":"123456789012","type":"AWS Account","type_id":10}},"resource":{"uid":"arn:aws:sns:us-east-1:123456789012:example-sns-topic-name","name":"example-sns-topic-name","type":"sns","region":"us-east-1","cloud_partition":"aws"}}'),
      ('arn:aws:iam::aws:policy/AdministratorAccess',
       '{"cloud":{"provider":"AWS","cloud_partition":"aws"},"resource":{"uid":"arn:aws:iam::aws:policy/AdministratorAccess","name":"AdministratorAccess","type":"iam:policy","cloud_partition":"aws"}}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1',
       '{"cloud":{"provider":"Azure","account":{"uid":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1","name":"vm1","type":"Microsoft.Compute/virtualMachines","group":{"name":"rg1"}}}'),
      ('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f',
       '{"cloud":{"provider":"Azure","account":{"uid":"aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Authorization/roleAssignments/bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","name":"bbbb1b1b-cc2c-dd3d-ee4e-ffffff5f5f5f","type":"Microsoft.Authorization/roleAssignments","group":{"name":"rg1"}}}'),
      ('/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam',
       '{"cloud":{"provider":"Azure","org":{"uid":"aaaabbbb-0000-cccc-1111-dddd2222eeee"}},"resource":{"uid":"/tenants/aaaabbbb-0000-cccc-1111-dddd2222eeee/providers/Microsoft.aadiam","type":"Microsoft.aadiam"}}'),
      ('"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1/"',
       '{"cloud":{"provider":"Azure","account":{"uid":"AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E","type":"Azure Subscription","type_id":13}},"resource":{"uid":"/SUBSCRIPTIONS/AAAA0A0A-BB1B-CC2C-DD3D-EEEEEE4E4E4E/RESOURCEGROUPS/RG1/PROVIDERS/MICROSOFT.NETWORK/NETWORKSECURITYGROUPS/NSG1","name":"NSG1","type":"MICROSOFT.NETWORK/NETWORKSECURITYGROUPS","group":{"name":"RG1"}}}'),
      ('//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1',
       '{"cloud":{"provider":"GCP","region":"us-central1","zone":"us-central1-a","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm1","name":"vm1","type":"compute.googleapis.com/instances","region":"us-central1","zone":"us-central1-a"}}'),
      ('https://www.googleapis.com/compute/v1/projects/my-project/global/networks/default',
       '{"cloud":{"provider":"GCP","region":"global","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"//compute.googleapis.com/projects/my-project/global/networks/default","name":"default","type":"compute.googleapis.com/networks","region":"global"}}'),
      ('//cloudresourcemanager.googleapis.com/folders/123456789012',
       '{"cloud":{"provider":"GCP","org":{"ou_uid":"123456789012"}},"resource":{"uid":"//cloudresourcemanager.googleapis.com/folders/123456789012","name":"123456789012","type":"cloudresourcemanager.googleapis.com/folders"}}'),
      ('projects/my-project/topics/t1',
       '{"cloud":{"provider":"GCP","account":{"uid":"my-project","type":"GCP Project","type_id":11}},"resource":{"uid":"projects/my-project/topics/t1","name":"t1","type":"topics"}}'),
      ('arn:aws:sqs', NULL),
      ('/foo/bar', NULL),
      ('s3://my-bucket/k', NULL),
      ('vm1', NULL),
      ('', NULL),
      (NULL, NULL)
      AS t(input, expected)
  )
  WHERE NOT (actual <=> expected)
);
```

- [ ] **Step 2: Run the tests to verify that they fail**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/aws_identifiers.sql uc-udfs/azure_identifiers.sql uc-udfs/gcp_identifiers.sql uc-udfs/tests/cloud_resources_test.sql`

Expected: `FAILED uc-udfs/tests/cloud_resources_test.sql: SELECT if(count(*) = 0, ...` with `[UNRESOLVED_ROUTINE] Cannot resolve routine `storage_uri_parse``, exit code 1.

- [ ] **Step 3: Create `uc-udfs/cloud_resources.sql`**

```sql
-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Cross-cloud user-defined functions: storage URIs and OCSF
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `storage_uri_parse(uri)` | `STRUCT<cloud, scheme, account, container, path>` for `s3://`, `s3a://`, `s3n://`, `gs://`, `abfss://`, `abfs://`, `wasbs://`, `wasb://` URIs |
-- MAGIC | `cloud_resource_to_ocsf(id)` | `STRUCT<cloud, resource>` in the shape of the OCSF [`cloud`](https://schema.ocsf.io/objects/cloud) and [`resource_details`](https://schema.ocsf.io/objects/resource_details) objects, for an AWS ARN, an Azure resource ID or a GCP resource name |
-- MAGIC | `storage_uri_parse_trimmed`, `cloud_resource_ocsf_aws`, `cloud_resource_ocsf_azure`, `cloud_resource_ocsf_gcp` | helpers, don't call them directly |
-- MAGIC
-- MAGIC This file uses the functions from `aws_identifiers.sql`, `azure_identifiers.sql` and `gcp_identifiers.sql`. Create them first, in the same schema.
-- MAGIC
-- MAGIC Behavior of `storage_uri_parse`:
-- MAGIC
-- MAGIC - `cloud` is `aws`, `azure` or `gcp`. `account` is the Azure storage account (`NULL` for S3 and GCS). `container` is the bucket or container.
-- MAGIC   `path` has no leading `/`, and is `NULL` if empty.
-- MAGIC - Azure hosts are `<account>.dfs.core.<suffix>` or `<account>.blob.core.<suffix>`, so sovereign clouds (`core.chinacloudapi.cn`, `core.usgovcloudapi.net`) work.
-- MAGIC - Other values (`dbfs:/`, `/Volumes/...`, HTTPS URLs) give `NULL`.
-- MAGIC
-- MAGIC Behavior of `cloud_resource_to_ocsf`:
-- MAGIC
-- MAGIC - The format is detected from the value: `arn:` is AWS; `/subscriptions/`, `/tenants/` and `/providers/` are Azure;
-- MAGIC   `//`, `https://...googleapis.com/`, `projects/`, `folders/` and `organizations/` are GCP. Other values and invalid identifiers give `NULL`.
-- MAGIC - `cloud.provider` is `AWS`, `Azure` or `GCP`. `cloud.account` is the AWS account (`type_id` 10), the Azure subscription (13) or the GCP project (11),
-- MAGIC   and `NULL` if the identifier doesn't have one. `cloud.org` is the Azure tenant, or the GCP organization and folder.
-- MAGIC - `resource.type` is `service:resource_type` for AWS (`iam:role`), the resource type for Azure (`Microsoft.Compute/virtualMachines`),
-- MAGIC   and `service/resource_type` for GCP (`compute.googleapis.com/instances`). `resource.group.name` is the Azure resource group.
-- MAGIC - For a GCP zone (`us-central1-a`), the region is the zone without the last part (`us-central1`). Azure resource IDs don't have a region.
-- MAGIC - The struct has only the fields that an identifier can give. Use its fields when you build an OCSF row.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION storage_uri_parse_trimmed(uri STRING)
  RETURNS STRUCT<cloud: STRING, scheme: STRING, account: STRING, container: STRING, path: STRING>
  DETERMINISTIC
  COMMENT 'Helper for storage_uri_parse: parses the trimmed URI'
  RETURN CASE
    WHEN uri RLIKE '(?i)^(s3[an]?|gs)://[^/@]+(/|$)' THEN named_struct(
      'cloud', if(lower(regexp_extract(uri, '^([^:]+)://', 1)) = 'gs', 'gcp', 'aws'),
      'scheme', lower(regexp_extract(uri, '^([^:]+)://', 1)),
      'account', CAST(NULL AS STRING),
      'container', regexp_extract(uri, '^[^:]+://([^/]+)', 1),
      'path', nullif(regexp_extract(uri, '^[^:]+://[^/]+/?(.*)$', 1), ''))
    WHEN uri RLIKE r'(?i)^(abfss?|wasbs?)://[^/@]+@[^/.]+\.(dfs|blob)\.core\.[^/]+(/|$)' THEN named_struct(
      'cloud', 'azure',
      'scheme', lower(regexp_extract(uri, '^([^:]+)://', 1)),
      'account', regexp_extract(uri, '@([^/.]+)[.]', 1),
      'container', regexp_extract(uri, '^[^:]+://([^/@]+)@', 1),
      'path', nullif(regexp_extract(uri, '^[^:]+://[^/]+/?(.*)$', 1), ''))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION storage_uri_parse(uri STRING)
  RETURNS STRUCT<cloud: STRING, scheme: STRING, account: STRING, container: STRING, path: STRING>
  DETERMINISTIC
  COMMENT 'Parses a cloud storage URI (s3://, s3a://, s3n://, gs://, abfss://, abfs://, wasbs://, wasb://) into cloud, scheme, account, container and path. Returns NULL for other values'
  RETURN storage_uri_parse_trimmed(trim(uri));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_aws(a STRUCT<`partition`: STRING, service: STRING, region: STRING, account_id: STRING, resource: STRING, resource_type: STRING, resource_id: STRING>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the result of aws_arn_parse to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN a IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'AWS',
        'region', a.region,
        'zone', CAST(NULL AS STRING),
        'cloud_partition', a.`partition`,
        'account', CASE WHEN a.account_id RLIKE '^[0-9]{12}$' THEN named_struct('uid', a.account_id, 'type', 'AWS Account', 'type_id', 10) END,
        'org', CAST(NULL AS STRUCT<uid: STRING, ou_uid: STRING>)),
      'resource', named_struct(
        'uid', concat('arn:', a.`partition`, ':', a.service, ':', coalesce(a.region, ''), ':', coalesce(a.account_id, ''), ':', a.resource),
        'name', CASE
          WHEN a.service = 'iam' AND a.resource_type IN ('user', 'group', 'role', 'policy', 'instance-profile', 'server-certificate')
            THEN element_at(split(a.resource_id, '/'), -1)
          ELSE a.resource_id
        END,
        'type', if(a.resource_type IS NULL, a.service, concat(a.service, ':', a.resource_type)),
        'region', a.region,
        'zone', CAST(NULL AS STRING),
        'cloud_partition', a.`partition`,
        'group', CAST(NULL AS STRUCT<name: STRING>)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_azure(id STRING, z STRUCT<tenant_id: STRING, subscription_id: STRING, resource_group: STRING, management_group: STRING,
                                                                         provider_namespace: STRING, resource_type: STRING, resource_name: STRING, parent_id: STRING>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the cleaned Azure resource ID and the result of azure_resource_id_parse to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN z IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'Azure',
        'region', CAST(NULL AS STRING),
        'zone', CAST(NULL AS STRING),
        'cloud_partition', CAST(NULL AS STRING),
        'account', CASE WHEN z.subscription_id IS NOT NULL THEN named_struct('uid', z.subscription_id, 'type', 'Azure Subscription', 'type_id', 13) END,
        'org', CASE WHEN z.tenant_id IS NOT NULL THEN named_struct('uid', z.tenant_id, 'ou_uid', CAST(NULL AS STRING)) END),
      'resource', named_struct(
        'uid', id,
        'name', z.resource_name,
        'type', z.resource_type,
        'region', CAST(NULL AS STRING),
        'zone', CAST(NULL AS STRING),
        'cloud_partition', CAST(NULL AS STRING),
        'group', CASE WHEN z.resource_group IS NOT NULL THEN named_struct('name', z.resource_group) END))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_ocsf_gcp(g STRUCT<service: STRING, relative_name: STRING, project: STRING, location: STRING, resource_type: STRING,
                                                          resource_name: STRING, parent: STRING, segments: MAP<STRING, STRING>>)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Helper for cloud_resource_to_ocsf: maps the result of gcp_resource_name_parse to the OCSF cloud and resource_details fields'
  RETURN CASE
    WHEN g IS NOT NULL THEN named_struct(
      'cloud', named_struct(
        'provider', 'GCP',
        'region', if(try_element_at(g.segments, 'zones') IS NULL, g.location, nullif(regexp_extract(try_element_at(g.segments, 'zones'), '^(.*)-[^-]+$', 1), '')),
        'zone', try_element_at(g.segments, 'zones'),
        'cloud_partition', CAST(NULL AS STRING),
        'account', CASE WHEN g.project IS NOT NULL THEN named_struct('uid', g.project, 'type', 'GCP Project', 'type_id', 11) END,
        'org', CASE
          WHEN try_element_at(g.segments, 'organizations') IS NOT NULL OR try_element_at(g.segments, 'folders') IS NOT NULL
            THEN named_struct('uid', try_element_at(g.segments, 'organizations'), 'ou_uid', try_element_at(g.segments, 'folders'))
        END),
      'resource', named_struct(
        'uid', if(g.service IS NULL, g.relative_name, concat('//', g.service, '/', g.relative_name)),
        'name', g.resource_name,
        'type', if(g.service IS NULL, g.resource_type, concat(g.service, '/', g.resource_type)),
        'region', if(try_element_at(g.segments, 'zones') IS NULL, g.location, nullif(regexp_extract(try_element_at(g.segments, 'zones'), '^(.*)-[^-]+$', 1), '')),
        'zone', try_element_at(g.segments, 'zones'),
        'cloud_partition', CAST(NULL AS STRING),
        'group', CAST(NULL AS STRUCT<name: STRING>)))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION cloud_resource_to_ocsf(id STRING)
  RETURNS STRUCT<cloud: STRUCT<provider: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                               account: STRUCT<uid: STRING, type: STRING, type_id: INT>, org: STRUCT<uid: STRING, ou_uid: STRING>>,
                 resource: STRUCT<uid: STRING, name: STRING, type: STRING, region: STRING, zone: STRING, cloud_partition: STRING,
                                  `group`: STRUCT<name: STRING>>>
  DETERMINISTIC
  COMMENT 'Maps an AWS ARN, an Azure resource ID or a GCP resource name to the OCSF cloud and resource_details fields (provider, account, org, region, zone, resource uid, name, type). Returns NULL for other values'
  RETURN CASE
    WHEN trim(id) LIKE 'arn:%' THEN cloud_resource_ocsf_aws(aws_arn_parse(id))
    WHEN trim(id) RLIKE '(?i)^"?/(subscriptions|tenants|providers)/' THEN cloud_resource_ocsf_azure(azure_resource_id_clean(id), azure_resource_id_parse(id))
    WHEN trim(id) RLIKE r'^(//|https://[^/]*googleapis\.com/|(projects|folders|organizations)/)' THEN cloud_resource_ocsf_gcp(gcp_resource_name_parse(id))
  END;
```

- [ ] **Step 4: Run the tests to verify that they pass**

Run: `python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" uc-udfs/aws_identifiers.sql uc-udfs/azure_identifiers.sql uc-udfs/gcp_identifiers.sql uc-udfs/cloud_resources.sql uc-udfs/tests/cloud_resources_test.sql`

Expected: one `ok` line per `CREATE OR REPLACE FUNCTION` and per test cell (for example `ok uc-udfs/tests/cloud_resources_test.sql: SELECT if(count(*) = 0, 'ok: ... ok: ...`), exit code 0.
If a test cell fails, its error lists every failed case as `input => actual, expected expected`. Fix the function, not the expected value, unless the expected value contradicts the spec (then stop and ask).

- [ ] **Step 5: Add the README entry**

In `uc-udfs/README.md`, add this bullet after the `gcp_identifiers.sql` bullet:

```markdown
- `cloud_resources.sql` contains `storage_uri_parse` (`s3://`, `gs://`, `abfss://` and `wasbs://` URIs into cloud, account, container and path) and `cloud_resource_to_ocsf`, which maps an AWS ARN, an Azure resource ID or a GCP resource name to the fields of the OCSF `cloud` and `resource_details` objects. Create the functions from `aws_identifiers.sql`, `azure_identifiers.sql` and `gcp_identifiers.sql` first.
```

- [ ] **Step 6: Check the time on 1,000,000 rows (Review Focus 1)**

Create `/tmp/cloud_resources_perf.sql` (outside the repository; don't commit it):

```sql
-- Databricks notebook source
SELECT count(cloud_resource_to_ocsf(id))
FROM (
  SELECT CASE id % 3
    WHEN 0 THEN concat('arn:aws:iam::123456789012:role/r', id)
    WHEN 1 THEN concat('/subscriptions/aaaa0a0a-bb1b-cc2c-dd3d-eeeeee4e4e4e/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm', id)
    ELSE concat('//compute.googleapis.com/projects/my-project/zones/us-central1-a/instances/vm', id)
  END AS id
  FROM range(1000000)
);
```

Run: `time python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" /tmp/cloud_resources_perf.sql`

Expected: `ok /tmp/cloud_resources_perf.sql: SELECT count(cloud_resource_to_ocsf(id)) 1000000`, in less than 2 minutes on a Small warehouse. If it is slower or the statement fails while the query is analyzed or compiled (for example, the plan is too large), stop and report the time and the error to the user. Don't change the design without them.

- [ ] **Step 7: Run all files and tests together**

Run:
```bash
python3 uc-udfs/tests/run_sql.py --profile "$PROFILE" --warehouse-id "$WAREHOUSE_ID" --catalog "$CATALOG" --schema "$SCHEMA" \
  uc-udfs/aws_identifiers.sql uc-udfs/azure_identifiers.sql uc-udfs/gcp_identifiers.sql uc-udfs/cloud_resources.sql \
  uc-udfs/tests/aws_identifiers_test.sql uc-udfs/tests/azure_identifiers_test.sql uc-udfs/tests/gcp_identifiers_test.sql uc-udfs/tests/cloud_resources_test.sql
```

Expected: only `ok` lines (11 test cells: 4 AWS, 2 Azure, 3 GCP, 2 cross-cloud), exit code 0.

- [ ] **Step 8: Commit**

```bash
git add uc-udfs/cloud_resources.sql uc-udfs/tests/cloud_resources_test.sql uc-udfs/README.md
git commit -m "uc-udfs: storage URI parser and OCSF mapping for cloud identifiers

Co-authored-by: Isaac <no-reply@databricks.com>"
```

Expected: the secret-scan pre-commit hook passes. If it reports `aws-access-token`, a key-shaped literal is in the staged files: build it with `concat` (see Global Constraints). Never bypass the hook.
