# dlt_modern_stuff

This directory contains a source code that demonstrates use of latest Delta Live Tables (DLT) features for cybersecurity use cases.  You can find more information in the blog post (WIP).

## Getting started

1. Install the latest version of [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).

2. Authenticate to your Databricks workspace, if you have not done so already:

```sh
databricks configure
```

3. Set workspace URL and configure necessary variables in the `dev` profile of `databricks.yml` file.  You need to specify the following:

 - `catalog_name` - the name of the default UC Catalog used in configuration.
 - `silver_schema_name` - the name of UC Schema to put processed data of individual log sources.
 - `normalized_schema_name` - the name of UC Schema to put tables with normalized data, IoCs and Detections tables.
 - `log_files_path` - the path to UC Volume where raw log data will be stored.

4. To deploy a development copy of this project, type:

```sh
databricks bundle deploy
```

5. Run a job to set up the normalized tables and download sample log files:

```sh
databricks bundle run dlt_cyber_demo_setup
```

6. Run DLT pipelines to ingest data:

```sh
databricks bundle run demo_ingest_zeek_data
databricks bundle run demo_ingest_apache_data
```

7. Run DLT pipeline that emulates detections against normalized data:

```sh
databricks bundle run demo_detections
```

