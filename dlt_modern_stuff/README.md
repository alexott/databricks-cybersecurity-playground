# dlt_modern_stuff

This directory contains a source code that demonstrates use of latest Delta Live Tables (DLT) features for cybersecurity use cases.  You can find more information in the [blog post](https://alexott.blogspot.com/2025/03/effective-use-of-latest-dlt-features.html).

In general, this project consists of three DLT pipelines that perform data ingestion, normalization to [Open Cybersecurity Schema Framework (OCSF)](https://schema.ocsf.io/), and doing a rudimentary detections against normalized data as it's shown on the image below:

1. Ingestion of Apache Web and Nginx logs into `apache_web` table and then normalizing it into a table corresponding to OCSF's HTTP activity.
2. Ingestion of Zeek data:
  * Zeek HTTP data into `zeek_http` table,  and then normalizing it into an `http` table corresponding to OCSF's HTTP activity.  
  * Zeek Conn data into `zeek_conn` table,  and then normalizing it into a `network` table corresponding to OCSF's Network activity.
3. Detection pipeline that does the following:
  * Matches network connections data from `network` table against `iocs` table.
  * Checks HTTP logs from `http` table for admin pages scans from external parties.
  * All matches are stored in the `detections` table, and optionally pushed to EventHubs and/or Splunk.

![Implemented pipelines](images/cyber-pipeline-impl.png)


## Setting up & running

> [!IMPORTANT]
This bundle uses Serverless compute, so make sure that it's enabled for your workspace. If it's not, then you need to adjust parameters of the job and DLT pipelines!

1. Install the latest version of [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html).

2. Authenticate to your Databricks workspace, if you have not done so already:

```sh
databricks configure
```

3. Set environment variable `DATABRICKS_CONFIG_PROFILE` to the name of Databricks CLI profile you configured, and configure necessary variables in the `dev` profile of `databricks.yml` file.  You need to specify the following:

 - `catalog_name` - the name of the default UC Catalog used in configuration.
 - `silver_schema_name` - the name of an existing UC Schema to put processed data of individual log sources.
 - `normalized_schema_name` - the name of an existing UC Schema to put tables with normalized data, IoCs and Detections tables.
 - `log_files_path` - the path to an existing UC Volume where raw log data will be stored.

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

