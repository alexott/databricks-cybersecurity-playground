# Threat analyst agent built with the Databricks Agent Bricks

This folder contains an example of cybersecurity agent built on top of Databricks Agent Bricks.  The code is a companion to the [blog post](link here).

This example contains materials obtained from different external sources:

- Threat Intelligence bulletins by [Check Point Research](https://research.checkpoint.com/intelligence-reports/).
- [Playbooks examples from Microsoft Learn](https://learn.microsoft.com/en-us/security/operations/incident-response-playbooks).
- [AWS Security Incident Response User Guide](https://docs.aws.amazon.com/security-ir/latest/userguide/what-is.html)

## Installation

1. Make sure that all prerequisites are available: Lakewatch gold tables, tables with IoCs and CVEs (see below); UC Volume that will store files for knowledge assistant.  If you don't want to create tables for IoCs and CVEs, remove corresponding objects from the `manifest.json`.
1. Use `import_supervisor_agent.py` from the [Databricks Labs Sandbox repo](https://github.com/databrickslabs/sandbox/tree/main/supervisor-agent-export-import) to import the agent definition from the current directory.  You will need to remap the original `fe_lakewatch_catalog` location of Lakewatch data plus IoCs and CVEs tables (in the `fe_lakewatch_catalog.lakewatch_field` schema).


### Setting up IoCs and CVEs tables

The [auxiliary](auxiliary/) directory contains SQL with tables definitions - adjust the catalog and schema names to match your setup.
