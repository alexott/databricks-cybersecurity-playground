# Databricks notebook source
# MAGIC %pip install -U graphframes-py tldextract
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC # Web-graph domain reputation, propagation, clustering, two-sided scoring, freshness
# MAGIC Companion notebook for the blog post.
# MAGIC Parts A-C re-ground the published numbers; Part D adds a known-good baseline and Part E a
# MAGIC freshness feed. Emits all metrics via `dbutils.notebook.exit(json)` so a serverless job run
# MAGIC captures them.

# COMMAND ----------

import json
import pandas as pd
import tldextract
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast, pandas_udf
from pyspark.sql.types import StringType
from graphframes import GraphFrame

GRAPH = "cybersecurity.graph"
IOCS  = "cybersecurity.threatintel.iocs"
DNS   = "lw_aottdb_ws.gold.dns_activity"       # OCSF gold; malware-traffic-analysis.net captures
TOP   = "cybersecurity.datasets.top_sites"     # Cisco Umbrella + Tranco top 1M (known-good)
NEW   = "cybersecurity.datasets.new_sites"     # daily feed of newly-observed domains (freshness)
WHOIS = "cybersecurity.datasets.whois"         # registration dates (mostly established sites)
DOM_V = f"{GRAPH}.domain_vertices_2026_may_jun_jul"
DOM_E = f"{GRAPH}.domain_edges_2026_may_jun_jul"
R = {}  # results accumulator

def fwd(col):  # SURT com.blogspot -> blogspot.com
    return F.array_join(F.reverse(F.split(F.col(col), r"\.")), ".")

# Registered (pay-level) domain via the Public Suffix List, so multi-label suffixes
# such as co.uk resolve to bbc.co.uk instead of the crude last-two-labels co.uk.
# suffix_list_urls=() keeps every executor offline and deterministic on the bundled snapshot.
# iocs already ships psl.registered_domain, so the UDF is only needed for DNS and top_sites.
# A matching Unity Catalog function is in ../uc-udfs/tldextract.sql
_no_fetch_extract = tldextract.TLDExtract(suffix_list_urls=())

@pandas_udf(StringType())
def reg_domain_udf(names: pd.Series) -> pd.Series:
    def to_domain(name):
        if pd.isna(name):
            return None
        extracted = _no_fetch_extract(name)
        if extracted.top_domain_under_public_suffix:
            return extracted.top_domain_under_public_suffix
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}"
        return None
    return names.apply(to_domain)

def reg_domain(col):
    return reg_domain_udf(F.lower(F.col(col)))

domains = spark.table(DOM_V).select("id", "num_hosts", fwd("rev_domain").alias("domain"))
R["domain_vertices"] = domains.count()

# COMMAND ----------
# Part A - reputation as a feature -----------------------------------------------------------

ioc_dom = (
    spark.table(IOCS)
    .where("ioc_type IN ('domain','hostname','URL') AND psl.registered_domain IS NOT NULL")
    .select(F.lower(F.col("psl.registered_domain")).alias("domain"))
    .where("domain LIKE '%.%'").distinct()
    .withColumn("label", F.lit("known_bad"))
)
# Observed-in-DNS = DNS queries from malware-traffic-analysis.net captures (OCSF gold),
# excluding the synthetic demo data on this workspace.
dns_dom = (
    spark.table(DNS).where("query.hostname IS NOT NULL")
    .where("contains(coalesce(_meta.file_path,''), 'malware-traffic-analysis')")
    .select(reg_domain("query.hostname").alias("domain"))
    .where("domain IS NOT NULL AND domain LIKE '%.%'").distinct()
    .withColumn("label", F.lit("dns_observed"))
)
targets = ioc_dom.unionByName(dns_dom).groupBy("domain").agg(
    F.array_join(F.array_sort(F.array_distinct(F.collect_list("label"))), "+").alias("label")
)
R["targets_by_label"] = {r["label"]: r["count"] for r in targets.groupBy("label").count().collect()}

tgt_present = targets.join(domains, "domain", "left").select("domain", "label", "id", "num_hosts")
R["present_by_label"] = [r.asDict() for r in tgt_present.groupBy("label").agg(
    F.count("*").alias("n"),
    F.sum(F.when(F.col("id").isNotNull(), 1).otherwise(0)).alias("present"),
    F.round(F.avg(F.when(F.col("id").isNotNull(), 1.0).otherwise(0.0)), 4).alias("present_rate"),
).collect()]

present_ids = tgt_present.where("id IS NOT NULL").select("id").distinct()
in_deg = (spark.table(DOM_E).join(broadcast(present_ids), F.col("to_id") == F.col("id"))
          .groupBy("to_id").agg(F.count("*").alias("in_degree"))
          .select(F.col("to_id").alias("id"), "in_degree"))
scored = (tgt_present.join(in_deg, "id", "left")
          .withColumn("in_degree", F.coalesce("in_degree", F.lit(0)))
          .withColumn("present", F.col("id").isNotNull()))

R["reputation_by_label"] = [r.asDict() for r in scored.where("present").groupBy("label").agg(
    F.count("*").alias("present_n"),
    F.expr("percentile(in_degree, 0.5)").alias("median_in_degree"),
).collect()]

enriched = scored.withColumn(
    "web_reputation",
    F.when(~F.col("present"), "absent").when(F.col("in_degree") == 0, "isolated")
     .when(F.col("in_degree") < 5, "weak").otherwise("established"))
R["reputation_buckets"] = [r.asDict() for r in
    enriched.groupBy("label", "web_reputation").count().orderBy("label", "web_reputation").collect()]
R["sample_bad_absent"] = [r["domain"] for r in
    enriched.where("label LIKE '%known_bad%' AND web_reputation='absent'").select("domain").limit(15).collect()]
R["sample_bad_established"] = [r.asDict() for r in
    enriched.where("label LIKE '%known_bad%' AND web_reputation='established'")
    .orderBy(F.desc("in_degree")).select("domain", "in_degree", "num_hosts").limit(15).collect()]

# COMMAND ----------
# Part B - propagate badness from known-bad seeds (seeded PageRank, one hop) ------------------
# Seed on the REAL-infra bad set (present, in-degree < 500 so the abused-legit hubs are
# excluded), then score every domain by how many distinct known-bad domains it links to.

bad_seed = scored.where("label LIKE '%known_bad%' AND present AND in_degree < 500").select("id", "domain")
R["badrank_seed_count"] = bad_seed.count()
bad_seed_to = bad_seed.select(F.col("id").alias("to_id"))

link_to_bad = (spark.table(DOM_E)
    .join(broadcast(bad_seed_to), "to_id")
    .select("from_id", "to_id").distinct()
    .groupBy("from_id").agg(F.countDistinct("to_id").alias("bad_links")))

known_bad_ids = (scored.where("label LIKE '%known_bad%' AND present")
                 .select(F.col("id").alias("from_id")).distinct()
                 .withColumn("is_known_bad", F.lit(True)))
cand = (link_to_bad
    .join(domains, link_to_bad.from_id == domains.id)
    .join(known_bad_ids, "from_id", "left")
    .withColumn("is_known_bad", F.coalesce("is_known_bad", F.lit(False)))
    .select("from_id", "domain", "bad_links", "num_hosts", "is_known_bad"))
R["badrank_candidates_total"] = cand.count()
R["badrank_new_ge2"] = cand.where("NOT is_known_bad AND bad_links >= 2").count()
R["badrank_new_ge3"] = cand.where("NOT is_known_bad AND bad_links >= 3").count()
R["badrank_top_new"] = [r.asDict() for r in
    cand.where("NOT is_known_bad").orderBy(F.desc("bad_links"))
    .select("domain", "bad_links", "num_hosts").limit(25).collect()]

# COMMAND ----------
# Part C - do the feed's own bad domains cluster in the web graph? ----------------------------
# Induced subgraph on the real-infra known-bad set: keep only edges where both ends are bad.

real_bad_ids = bad_seed.select("id").distinct()
bad_from = real_bad_ids.select(F.col("id").alias("from_id"))
bad_to   = real_bad_ids.select(F.col("id").alias("to_id"))
bad_edges = (spark.table(DOM_E).join(broadcast(bad_from), "from_id").join(broadcast(bad_to), "to_id")
             .select("from_id", "to_id").distinct())
R["bad_to_bad_edges"] = bad_edges.count()

bad_deg = (bad_edges.select(F.col("from_id").alias("id"))
           .union(bad_edges.select(F.col("to_id").alias("id")))
           .groupBy("id").agg(F.count("*").alias("bad_degree")))
R["bad_nodes_with_bad_edge"] = bad_deg.count()

bad_vertices = bad_deg.select("id").join(domains, "id", "left")
sg_bad = GraphFrame(bad_vertices, bad_edges.select(F.col("from_id").alias("src"), F.col("to_id").alias("dst")))
cc = sg_bad.connectedComponents().select("id", "component")
comp_sizes = cc.groupBy("component").agg(F.count("*").alias("n")).orderBy(F.desc("n"))
R["bad_cc_total_components"] = comp_sizes.count()
R["bad_cc_top_sizes"] = [r["n"] for r in comp_sizes.limit(12).collect()]

# Most connected node in the largest component (the bridge)
seed_row = bad_deg.join(domains, "id").orderBy(F.desc("bad_degree")).first()
R["bridge_node"] = {"domain": seed_row["domain"], "bad_degree": seed_row["bad_degree"]}
top_comp = comp_sizes.first()["component"]
R["largest_component_domains"] = [r["domain"] for r in
    cc.where(F.col("component") == top_comp).join(domains, "id", "left").select("domain").limit(40).collect()]

# COMMAND ----------
# Part D - add a known-good baseline (two-sided scoring) --------------------------------------
# Known-good = latest top_sites snapshot, reduced to pay-level domains present in the graph.

latest_pd = spark.table(TOP).agg(F.max("p_date").alias("d")).first()["d"]
good_seed = (spark.table(TOP).where(F.col("p_date") == F.lit(latest_pd))
    .where("host IS NOT NULL")
    .select(reg_domain("host").alias("domain")).where("domain LIKE '%.%'").distinct()
    .join(domains, "domain").select("id", "domain"))
N_GOOD = good_seed.count()
N_BAD = R["badrank_seed_count"]
R["good_seed_count"] = N_GOOD

# How many known-good domains does each Part B linker point to (scoped to the linker set)?
linkers = cand.select("from_id").distinct()
link_to_good = (spark.table(DOM_E)
    .join(broadcast(good_seed.select(F.col("id").alias("to_id"))), "to_id")
    .join(broadcast(linkers), "from_id")
    .select("from_id", "to_id").distinct()
    .groupBy("from_id").agg(F.countDistinct("to_id").alias("good_links")))

# Total out-degree for the linker set, for the bad-fraction signal.
out_deg = (spark.table(DOM_E)
    .join(broadcast(linkers), "from_id")
    .select("from_id", "to_id").distinct()
    .groupBy("from_id").agg(F.countDistinct("to_id").alias("out_deg")))

good_ids = good_seed.select(F.col("id").alias("from_id")).withColumn("is_top_site", F.lit(True))
cand3 = (cand
    .join(link_to_good, "from_id", "left").withColumn("good_links", F.coalesce("good_links", F.lit(0)))
    .join(out_deg, "from_id")
    .join(good_ids, "from_id", "left").withColumn("is_top_site", F.coalesce("is_top_site", F.lit(False)))
    .withColumn("bad_frac", F.round(F.col("bad_links") / F.col("out_deg"), 4))
    .withColumn("net", F.round(F.col("bad_links") / F.lit(N_BAD) - F.col("good_links") / F.lit(N_GOOD), 4)))

new_ge2 = cand3.where("NOT is_known_bad AND bad_links >= 2")
R["d_new_ge2"] = new_ge2.count()
R["d_itself_top_site"] = new_ge2.where("is_top_site").count()
R["d_top_by_raw_count"] = [r.asDict() for r in
    new_ge2.orderBy(F.desc("bad_links")).select("domain", "bad_links", "good_links", "net", "is_top_site").limit(12).collect()]
R["d_top_by_bad_frac"] = [r.asDict() for r in
    new_ge2.where("NOT is_top_site AND bad_links >= 10").orderBy(F.desc("bad_frac"))
    .select("domain", "bad_links", "good_links", "out_deg", "bad_frac").limit(20).collect()]
R["d_sinks_examples"] = [r.asDict() for r in
    cand3.where("domain IN ('blogspot.com','backlinks.sbs','worldbusinesspromote.com','wikipedia.org','mex.com')")
    .select("domain", "bad_links", "good_links", "bad_frac", "net", "is_top_site").collect()]

# COMMAND ----------
# Part E - freshness and age -----------------------------------------------------------------
# whois (registration dates) covers mostly established sites; new_sites is the freshness feed.

whois_reg = (spark.table(WHOIS)
    .where("registered_domain IS NOT NULL AND created IS NOT NULL "
           "AND created BETWEEN TIMESTAMP'1991-01-01' AND current_timestamp()")
    .groupBy(F.lower(F.col("registered_domain")).alias("domain"))
    .agg(F.min("created").alias("created")))
new_reg = (spark.table(NEW).where("domain IS NOT NULL")
    .groupBy(F.lower(F.col("domain")).alias("domain")).agg(F.min("date").alias("first_seen")))

kb = scored.where("label LIKE '%known_bad%'").select("domain", "present",
        (F.when(~scored.present, "absent").when(scored.in_degree == 0, "isolated")
          .when(scored.in_degree < 5, "weak").otherwise("established")).alias("web_rep"))
# whois coverage of the known-bad feed, by graph presence
R["e_whois_coverage"] = [r.asDict() for r in
    kb.join(whois_reg, "domain", "left").groupBy("present").agg(
        F.count("*").alias("kb"), F.sum(F.when(F.col("created").isNotNull(), 1).otherwise(0)).alias("have_whois")
    ).collect()]

# Coverage: graph vs new_sites vs union
cov = (kb.select("domain", F.col("present").alias("in_graph"))
       .join(new_reg.withColumn("in_new", F.lit(True)), "domain", "left")
       .withColumn("in_new", F.coalesce("in_new", F.lit(False))))
R["e_coverage"] = cov.agg(
    F.count("*").alias("total_kb"),
    F.round(F.avg(F.col("in_graph").cast("int")), 4).alias("graph_cov"),
    F.round(F.avg(F.col("in_new").cast("int")), 4).alias("newsites_cov"),
    F.round(F.avg((F.col("in_graph") | F.col("in_new")).cast("int")), 4).alias("union_cov"),
    F.sum(F.when(~F.col("in_graph") & F.col("in_new"), 1).otherwise(0)).alias("recovered_by_newsites"),
).first().asDict()

# Recency of known-bad in new_sites, by graph presence
R["e_recency"] = [r.asDict() for r in
    kb.join(new_reg, "domain").groupBy("present").agg(
        F.expr("percentile(datediff(DATE'2026-09-19', date(first_seen)), 0.5)").alias("median_days"),
        F.round(F.avg((F.col("first_seen") >= F.lit("2024-01-01")).cast("int")), 4).alias("frac_since_2024"),
    ).collect()]

# Reputation x freshness triage cross-tab
R["e_triage"] = [r.asDict() for r in
    kb.join(new_reg.withColumn("in_new", F.lit(True)), "domain", "left")
    .groupBy("web_rep").agg(
        F.count("*").alias("kb"),
        F.round(F.avg(F.col("in_new").isNotNull().cast("int")), 4).alias("frac_new")
    ).collect()]

dbutils.notebook.exit(json.dumps(R, default=str))
