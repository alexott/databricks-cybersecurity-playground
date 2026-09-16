# Databricks notebook source
# MAGIC %pip install -U pyvis graphframes # or graphframes-py for spark-connect
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

gold_schema = "fe_lakewatch_catalog.gold"

# COMMAND ----------

from graphframes_serverless import GraphFrame

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Example 1 - Following lateral movement

# COMMAND ----------

from pyspark.sql import functions as F

def endpoint_id(hostname_col, ip_col):
    hostname = F.lower(F.col(hostname_col))
    return F.when(
        hostname.isNotNull(),
        F.regexp_replace(hostname, r"\..*$", ""),
    ).otherwise(F.col(ip_col))

auth = (
    spark.table(f"{gold_schema}.authentication")
    .select(
        F.col("time").alias("event_time"),
        F.lower(
            F.coalesce(F.col("user.name"), F.col("actor.user.name"))
        ).alias("account"),
        endpoint_id("src_endpoint.hostname", "src_endpoint.ip").alias("src"),
        endpoint_id("dst_endpoint.hostname", "dst_endpoint.ip").alias("dst"),
        F.col("activity_name"),
        F.col("auth_protocol"),
        F.col("status_id"),
    )
    .where("status_id = 1")              # success
    .where("src IS NOT NULL AND dst IS NOT NULL")
    .where("account IS NOT NULL")
    .where("src <> dst")                 # remove local self-loops
)

host_to_host_logons = (
    auth.withColumn(
        "account_id",
        F.regexp_replace("account", "@.*$", ""),
    )
    .where(F.col("src").rlike(r"^(srv|win|paws)-"))
    .where(F.col("dst").rlike(r"^(srv|win|paws)-"))
    .select(
        "src",
        "dst",
        "event_time",
        "account",
        "account_id",
        F.lit("logged_into").alias("relationship"),
    )
    .dropDuplicates(["src", "dst", "account_id", "event_time"])
)

hosts = (
    host_to_host_logons.select(F.col("src").alias("id"))
    .union(host_to_host_logons.select(F.col("dst").alias("id")))
    .distinct()
)

g = GraphFrame(hosts, host_to_host_logons)

# COMMAND ----------

import re

suspect_account = "alice.suspect"
max_hop_gap_seconds = 30 * 60

candidate_paths = g.bfs(
    fromExpr="id = 'win-ep05'",
    toExpr="id = 'srv-dc-01'",
    edgeFilter=f"account_id = '{suspect_account}'",
    maxPathLength=5,
)

edge_cols = sorted(
    [c for c in candidate_paths.columns if re.fullmatch(r"e\d+", c)],
    key=lambda c: int(c[1:]),
)

same_timeline = None
for left, right in zip(edge_cols, edge_cols[1:]):
    step_is_ordered = (
        F.col(f"{left}.event_time") <= F.col(f"{right}.event_time")
    )
    step_is_close = (
        F.unix_timestamp(F.col(f"{right}.event_time"))
        - F.unix_timestamp(F.col(f"{left}.event_time"))
        <= max_hop_gap_seconds
    )
    step_condition = step_is_ordered & step_is_close
    same_timeline = (
        step_condition if same_timeline is None else same_timeline & step_condition
    )

ordered_paths = (
    candidate_paths.where(same_timeline)
    if same_timeline is not None
    else candidate_paths
)
ordered_paths.display()

# COMMAND ----------

ranked = g.pageRank(resetProbability=0.15, maxIter=10)
ranked.vertices.orderBy("pagerank", ascending=False).display()

# COMMAND ----------

ordered_3hop = (
    g.find("(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(d)")
    .where("a.id = 'win-ep05' AND d.id = 'srv-dc-01'")
    .where(f"e1.account_id = '{suspect_account}'")
    .where(f"e2.account_id = '{suspect_account}'")
    .where(f"e3.account_id = '{suspect_account}'")
    .where("e1.event_time <= e2.event_time")
    .where("e2.event_time <= e3.event_time")
    .where(
        F.unix_timestamp(F.col("e2.event_time"))
        - F.unix_timestamp(F.col("e1.event_time"))
        <= max_hop_gap_seconds
    )
    .where(
        F.unix_timestamp(F.col("e3.event_time"))
        - F.unix_timestamp(F.col("e2.event_time"))
        <= max_hop_gap_seconds
    )
)

ordered_3hop.select("a", "e1", "b", "e2", "c", "e3", "d").display()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Example 2 - Pivoting on a single indicator

# COMMAND ----------

def host_id(hostname_col, ip_col):
    hostname = F.lower(F.col(hostname_col))
    return F.when(
        hostname.isNotNull(),
        F.concat(F.lit("host:"), F.regexp_replace(hostname, r"\..*$", "")),
    ).otherwise(F.concat(F.lit("host:"), F.col(ip_col)))

def ip_id(ip_col):
    return F.concat(F.lit("ip:"), F.col(ip_col))

def domain_id(domain_col):
    return F.concat(F.lit("domain:"), F.lower(F.col(domain_col)))

def hash_id(hash_col):
    return F.concat(F.lit("hash:"), F.lower(F.col(hash_col)))

def is_external_ip(ip_col):
    ip = F.col(ip_col)
    return ip.isNotNull() & ~ip.rlike(
        r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.)"
    )

dns_edges = (
    spark.table(f"{gold_schema}.dns_activity")
    .select(
        F.col("time").alias("event_time"),
        domain_id("query.hostname").alias("src"),
        F.explode_outer("answers").alias("answer"),
    )
    .where(is_external_ip("answer.rdata"))
    .select(
        "event_time",
        "src",
        ip_id("answer.rdata").alias("dst"),
        F.lit("resolves_to").alias("relationship"),
    )
    .where("src IS NOT NULL AND dst IS NOT NULL")
)

network_edges = (
    spark.table(f"{gold_schema}.network_activity")
    .where(is_external_ip("dst_endpoint.ip"))
    .select(
        F.col("time").alias("event_time"),
        host_id("src_endpoint.hostname", "src_endpoint.ip").alias("src"),
        ip_id("dst_endpoint.ip").alias("dst"),
    )
    .withColumn("relationship", F.lit("connects_to"))
    .where("src IS NOT NULL AND dst IS NOT NULL")
)

process_edges = (
    spark.table(f"{gold_schema}.process_activity")
    .select(
        F.col("time").alias("event_time"),
        host_id("device.hostname", "device.ip").alias("src"),
        F.explode_outer("process.file.hashes").alias("hash"),
    )
    .select(
        "event_time",
        "src",
        hash_id("hash.value").alias("dst"),
        F.lit("executed").alias("relationship"),
    )
    .where("src IS NOT NULL AND dst IS NOT NULL")
)

edges = (
    dns_edges
    .unionByName(network_edges)
    .unionByName(process_edges)
    .dropDuplicates(["src", "dst", "relationship"])
)

vertices = (
    edges.select(F.col("src").alias("id"))
    .union(edges.select(F.col("dst").alias("id")))
    .distinct()
)

g = GraphFrame(vertices, edges)


# COMMAND ----------

cc = g.connectedComponents()

seed_component = cc.where("id = 'ip:185.220.101.42'").first()["component"]
campaign = cc.where(cc.component == seed_component)
campaign.display()

# COMMAND ----------

from pyvis.network import Network
from pyspark.sql.window import Window

node_colors = {
    "ip": "#e64545",
    "domain": "#f0932b",
    "host": "#8a8f98",
    "hash": "#7c5cff",
}

max_hosts = 20
max_hashes = 15
seed_id = "ip:185.220.101.42"

campaign_edges_full = (
    edges.join(campaign.select(F.col("id").alias("src")), "src")
    .join(campaign.select(F.col("id").alias("dst")), "dst")
)

degree = (
    campaign_edges_full.select(F.col("src").alias("id"))
    .union(campaign_edges_full.select(F.col("dst").alias("id")))
    .groupBy("id")
    .count()
)

ranked_nodes = degree.withColumn(
    "kind",
    F.split("id", ":").getItem(0),
).withColumn(
    "rank",
    F.row_number().over(
        Window.partitionBy("kind").orderBy(F.desc("count"), F.asc("id"))
    ),
)

visible_nodes = (
    ranked_nodes.where(
        (F.col("kind").isin("domain", "ip"))
        | ((F.col("kind") == "host") & (F.col("rank") <= max_hosts))
        | ((F.col("kind") == "hash") & (F.col("rank") <= max_hashes))
    )
    .select("id")
    .union(spark.createDataFrame([(seed_id,)], ["id"]))
    .distinct()
)

campaign_edges = (
    campaign_edges_full
    .join(visible_nodes.withColumnRenamed("id", "src"), "src")
    .join(visible_nodes.withColumnRenamed("id", "dst"), "dst")
)

campaign_nodes = (
    campaign_edges.select(F.col("src").alias("id"))
    .union(campaign_edges.select(F.col("dst").alias("id")))
    .union(spark.createDataFrame([(seed_id,)], ["id"]))
    .distinct()
)

net = Network(height="700px", width="100%", directed=True, notebook=False)

for row in campaign_nodes.collect():
    kind, label = row.id.split(":", 1)
    net.add_node(
        row.id,
        label=label,
        color=node_colors.get(kind, "#8a8f98"),
        borderWidth=4 if row.id == seed_id else 1,
    )

for row in campaign_edges.select("src", "dst", "relationship").collect():
    net.add_edge(row.src, row.dst, title=row.relationship)

displayHTML(net.generate_html())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Why scoping matters (Figure 6)
# MAGIC
# MAGIC The cluster above came out clean because the graph was scoped to *external*
# MAGIC infrastructure. Drop that filter and internal endpoint-to-server traffic pulls
# MAGIC the whole estate into one giant component - the attacker cluster disappears into
# MAGIC it. The cell below builds the unscoped graph, runs connected components, and
# MAGIC renders a bounded sample so you can see the "before" (one grey blob) next to the
# MAGIC scoped "after" (Figure 5).

# COMMAND ----------

# "Before": the same three edge sets WITHOUT the external-only filter, so internal
# RFC1918 traffic is kept. DNS answers are already almost all external; the mesh comes
# from network_activity - every endpoint talks to the same handful of internal servers.
network_edges_all = (
    spark.table(f"{gold_schema}.network_activity")
    .select(
        host_id("src_endpoint.hostname", "src_endpoint.ip").alias("src"),
        ip_id("dst_endpoint.ip").alias("dst"),
        F.lit("connects_to").alias("relationship"),
    )
    .where("src IS NOT NULL AND dst IS NOT NULL")
)

dns_edges_all = (
    spark.table(f"{gold_schema}.dns_activity")
    .select(
        domain_id("query.hostname").alias("src"),
        F.explode_outer("answers").alias("answer"),
    )
    .select(
        "src",
        ip_id("answer.rdata").alias("dst"),
        F.lit("resolves_to").alias("relationship"),
    )
    .where("src IS NOT NULL AND dst IS NOT NULL")
)

edges_all = (
    dns_edges_all
    .unionByName(network_edges_all)
    .unionByName(process_edges.select("src", "dst", "relationship"))
    .dropDuplicates(["src", "dst", "relationship"])
)

vertices_all = (
    edges_all.select(F.col("src").alias("id"))
    .union(edges_all.select(F.col("dst").alias("id")))
    .distinct()
)

cc_all = GraphFrame(vertices_all, edges_all).connectedComponents()

# The quantitative proof of the collapse: the seed's component now swallows the estate.
unscoped_component = cc_all.where(f"id = '{seed_id}'").first()["component"]
unscoped_size = cc_all.where(cc_all.component == unscoped_component).count()
print(f"seed component - unscoped: {unscoped_size} nodes vs scoped: {campaign.count()} nodes")

# COMMAND ----------

# You cannot draw millions of internal edges, so render a bounded SAMPLE of that giant
# component: the colored campaign cluster (from Figure 5) plus the top internal server
# hubs and every host hanging off them. Internal nodes are grey - the point is that the
# colored attacker cluster is a small corner of one grey blob.
import re

internal_ip_id = r"^ip:(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.)"
_INTERNAL_LABEL = re.compile(r"^(10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.)")

top_hubs = [
    row.id
    for row in (
        network_edges_all
        .where(F.col("dst").rlike(internal_ip_id))
        .groupBy(F.col("dst").alias("id"))
        .agg(F.countDistinct("src").alias("fanin"))
        .orderBy(F.desc("fanin"))
        .limit(20)
        .collect()
    )
]

# host -> internal-hub edges are the grey mesh; the campaign's own hosts sit in it too,
# so the colored cluster stays attached to the blob rather than floating off on its own.
mesh_edges = (
    network_edges_all
    .where(F.col("dst").isin(top_hubs))
    .select("src", "dst", "relationship")
    .distinct()
)

before_edges = (
    campaign_edges.select("src", "dst", "relationship")
    .union(mesh_edges)
    .distinct()
)
before_nodes = (
    before_edges.select(F.col("src").alias("id"))
    .union(before_edges.select(F.col("dst").alias("id")))
    .union(spark.createDataFrame([(seed_id,)], ["id"]))
    .distinct()
)

net = Network(height="700px", width="100%", directed=True, notebook=False)

for row in before_nodes.collect():
    kind, label = row.id.split(":", 1)
    is_internal = kind == "host" or (kind == "ip" and _INTERNAL_LABEL.match(label))
    net.add_node(
        row.id,
        label=label,
        color="#8a8f98" if is_internal else node_colors.get(kind, "#8a8f98"),
        borderWidth=4 if row.id == seed_id else 1,
    )

for row in before_edges.collect():
    net.add_edge(row.src, row.dst, title=row.relationship)

displayHTML(net.generate_html())
