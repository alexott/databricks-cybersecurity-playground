# Databricks notebook source
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from urllib.parse import urlparse
import os

# COMMAND ----------

crawl = "cc-main-2026-may-jun-jul"
uc_volume = "/Volumes/cybersecurity/graph/common_crawl/web_graph/"
dest_schema = "cybersecurity.graph"

# COMMAND ----------

def download_s3_to_uc_volume(s3_url: str, volume_path: str, anonymous: bool = False) -> str:
    """
    Download a file from an S3 URL and store it in a Unity Catalog Volume.

    Parameters
    ----------
    s3_url : str
        The S3 URL of the file to download (e.g., 's3://my-bucket/path/to/file.csv').
    volume_path : str
        The destination path in UC Volume 
        (e.g., '/Volumes/catalog/schema/volume_name/subfolder/file.csv').
    anonymous : bool, default False
        If True, use unsigned (anonymous) requests for publicly accessible buckets.

    Returns
    -------
    str
        The full path where the file was stored in the UC Volume.
    """
    # Parse the S3 URL to extract bucket and key
    parsed = urlparse(s3_url)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected an S3 URL (s3://...), got: {s3_url}")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    if not bucket or not key:
        raise ValueError(f"Invalid S3 URL. Could not parse bucket/key from: {s3_url}")

    # Ensure the destination directory exists
    dest_dir = os.path.dirname(volume_path)
    os.makedirs(dest_dir, exist_ok=True)

    # Download the file from S3 using boto3
    if anonymous:
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    else:
        s3_client = boto3.client("s3")
    s3_client.download_file(bucket, key, volume_path)

    print(f"Successfully downloaded s3://{bucket}/{key} to {volume_path}")
    return volume_path


# COMMAND ----------

import requests
import os


def download_http_to_uc_volume(url: str, volume_path: str, chunk_size: int = 65536) -> str:
    """
    Download a file from an HTTP/HTTPS URL and store it in a Unity Catalog Volume.

    Parameters
    ----------
    url : str
        The HTTP or HTTPS URL of the file to download
        (e.g., 'https://example.com/path/to/file.csv').
    volume_path : str
        The destination path in UC Volume
        (e.g., '/Volumes/catalog/schema/volume_name/subfolder/file.csv').
    chunk_size : int, default 8192
        The size of each chunk (in bytes) when streaming the download.

    Returns
    -------
    str
        The full path where the file was stored in the UC Volume.
    """
    # Validate the URL scheme
    if not url.startswith(("http://", "https://")):
        raise ValueError(f"Expected an HTTP/HTTPS URL, got: {url}")

    # Ensure the destination directory exists
    dest_dir = os.path.dirname(volume_path)
    os.makedirs(dest_dir, exist_ok=True)

    # Download the file using requests with streaming
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(volume_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)

    print(f"Successfully downloaded {url} to {volume_path}")
    return volume_path


# COMMAND ----------

import gzip
import os


def download_paths_gz_to_uc_volume(
    paths_gz_file: str,
    dest_dir: str,
    base_url: str = "https://data.commoncrawl.org/",
    chunk_size: int = 65536,
) -> list:
    """
    Read a .paths.gz file containing relative file paths (one per line),
    then download each file by appending the path to the base URL.

    Parameters
    ----------
    paths_gz_file : str
        Path to the local .paths.gz file
        (e.g., '/Volumes/catalog/schema/volume/cc-main-host-vertices.paths.gz').
    dest_dir : str
        Destination directory in UC Volume where downloaded files will be stored
        (e.g., '/Volumes/catalog/schema/volume/host_vertices/').
    base_url : str, default "https://data.commoncrawl.org/"
        The base URL to prepend to each relative path found in the .paths.gz file.
    chunk_size : int, default 65536
        The size of each chunk (in bytes) when streaming downloads.

    Returns
    -------
    list
        A list of local file paths that were successfully downloaded.
    """
    # Read all relative paths from the gzipped file
    with gzip.open(paths_gz_file, "rt", encoding="utf-8") as f:
        paths = [line.strip() for line in f if line.strip()]

    print(f"Found {len(paths)} file(s) to download from {paths_gz_file}")

    os.makedirs(dest_dir, exist_ok=True)

    downloaded_files = []
    for i, relative_path in enumerate(paths, start=1):
        url = base_url.rstrip("/") + "/" + relative_path.lstrip("/")
        # Preserve the filename (last segment of the path)
        filename = os.path.basename(relative_path)
        dest_path = os.path.join(dest_dir, filename)

        print(f"[{i}/{len(paths)}] Downloading {url} -> {dest_path}")
        try:
            download_http_to_uc_volume(url, dest_path, chunk_size=chunk_size)
            downloaded_files.append(dest_path)
        except Exception as e:
            print(f"  ERROR downloading {url}: {e}")

    print(f"\nDone. Successfully downloaded {len(downloaded_files)}/{len(paths)} file(s).")
    return downloaded_files


# COMMAND ----------

def get_graph_directory_names(dest: str, type: str):
    vertices_dir = os.path.join(dest, crawl+"-"+type+"-vertices")
    edges_dir = os.path.join(dest, crawl+"-"+type+"-edges")
    return (vertices_dir, edges_dir)

# COMMAND ----------

def get_graph_table_names(schema: str, type: str):
    crawl_name = crawl.replace("-", "_").replace("cc_main_", "")
    vertices_name = type+"_vertices_" + crawl_name
    edges_name = type+"_edges_" + crawl_name
    return (f"{schema}.{vertices_name}", f"{schema}.{edges_name}")

# COMMAND ----------

def split_gz_file(src_gz_path: str, dest_dir: str, prefix: str, rows_per_file: int = 10_000_000):
    """
    Read a gzipped text file from local disk and split it into multiple
    gzipped files with at most `rows_per_file` lines each.

    Strategy for speed — everything in shell (no Python I/O in the hot path):
      1. `gzip -dc` to decompress the source file (native C, streams stdout).
      2. Pipe into `split -l N` which writes fixed-line-count chunks to /tmp.
      3. For each chunk: compress with `gzip -1` and copy to the UC Volume.

    This is dramatically faster than Python's gzip.open + line iteration,
    especially for multi-GB edge files.
    """
    import tempfile
    import subprocess
    import shutil
    import time
    import glob

    os.makedirs(dest_dir, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix="split_gz_")

    try:
        # --- Step 1+2: decompress and split in one pipeline ---
        split_prefix = os.path.join(tmp_dir, "chunk_")
        t0 = time.time()
        # split with numeric suffixes of 5 digits, producing chunk_00000, chunk_00001, ...
        cmd = f"gzip -dc {src_gz_path} | split -l {rows_per_file} -d -a 5 - {split_prefix}"
        subprocess.run(cmd, shell=True, check=True)
        t1 = time.time()

        chunk_files = sorted(glob.glob(f"{split_prefix}*"))
        print(f"  decompressed + split into {len(chunk_files)} chunk(s) in {t1-t0:.1f}s")

        # --- Step 3: compress each chunk and copy to dest_dir (parallel) ---
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _compress_and_copy(i, chunk_path):
            named_path = os.path.join(tmp_dir, f"{prefix}-{i:05d}.txt")
            os.rename(chunk_path, named_path)

            tc0 = time.time()
            subprocess.run(["gzip", "-1", named_path], check=True)
            gz_path = named_path + ".gz"
            tc1 = time.time()

            final_name = os.path.basename(gz_path)
            final_path = os.path.join(dest_dir, final_name)
            shutil.copy2(gz_path, final_path)
            tc2 = time.time()

            os.remove(gz_path)
            return f"  chunk {final_name}: compress {tc1-tc0:.1f}s, copy {tc2-tc1:.1f}s"

        max_workers = min(10, len(chunk_files))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_compress_and_copy, i, chunk_path): i
                for i, chunk_path in enumerate(chunk_files)
            }
            for future in as_completed(futures):
                print(future.result())

        os.remove(src_gz_path)
        print(f"Split {src_gz_path} into {len(chunk_files)} file(s) in {dest_dir}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def download_web_graph(dest: str, type: str):
    """
    Download web graph files. For 'domain' type, downloads large single files
    to local disk first (much faster I/O), splits them there, then copies
    chunks to the UC Volume. For 'host' type, downloads directly to the volume.
    """
    import tempfile

    vertices_dir, edges_dir = get_graph_directory_names(dest, type)
    os.makedirs(vertices_dir, exist_ok=True)
    os.makedirs(edges_dir, exist_ok=True)
    if type == "domain":
        # Download large single files to LOCAL disk first for speed,
        # then split and copy chunks to the UC Volume
        local_tmp = tempfile.mkdtemp(prefix="webgraph_")
        vertices_local = os.path.join(local_tmp, f"{crawl}-{type}-vertices.txt.gz")
        edges_local = os.path.join(local_tmp, f"{crawl}-{type}-edges.txt.gz")

        print(f"Downloading vertices to local disk: {vertices_local}")
        download_http_to_uc_volume(
            f"https://data.commoncrawl.org/projects/hyperlinkgraph/{crawl}/{type}/{crawl}-{type}-vertices.txt.gz",
            vertices_local)
        split_gz_file(vertices_local, vertices_dir, f"{crawl}-{type}-vertices", rows_per_file=7_000_000)

        print(f"Downloading edges to local disk: {edges_local}")
        download_http_to_uc_volume(
            f"https://data.commoncrawl.org/projects/hyperlinkgraph/{crawl}/{type}/{crawl}-{type}-edges.txt.gz",
            edges_local)
        split_gz_file(edges_local, edges_dir, f"{crawl}-{type}-edges", rows_per_file=20_000_000)

        # Clean up local temp directory
        import shutil
        shutil.rmtree(local_tmp, ignore_errors=True)
    elif type == "host":
        vertices_paths = os.path.join(dest, f"{crawl}-{type}-vertices.paths.gz")
        edges_paths = os.path.join(dest, f"{crawl}-{type}-edges.paths.gz")
        download_http_to_uc_volume(
            f"https://data.commoncrawl.org/projects/hyperlinkgraph/{crawl}/{type}/{crawl}-{type}-vertices.paths.gz",
            vertices_paths)
        download_http_to_uc_volume(
            f"https://data.commoncrawl.org/projects/hyperlinkgraph/{crawl}/{type}/{crawl}-{type}-edges.paths.gz",
            edges_paths)
        download_paths_gz_to_uc_volume(vertices_paths, vertices_dir)
        download_paths_gz_to_uc_volume(edges_paths, edges_dir)
        os.remove(vertices_paths)
        os.remove(edges_paths)
    else:
        raise ValueError(f"Invalid type: {type}")


# COMMAND ----------

download_web_graph(uc_volume, "domain")
download_web_graph(uc_volume, "host")

# COMMAND ----------

# DBTITLE 1,Domain data load
domain_vertices_dir, domain_edges_dir = get_graph_directory_names(uc_volume, "domain")
domain_vertices = spark.read.csv(domain_vertices_dir, schema="id long, rev_domain string, num_hosts int", sep="\t", header=False).selectExpr("*", r"array_join(reverse(split(rev_domain, '\\.')), '.') as domain")
domain_edges = spark.read.csv(domain_edges_dir, sep="\t", header=False, schema="from_id long, to_id long")
domain_vertices_table, domain_edges_table = get_graph_table_names(dest_schema, "domain")
domain_vertices.write.mode("overwrite").saveAsTable(domain_vertices_table)
domain_edges.write.mode("overwrite").saveAsTable(domain_edges_table)
#display(domain_vertices)
#display(domain_edges)

# COMMAND ----------

# DBTITLE 1,Host data load
host_vertices_dir, host_edges_dir = get_graph_directory_names(uc_volume, "host")

host_vertices = spark.read.csv(host_vertices_dir, schema="id long, rev_host string", sep="\t", header=False).selectExpr("*", r"array_join(reverse(split(rev_host, '\\.')), '.') as host")
host_edges = spark.read.csv(host_edges_dir, sep="\t", header=False, schema="from_id long, to_id long")
host_vertices_table, host_edges_table = get_graph_table_names(dest_schema, "host")
host_vertices.write.mode("overwrite").saveAsTable(host_vertices_table)
host_edges.write.mode("overwrite").saveAsTable(host_edges_table)
#display(host_vertices)
#display(domain_edges)
