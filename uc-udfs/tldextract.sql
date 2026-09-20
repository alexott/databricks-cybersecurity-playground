-- Databricks notebook source
CREATE OR REPLACE FUNCTION extract_registered_domain(url_or_hostname STRING)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
PARAMETER STYLE PANDAS
HANDLER 'handler_func'
ENVIRONMENT (
  dependencies = '["tldextract"]',
  environment_version = '5'
)
COMMENT 'Extract the registered domain (domain + suffix) from URLs or hostname using tldextract'
AS $$
import tldextract
import pandas as pd
from typing import Iterator

no_fetch_extract = tldextract.TLDExtract(suffix_list_urls=())

def handler_func(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for url_batch in batch_iter:
        def get_registered_domain(url_or_hostname):
            if pd.isna(url_or_hostname):
                return None
            extracted = no_fetch_extract(url_or_hostname)
            if extracted.top_domain_under_public_suffix:
                return extracted.top_domain_under_public_suffix
            if extracted.domain and extracted.suffix:
                return f"{extracted.domain}.{extracted.suffix}"
            return None
        
        yield url_batch.apply(get_registered_domain)
$$;

CREATE OR REPLACE FUNCTION extract_public_suffix(url_or_hostname STRING)
RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
PARAMETER STYLE PANDAS
HANDLER 'handler_func'
ENVIRONMENT (
  dependencies = '["tldextract"]',
  environment_version = '5'
)
COMMENT 'Extract the public suffix from URLs or hostname using tldextract (filter IPs first)'
AS $$
import tldextract
import pandas as pd
from typing import Iterator

no_fetch_extract = tldextract.TLDExtract(suffix_list_urls=())

def handler_func(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for url_batch in batch_iter:
        def get_registered_domain(url_or_hostname):
            if pd.isna(url_or_hostname):
                return None
            extracted = no_fetch_extract(url_or_hostname)
            return extracted.suffix
        
        yield url_batch.apply(get_registered_domain)
$$;
