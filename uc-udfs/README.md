# Cybersecurity-related user-defined functions for Unity Catalog

There is a number of user-defined functions that could be useful when working with heterogeneous log sources.

Now available:

- `protocols.sql` contains two functions `proto_name_to_code` and `proto_code_to_name` to remap network protocol codes and names.
- `ocsf.sql` contains functions that map `activity_id` into `activity_name` for different categories.
- `community_id.sql` contains the `community_id_hash` function that calculates the [Community ID](https://github.com/corelight/community-id-spec) flow hash.
- `ip_classification.sql` contains IP classification functions `ip_is_private`, `ip_is_loopback`, `ip_is_link_local`, `ip_is_multicast`, `ip_is_reserved`, `ip_is_global`, and `ip_classify`. They are SQL functions over the native `ip_cidr_contains`, so they work for IPv4 and IPv6 (IPv4-mapped IPv6 addresses are classified by the embedded IPv4 address). These functions take a string. The `ip_*_binary` variants (`ip_is_private_binary`, ..., `ip_classify_binary`) take the binary form from `ip_as_binary`. The address is parsed only one time per call.
