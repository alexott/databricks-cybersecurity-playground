-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## User-defined function to parse User-Agent strings
-- MAGIC
-- MAGIC `user_agent_parse(ua)` parses an HTTP `User-Agent` string into the browser (or other client), the operating system and the device, in pure SQL, without a Python sandbox.
-- MAGIC The field names and the family names are the same as in [ua-parser](https://github.com/ua-parser/uap-python) (`parser.parse(ua).with_defaults()`).
-- MAGIC
-- MAGIC | Function | Result |
-- MAGIC |----------|--------|
-- MAGIC | `user_agent_parse(ua)` | `STRUCT<device_brand, device_family, device_model, os_family, os_major, os_minor, os_patch, os_patch_minor, user_agent_family, user_agent_major, user_agent_minor, user_agent_patch>` |
-- MAGIC | `user_agent_match`, `user_agent_family_parts`, `user_agent_os_parts`, `user_agent_device_parts`, `user_agent_parse_parts` | helpers, don't call them directly |
-- MAGIC
-- MAGIC Behavior:
-- MAGIC
-- MAGIC - All fields are strings. An unknown client, OS or device gives the family `Other` and `NULL` for the other fields.
-- MAGIC - `NULL`, an empty string or only spaces give a struct with all fields `NULL`.
-- MAGIC - The rules are a subset of the [uap-core regexes](https://github.com/ua-parser/uap-core/blob/master/regexes.yaml): the most frequent browsers, operating systems, devices,
-- MAGIC   search engine bots, HTTP libraries, cloud SDKs and CLIs. For other values the result can be different from ua-parser (usually `Other`, or a less specific family).
-- MAGIC   For example, there are no device rules for Android brands other than Samsung, Google, Xiaomi, Motorola and HTC: other Android devices give the brand `Generic_Android`.
-- MAGIC - Security scanners (`Nmap`, `sqlmap`, `Nikto`, `zgrab`, `masscan`) have their own family. ua-parser gives `Other` for most of them.
-- MAGIC - Bots (`Googlebot`, `bingbot`, `*bot/1.0`, `*spider/1.0`, ...) give the device `Spider`, with the model `Smartphone` for the mobile crawlers and `Desktop` for the others.
-- MAGIC
-- MAGIC Add a rule: add a `user_agent_match(ua, regex, array(replacements...))` line in the order of the rules (the first rule that matches is used).
-- MAGIC The replacements can have `$1` ... `$5` for the capture groups of the regex. A group that doesn't match and an empty replacement give `NULL`.
-- MAGIC
-- MAGIC Create all functions in the same schema: a function body finds the other functions in the schema of that function.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_match(ua STRING, re STRING, replacement ARRAY<STRING>)
  RETURNS ARRAY<STRING>
  DETERMINISTIC
  COMMENT 'Helper for user_agent_parse: if the regex matches the User-Agent, returns the replacements with $1...$5 replaced by the capture groups. Returns NULL if the regex does not match'
  -- One regexp_replace call fills all replacements: the regex matches the full string, the replacements are joined with \u0001 and split after.
  -- The empty groups at the end make $1...$5 valid for a regex that has less than 5 groups.
  RETURN CASE WHEN ua RLIKE re THEN transform(
    split(regexp_replace(ua, concat('(?s)^.*?(?:', re, ').*$()()()()()'), array_join(replacement, '\u0001')), '\u0001'),
    x -> nullif(trim(x), ''))
  END;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_family_parts(ua STRING)
  RETURNS ARRAY<STRING>
  DETERMINISTIC
  COMMENT 'Helper for user_agent_parse: returns the client family, major, minor and patch versions'
  RETURN coalesce(
    -- bots
    user_agent_match(ua, r'facebookexternalhit/(\d+)\.(\d+)', array('FacebookBot', '$1', '$2', '')),
    user_agent_match(ua, r'(Slackbot-LinkExpanding) (\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    user_agent_match(ua, r'(ChatGPT-User)/(\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    user_agent_match(ua, r'Yahoo! Slurp', array('Yahoo! Slurp', '', '', '')),
    user_agent_match(ua, r'\b([A-Za-z0-9_-]*(?:[Bb]ot|[Ss]pider|[Cc]rawler[a-z]*))/v?(\d+)(?:\.(\d+))?(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    -- security scanners
    user_agent_match(ua, r'(?i)\bNmap Scripting Engine', array('Nmap', '', '', '')),
    user_agent_match(ua, r'^sqlmap/(\d+)\.(\d+)(?:\.(\d+))?', array('sqlmap', '$1', '$2', '$3')),
    user_agent_match(ua, r'\bNikto/(\d+)\.(\d+)(?:\.(\d+))?', array('Nikto', '$1', '$2', '$3')),
    user_agent_match(ua, r'\bzgrab/(\d+)(?:\.(\d+))?', array('zgrab', '$1', '$2', '')),
    user_agent_match(ua, r'\b(masscan)/(\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    -- mobile apps and email clients
    user_agent_match(ua, r'(Instagram) (\d+)\.(\d+)\.(\d+)', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'FBAV/(\d+)(?:\.(\d+))?(?:\.(\d+))?', array('Facebook', '$1', '$2', '$3')),
    user_agent_match(ua, r'\[FB(?:AN|_IAB)/', array('Facebook', '', '', '')),
    user_agent_match(ua, r'Microsoft Outlook 16\.', array('Outlook', '2016', '', '')),
    user_agent_match(ua, r'Microsoft Outlook 15\.', array('Outlook', '2013', '', '')),
    user_agent_match(ua, r'(Thunderbird)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    -- browsers based on Chrome: before Chrome
    user_agent_match(ua, r'Edg(?:e|A|iOS)?/(\d+)\.(\d+)(?:\.(\d+))?', array('Edge', '$1', '$2', '$3')),
    user_agent_match(ua, r'OPR/(\d+)\.(\d+)(?:\.(\d+))?', array('Opera', '$1', '$2', '$3')),
    user_agent_match(ua, r'Opera/9\.80.*Version/(\d+)\.(\d+)(?:\.(\d+))?', array('Opera', '$1', '$2', '$3')),
    user_agent_match(ua, r'Opera[/ ](\d+)\.(\d+)', array('Opera', '$1', '$2', '')),
    user_agent_match(ua, r'UCBrowser/(\d+)\.(\d+)(?:\.(\d+))?', array('UC Browser', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Vivaldi)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'YaBrowser/(\d+)\.(\d+)(?:\.(\d+))?', array('Yandex Browser', '$1', '$2', '$3')),
    user_agent_match(ua, r'SamsungBrowser/(\d+)\.(\d+)(?:\.(\d+))?', array('Samsung Internet', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Brave)/(\d+)(?:\.(\d+))?(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(HeadlessChrome)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'CriOS/(\d+)\.(\d+)(?:\.(\d+))?', array('Chrome Mobile iOS', '$1', '$2', '$3')),
    user_agent_match(ua, r'FxiOS/(\d+)\.(\d+)(?:\.(\d+))?', array('Firefox iOS', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Chromium)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'; wv\).*Chrome/(\d+)\.(\d+)(?:\.(\d+))?', array('Chrome Mobile WebView', '$1', '$2', '$3')),
    user_agent_match(ua, r'Chrome/(\d+)\.(\d+)(?:\.(\d+))?.* Mobile', array('Chrome Mobile', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Chrome)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    -- other browsers. the Trident version gives the real IE version (MSIE is 7.0 in the compatibility mode)
    user_agent_match(ua, r'IEMobile[ /](\d+)\.(\d+)', array('IE Mobile', '$1', '$2', '')),
    user_agent_match(ua, r'MSIE \d+\.\d+;.*Trident/4\.0', array('IE', '8', '0', '')),
    user_agent_match(ua, r'MSIE \d+\.\d+;.*Trident/5\.0', array('IE', '9', '0', '')),
    user_agent_match(ua, r'MSIE \d+\.\d+;.*Trident/6\.0', array('IE', '10', '0', '')),
    user_agent_match(ua, r'MSIE \d+\.\d+;.*Trident/7\.0', array('IE', '11', '0', '')),
    user_agent_match(ua, r'MSIE (\d+)\.(\d+)', array('IE', '$1', '$2', '')),
    user_agent_match(ua, r'Trident/\d+\.\d+.*rv:(\d+)\.(\d+)', array('IE', '$1', '$2', '')),
    user_agent_match(ua, r'(?:Mobile|Tablet);.*Firefox/(\d+)\.(\d+)(?:\.(\d+))?', array('Firefox Mobile', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Firefox)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'Android (\d+)(?:\.(\d+))?(?:\.(\d+))?.*Version/[\d.]+ (?:Mobile )?Safari', array('Android', '$1', '$2', '$3')),
    user_agent_match(ua, r'Version/(\d+)\.(\d+)(?:\.(\d+))?.*Mobile/\w+ Safari', array('Mobile Safari', '$1', '$2', '$3')),
    user_agent_match(ua, r'(?:iPhone|iPad|iPod).*AppleWebKit/[\d.]+ \(KHTML, like Gecko\) Mobile/', array('Mobile Safari UI/WKWebView', '', '', '')),
    user_agent_match(ua, r'Version/(\d+)\.(\d+)(?:\.(\d+))?.*Safari/', array('Safari', '$1', '$2', '$3')),
    user_agent_match(ua, r'(?:iPhone|iPad|iPod).*AppleWebKit', array('Mobile Safari UI/WKWebView', '', '', '')),
    user_agent_match(ua, r'Android[ /-](\d+)(?:\.(\d+))?(?:\.(\d+))?', array('Android', '$1', '$2', '$3')),
    -- Windows components
    user_agent_match(ua, r'(WindowsPowerShell)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(Microsoft-CryptoAPI)/(\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    user_agent_match(ua, r'(Windows-Update-Agent)/(\d+)\.(\d+)\.(\d+)', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^(WinHTTP)\b', array('$1', '', '', '')),
    -- HTTP libraries, SDKs and CLIs
    user_agent_match(ua, r'^(curl)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^(Wget)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(?i)python-requests/(\d+)\.(\d+)', array('Python Requests', '$1', '$2', '')),
    user_agent_match(ua, r'(Python-urllib)/(\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    user_agent_match(ua, r'aiohttp/(\d+)\.(\d+)(?:\.(\d+))?', array('Python aiohttp', '$1', '$2', '$3')),
    user_agent_match(ua, r'(Go-http-client)/(\d+)\.(\d+)', array('$1', '$2', '$3', '')),
    user_agent_match(ua, r'(okhttp)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(Apache-HttpClient)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(PostmanRuntime)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^(axios)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^(aws-cli)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^(Boto3)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'(aws-sdk-[a-z]+(?:-v\d)?)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'^Java/1\.(\d+)\.(\d+)_(\d+)', array('Java', '$1', '$2', '$3')),
    user_agent_match(ua, r'^(Java)/(\d+)\.(\d+)(?:\.(\d+))?', array('$1', '$2', '$3', '$4')),
    array('Other'));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_os_parts(ua STRING)
  RETURNS ARRAY<STRING>
  DETERMINISTIC
  COMMENT 'Helper for user_agent_parse: returns the OS family, major, minor, patch and patch_minor versions'
  RETURN coalesce(
    user_agent_match(ua, r'Windows Phone(?: OS)? (\d+)\.(\d+)', array('Windows Phone', '$1', '$2', '', '')),
    user_agent_match(ua, r'Windows NT 10\.0', array('Windows', '10', '', '', '')),
    user_agent_match(ua, r'Windows NT 6\.3', array('Windows', '8', '1', '', '')),
    user_agent_match(ua, r'Windows NT 6\.2', array('Windows', '8', '', '', '')),
    user_agent_match(ua, r'Windows NT 6\.1', array('Windows', '7', '', '', '')),
    user_agent_match(ua, r'Windows NT 6\.0', array('Windows', 'Vista', '', '', '')),
    user_agent_match(ua, r'Windows NT 5\.[12]', array('Windows', 'XP', '', '', '')),
    user_agent_match(ua, r'Windows (98|95|ME)\b', array('Windows', '$1', '', '', '')),
    user_agent_match(ua, r'^(?:Windows Installer|Windows-)', array('Windows', '', '', '', '')),
    user_agent_match(ua, r'CPU (?:iPhone )?OS (\d+)_(\d+)(?:_(\d+))?(?:_(\d+))? like Mac OS X', array('iOS', '$1', '$2', '$3', '$4')),
    user_agent_match(ua, r'Mac OS X (\d+)[_.](\d+)(?:[_.](\d+))?', array('Mac OS X', '$1', '$2', '$3', '')),
    user_agent_match(ua, r'CrOS \w+ (\d+)\.(\d+)(?:\.(\d+))?', array('Chrome OS', '$1', '$2', '$3', '')),
    user_agent_match(ua, r'Android[ /]?(\d+)(?:\.(\d+))?(?:\.(\d+))?', array('Android', '$1', '$2', '$3', '')),
    user_agent_match(ua, r'Tizen (\d+)\.(\d+)', array('Tizen', '$1', '$2', '', '')),
    user_agent_match(ua, r'\b(Ubuntu|Debian|Fedora|FreeBSD|NetBSD|OpenBSD)\b', array('$1', '', '', '', '')),
    user_agent_match(ua, r'(?i)\blinux\b', array('Linux', '', '', '', '')),
    array('Other'));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_device_parts(ua STRING)
  RETURNS ARRAY<STRING>
  DETERMINISTIC
  COMMENT 'Helper for user_agent_parse: returns the device family, brand and model'
  RETURN coalesce(
    -- bots. The ua-parser rules match "bot" anywhere, so Boto3 is a Spider too
    user_agent_match(ua, r'(?i)(?:iPhone|Android|Windows Phone).*(?:bot|spider|crawler)', array('Spider', 'Spider', 'Smartphone')),
    user_agent_match(ua, r'(?i)bot|spider|crawl|slurp|facebookexternalhit|python-urllib|^Java/|masscan/|^WinHTTP', array('Spider', 'Spider', 'Desktop')),
    -- Apple
    user_agent_match(ua, r'FBDV/(iPhone[\d,]+)', array('iPhone', 'Apple', '$1')),
    user_agent_match(ua, r'\b(iPhone|iPad|iPod)\b', array('$1', 'Apple', '$1')),
    user_agent_match(ua, r'Macintosh', array('Mac', 'Apple', 'Mac')),
    -- Windows Phone
    user_agent_match(ua, r'NOKIA; (Lumia [^;)]+)', array('$1', 'Nokia', '$1')),
    user_agent_match(ua, r'IEMobile.*\bLG-([^;)]+)', array('LG-$1', 'LG', '$1')),
    -- generic phones and tablets
    user_agent_match(ua, r'Windows Phone|IEMobile', array('Generic Smartphone', 'Generic', 'Smartphone')),
    user_agent_match(ua, r'Android[\d. ]*; Mobile;', array('Generic Smartphone', 'Generic', 'Smartphone')),
    user_agent_match(ua, r'Android[\d. ]*; Tablet;', array('Generic Tablet', 'Generic', 'Tablet')),
    -- Android brands. The model ends at "; ", " Build/" or ") "
    user_agent_match(ua, r'\b(SM-[A-Z0-9]+)', array('Samsung $1', 'Samsung', '$1')),
    user_agent_match(ua, r'; (Pixel(?: [^;]+?)??)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('$1', 'Google', '$1')),
    user_agent_match(ua, r'; ((?:Redmi|POCO) [^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('XiaoMi $1', 'XiaoMi', '$1')),
    user_agent_match(ua, r'; moto ([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('Motorola $1', 'Motorola', '$1')),
    user_agent_match(ua, r'\bHTC[ _]([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('HTC $1', 'HTC', '$1')),
    user_agent_match(ua, r'; Lenovo[ _-]?([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('Lenovo $1', 'Lenovo', '$1')),
    user_agent_match(ua, r'\bLG-([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('LG-$1', 'LG', '$1')),
    user_agent_match(ua, r'; (CPH\d+)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('Oppo $1', 'Oppo', '$1')),
    -- other Android devices: a tablet if there is no "Mobile" in a full User-Agent without the language
    user_agent_match(ua, r'^(?!.*Mobile)(?=.*Safari).*Android [\d.]+; (?![a-z]{2}[-_][a-zA-Z]{2};)([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('$1', 'Generic_Android_Tablet', '$1')),
    user_agent_match(ua, r'Android [\d.]+; (?:[a-z]{2}[-_][a-zA-Z]{2}; )?([^;]+?)(?: Build/[^;)]*)?(?:;|\)(?: |$))', array('$1', 'Generic_Android', '$1')),
    -- consoles and TVs
    user_agent_match(ua, r'(PlayStation (?:\d+|Vita|Portable))', array('$1', 'Sony', '$1')),
    user_agent_match(ua, r'(SMART-TV); .*Tizen', array('Samsung $1', 'Samsung', '$1')),
    array('Other'));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_parse_parts(is_empty BOOLEAN, a ARRAY<STRING>, o ARRAY<STRING>, d ARRAY<STRING>)
  RETURNS STRUCT<device_brand: STRING, device_family: STRING, device_model: STRING, os_family: STRING, os_major: STRING, os_minor: STRING, os_patch: STRING, os_patch_minor: STRING, user_agent_family: STRING, user_agent_major: STRING, user_agent_minor: STRING, user_agent_patch: STRING>
  DETERMINISTIC
  COMMENT 'Helper for user_agent_parse: builds the result from the client, OS and device parts'
  RETURN named_struct(
    'device_brand', if(is_empty, NULL, get(d, 1)),
    'device_family', if(is_empty, NULL, get(d, 0)),
    'device_model', if(is_empty, NULL, get(d, 2)),
    'os_family', if(is_empty, NULL, get(o, 0)),
    'os_major', if(is_empty, NULL, get(o, 1)),
    'os_minor', if(is_empty, NULL, get(o, 2)),
    'os_patch', if(is_empty, NULL, get(o, 3)),
    'os_patch_minor', if(is_empty, NULL, get(o, 4)),
    'user_agent_family', if(is_empty, NULL, get(a, 0)),
    'user_agent_major', if(is_empty, NULL, get(a, 1)),
    'user_agent_minor', if(is_empty, NULL, get(a, 2)),
    'user_agent_patch', if(is_empty, NULL, get(a, 3)));

-- COMMAND ----------

CREATE OR REPLACE FUNCTION user_agent_parse(ua STRING)
  RETURNS STRUCT<device_brand: STRING, device_family: STRING, device_model: STRING, os_family: STRING, os_major: STRING, os_minor: STRING, os_patch: STRING, os_patch_minor: STRING, user_agent_family: STRING, user_agent_major: STRING, user_agent_minor: STRING, user_agent_patch: STRING>
  DETERMINISTIC
  COMMENT 'Parses a User-Agent string into the device (brand, family, model), the OS (family and versions) and the client (family and versions), like ua-parser. NULL or an empty string gives a struct with all fields NULL'
  RETURN user_agent_parse_parts(
    coalesce(trim(ua), '') = '',
    user_agent_family_parts(ua),
    user_agent_os_parts(ua),
    user_agent_device_parts(ua));
