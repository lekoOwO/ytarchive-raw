# ytarchive-raw

## Description

This project introduces a new method to grab **Privated**, **Removed** or **any** unavailable YouTube livestreams with prepared metadata files.

Use with [Auto YTA](https://github.com/lekoOwO/auto-ytarchive-raw) prevent any missing livestreams!

## Dependencies

 - ffmpeg

 - python3 > 3.4

## Usage

Get freg json file using the [bookmark script](https://gist.github.com/lekoOwO/c90c09409446e6c7663c489bf06dc649).

And do `python index.py -i FREG_JSON_FILE`

TADA!

See full parameter lists by `--help`:

```
Parameters:
-i, --input [JSON_FILE]     Input JSON file. Do not use with -iv or -ia.

-o, --output [OUTPUT_FILE]  Output file path. Uses `YYYYMMDD TITLE (VIDEO_ID).mkv` by default.
-s, --socks5-proxy [proxy]  Socks5 Proxy. No schema should be provided in the proxy url. PySocks should be installed.
-P, --http-proxy [proxy]    HTTP Proxy.
-t, --threads [INT]         Multi-thread download, experimental.
-p, --pool [FILE]           IP Pool file.
-d, --temp-dir [DIR]        Temp file dir.
-v, --verbose               Enable debug mode.
```
