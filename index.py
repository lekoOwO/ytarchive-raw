import urllib.request
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode
import urllib.error
import http.client
import shutil
import time
import threading
import os
import tempfile
import subprocess
import shlex
from datetime import date
import re
import itertools

FAIL_THRESHOLD = 20
RETRY_THRESHOLD = 3
SLEEP_AFTER_FETCH_FREG = 0.1
DEBUG = True
ACCENT_CHARS = dict(zip('ÂÃÄÀÁÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖŐØŒÙÚÛÜŰÝÞßàáâãäåæçèéêëìíîïðñòóôõöőøœùúûüűýþÿ',
                        itertools.chain('AAAAAA', ['AE'], 'CEEEEIIIIDNOOOOOOO', ['OE'], 'UUUUUY', ['TH', 'ss'],
                                        'aaaaaa', ['ae'], 'ceeeeiiiionooooooo', ['oe'], 'uuuuuy', ['th'], 'y')))
global opener
opener = None

def set_http_proxy(proxy):
    global opener
    handler =  urllib.request.ProxyHandler({
        "http": f"http://{proxy}",
        "https": f"http://{proxy}"
    })
    opener = urllib.request.build_opener(handler)

def set_socks5_proxy(host, port):
    import socks
    import socket

    socks.set_default_proxy(socks.SOCKS5, proxy, port)
    socket.socket = socks.socksocket

class SegmentStatus:
    def __init__(self):
        self.segs = {}
        self.merged_seg = -1
        self.end_seg = None


def get_seg_url(url, seg):
    parsed_url = urlsplit(url)
    qs = parse_qs(parsed_url.query)

    qs["sq"] = str(seg)

    parsed_url = list(parsed_url)
    parsed_url[3] = urlencode(qs, doseq=True)

    return urlunsplit(parsed_url)

def openurl(url, retry=0):
    global opener

    try:
        if opener:
            return opener.open(url)
        else:
            return urllib.request.urlopen(url)
    except http.client.IncompleteRead as e:
        if retry >= RETRY_THRESHOLD:
            raise e
        else:
            return openurl(url, retry+1)
    except urllib.error.HTTPError as e:
        raise e
    except urllib.error.URLError as e:
        if retry >= RETRY_THRESHOLD:
            raise e
        else:
            return openurl(url, retry+1)

def download_segment(base_url, seg, seg_status, log_prefix=""):
    target_url = get_seg_url(base_url, seg)

    try:
        with openurl(target_url) as response:
            if response.getcode() >= 300 or response.getcode() < 200:
                return False
            
            if SLEEP_AFTER_FETCH_FREG > 0:
                time.sleep(SLEEP_AFTER_FETCH_FREG)
            
            with tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw_",  suffix=".seg") as tmp_file:
                shutil.copyfileobj(response, tmp_file)
                seg_status.segs[seg] = tmp_file.name
            return True
               
    except urllib.error.HTTPError as e:
        if DEBUG:
            print(f"[DEBUG]{log_prefix} Seg {seg} Failed with {e.code}")
        return False

def merge_segs(target_file, seg_status):
    while seg_status.end_seg is None or seg_status.merged_seg != seg_status.end_seg:
        if (seg_status.merged_seg + 1) not in seg_status.segs:
            time.sleep(0.1)
            continue
        
        if os.path.exists(target_file):
            mode = "ab"
        else:
            mode = "wb"

        with open(target_file, mode) as target:
            with open(seg_status.segs[seg_status.merged_seg + 1], "rb") as source:
                shutil.copyfileobj(source, target)

        try:
            os.remove(seg_status.segs[seg_status.merged_seg + 1])
        except:
            pass

        seg_status.merged_seg += 1
        seg_status.segs.pop(seg_status.merged_seg)

def main(url, target_file, log_prefix=""):
    seg_status = SegmentStatus()

    merge_thread = threading.Thread(target=merge_segs, args=[target_file, seg_status])
    merge_thread.start()

    seg = seg_status.merged_seg + 1
    fail_count = 0

    while fail_count < FAIL_THRESHOLD:
        if DEBUG:
            print(f"[DEBUG]{log_prefix} Current Seg: {seg}")
            
        status = download_segment(url, seg, seg_status, log_prefix)
        if status:
            seg += 1
            fail_count = 0
        else:
            fail_count += 1
            if DEBUG:
                print(f"[DEBUG]{log_prefix} Failed Seg: {seg} [{fail_count}/{FAIL_THRESHOLD}]")
            time.sleep(1)

    seg_status.end_seg = seg - 1 # Current seg is not available.
    merge_thread.join() # Wait for merge finished

# ===== utils =====
def sanitize_filename(s, restricted=False, is_id=False):
    """Sanitizes a string so it could be used as part of a filename.
    If restricted is set, use a stricter subset of allowed characters.
    Set is_id if this is not an arbitrary string, but an ID that should be kept
    if possible.
    """
    def replace_insane(char):
        if restricted and char in ACCENT_CHARS:
            return ACCENT_CHARS[char]
        if char == '?' or ord(char) < 32 or ord(char) == 127:
            return ''
        elif char == '"':
            return '' if restricted else '\''
        elif char == ':':
            return '_-' if restricted else ' -'
        elif char in '\\/|*<>':
            return '_'
        if restricted and (char in '!&\'()[]{}$;`^,#' or char.isspace()):
            return '_'
        if restricted and ord(char) > 127:
            return '_'
        return char

    # Handle timestamps
    s = re.sub(r'[0-9]+(?::[0-9]+)+', lambda m: m.group(0).replace(':', '_'), s)
    result = ''.join(map(replace_insane, s))
    if not is_id:
        while '__' in result:
            result = result.replace('__', '_')
        result = result.strip('_')
        # Common case of "Foreign band name - English song title"
        if restricted and result.startswith('-_'):
            result = result[2:]
        if result.startswith('-'):
            result = '_' + result[len('-'):]
        result = result.lstrip('.')
        if not result:
            result = '_'
    return result
# ===== utils end =====

if __name__ == "__main__":
    import sys
    import pathlib

    try:
        # Parse params

        param = {
            "output": None,
            "iv": [],
            "ia": []
        }

        input_data = None

        args = sys.argv[1:]
        if len(args):
            i = 0
            while i < len(args):
                if args[i] == "-h" or args[i] == "--help":
                    print("""
    Parameters:
    -i, --input [JSON_FILE]     Input JSON file. Do not use with -iv or -ia.
    -iv, --input-video [URL]    Input video URL. Use with -ia.
    -ia, --input-audio [URL]    Input audio URL. Use with -iv.
    -o, --output [OUTPUT_FILE]  Output file path. Uses `YYYYMMDD TITLE (VIDEO_ID).mkv` by default.
    -s5, --socks5-proxy [proxy] Socks5 Proxy. No schema should be provided in the proxy url. PySocks should be installed.
    -hp, --http-proxy [proxy]   HTTP Proxy.
                    """)
                    sys.exit()
                if args[i] == "-i" or args[i] == "--input":
                    import json
                    with open(args[i+1], encoding='UTF-8') as f:
                        input_data = json.load(f)
                        param["iv"].append([*input_data["video"].values()][0])
                        param["ia"].append([*input_data["audio"].values()][0])
                    i += 1
                elif args[i] == "-iv" or args[i] == "--input-video":
                    param["iv"].append(args[i+1])
                    i += 1
                elif args[i] == "-ia" or args[i] == "--input-audio":
                    param["ia"].append(args[i+1])
                    i += 1
                elif args[i] == "-o" or args[i] == "--output":
                    param["output"] = args[i+1]
                    i += 1
                elif args[i] == "-s5" or args[i] == "--socks5-proxy":
                    proxy = args[i+1]
                    if ":" in proxy:
                        host, port = proxy.split(":")
                        port = int(port)
                    else:
                        host = proxy
                        port = 3128
                    set_socks5_proxy(host, port)
                    i += 1
                elif args[i] == "-hp" or args[i] == "--http-proxy":
                    proxy = args[i+1]
                    set_http_proxy(proxy)
                    i += 1
                else:
                    raise KeyError(f"Parameter not recognized: {args[i]}")
                
                i += 1

            if param["output"] is None:
                if input_data is not None:
                    try:
                        param["output"] = f"{date.today().strftime('%Y%m%d')} {sanitize_filename(input_data['metadata']['title'])} ({input_data['metadata']['id']}).mkv"
                    except Exception as e:
                        raise RuntimeError("JSON Version should be > 1.0, please update to the latest grabber.")
                else:
                    raise RuntimeError("Output param not found.")
            if pathlib.Path(param["output"]).suffix.lower() != ".mkv":
                raise RuntimeError("Output should be a mkv file.")
            if not param["ia"] or not param["iv"]:
                raise RuntimeError("Input data not sufficient. Both video and audio has to be inputed.")
            if len(param["ia"]) != len(param["iv"]):
                raise RuntimeError("Input video and audio length mismatch.")

        tmp_video_f = tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw_",  suffix="_video")
        tmp_video = tmp_video_f.name
        tmp_video_f.close()

        tmp_audio_f = tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw_",  suffix="_audio")
        tmp_audio = tmp_audio_f.name
        tmp_audio_f.close()

        for i in range(len(param["iv"])):
            video_thread = threading.Thread(target=main, args=(param["iv"][i], tmp_video, f"[Video.{i}]"), daemon=True)
            audio_thread = threading.Thread(target=main, args=(param["ia"][i], tmp_audio, f"[Audio.{i}]"), daemon=True)

            video_thread.start()
            audio_thread.start()

            while video_thread.is_alive():
                video_thread.join(0.5)
            while audio_thread.is_alive():
                audio_thread.join(0.5)

        if DEBUG:
            print("[DEBUG] Download finished. Merging...")

        if input_data is not None:
            tmp_thumbnail = None
            with urllib.request.urlopen(input_data['metadata']["thumbnail"]) as response:
                with tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw_", suffix=".jpg") as tmp_file:
                    shutil.copyfileobj(response, tmp_file)
                    tmp_thumbnail = tmp_file.name
            
            ffmpeg_params = [
                "-metadata", f"title=\"{input_data['metadata']['title']}\"",
                "-metadata", f"comment=\"{input_data['metadata']['description']}\"",
                "-metadata", f"author=\"{input_data['metadata']['channelName']}\"",
                "-metadata", f"episode_id=\"{input_data['metadata']['id']}\"",

                "-attach", f"'{tmp_thumbnail}'", 
                "-metadata:s:t", "mimetype=image/jpeg", 
                "-metadata:s:t", "filename=\"thumbnail.jpg\""
            ]
            ffmpeg_params = " ".join(ffmpeg_params)
        else:
            ffmpeg_params = ""
                
        cmd = f"ffmpeg -i '{tmp_video}' -i '{tmp_audio}' -c copy {ffmpeg_params} '{param['output']}'"
        if DEBUG:
            print(f"[DEBUG] ffmpeg command: {cmd}")
        p = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
                
        if type(out) == bytes:
            out = out.decode(sys.stdout.encoding)
        if type(err) == bytes:
            err = err.decode(sys.stdout.encoding)

        if len(err):
            print(f"[ERROR] FFmpeg: {err}")

    except KeyboardInterrupt as e:
        print("Program stopped.")

    finally:
        try:
            os.remove(tmp_video)
        except:
            pass

        try:
            os.remove(tmp_audio)
        except:
            pass

        try:
            if tmp_thumbnail:
                os.remove(tmp_thumbnail)
        except:
            pass