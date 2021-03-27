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
import traceback
import functools
import random
import ipaddress

FAIL_THRESHOLD = 20
RETRY_THRESHOLD = 3
SLEEP_AFTER_FETCH_FREG = 0
DEBUG = False
THREADS = 1
IP_POOL = None

BASE_DIR = None

PBAR_LEN = 80
PBAR_SYMBOL = "█"
PBAR_EMPTY_SYMBOL = "-"
PBAR_PRINT_INTERVAL = 5

ACCENT_CHARS = dict(zip('ÂÃÄÀÁÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖŐØŒÙÚÛÜŰÝÞßàáâãäåæçèéêëìíîïðñòóôõöőøœùúûüűýþÿ',
                        itertools.chain('AAAAAA', ['AE'], 'CEEEEIIIIDNOOOOOOO', ['OE'], 'UUUUUY', ['TH', 'ss'],
                                        'aaaaaa', ['ae'], 'ceeeeiiiionooooooo', ['oe'], 'uuuuuy', ['th'], 'y')))

# Beautiful stuff
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

class ProgressBar:
    def __init__(self, total, print=print):
        self.total = total
        self.progress = []
        self.progress_index = {}
        self.print = print
        self.finished = 0

        for i in range(PBAR_LEN):
            x = int(total / PBAR_LEN) * (i+1)
            self.progress.append([x, False])
            self.progress_index[x] = i

    def done(self, i):
        if i in self.progress_index:
            self.progress[self.progress_index[i]][1] = True
        self.finished += 1
        if not self.finished % PBAR_PRINT_INTERVAL or self.finished == self.total:
            self.print_progress()

    def print_progress(self):
        bar = ''
        for x in self.progress:
            bar += PBAR_SYMBOL if x[1] else PBAR_EMPTY_SYMBOL
        self.print(bar, self.finished / self.total)

# Beautiful stuff end
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

def get_seg_url(url, seg):
    parsed_url = urlsplit(url)
    qs = parse_qs(parsed_url.query)

    qs["sq"] = str(seg)

    parsed_url = list(parsed_url)
    parsed_url[3] = urlencode(qs, doseq=True)

    return urlunsplit(parsed_url)

def get_total_segment(url):
    seg_url = get_seg_url(url, 0)
    headers = None
    try:
        with urllib.request.urlopen(seg_url) as f:
            headers = f.headers
    except urllib.error.HTTPError as e:
        headers = e.headers
    return int(headers["x-head-seqnum"])
class SegmentStatus:
    def __init__(self, url, log_prefix="", print=print):
        self.segs = {}
        self.merged_seg = -1

        print(f"[INFO]{log_prefix} Try getting total segments...")
        self.end_seg = get_total_segment(url)
        print(f"[INFO]{log_prefix} Total segments: {self.end_seg}")

        self.seg_groups = []

        # Groups
        last_seg = -1
        interval = int(self.end_seg / THREADS)
        while True:
            if last_seg+1 + interval < self.end_seg:
                self.seg_groups.append((last_seg + 1, last_seg + 1 + interval))
                last_seg = last_seg + 1 + interval
            else:
                self.seg_groups.append((last_seg + 1, self.end_seg))
                break

## IP Pool
class BoundHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, *args, source_address=None, **kwargs):
        urllib.request.HTTPHandler.__init__(self, *args, **kwargs)
        self.http_class = functools.partial(http.client.HTTPConnection,
                source_address=source_address)

    def http_open(self, req):
        return self.do_open(self.http_class, req)

class BoundHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, *args, source_address=None, **kwargs):
        urllib.request.HTTPSHandler.__init__(self, *args, **kwargs)
        self.https_class = functools.partial(http.client.HTTPSConnection,
                source_address=source_address)

    def https_open(self, req):
        return self.do_open(self.https_class, req,
                context=self._context, check_hostname=self._check_hostname)

def get_random_line(filepath: str) -> str:
    file_size = os.path.getsize(filepath)
    with open(filepath, 'rb') as f:
        while True:
            pos = random.randint(0, file_size)
            if not pos:  # the first line is chosen
                return f.readline().decode()  # return str
            f.seek(pos)  # seek to random position
            f.readline()  # skip possibly incomplete line
            line = f.readline()  # read next (full) line
            if line:
                return line.decode()
            # else: line is empty -> EOF -> try another position in next iteration

def is_ip(ip):
    try:
        ip = ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False

def get_pool_ip():
    if IP_POOL:
        if os.path.isfile(IP_POOL):
            for _ in range(3): 
                ip = get_random_line(IP_POOL).rstrip().lstrip()
                if is_ip(ip):
                    return ip
    return None

## IP Pool end

def openurl(url, retry=0, source_address="random"):
    global opener

    try:
        if opener:
            return opener.open(url)
        else:
            if source_address == "random":
                source_address = get_pool_ip()
            if not is_ip(source_address):
                source_address = None
            if source_address:
                if DEBUG:
                    print(f"Using IP: {source_address}")
                if type(url) == str:
                    schema = urllib.parse.urlsplit(url).scheme
                elif isinstance(url, urllib.request.Request):
                    schema = urllib.parse.urlsplit(url.full_url).scheme

                handler = (BoundHTTPHandler if schema == "http" else BoundHTTPSHandler)(source_address=(source_address, 0))
                return urllib.request.build_opener(handler).open(url)
            else:
                return urllib.request.urlopen(url)
    except http.client.IncompleteRead as e:
        if retry >= RETRY_THRESHOLD:
            raise e
        else:
            return openurl(url, retry+1, source_address)
    except urllib.error.HTTPError as e:
        raise e
    except urllib.error.URLError as e:
        if retry >= RETRY_THRESHOLD:
            raise e
        else:
            return openurl(url, retry+1, source_address)

def download_segment(base_url, seg, seg_status, log_prefix="", print=print):
    target_url = get_seg_url(base_url, seg)

    target_url_with_header = urllib.request.Request(target_url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
    })

    try:
        with openurl(target_url_with_header) as response:
            with tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw.", suffix=".seg", dir=BASE_DIR) as tmp_file:
                shutil.copyfileobj(response, tmp_file)
                seg_status.segs[seg] = tmp_file.name
            return True
               
    except urllib.error.HTTPError as e:
        if DEBUG:
            print(f"[DEBUG]{log_prefix} Seg {seg} Failed with {e.code}")
        if e.code == 403:
            openurl(base_url)
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

def download_seg_group(url, seg_group_index, seg_status, log_prefix="", print=print, post_dl_seg=lambda x:True):
    seg_range = seg_status.seg_groups[seg_group_index]
    seg = seg_range[0]
    fail_count = 0

    try:
        while fail_count < FAIL_THRESHOLD:
            if DEBUG:
                print(f"[DEBUG]{log_prefix} Current Seg: {seg}")
            status = download_segment(url, seg, seg_status, log_prefix, print)
            if status:
                if DEBUG:
                    print(f"[DEBUG]{log_prefix} Success Seg: {seg}")
                if seg == seg_range[1]:
                    return True
                post_dl_seg(seg)
                seg += 1
                fail_count = 0
            else:
                fail_count += 1
                if DEBUG:
                    print(f"[DEBUG]{log_prefix} Failed Seg: {seg} [{fail_count}/{FAIL_THRESHOLD}]")
                time.sleep(1)
    except:
        traceback.print_exc()
        sys.exit(1)


def main(url, target_file, log_prefix="", print=print):
    seg_status = SegmentStatus(url, log_prefix, print)
    pbar = ProgressBar(seg_status.end_seg, lambda bar,p: print(f"{log_prefix}: |{bar}| {'{:.2f}'.format(p*100)}%"))

    merge_thread = threading.Thread(target=merge_segs, args=(target_file, seg_status), daemon=True)
    merge_thread.start()

    for i in range(len(seg_status.seg_groups)):
        threading.Thread(target=download_seg_group, args=(url, i, seg_status, log_prefix, print, lambda x: pbar.done(x)), daemon=True).start()

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
    -t, --threads [INT]         Multi-thread download, experimental.
    -ft, --fail-threshold [INT] Secs for retrying when encounter HTTP errors. Default 20.
    -p, --pool [FILE]           IP Pool file.
    -td, --temp-dir [DIR]       Temp file dir.
    -d, --debug                 Enable debug mode.
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
                elif args[i] == "-t" or args[i] == "--threads":
                    THREADS = int(args[i + 1])
                    i += 1
                elif args[i] == "-ft" or args[i] == "--fail-threshold":
                    FAIL_THRESHOLD = int(args[i + 1])
                    i += 1
                elif args[i] == "-p" or args[i] == "--pool":
                    IP_POOL = args[i+1]
                    i += 1
                elif args[i] == "-d" or args[i] == "--debug":
                    DEBUG = True
                elif args[i] == "-td" or args[i] == "--temp-dir":
                    BASE_DIR = args[i+1]
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

        if not BASE_DIR:
            BASE_DIR = tempfile.mkdtemp(prefix="ytarchive_raw.", suffix=f".{input_data['metadata']['id']}" if input_data is not None else None)
        elif os.path.isdir(BASE_DIR):
            BASE_DIR = tempfile.mkdtemp(prefix="ytarchive_raw.", suffix=f".{input_data['metadata']['id']}" if input_data is not None else None, dir=BASE_DIR)
        else:
            os.makedirs(BASE_DIR)

        tmp_video_f = tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw.", suffix=".video", dir=BASE_DIR)
        tmp_video = tmp_video_f.name
        tmp_video_f.close()

        tmp_audio_f = tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw.", suffix=".audio", dir=BASE_DIR)
        tmp_audio = tmp_audio_f.name
        tmp_audio_f.close()

        for i in range(len(param["iv"])):
            video_thread = threading.Thread(target=main, args=(param["iv"][i], tmp_video, f"[Video.{i}]", lambda x:print(f"{bcolors.OKBLUE}{x}{bcolors.ENDC}")), daemon=True)
            audio_thread = threading.Thread(target=main, args=(param["ia"][i], tmp_audio, f"[Audio.{i}]", lambda x:print(f"{bcolors.OKGREEN}{x}{bcolors.ENDC}")), daemon=True)

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
                with tempfile.NamedTemporaryFile(delete=False, prefix="ytarchive_raw.", suffix=".jpg", dir=BASE_DIR) as tmp_file:
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
            shutil.rmtree(BASE_DIR, ignore_errors=True)
        except:
            pass