"""This project introduces a new method to grab Privated,
Removed or any unavailable YouTube livestreams with prepared metadata files."""
import argparse
import functools
import http.client
import ipaddress
import itertools
import json
import logging
import os
import pathlib
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.request
from argparse import Namespace
from datetime import date
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

FAIL_THRESHOLD = 20
RETRY_THRESHOLD = 3
SLEEP_AFTER_FETCH_FREG = 0
DEBUG = False
THREADS = 1
IP_POOL = None
HTTP_TIMEOUT = 5

BASE_DIR = None

PBAR_LEN = 80
PBAR_SYMBOL = "█"
PBAR_EMPTY_SYMBOL = "-"
PBAR_PRINT_INTERVAL = 5

ACCENT_CHARS = dict(
    zip(
        "ÂÃÄÀÁÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖŐØŒÙÚÛÜŰÝÞßàáâãäåæçèéêëìíîïðñòóôõöőøœùúûüűýþÿ",
        itertools.chain(
            "AAAAAA",
            ["AE"],
            "CEEEEIIIIDNOOOOOOO",
            ["OE"],
            "UUUUUY",
            ["TH", "ss"],
            "aaaaaa",
            ["ae"],
            "ceeeeiiiionooooooo",
            ["oe"],
            "uuuuuy",
            ["th"],
            "y",
        ),
    )
)

socket.setdefaulttimeout(HTTP_TIMEOUT)
# ===== utils =====
def sanitize_filename(substitution, restricted=False, is_id=False):
    """Sanitizes a string so it could be used as part of a filename.
    If restricted is set, use a stricter subset of allowed characters.
    Set is_id if this is not an arbitrary string, but an ID that should be kept
    if possible.
    """

    def replace_insane(char):
        if restricted and char in ACCENT_CHARS:
            return ACCENT_CHARS[char]
        if char == "?" or ord(char) < 32 or ord(char) == 127:
            return ""
        if char == '"':
            return "" if restricted else "'"
        if char == ":":
            return "_-" if restricted else " -"
        if char in "\\/|*<>":
            return "_"
        if restricted and (char in "!&'()[]{}$;`^,#" or char.isspace()):
            return "_"
        if restricted and ord(char) > 127:
            return "_"
        return char

    # Handle timestamps
    substitution = re.sub(
        r"[0-9]+(?::[0-9]+)+", lambda m: m.group(0).replace(":", "_"), substitution
    )
    result = "".join(map(replace_insane, substitution))
    if not is_id:
        while "__" in result:
            result = result.replace("__", "_")
        result = result.strip("_")
        # Common case of "Foreign band name - English song title"
        if restricted and result.startswith("-_"):
            result = result[2:]
        if result.startswith("-"):
            result = "_" + result[len("-") :]
        result = result.lstrip(".")
        if not result:
            result = "_"
    return result


# ===== utils end =====

##### Beautiful stuff #####
bcolors = Namespace(
    HEADER="\033[95m",
    OKBLUE="\033[94m",
    OKCYAN="\033[96m",
    OKGREEN="\033[92m",
    WARNING="\033[93m",
    FAIL="\033[91m",
    ENDC="\033[0m",
    BOLD="\033[1m",
    UNDERLINE="\033[4m",
)

# Custom formatter https://stackoverflow.com/questions/1343227/
class Formatter(logging.Formatter):

    err_fmt = f"{bcolors.FAIL}[ERROR] %(msg)s{bcolors.ENDC}" "ERROR: %(msg)s"
    dbg_fmt = "[DEBUG] %(msg)s"
    info_fmt = "[INFO] %(msg)s"
    warn_fmt = f"{bcolors.WARNING}[WARN] %(msg)s{bcolors.ENDC}"

    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):

        # Save the original format configured by the user
        # when the logger formatter was instantiated
        format_orig = self._fmt

        # Replace the original format with one customized by logging level
        if record.levelno == logging.DEBUG:
            self._fmt = self.dbg_fmt

        elif record.levelno == logging.INFO:
            self._fmt = self.info_fmt

        elif record.levelno == logging.ERROR:
            self._fmt = self.err_fmt

        elif record.levelno == logging.WARN:
            self._fmt = self.warn_fmt

        # Call the original formatter class to do the grunt work
        result = logging.Formatter.format(self, record)

        # Restore the original format configured by the user
        self._fmt = format_orig

        return result


logger = logging.getLogger(__name__)
formatter = Formatter()
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)


class ProgressBar:
    def __init__(self, total, print_func=print):
        self.total = total
        self.progress = []
        self.progress_index = {}
        self.print = print_func
        self.finished = 0

        for i in range(PBAR_LEN):
            x = int(total / PBAR_LEN) * (i + 1)
            self.progress.append([x, False])
            self.progress_index[x] = i

    def done(self, index):
        if index in self.progress_index:
            self.progress[self.progress_index[index]][1] = True
        self.finished += 1
        if not self.finished % PBAR_PRINT_INTERVAL or self.finished == self.total:
            self.print_progress()

    def print_progress(self):
        bar_str = ""
        for x in self.progress:
            bar_str += PBAR_SYMBOL if x[1] else PBAR_EMPTY_SYMBOL
        self.print(bar_str, self.finished / self.total)


##### - Beautiful stuff - #####
opener = None


def set_http_proxy(proxy):
    global opener
    handler = urllib.request.ProxyHandler(
        {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    )
    opener = urllib.request.build_opener(handler)


def set_socks5_proxy(host, port):
    import socks

    socks.set_default_proxy(socks.SOCKS5, host, port)
    socket.socket = socks.socksocket


def get_seg_url(url, seg):
    parsed_url = urlsplit(url)
    parsed_query_string = parse_qs(parsed_url.query)

    parsed_query_string["sq"] = str(seg)

    parsed_url = list(parsed_url)
    parsed_url[3] = urlencode(parsed_query_string, doseq=True)

    return urlunsplit(parsed_url)


def get_total_segment(url):
    seg_url = get_seg_url(url, 0)
    headers = None
    try:
        with urllib.request.urlopen(seg_url) as f:
            headers = f.headers
    except urllib.error.HTTPError as http_error:
        headers = http_error.headers
    return int(headers["x-head-seqnum"])


class SegmentStatus:
    def __init__(self, url, log_prefix=""):
        self.segs = {}
        self.merged_seg = -1

        logger.info(f"{log_prefix} Try getting total segments...")
        self.end_seg = get_total_segment(url)
        logger.info(f"{log_prefix} Total segments: {self.end_seg}")

        self.seg_groups = []

        # Groups
        last_seg = -1
        interval = int(self.end_seg / THREADS)
        while True:
            if last_seg + 1 + interval < self.end_seg:
                self.seg_groups.append((last_seg + 1, last_seg + 1 + interval))
                last_seg = last_seg + 1 + interval
            else:
                self.seg_groups.append((last_seg + 1, self.end_seg))
                break


## IP Pool
class BoundHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, *args, source_address=None, **kwargs):
        urllib.request.HTTPHandler.__init__(self, *args, **kwargs)
        self.http_class = functools.partial(
            http.client.HTTPConnection,
            source_address=source_address,
            timeout=HTTP_TIMEOUT,
        )

    def http_open(self, req):
        return self.do_open(self.http_class, req)


class BoundHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, *args, source_address=None, **kwargs):
        urllib.request.HTTPSHandler.__init__(self, *args, **kwargs)
        self.https_class = functools.partial(
            http.client.HTTPSConnection,
            source_address=source_address,
            timeout=HTTP_TIMEOUT,
        )

    def https_open(self, req):
        return self.do_open(
            self.https_class,
            req,
            context=self._context,
            check_hostname=self._check_hostname,
        )


def get_random_line(filepath: str) -> str:
    file_size = os.path.getsize(filepath)
    with open(filepath, "rb") as file_io:
        while True:
            pos = random.randint(0, file_size)
            if not pos:  # the first line is chosen
                return file_io.readline().decode()  # return str
            file_io.seek(pos)  # seek to random position
            file_io.readline()  # skip possibly incomplete line
            line = file_io.readline()  # read next (full) line
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


def readfile(filepath, encoding="utf-8"):
    try:
        with open(filepath, "r", encoding=encoding) as file_io:
            return file_io.read()
    except FileNotFoundError:
        return ""


def openurl(url, retry=0, source_address="random"):
    global opener

    def error_handle(e):
        if retry >= RETRY_THRESHOLD:
            raise e
        return openurl(url, retry + 1, source_address)

    try:
        if opener:
            return opener.open(url)
        if source_address == "random":
            source_address = get_pool_ip()
        if not is_ip(source_address):
            source_address = None
        if source_address:
            logger.debug(f"Using IP: {source_address}")
            if isinstance(url, str):
                schema = urllib.parse.urlsplit(url).scheme
            elif isinstance(url, urllib.request.Request):
                schema = urllib.parse.urlsplit(url.full_url).scheme

            handler = (BoundHTTPHandler if schema == "http" else BoundHTTPSHandler)(
                source_address=(source_address, 0)
            )
            return urllib.request.build_opener(handler).open(url)
        return urllib.request.urlopen(url)
    except (http.client.IncompleteRead, socket.timeout) as e:
        error_handle(e)
    except urllib.error.HTTPError as e:
        raise e
    except urllib.error.URLError as e:
        error_handle(e)
    except Exception as e:
        error_handle(e)


def download_segment(base_url, seg, seg_status, log_prefix=""):
    target_url = get_seg_url(base_url, seg)

    target_url_with_header = urllib.request.Request(
        target_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36"
            )
        },
    )

    try:
        with openurl(target_url_with_header) as response:
            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="ytarchive_raw.",
                suffix=f".{seg}.seg",
                dir=BASE_DIR,
            ) as tmp_file:
                shutil.copyfileobj(response, tmp_file)
                seg_status.segs[seg] = tmp_file.name
            return True

    except urllib.error.HTTPError as e:
        logger.debug(f"{log_prefix} Seg {seg} Failed with {e.code}")
        if e.code == 403:
            try:
                openurl(base_url)
            except urllib.error.HTTPError:
                return False
        return False

    except (http.client.IncompleteRead, socket.timeout):
        return False

    except:
        return False


def merge_segs(target_file, seg_status, not_merged_segs=[], log_prefix=""):
    while seg_status.merged_seg != seg_status.end_seg:
        if (seg_status.merged_seg + 1) not in seg_status.segs:
            logger.debug(
                f"{log_prefix} Waiting for Segment {seg_status.merged_seg + 1} ready for merging..."
            )
            time.sleep(1)
            continue

        if seg_status.segs[seg_status.merged_seg + 1] is not None:
            if os.path.exists(target_file):
                mode = "ab"
            else:
                mode = "wb"

            with open(target_file, mode) as target:
                with open(seg_status.segs[seg_status.merged_seg + 1], "rb") as source:
                    shutil.copyfileobj(source, target)

            try:
                os.remove(seg_status.segs[seg_status.merged_seg + 1])
            except FileNotFoundError:
                pass
        else:
            not_merged_segs.append(seg_status.merged_seg + 1)

        seg_status.merged_seg += 1
        seg_status.segs.pop(seg_status.merged_seg)


def download_seg_group(
    url, seg_group_index, seg_status, log_prefix="", post_dl_seg=lambda _: True
):
    seg_range = seg_status.seg_groups[seg_group_index]
    seg = seg_range[0]
    fail_count = 0

    try:
        while True:
            if fail_count < FAIL_THRESHOLD:
                logger.debug(f"{log_prefix} Current Seg: {seg}")

                status = download_segment(url, seg, seg_status, log_prefix)

                if status:
                    logger.debug(f"{log_prefix} Success Seg: {seg}")
                    post_dl_seg(seg)
                    if seg == seg_range[1]:
                        return True
                    seg += 1
                    fail_count = 0
                else:
                    fail_count += 1
                    logger.debug(
                        f"{log_prefix} Failed Seg: {seg} [{fail_count}/{FAIL_THRESHOLD}]"
                    )
                    time.sleep(1)
            else:
                logger.warn(f"{log_prefix} Giving up seg: {seg}")
                seg_status.segs[seg] = None  # Skip this seg
                post_dl_seg(seg)
                if seg == seg_range[1]:
                    return True
                seg += 1
                fail_count = 0

    except:
        traceback.print_exc()
        sys.exit(1)


def get_args():
    parser = argparse.ArgumentParser(description="")
    arg_dict = {
        "input": {
            "switch": ["-i", "--input"],
            "help": "Input JSON file.",
            "type": str,
        },
        "output": {
            "switch": ["-o", "--output"],
            "help": "Output file path. Uses `YYYYMMDD TITLE (VIDEO_ID).mkv` by default.",
            "type": str,
        },
        "socks": {
            "switch": ["-s", "--socks5-proxy"],
            "help": (
                "Socks5 Proxy. "
                "No schema should be provided in the proxy url. "
                "PySocks should be installed."
            ),
            "type": str,
        },
        "http-proxy": {
            "switch": ["-P", "--http-proxy"],
            "help": "HTTP Proxy",
            "type": str,
        },
        "threads": {
            "switch": ["-t", "--threads"],
            "help": "Multi-threaded download",
            "type": int,
        },
        "pool": {
            "switch": ["-p", "--pool"],
            "help": "IP Pool file.",
            "type": str,
        },
        "temp-dir": {
            "switch": ["-d", "--temp-dir"],
            "help": "Directory containing the temporary files",
            "type": str,
        },
        "timeout": {
            "switch": ["-T", "--timeout"],
            "help": "Secs for retrying when encounter HTTP errors. Default 20.",
            "type": int,
        },
    }
    for value in arg_dict.values():
        parser.add_argument(
            *value["switch"],
            help=value["help"],
            type=value["type"],
            default=None,
        )
    parser.add_argument(
        "-v", "--verbose", help="Enable debug mode", action="store_true"
    )
    parser.add_argument(
        "-k", "--keep-files", help="Do not delete temporary files", action="store_true"
    )
    return parser.parse_args()


def main(url, target_file, not_merged_segs=[], log_prefix="", print_func=print):
    seg_status = SegmentStatus(url, log_prefix)
    pbar = ProgressBar(
        seg_status.end_seg,
        lambda bar, p: print_func(f"{log_prefix}: |{bar}| {'{:.2f}'.format(p*100)}%"),
    )

    merge_thread = threading.Thread(
        target=merge_segs,
        args=(target_file, seg_status, not_merged_segs, log_prefix),
        daemon=True,
    )
    merge_thread.start()

    for i in range(len(seg_status.seg_groups)):
        threading.Thread(
            target=download_seg_group,
            args=(url, i, seg_status, log_prefix, pbar.done),
            daemon=True,
        ).start()

    merge_thread.join()  # Wait for merge finished


if __name__ == "__main__":
    os.system("")  # enable colors on windows

    try:
        # Parse params
        args = get_args()
        param = {"output": None, "iv": [], "ia": [], "delete_tmp": True}
        with open(args.input, "r", encoding="UTF-8") as input_io:
            input_data = json.load(input_io)
            param["iv"].append(list(input_data["video"].values())[0])
            param["ia"].append(list(input_data["audio"].values())[0])
        if args.output:
            param["output"] = args.output
        if args.socks5_proxy:
            if ":" in args.socks5_proxy:
                host, port = args.socks5_proxy.split(":")
                port = int(port)
            else:
                host = args.socks5_proxy
                port = 3128
            set_socks5_proxy(host, port)
        if args.http_proxy:
            set_http_proxy(args.http_proxy)
        if args.threads:
            THREADS = args.threads
        if args.pool:
            IP_POOL = args.pool
        if args.verbose:
            DEBUG = True
        if args.temp_dir:
            BASE_DIR = args.temp_dir
        if args.keep_files:
            param["delete_tmp"] = False
        if args.timeout:
            FAIL_THRESHOLD = args.timeout

        if param["output"] is None:
            if input_data is not None:
                try:
                    param["output"] = (
                        f"{date.today().strftime('%Y%m%d')} "
                        f"{sanitize_filename(input_data['metadata']['title'])} "
                        f"({input_data['metadata']['id']}).mkv"
                    )
                except Exception as e:
                    raise RuntimeError(
                        "JSON Version should be > 1.0, please update to the latest grabber."
                    ) from e
            else:
                raise RuntimeError("Output param not found.")
        if pathlib.Path(param["output"]).suffix.lower() != ".mkv":
            raise RuntimeError("Output should be a mkv file.")
        if not param["ia"] or not param["iv"]:
            raise RuntimeError(
                "Input data not sufficient. Both video and audio has to be inputed."
            )
        if len(param["ia"]) != len(param["iv"]):
            raise RuntimeError("Input video and audio length mismatch.")

        if not BASE_DIR:
            BASE_DIR = tempfile.mkdtemp(
                prefix="ytarchive_raw.",
                suffix=f".{input_data['metadata']['id']}"
                if input_data is not None
                else None,
            )
        elif os.path.isdir(BASE_DIR):
            BASE_DIR = tempfile.mkdtemp(
                prefix="ytarchive_raw.",
                suffix=f".{input_data['metadata']['id']}"
                if input_data is not None
                else None,
                dir=BASE_DIR,
            )
        else:
            os.makedirs(BASE_DIR)

        tmp_video = []
        tmp_audio = []
        video_not_merged_segs = []
        audio_not_merged_segs = []

        for video_idx, _ in enumerate(param["iv"]):
            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="ytarchive_raw.",
                suffix=f".video.{video_idx}",
                dir=BASE_DIR,
            ) as tmp_video_f:
                tmp_video.append(tmp_video_f.name)

            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="ytarchive_raw.",
                suffix=f".audio.{video_idx}",
                dir=BASE_DIR,
            ) as tmp_audio_f:
                tmp_audio.append(tmp_audio_f.name)

        for video_idx, _ in enumerate(param["iv"]):
            video_thread = threading.Thread(
                target=main,
                args=(
                    param["iv"][video_idx],
                    tmp_video[video_idx],
                    video_not_merged_segs,
                    f"[Video.{video_idx}]",
                    lambda x: print(f"{bcolors.OKBLUE}{x}{bcolors.ENDC}", end="\r"),
                ),
                daemon=True,
            )
            audio_thread = threading.Thread(
                target=main,
                args=(
                    param["ia"][video_idx],
                    tmp_audio[video_idx],
                    audio_not_merged_segs,
                    f"[Audio.{video_idx}]",
                    lambda x: print(f"{bcolors.OKGREEN}{x}{bcolors.ENDC}", end="\r"),
                ),
                daemon=True,
            )

            video_thread.start()
            audio_thread.start()

            while video_thread.is_alive():
                video_thread.join(0.5)
            while audio_thread.is_alive():
                audio_thread.join(0.5)

        if video_not_merged_segs:
            logger.warn(f"Gived up video segments: {video_not_merged_segs}")
        if audio_not_merged_segs:
            logger.warn(f"Gived up audio segments: {audio_not_merged_segs}")

        logger.info("Download finished. Merging...")

        ffmpeg_params = []
        if input_data is not None:
            tmp_thumbnail = None
            with urllib.request.urlopen(
                input_data["metadata"]["thumbnail"]
            ) as response:
                with tempfile.NamedTemporaryFile(
                    delete=False, prefix="ytarchive_raw.", suffix=".jpg", dir=BASE_DIR
                ) as tmp_file:
                    shutil.copyfileobj(response, tmp_file)
                    tmp_thumbnail = tmp_file.name

            ffmpeg_params = [
                "-metadata",
                'title="{}"'.format(input_data["metadata"]["title"].replace('"', "''")),
                "-metadata",
                'comment="{}"'.format(
                    input_data["metadata"]["description"].replace('"', "''")
                ),
                "-metadata",
                'author="{}"'.format(
                    input_data["metadata"]["channelName"].replace('"', "''")
                ),
                "-metadata",
                'episode_id="{}"'.format(
                    input_data["metadata"]["id"].replace('"', "''")
                ),
                "-attach",
                tmp_thumbnail,
                "-metadata:s:t",
                "mimetype=image/jpeg",
                "-metadata:s:t",
                'filename="thumbnail.jpg"',
            ]

        # have FFmpeg write the full log to a tempfile,
        # in addition to the terse log on stdout/stderr.
        # The logfile will be overwritten every time
        # so we'll keep appending the contents to ff_logtext
        with tempfile.NamedTemporaryFile(
            delete=False, prefix="ytarchive_raw.", suffix=".ffmpeg.log", dir=BASE_DIR
        ) as tmp_file:
            ff_logpath = tmp_file.name

        ff_logtext = ""
        ff_env = os.environ.copy()
        ff_env["FFREPORT"] = f"file='{ff_logpath}':level=32"  # 32=info/normal

        if len(tmp_video) == 1:
            cmd = (
                [
                    "ffmpeg",
                    "-y",
                    "-v",
                    "warning",
                    "-i",
                    tmp_video[0],
                    "-i",
                    tmp_audio[0],
                    "-c",
                    "copy",
                ]
                + ffmpeg_params
                + [param["output"]]
            )
            logger.debug(f"ffmpeg command: {cmd}")
            p = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ff_env
            )
            out, err = p.communicate()
            retcode = p.returncode
            ff_logtext += readfile(ff_logpath)

            if isinstance(out, bytes):
                out = out.decode(sys.stdout.encoding)
            if isinstance(err, bytes):
                err = err.decode(sys.stdout.encoding)
        else:
            tmp_merged = []
            out = ""
            err = ""
            retcode = 0
            for video_idx, _ in enumerate(param["iv"]):
                with tempfile.NamedTemporaryFile(
                    prefix="ytarchive_raw.",
                    suffix=f".merged.{video_idx}.mkv",
                    dir=BASE_DIR,
                ) as tmp_merged_f:
                    tmp_merged.append(tmp_merged_f.name)

                cmd = [
                    "ffmpeg",
                    "-y",
                    "-v",
                    "warning",
                    "-i",
                    tmp_video[video_idx],
                    "-i",
                    tmp_audio[video_idx],
                    "-c",
                    "copy",
                    tmp_merged[video_idx],
                ]
                logger.debug(f"ffmpeg command merging [{video_idx}]: {cmd}")
                p = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ff_env
                )

                out_i, err_i = p.communicate()
                retcode = retcode or p.returncode
                ff_logtext += readfile(ff_logpath)

                if isinstance(out_i, bytes):
                    out += out_i.decode(sys.stdout.encoding)
                if isinstance(err_i, bytes):
                    err += err_i.decode(sys.stdout.encoding)

            merged_file_list = ""
            with tempfile.NamedTemporaryFile(
                delete=False,
                prefix="ytarchive_raw.",
                suffix=".merged.txt",
                dir=BASE_DIR,
                encoding="utf-8",
                mode="w+",
            ) as tmp_file:
                data = []
                for filename in tmp_merged:
                    data.append(f"file '{filename}'")
                data = "\n".join(data)
                tmp_file.write(data)
                merged_file_list = tmp_file.name
            if os.name == "nt":
                cmd = ["ffmpeg", "-y", "-safe", "0", "-f", "concat"]
            else:
                cmd = ["ffmpeg", "-y", "-f", "concat", "-safe", "0"]

            cmd += (
                ["-v", "warning", "-i", merged_file_list, "-c", "copy"]
                + ffmpeg_params
                + [param["output"]]
            )
            p = subprocess.Popen(
                cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=ff_env
            )
            retcode = retcode or p.returncode
            ff_logtext += readfile(ff_logpath)

            out_i, err_i = p.communicate()

            if isinstance(out_i, bytes):
                out += out_i.decode(sys.stdout.encoding)
            if isinstance(err_i, bytes):
                err += err_i.decode(sys.stdout.encoding)

        logger.debug(f"FFmpeg complete log:\n{ff_logtext}\n")

        # remove harmless warnings
        err = err.split("\n")
        for ignore in [
            "    Last message repeated ",
            "Found duplicated MOOV Atom. Skipped it",
            "Found unknown-length element with ID 0x18538067 at pos.",  # segment header
        ]:
            err = [x for x in err if ignore not in x]
        err = "\n".join(err)

        if retcode:
            logger.error(f"FFmpeg complete log:\n{ff_logtext}\n")
            logger.error(f"FFmpeg:\n{err}\n\nFailed with error {retcode}")
        elif err:
            logger.warn(f"FFmpeg:\n{err}\n\nSuccess, but with warnings")
        else:
            logger.info("All good!")

    except KeyboardInterrupt as e:
        logger.info("Program stopped.")

    finally:
        try:
            if param["delete_tmp"]:
                shutil.rmtree(BASE_DIR, ignore_errors=True)
        except:
            pass
