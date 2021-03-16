import urllib.request
from urllib.parse import parse_qs, urlsplit, urlunsplit, urlencode
import urllib.error
import shutil
import time
import threading
import os
import tempfile
import subprocess
import shlex

FAIL_THRESHOLD = 5
RETRY_THRESHOLD = 3
DEBUG = True

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
    try:
        return urllib.request.urlopen(url)
    except urllib.error.HTTPError as e:
        raise e
    except urllib.error.URLError as e:
        if retry >= RETRY_THRESHOLD:
            raise e
        else:
            return openurl(url, retry+1)

def download_segment(base_url, seg, seg_status):
    target_url = get_seg_url(base_url, seg)

    try:
        with openurl(target_url) as response:
            if response.getcode() >= 300 or response.getcode() < 200:
                return False
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                shutil.copyfileobj(response, tmp_file)
                seg_status.segs[seg] = tmp_file.name
            return True
               
    except urllib.error.HTTPError as e:
        if DEBUG:
            print(f"[DEBUG] Failed: {e.code}")
            print(f"[DEBUG] url: {target_url}")
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

def main(url, target_file):
    seg_status = SegmentStatus()

    merge_thread = threading.Thread(target=merge_segs, args=[target_file, seg_status])
    merge_thread.start()

    seg = seg_status.merged_seg + 1
    fail_count = 0

    while fail_count < FAIL_THRESHOLD:
        if DEBUG:
            print(f"[DEBUG] Current Seg: {seg}")
            
        status = download_segment(url, seg, seg_status)
        if status:
            seg += 1
            fail_count = 0
        else:
            fail_count += 1
            if DEBUG:
                print(f"[DEBUG] Failed Seg: {seg}")
                print(f"[DEBUG] Fail Count: {fail_count}")
            time.sleep(1)

    seg_status.end_seg = seg - 1 # Current seg is not available.
    merge_thread.join() # Wait for merge finished

if __name__ == "__main__":
    import sys

    try:
        # Parse params

        param = {
            "output": None,
            "iv": None,
            "ia": None
        }

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
    -o, --output [OUTPUT_FILE]  Output file path.
                    """)
                    sys.exit()
                if args[i] == "-i" or args[i] == "--input":
                    import json
                    with open(args[i+1]) as f:
                        tmp = json.load(f)
                        param["iv"] = [*tmp["video"].values()][0]
                        param["ia"] = [*tmp["audio"].values()][0]
                    i += 1
                elif args[i] == "-iv" or args[i] == "--input-video":
                    param["iv"] = args[i+1]
                    i += 1
                elif args[i] == "-ia" or args[i] == "--input-audio":
                    param["ia"] = args[i+1]
                    i += 1
                elif args[i] == "-o" or args[i] == "--output":
                    param["output"] = args[i+1]
                    i += 1
                else:
                    raise KeyError(f"Parameter not recognized: {args[i]}")
                
                i += 1

            if param["output"] is None:
                raise RuntimeError("Output param not found.")
            if param["ia"] is None or param["iv"] is None:
                raise RuntimeError("Input data not sufficient. Both video and audio has to be inputed.")

        tmp_video_f = tempfile.NamedTemporaryFile(delete=False)
        tmp_video = tmp_video_f.name
        tmp_video_f.close()

        tmp_audio_f = tempfile.NamedTemporaryFile(delete=False)
        tmp_audio = tmp_audio_f.name
        tmp_audio_f.close()

        video_thread = threading.Thread(target=main, args=(param["iv"], tmp_video), daemon=True)
        audio_thread = threading.Thread(target=main, args=(param["ia"], tmp_audio), daemon=True)

        video_thread.start()
        audio_thread.start()

        while video_thread.is_alive():
            video_thread.join(0.5)
        while audio_thread.is_alive():
            audio_thread.join(0.5)

        if DEBUG:
            print("[DEBUG] Download finished. Merging...")
                
        output_file = param["output"]
        cmd = f"ffmpeg -i '{tmp_video}' -i '{tmp_audio}' -c copy '{output_file}'"
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
        os.remove(tmp_video)
        os.remove(tmp_audio)