import os
import glob
import tempfile
import asyncio
import psutil
import subprocess
import yt_dlp

# --- Hardcoded Cookies ---
COOKIE_CONTENT = """# Netscape HTTP Cookie File
# https://curl.haxx.se/rfc/cookie_spec.html
# This is a generated file! Do not edit.

.youtube.com	TRUE	/	TRUE	1800077643	PREF	tz=Asia.Colombo&f6=40000000&f7=100
.youtube.com	TRUE	/	TRUE	1787901316	__Secure-3PAPISID	3tFTcT-bQGul-Mdj/AKKWsSmqamNTh8yQ6
.youtube.com	TRUE	/	TRUE	1784877316	__Secure-1PSIDTS	sidts-CjAB5H03PyeasaPG9wIKHD7VKkiVrbkWzhR9kd5aCL05uSw38fK1K2YNg7Od5xvG0fQQAA
.youtube.com	TRUE	/	TRUE	1784877316	__Secure-3PSIDTS	sidts-CjAB5H03PyeasaPG9wIKHD7VKkiVrbkWzhR9kd5aCL05uSw38fK1K2YNg7Od5xvG0fQQAA
.youtube.com	TRUE	/	TRUE	1787901316	__Secure-3PSID	g.a000zQjuCKhHm2Vkbut5ywtdx8qDlxm0k9KwaN7Odg0fceRdZlck955SNAFKO6kc4ag8fImxzgACgYKAScSARQSFQHGX2MiNGRLZgtqe6bg6AbzvgRBrBoVAUF8yKo1BbqZvxuqe4v--oFejMsx0076
.youtube.com	TRUE	/	TRUE	1787901316	LOGIN_INFO	AFmmF2swRQIgZzTwhbVrxH2jwjT_s5_bmwuKbwtxdrnWJ0oosIvWUMACIQC2JdHuKDxCVgOdyCtp6OBL1ziPBwLUvXVXB0YiF0SmnA:QUQ3MjNmekZ6blZYZXVrclczc1NRcHNnajZic1YxZWNROHpCYmxqb1dOYmVQcWZsMHVpMGxMMktzQXo5dHNjYVZ5QTl3ZkRYbkZrRFdWX0RxcV8wdWlNb3VTcU40cVl1bTBMWG9BQkpTUUtjd1RtQmdTZUFSSVVYQ2xtV192dlZPOWJySHVJZ2xCdEZHRFl4ZGdVbDFCQ2tYdmtVNkNyX0tR
.youtube.com	TRUE	/	FALSE	1765517646	ST-3opvp5	session_logininfo=AFmmF2swRQIgZzTwhbVrxH2jwjT_s5_bmwuKbwtxdrnWJ0oosIvWUMACIQC2JdHuKDxCVgOdyCtp6OBL1ziPBwLUvXVXB0YiF0SmnA%3AQUQ3MjNmekZ6blZYZXVrclczc1NRcHNnajZic1YxZWNROHpCYmxqb1dOYmVQcWZsMHVpMGxMMktzQXo5dHNjYVZ5QTl3ZkRYbkZrRFdWX0RxcV8wdWlNb3VTcU40cVl1bTBMWG9BQkpTUUtjd1RtQmdTZUFSSVVYQ2xtV192dlZPOWJySHVJZ2xCdEZHRFl4ZGdVbDFCQ2tYdmtVNkNyX0tR
.youtube.com	TRUE	/	FALSE	1765517647	ST-wrlc9b	itct=CPYDENwwIhMIuKGa3am3kQMV_PIWBR3_hB8AMgpnLWhpZ2gtcmVjWg9GRXdoYXRfdG9fd2F0Y2iaAQYQjh4YngHKAQSI8ERE&csn=YQxCFGe6VaoF21_B&session_logininfo=AFmmF2swRQIgZzTwhbVrxH2jwjT_s5_bmwuKbwtxdrnWJ0oosIvWUMACIQC2JdHuKDxCVgOdyCtp6OBL1ziPBwLUvXVXB0YiF0SmnA%3AQUQ3MjNmekZ6blZYZXVrclczc1NRcHNnajZic1YxZWNROHpCYmxqb1dOYmVQcWZsMHVpMGxMMktzQXo5dHNjYVZ5QTl3ZkRYbkZrRFdWX0RxcV8wdWlNb3VTcU40cVl1bTBMWG9BQkpTUUtjd1RtQmdTZUFSSVVYQ2xtV192dlZPOWJySHVJZ2xCdEZHRFl4ZGdVbDFCQ2tYdmtVNkNyX0tR&endpoint=%7B%22clickTrackingParams%22%3A%22CPYDENwwIhMIuKGa3am3kQMV_PIWBR3_hB8AMgpnLWhpZ2gtcmVjWg9GRXdoYXRfdG9fd2F0Y2iaAQYQjh4YngHKAQSI8ERE%22%2C%22commandMetadata%22%3A%7B%22webCommandMetadata%22%3A%7B%22url%22%3A%22%2Fwatch%3Fv%3D-bMDMLvIIBg%26list%3DRD-bMDMLvIIBg%26start_radio%3D1%26pp%3DoAcB%22%2C%22webPageType%22%3A%22WEB_PAGE_TYPE_WATCH%22%2C%22rootVe%22%3A3832%7D%7D%2C%22watchEndpoint%22%3A%7B%22videoId%22%3A%22-bMDMLvIIBg%22%2C%22playlistId%22%3A%22RD-bMDMLvIIBg%22%2C%22params%22%3A%22OAHAAQG4BQE%253D%22%2C%22playerParams%22%3A%22oAcB%22%2C%22loggingContext%22%3A%7B%22vssLoggingContext%22%3A%7B%22serializedContextData%22%3A%22Gg1SRC1iTURNTHZJSUJn%22%7D%7D%2C%22watchEndpointSupportedOnesieConfig%22%3A%7B%22html5PlaybackOnesieConfig%22%3A%7B%22commonConfig%22%3A%7B%22url%22%3A%22https%3A%2F%2Frr2---sn-icnxg8pjxn-qxae.googlevideo.com%2Finitplayback%3Fsource%3Dyoutube%26oeis%3D1%26c%3DWEB%26oad%3D3200%26ovd%3D3200%26oaad%3D11000%26oavd%3D11000%26ocs%3D700%26oewis%3D1%26oputc%3D1%26ofpcc%3D1%26siu%3D1%26msp%3D1%26odepv%3D1%26id%3Df9b30330bbc82018%26ip%3D43.227.227.163%26initcwndbps%3D1737500%26mt%3D1765517328%26oweuc%3D%22%7D%7D%7D%7D%7D
.youtube.com	TRUE	/	TRUE	1797053645	__Secure-3PSIDCC	AKEyXzUh2HMJLix4F0sfj3piNUVI02G4NDkAieHEzONk193jiBxn3jLn9fjJ5lNVtja7NO1VlSo
.youtube.com	TRUE	/	TRUE	1781069639	VISITOR_INFO1_LIVE	vexWIg6Hlw8
.youtube.com	TRUE	/	TRUE	1781069639	VISITOR_PRIVACY_METADATA	CgJJThIEGgAgGQ%3D%3D
.youtube.com	TRUE	/	TRUE	0	YSC	ZNQMpCfzZU0
.youtube.com	TRUE	/	TRUE	1781069634	__Secure-ROLLOUT_TOKEN	CNC--4Wlx9b1VBDG1PKtjLKMAxiL7JLdqbeRAw%3D%3D
.youtube.com	TRUE	/	TRUE	1781069637	__Secure-YNID	14.YT=cFNHwXrRHX8AAGk27_gBtnb9lYeFjUbLKFf8R8O6kFekAsrJkQLEYuklWtUr2aRdmffN4ES-vAgusaItj-jsIrmA_oBypwRvdVZn3y6EOczN5bNuESFcjZs2Pv5ENGFN-weFd6jSYgzH9E0xHAA3LheC_YTrKy6X004t7cP21jWvJfxP7rUYQ9ld_QK4-JuHZ57-Ed03Y7pzqDS-UTmRbuq2mhmw9P7aUMmbIKOm1iq0SO99MlhMFsHvktk7oR0G8euyRve4r0y6zbb4K_DLUilgDZEayN_DBsKVQk4lrdJHm2G4s4DujhFisw3AoO1ku7dqy9nfjRmfYyB1r6fuJA
"""

_SPOTIFY_COOKIES_CONTENT = """# Netscape HTTP Cookie File
# http://curl.haxx.se/rfc/cookie_spec.html
# This is a generated file!  Do not edit.

open.spotify.com	FALSE	/episode	FALSE	0	sss	1
.spotify.com	TRUE	/	TRUE	1791383497	sp_t	ccc2c8d181eb192906a11c40b50b127b
.spotify.com	TRUE	/	TRUE	1759933824	sp_landing	https%3A%2F%2Fopen.spotify.com%2F%3Fsp_cid%3Dccc2c8d181eb192906a11c40b50b127b%26device%3Ddesktop
.spotify.com	TRUE	/	TRUE	1759849226	_cs_mk_ga	0.7963095260015378_1759847426366
.spotify.com	TRUE	/	FALSE	1759933898	_gid	GA1.2.485530918.1759847426
open.spotify.com	FALSE	/	FALSE	0	sss	1
.spotify.com	TRUE	/	TRUE	1791383499	sp_adid	2ba973b8-f704-4d62-92d6-0b369e5ee9cc
.spotify.com	TRUE	/	TRUE	1794407440	sp_m	in-en
.spotify.com	TRUE	/	TRUE	1761057061	sp_dc	AQBJYlJM1GJO_hI99M6cuxEMWBDMZ28tSOY8-hgHjKWCLn5KM-wGXltLn1HA1m5nv5HJQCmBrg8iVqyFWynYMqf2YIuI1iGgD7ExQt2qiHCZBGjAxUT67T28hlCrJ_8pT-wsWNqEjX0N7EcNRb52e5DQ-dj8qnROzKAmlwF2hUZbSisR3u9wI42P0jq1H4i9qe-mPQ-nDO1Idiq0h40
.spotify.com	TRUE	/	FALSE	1791383471	OptanonAlertBoxClosed	2025-10-07T14:31:11.162Z
.spotify.com	TRUE	/	FALSE	1791383498	OptanonConsent	isGpcEnabled=0&datestamp=Tue+Oct+07+2025+20%3A01%3A38+GMT%2B0530+(India+Standard+Time)&version=202411.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=s00%3A1%2Cf00%3A1%2Cm00%3A1%2Ct00%3A1%2Ci00%3A1%2Cf11%3A1%2Cm03%3A1&AwaitingReconsent=false&geolocation=IN%3BUP
.spotify.com	TRUE	/	FALSE	1794407499	_ga_ZWG1NSHWD8	GS2.1.s1759847426$o1$g1$t1759847498$j59$l0$h0
.spotify.com	TRUE	/	FALSE	1794407499	_ga	GA1.2.2077846945.1759847426
.spotify.com	TRUE	/	FALSE	1759847558	_gat_UA-5784146-31	1
.spotify.com	TRUE	/	FALSE	1794407499	_ga_BMC5VGR8YS	GS2.2.s1759847427$o1$g1$t1759847498$j60$l0$h0
"""

COOKIE_FILE_PATH = os.path.join(tempfile.gettempdir(), "yt_cookies.txt")
with open(COOKIE_FILE_PATH, "w", encoding="utf-8") as f:
    f.write(COOKIE_CONTENT)

# optional globals: COOKIE_FILE_PATH, SHARD_CACHE_MATRIX
SHARD_CACHE_MATRIX = {}

def make_ydl_opts_audio(output_template: str):
    ffmpeg_path = os.getenv("FFMPEG_PATH", "/usr/bin/ffmpeg")
    opts = {
        'format': 'bestaudio/best',     # let yt-dlp pick whatever works
        'outtmpl': output_template,     # temp name + correct extension
        'noplaylist': True,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 60,
        'ffmpeg_location': ffmpeg_path,
        'concurrent_fragment_downloads': 4,

        # first extract audio as-is, then manually convert to .webm
        'postprocessors': [
            {  # extract audio without forcing codec
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'opus',
                'preferredquality': '0',
            },
            {  # re-mux result to .webm container
                'key': 'FFmpegVideoRemuxer',
                'preferedformat': 'webm'
            }
        ],
    }
    if 'COOKIE_FILE_PATH' in globals() and COOKIE_FILE_PATH:
        opts['cookiefile'] = COOKIE_FILE_PATH
    return opts


# Main resolver
async def vector_transport_resolver(url: str) -> str:
    """
    If url is a local file -> return it.
    If url is Spotify -> call votify CLI (with hardcoded cookies) and return downloaded file path.
    Otherwise -> use yt-dlp (letting it pick formats), postprocess to .webm and return file path.
    """
    # shortcut: already a local file
    if os.path.exists(url) and os.path.isfile(url):
        return url

    # cache shortcut
    if url in SHARD_CACHE_MATRIX:
        cached = SHARD_CACHE_MATRIX[url]
        if os.path.exists(cached) and os.path.getsize(cached) > 0:
            return cached
        else:
            # remove invalid cache
            SHARD_CACHE_MATRIX.pop(url, None)

    # try to lower process priority (best-effort)
    try:
        proc = psutil.Process(os.getpid())
        if os.name == "nt":
            proc.nice(psutil.IDLE_PRIORITY_CLASS)
        else:
            proc.nice(19)
    except Exception:
        pass

    # Detect Spotify URL (open.spotify.com or spotify: URIs)
    is_spotify = False
    if isinstance(url, str):
        u = url.strip()
        if u.startswith("spotify:") or "open.spotify.com" in u:
            is_spotify = True

    if is_spotify:
        # --- Use Votify CLI ---
        # Requirements: votify must be installed and in PATH (pip install votify)
        # We will write the hardcoded cookies to a temporary cookies file and pass --cookies-path.
        tmp_dir = tempfile.mkdtemp(prefix="votify_out_")
        cookies_path = os.path.join(tmp_dir, "cookies.txt")
        with open(cookies_path, "w", encoding="utf-8") as f:
            f.write(_SPOTIFY_COOKIES_CONTENT)

        # ffmpeg path for votify
        ffmpeg_path = os.getenv("FFMPEG_PATH", "ffmpeg")

        # Build votify command:
        # - --cookies-path <path>
        # - --output-path <tmp_dir>
        # - --temp-path <tmp_dir>
        # - --ffmpeg-path <ffmpeg_path>
        # - url
        cmd = [
            "votify",
            "--cookies-path", cookies_path,
            "--output-path", tmp_dir,
            "--temp-path", tmp_dir,
            "--ffmpeg-path", ffmpeg_path,
            "--no-exceptions",  # try to keep stdout stable; optional
            url
        ]

        # Run votify; capture output in case of error
        try:
            # run votify synchronously in executor to avoid blocking event loop
            loop = asyncio.get_running_loop()
            def run_votify():
                # Ensure environment PATH preserved; capture stdout/stderr for debugging
                res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                return res

            res = await loop.run_in_executor(None, run_votify)

            if res.returncode != 0:
                # include stderr for debugging
                raise Exception(f"Votify failed (rc={res.returncode}): {res.stderr.strip() or res.stdout.strip()}")

            # find any file created in tmp_dir (votify places outputs into output-path)
            # prefer audio-like extensions
            patterns = ["*.webm", "*.mp4", "*.m4a", "*.aac", "*.ogg", "*.opus", "*.mp3", "*.*"]
            found = []
            for pat in patterns:
                found = glob.glob(os.path.join(tmp_dir, pat))
                if found:
                    break

            if not found:
                raise Exception("Votify ran successfully but no output file was found in votify output directory.")

            # pick the largest non-empty file
            found = [f for f in found if os.path.getsize(f) > 0]
            if not found:
                raise Exception("Votify produced files but they are empty.")

            found.sort(key=lambda p: os.path.getsize(p), reverse=True)
            downloaded = found[0]

            # cache and return
            SHARD_CACHE_MATRIX[url] = downloaded
            return downloaded

        except Exception as e:
            # bubble up with context
            raise Exception(f"Error downloading from Spotify with Votify: {e}")

    else:
        # --- Use yt-dlp path (let yt-dlp pick stream, then remux to .webm) ---
        base = tempfile.NamedTemporaryFile(delete=False)
        base_name = base.name
        base.close()

        output_tmpl = base_name + ".%(ext)s"
        loop = asyncio.get_running_loop()
        ydl_opts = make_ydl_opts_audio(output_tmpl)

        def download_audio():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])

        try:
            await loop.run_in_executor(None, download_audio)
        except Exception as e:
            raise Exception(f"Error downloading audio with yt-dlp: {e}")

        # find any produced .webm file first (postprocessor should remux to .webm)
        matches = glob.glob(base_name + "*.webm")
        if not matches:
            matches = glob.glob(base_name + ".*")

        if not matches:
            # cleanup and error
            try:
                os.unlink(base_name)
            except Exception:
                pass
            raise Exception("yt-dlp did not produce any output file.")

        downloaded = matches[0]
        if os.path.getsize(downloaded) == 0:
            try:
                os.unlink(downloaded)
            except Exception:
                pass
            raise Exception("Downloaded file is empty.")

        SHARD_CACHE_MATRIX[url] = downloaded
        return downloaded
