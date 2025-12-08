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

.youtube.com	TRUE	/	TRUE	1799737813	__Secure-3PAPISID	9J04OeAfGkf8qGVS/A8-gihh-q1jlJfabC
.youtube.com	TRUE	/	TRUE	1799737847	PREF	tz=Asia.Colombo&f6=40000000&f7=100
.youtube.com	TRUE	/	TRUE	1793633991	__Secure-1PSIDTS	sidts-CjUBwQ9iI24hcPZhDd2BniIBCUl-hzex4AP8bTEte71GQkAC4I8kXT6SFTQ8ToPuHnsmN7Mh4RAA
.youtube.com	TRUE	/	FALSE	1799737813	HSID	A5fvGuuDXOyQppsB-
.youtube.com	TRUE	/	TRUE	1799737813	SSID	AgQbGvegRVnbqO0RH
.youtube.com	TRUE	/	FALSE	1799737813	APISID	ps4LrYd1w2ylv9cc/Agr_PhE6_UNJX6KO7
.youtube.com	TRUE	/	TRUE	1799737813	SAPISID	9J04OeAfGkf8qGVS/A8-gihh-q1jlJfabC
.youtube.com	TRUE	/	TRUE	1799737813	__Secure-1PAPISID	9J04OeAfGkf8qGVS/A8-gihh-q1jlJfabC
.youtube.com	TRUE	/	TRUE	1796658013	LOGIN_INFO	AFmmF2swRQIgZdmP5ckFCyd28OMzqM-bX1DHCAXTHYAY_6kHFFhiqMgCIQC22vgLvfrN64kP2ToNz-RsgaiOaa_PaoJNowGenNikhw:QUQ3MjNmd3lwRGtYcnhMcmU1dDBRdHVEeE00bHNJeS1HeU1nX1pST0VlN0ZScTRsY0RXVDdHZXA2b0MzSHpvVmJaZHJfSWxPY2E0aE9RVDFNa0tmTlNnYzR1VWRaQWc0RmJHSzBZTEN1MzZmU2JtTHBqY04wQWItaXM4aXhGX1pZdU1SV1FiSmE3U3N1UW1MR0FjT0lVdHJsOTl6NS1qeXN3
.youtube.com	TRUE	/	TRUE	1796658046	__Secure-3PSIDTS	sidts-CjUBwQ9iI24hcPZhDd2BniIBCUl-hzex4AP8bTEte71GQkAC4I8kXT6SFTQ8ToPuHnsmN7Mh4RAA
.youtube.com	TRUE	/	FALSE	1799737813	SID	g.a0004QhpA2ybJoi3nCsT8rH9U4qPI9r_xvgu-cNHK_ePxUYkgzYCQ89Z9-4k96QF0TG8WZvQvQACgYKAUUSARESFQHGX2Mi8nZX9fYFw-a3WcSyf0J1sRoVAUF8yKoK0Xmtn29o3wDFVEgwv-Uu0076
.youtube.com	TRUE	/	TRUE	1799737813	__Secure-1PSID	g.a0004QhpA2ybJoi3nCsT8rH9U4qPI9r_xvgu-cNHK_ePxUYkgzYCTAvZ1tgSzlICSUYtlv6RVgACgYKAb8SARESFQHGX2MigZgxmRWu0vI8wRaZt-SAbhoVAUF8yKpOpUdP-YkW2jky1riVd0M60076
.youtube.com	TRUE	/	TRUE	1799737813	__Secure-3PSID	g.a0004QhpA2ybJoi3nCsT8rH9U4qPI9r_xvgu-cNHK_ePxUYkgzYCc8MVN78v74UeMQXvzywEGQACgYKAcESARESFQHGX2Mi1Zn7D0qSRXysumacxQz6oRoVAUF8yKqb56Z-JfNon59YVe_Qfvss0076
.youtube.com	TRUE	/	FALSE	1796713852	SIDCC	AKEyXzXuEaFNek-RYtxEAGaekCZ5GIaYdE9vx4uaCRs5tHr94l1mFmS19Vp92pTWUaUmVARQ
.youtube.com	TRUE	/	TRUE	1796713852	__Secure-1PSIDCC	AKEyXzWGMdRq87vxHH7rHgVI0ibc7FKD0CcTeSIZajj1qITUx89TzVGvKhiX-o2IJqJPnYh5
.youtube.com	TRUE	/	TRUE	1796713852	__Secure-3PSIDCC	AKEyXzVcOyxsn3MXs_ZVfynCpKP--7GjtcGAy53WNNHpXXHc8GDCCrw8lqeslENOJ0BVMB2cXA
.youtube.com	TRUE	/	TRUE	1780729818	VISITOR_INFO1_LIVE	iqy9junpnOc
.youtube.com	TRUE	/	TRUE	1780729818	VISITOR_PRIVACY_METADATA	CgJJThIEGgAgMQ%3D%3D
.youtube.com	TRUE	/	TRUE	0	YSC	wzJmyJjPzwc
.youtube.com	TRUE	/	TRUE	1780729813	__Secure-ROLLOUT_TOKEN	CJ-1oe2DmrSJ3wEQwpnploChjwMYqvKs5betkQM%3D
.youtube.com	TRUE	/	TRUE	1780729815	__Secure-YNID	14.YT=l2iCthJRYTAqe-rV7wIdVgQjS0pJiyTDhJ0FUwLA6Z_PrZxYjqDk-ugHXOuWVXnxh-2dDCIVpbP7kKTJudRTBghgCtX4sSre2XeyF6dkEjD-oRXxBOBCPz7iXy-6eHxx1wYinhV8UYQ0MElDdkNvT6jYv7rqudQub2AWo8QI2w6wh-O7jSDDgLGmOFhsVujUv2zswCaEtbpn8GfDxoqy5WvDZdHhdddupzS7uvWiBQAM91cSxXotVURHDNabjiBu7Wg1vrLEd9dEMicuX2Qn6lXeyzpTjUa1Oqq5lDlI-OsxUuksS2fwB8c-LK88IdDz19M6EMSIDOLcV_h29VtF7g
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
