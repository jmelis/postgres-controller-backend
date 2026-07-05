#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["edge-tts"]
# ///
"""
Build a narrated video from talk/slides.md.

Usage:
    ./talk/build_video.py                  # build all slides
    ./talk/build_video.py --voice en-US-AndrewNeural  # different voice
    ./talk/build_video.py --list-voices    # show available en-US voices

Requires: brew install marp-cli ffmpeg
"""

import argparse
import asyncio
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SLIDES_MD = Path(__file__).parent / "slides.md"
BUILD_DIR = Path(__file__).parent / "build"
OUTPUT = Path(__file__).parent / "output.mp4"

DEFAULT_VOICE = "en-US-AndrewNeural"

PRONUNCIATIONS = {
    "etcd": "et-see-dee",
    "kine": "kyne",
    "add-ons": "ad-ons",
    "addons": "ad-ons",
}


def fix_pronunciation(text: str) -> str:
    for word, phonetic in PRONUNCIATIONS.items():
        text = re.sub(rf"\b{re.escape(word)}\b", phonetic, text, flags=re.IGNORECASE)
    return text


def check_deps():
    for cmd in ("marp", "ffmpeg"):
        if not shutil.which(cmd):
            print(f"Missing: {cmd}. Install with: brew install {cmd.replace('marp', 'marp-cli')}")
            sys.exit(1)


def parse_scripts(md: str) -> list[dict]:
    frontmatter = re.match(r"^---\n.*?\n---\n", md, re.DOTALL)
    if frontmatter:
        md = md[frontmatter.end():]
    slides = md.split("\n---\n")
    result = []
    for i, slide in enumerate(slides):
        m = re.search(r"<!--\s*\n\s*SCRIPT:\s*\n(.*?)-->", slide, re.DOTALL)
        script = ""
        if m:
            lines = m.group(1).strip().splitlines()
            script = " ".join(line.strip() for line in lines if line.strip())
        result.append({"index": i, "script": script})
    return result


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:12]


def export_pngs(slides_path: Path, build_dir: Path):
    subprocess.run(
        ["marp", "--images", "png", "--allow-local-files",
         str(slides_path), "-o", str(build_dir / "slide.png")],
        check=True,
    )


async def generate_audio(text: str, output_path: Path, voice: str):
    import edge_tts
    communicate = edge_tts.Communicate(fix_pronunciation(text), voice)
    await communicate.save(str(output_path))


async def generate_all_audio(slides: list[dict], build_dir: Path, voice: str):
    tasks = []
    for s in slides:
        audio_path = build_dir / f"audio_{s['index']:03d}.mp3"
        hash_path = audio_path.with_suffix(".hash")
        current_hash = content_hash(s["script"] + voice + str(PRONUNCIATIONS))

        if audio_path.exists() and hash_path.exists() and hash_path.read_text() == current_hash:
            continue

        if not s["script"]:
            subprocess.run(
                ["ffmpeg", "-y", "-f", "lavfi", "-i", "anullsrc=r=24000:cl=mono",
                 "-t", "3", str(audio_path)],
                check=True, capture_output=True,
            )
        else:
            tasks.append((s, audio_path, current_hash, voice))

        hash_path.write_text(current_hash)

    for s, audio_path, h, v in tasks:
        print(f"  Generating audio for slide {s['index']}...")
        await generate_audio(s["script"], audio_path, v)


def get_duration(path: Path) -> float:
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "json", str(path)],
        capture_output=True, text=True, check=True,
    )
    return float(json.loads(r.stdout)["format"]["duration"])


def build_slide_clips(slides: list[dict], build_dir: Path) -> list[Path]:
    clips = []
    for s in slides:
        idx = s["index"]
        png = build_dir / f"slide.{idx + 1:03d}.png"
        audio = build_dir / f"audio_{idx:03d}.mp3"
        clip = build_dir / f"clip_{idx:03d}.mp4"

        if not png.exists():
            print(f"  Warning: {png.name} not found, skipping slide {idx}")
            continue

        duration = get_duration(audio)

        subprocess.run(
            ["ffmpeg", "-y",
             "-loop", "1", "-i", str(png),
             "-i", str(audio),
             "-c:v", "libx264", "-tune", "stillimage",
             "-c:a", "aac", "-b:a", "192k",
             "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:black",
             "-pix_fmt", "yuv420p",
             "-t", str(duration),
             "-shortest",
             str(clip)],
            check=True, capture_output=True,
        )
        clips.append(clip)
    return clips


def concatenate(clips: list[Path], output: Path, build_dir: Path):
    concat_file = build_dir / "concat.txt"
    concat_file.write_text(
        "\n".join(f"file '{c.resolve()}'" for c in clips)
    )
    subprocess.run(
        ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
         "-i", str(concat_file),
         "-c", "copy", str(output)],
        check=True, capture_output=True,
    )


async def list_voices():
    import edge_tts
    voices = await edge_tts.list_voices()
    for v in voices:
        if v["Locale"].startswith("en-US"):
            print(f"  {v['ShortName']:30s}  {v['Gender']}")


async def main():
    parser = argparse.ArgumentParser(description="Build narrated video from slides.md")
    parser.add_argument("--voice", default=DEFAULT_VOICE, help=f"TTS voice (default: {DEFAULT_VOICE})")
    parser.add_argument("--list-voices", action="store_true", help="List available en-US voices")
    args = parser.parse_args()

    if args.list_voices:
        await list_voices()
        return

    check_deps()

    BUILD_DIR.mkdir(exist_ok=True)

    md = SLIDES_MD.read_text()
    slides = parse_scripts(md)
    print(f"Found {len(slides)} slides, {sum(1 for s in slides if s['script'])} with narration.")

    print("Exporting PNGs...")
    export_pngs(SLIDES_MD, BUILD_DIR)

    print("Generating audio...")
    await generate_all_audio(slides, BUILD_DIR, args.voice)

    print("Building per-slide clips...")
    clips = build_slide_clips(slides, BUILD_DIR)

    print("Concatenating...")
    concatenate(clips, OUTPUT, BUILD_DIR)

    print(f"Done: {OUTPUT}")


if __name__ == "__main__":
    asyncio.run(main())
