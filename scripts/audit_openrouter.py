#!/usr/bin/env python3
"""Call OpenRouter API with bundled source files for audit."""

import argparse
import glob
import os
import sys
from pathlib import Path

import httpx


def gather_files(patterns: list[str], base: Path) -> str:
    """Resolve glob patterns and concatenate file contents."""
    sections: list[str] = []
    seen: set[Path] = set()
    for pattern in patterns:
        matches = sorted(glob.glob(str(base / pattern), recursive=True))
        if not matches:
            print(f"Warning: no files matched '{pattern}'", file=sys.stderr)
        for m in matches:
            p = Path(m).resolve()
            if p in seen or not p.is_file():
                continue
            seen.add(p)
            rel = p.relative_to(base)
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
            except Exception as e:
                print(f"Warning: could not read {rel}: {e}", file=sys.stderr)
                continue
            sections.append(f"--- FILE: {rel} ---\n{text}")
    return "\n\n".join(sections)


def call_openrouter(system_prompt: str, user_prompt: str, model: str) -> str:
    """Send chat completion to OpenRouter and return assistant content."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("Error: OPENROUTER_API_KEY env var not set", file=sys.stderr)
        sys.exit(1)

    with httpx.Client(timeout=300) as client:
        resp = client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://github.com/icarus-project",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": 8192,
                "temperature": 0.2,
            },
        )
        resp.raise_for_status()
        data = resp.json()

    return data["choices"][0]["message"]["content"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run audit via OpenRouter API")
    parser.add_argument("--domain", required=True, help="Audit domain name")
    parser.add_argument(
        "--files",
        required=True,
        help="Comma-separated glob patterns relative to repo root",
    )
    parser.add_argument("--prompt-file", required=True, help="Path to prompt .md file")
    parser.add_argument("--output", required=True, help="Output markdown file path")
    parser.add_argument(
        "--model",
        default="google/gemini-2.5-pro",
        help="OpenRouter model ID",
    )
    args = parser.parse_args()

    base = Path(__file__).resolve().parent.parent
    patterns = [p.strip() for p in args.files.split(",")]

    print(f"[{args.domain}] Gathering files...")
    source_bundle = gather_files(patterns, base)
    if not source_bundle:
        print(f"Error: no files gathered for {args.domain}", file=sys.stderr)
        sys.exit(1)

    prompt_text = Path(args.prompt_file).read_text(encoding="utf-8")

    system_prompt = (
        "You are a senior security and correctness auditor. "
        "Analyze the provided source code and report findings in structured markdown."
    )
    user_prompt = f"{prompt_text}\n\n## Source Files\n\n{source_bundle}"

    print(f"[{args.domain}] Calling {args.model} ({len(user_prompt):,} chars)...")
    result = call_openrouter(system_prompt, user_prompt, args.model)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(f"# {args.domain} — Gemini Audit\n\n{result}\n", encoding="utf-8")
    print(f"[{args.domain}] Done → {args.output}")


if __name__ == "__main__":
    main()
