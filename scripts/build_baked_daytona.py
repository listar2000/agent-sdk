#!/usr/bin/env python3
"""Build a daytona snapshot with the hivespace CLI AND default skills baked
into the agent HOME (``/home/daytona``).

Goal: let the hive-space backend send NO ``cli_tools`` / ``skills`` /
apt-bootstrap ``pre_start_commands`` and get identical functionality — the
hive CLI is on PATH (``$HOME/.local/bin``, which the supervisor already adds)
and the default skills are discoverable at ``$HOME/.claude/skills`` on first
boot. No agent-sdk code change, no supervisor symlink: everything lands in the
proper HOME at build time.

Layering:
  - Stage 1 (``hivecli``): throwaway builder that installs the PRIVATE CLI. The
    GitHub token is NOT written into the Dockerfile text — Daytona records the
    full Dockerfile as ``snapshot.build_info.dockerfile_content`` (readable via
    ``snapshot.list()``), so an inline token would leak there even with a
    multi-stage build, and Daytona has no build-secret mechanism. Instead the
    token is dropped into a build-CONTEXT file that the builder ``COPY``s to a
    git credential store and ``rm``s; the install URL is clean. Only the
    compiled ``/home/daytona/.local`` is COPYed into the final stage.
  - Final stage (the repo Dockerfile): bakes the public default skills directly
    via ``npx skills add`` with ``HOME=/home/daytona`` (no token needed).

Builds under a DISTINCT name (does NOT touch .runtime-snapshot-tag). Point the
hive-space agent-sdk deployment at it via ``DAYTONA_SNAPSHOT=<name>``.

Env: GH_TOKEN (repo-read), DAYTONA_API_KEY, SNAPSHOT_NAME, HIVE_REF (default staging).
"""
import os, sys, time, tempfile, pathlib
from daytona_sdk import Daytona, DaytonaConfig, CreateSnapshotParams, Image, Resources

TOKEN = os.environ["GH_TOKEN"]
NAME = os.environ["SNAPSHOT_NAME"]
REF = os.environ.get("HIVE_REF", "staging")
# Build-context filename the token is dropped into (NOT the Dockerfile text).
# Lives in the repo root only between write and build, deleted in ``finally``.
CRED_BASENAME = "hive_build_credentials.tmp"

# Default skills baked into the image — mirrors hive-space's
# ``DEFAULT_AGENT_SKILLS`` (agents.py). Each is a single-skill source
# (``owner/repo@skill``), so use ``--yes -g`` (NOT ``--all``) to respect the
# filter — matches agent-sdk's ``_skills_install_commands`` flag logic.
DEFAULT_SKILLS = [
    "claude-office-skills/skills@html-slides",
    "github/awesome-copilot@excalidraw-diagram-generator",
    "anthropics/skills@frontend-design",
]

base = pathlib.Path("Dockerfile").read_text()

builder = (
    "FROM python:3.12-slim AS hivecli\n"
    "RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates "
    "&& rm -rf /var/lib/apt/lists/*\n"
    "RUN pip install --no-cache-dir uv\n"
    "ENV HOME=/home/daytona\n"
    "RUN mkdir -p /home/daytona\n"
    # Token enters via the build CONTEXT (not the Dockerfile text → not in
    # build_info). COPY it to a git credential store, install from a CLEAN url
    # (so recorded install metadata under .local has no token either), then rm.
    # This whole stage is thrown away; only .local is COPYed to the final image.
    f"COPY {CRED_BASENAME} /home/daytona/.git-credentials\n"
    "RUN chmod 600 /home/daytona/.git-credentials "
    "&& git config --global credential.helper store "
    f'&& uv tool install "git+https://github.com/rllm-org/hive-space.git@{REF}" '
    "&& rm -f /home/daytona/.git-credentials\n\n"
)
# Append the bake COPY to the final stage (the repo Dockerfile is stage 2).
copy_line = "\n# --- bake: pull the CLI from the throwaway builder (no token here) ---\n" \
            "COPY --from=hivecli /home/daytona/.local /home/daytona/.local\n"
# Bake the public default skills straight into the agent HOME so the agent
# discovers them at ``~/.claude/skills`` on first boot (HOME=/home/daytona at
# runtime). ``export HOME`` so it applies across the whole && chain; skills are
# public repos, so no token is involved.
_skill_adds = " && ".join(
    f"npx -y skills add '{s}' --yes -g" for s in DEFAULT_SKILLS
)
skills_lines = (
    "\n# --- bake: default skills into the agent HOME (public, no token) ---\n"
    "RUN export HOME=/home/daytona && mkdir -p $HOME/.claude/skills && "
    f"{_skill_adds} "
    "&& echo BAKED > /home/daytona/BAKE_PROOF\n"
)
# Cache-bust the base stage. Daytona's builder caches layers by content, and
# the agent-sdk base spliced in here is byte-identical to the committed release
# snapshot's build — a stale cache can serve the pre-bake base as the final
# image (observed: bake layers succeed but don't appear in the sandbox). A
# unique RUN right after the base FROM forces a fresh build of everything below.
_cb = os.urandom(6).hex()
_bl0 = base.splitlines(keepends=True)
_fi = next(i for i, l in enumerate(_bl0) if l.lstrip().startswith("FROM "))
base = "".join(_bl0[:_fi + 1]) + f"RUN echo 'cachebust {_cb}'\n" + "".join(_bl0[_fi + 1:])
# Splice the bake in right after the base's LAST RUN/COPY — i.e. BEFORE the
# trailing metadata block (ENV/EXPOSE/CMD/...). Daytona's ``from_dockerfile``
# translation DROPS RUN/COPY instructions that appear after the first metadata
# instruction (verified via probe: a RUN after ENV/EXPOSE silently fails to
# persist, while the same RUN before ENV survives — the agent-sdk base ends with
# ENV/EXPOSE/ENV/CMD, so a bake spliced before CMD lands after ENV and vanishes).
_lines = base.splitlines(keepends=True)
_META = ("ENV ", "EXPOSE ", "CMD ", "ENTRYPOINT ", "VOLUME ", "USER ", "LABEL ")
_fs_idxs = [i for i, l in enumerate(_lines)
            if l.lstrip().startswith(("RUN ", "COPY ", "ADD "))]
_last_fs = _fs_idxs[-1] if _fs_idxs else -1
_meta_after = [i for i, l in enumerate(_lines)
               if i > _last_fs and l.lstrip().startswith(_META)]
_insert_at = _meta_after[0] if _meta_after else len(_lines)
base_with_bake = "".join(_lines[:_insert_at]) + copy_line + skills_lines + "".join(_lines[_insert_at:])
dockerfile_text = builder + base_with_bake

# DRY_RUN=1 renders the generated Dockerfile to stdout and exits — inspect the
# bake without DAYTONA_API_KEY / a live build.
if os.environ.get("DRY_RUN"):
    sys.stdout.write(dockerfile_text)
    sys.exit(0)

# Write the temp Dockerfile AND the token credential file into the repo root
# so both are part of the daytona build context (COPY src/ ui/ + the cred file
# resolve). The cred file is referenced ONLY by COPY in the throwaway builder
# stage; both temp files are deleted in ``finally``.
cred_path = pathlib.Path(CRED_BASENAME)
cred_path.write_text(f"https://x-access-token:{TOKEN}@github.com\n")
os.chmod(cred_path, 0o600)
fd, tmp = tempfile.mkstemp(suffix=".Dockerfile", dir=".")
with os.fdopen(fd, "w") as f:
    f.write(dockerfile_text)
try:
    client = Daytona(DaytonaConfig(api_key=os.environ["DAYTONA_API_KEY"]))
    print(f"[build] registering {NAME} (token via build-context file, never in Dockerfile text)...", file=sys.stderr)
    t0 = time.time()
    result = client.snapshot.create(CreateSnapshotParams(
        name=NAME, image=Image.from_dockerfile(tmp),
        resources=Resources(cpu=1, memory=1, disk=3),
    ))
    print(f"[build] snapshot {result.name} state={result.state} elapsed={time.time()-t0:.1f}s", file=sys.stderr)
finally:
    os.unlink(tmp)
    cred_path.unlink(missing_ok=True)
