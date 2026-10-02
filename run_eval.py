# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx>=0.27", "truststore>=0.9", "tzdata"]
# ///
"""Run a CSV of questions through the Parallel Task API and save every result.

    uv run run_eval.py questions.csv --config configs/chubb-v1.json --dry-run
    uv run run_eval.py questions.csv --config configs/chubb-v1.json --limit 3
    uv run run_eval.py questions.csv --config configs/chubb-v1.json

Requires PARALLEL_STRICT_ZDR_API_KEY in the environment. See README.md.
"""

import argparse
import concurrent.futures
import copy
import csv
import datetime
import hashlib
import json
import os
import random
import re
import ssl
import sys
import threading
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import truststore

RUNNER_VERSION = "1.3"
API_BASE = "https://api.parallel.ai"
EASTERN = ZoneInfo("America/New_York")

REQUIRED_COLUMNS = [
    "account_name",
    "account_address",
    "question_id",
    "question_type",
    "question",
    "answer_options",
]
QUESTION_TYPES = {"open", "yes_no", "multiple_choice"}
OPTIONS_PLACEHOLDER = "{{answer_options}}"
PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")

# USD per run, for the pre-run estimate only; the invoice is authoritative.
PRICE_PER_RUN = {
    "lite": 0.005,
    "base": 0.01,
    "core": 0.025,
    "core2x": 0.05,
    "pro": 0.10,
    "ultra": 0.30,
}

# The server holds /result open for at most ~595s; the client must outwait it
# or it would drop a response the server already counted as the one read.
RESULT_WAIT_S = 590
RESULT_HTTP_TIMEOUT_S = RESULT_WAIT_S + 60
CREATE_ATTEMPTS = 6

RESULT_COLUMNS = [
    "config_name",
    "config_version",
    "run_id",
    "status",
    "processor",
    "latency_s",
    "server_latency_s",
    "answer",
    "confidence",
    "reasoning",
    "citations",
    "error",
]


class UserError(Exception):
    pass


# ---------------------------------------------------------------- inputs


def load_questions(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = [c.strip() for c in reader.fieldnames or []]
        rows = [
            {k.strip(): (v or "").strip() for k, v in row.items() if k is not None}
            for row in reader
        ]
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise UserError(f"{path}: missing column(s): {', '.join(missing)}")

    problems, seen = [], set()
    for line, row in enumerate(rows, start=2):
        qid, qtype = row["question_id"], row["question_type"]
        where = f"row {line} ({qid or 'no question_id'})"
        if not qid:
            problems.append(f"{where}: question_id is empty")
        elif qid in seen:
            problems.append(f"{where}: duplicate question_id")
        seen.add(qid)
        for column in ("account_name", "question"):
            if not row[column]:
                problems.append(f"{where}: {column} is empty")
        options = split_options(row["answer_options"])
        if qtype not in QUESTION_TYPES:
            problems.append(
                f"{where}: question_type {qtype!r} must be one of "
                f"{', '.join(sorted(QUESTION_TYPES))}"
            )
        elif qtype == "open" and options:
            problems.append(f"{where}: open questions must leave answer_options empty")
        elif qtype != "open" and len(options) < 2:
            problems.append(
                f"{where}: {qtype} needs at least 2 answer_options separated by |"
            )
        if len(set(options)) != len(options):
            problems.append(f"{where}: answer_options has duplicates")
    if not rows:
        problems.append("the CSV has no question rows")
    if problems:
        raise UserError(f"{path} has problems:\n  " + "\n  ".join(problems))
    return columns, rows


def split_options(raw: str) -> list[str]:
    return [o.strip() for o in raw.split("|") if o.strip()]


def load_config(path: Path) -> dict[str, Any]:
    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise UserError(f"{path} is not valid JSON: {e}") from e
    if not isinstance(config, dict):
        raise UserError(f"{path}: expected a JSON object")
    for key in ("name", "version", "body"):
        if not config.get(key):
            raise UserError(f"{path}: missing required field {key!r}")
    if not isinstance(config["body"], dict) or not config["body"].get("processor"):
        raise UserError(f"{path}: body.processor is required")
    headers = config.get("headers") or {}
    if any(h.lower() in ("x-api-key", "authorization") for h in headers):
        raise UserError(
            f"{path}: put the API key in PARALLEL_STRICT_ZDR_API_KEY, not the config"
        )
    overrides = config.get("question_type_overrides") or {}
    unknown = set(overrides) - QUESTION_TYPES
    if unknown or not all(isinstance(v, dict) for v in overrides.values()):
        raise UserError(
            f"{path}: question_type_overrides must map "
            f"{', '.join(sorted(QUESTION_TYPES))} to objects"
        )
    needed = config.get("min_runner_version")
    if needed and _version(needed) > _version(RUNNER_VERSION):
        raise UserError(
            f"{path} needs run_eval.py {needed} or newer (this is {RUNNER_VERSION}). "
            "Ask Parallel for the latest run_eval.py."
        )
    return config


def _version(v: str) -> tuple[int, ...]:
    return tuple(int(p) for p in str(v).split("."))


def config_hash(config: dict[str, Any]) -> str:
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:12]


# -------------------------------------------------------------- templating

_DROP = object()


def build_request(config: dict[str, Any], row: dict[str, str]) -> Any:
    """The body, with that question type's override merged over it (null removes a key)."""
    override = (config.get("question_type_overrides") or {}).get(row["question_type"], {})
    return render(_merge(config["body"], override), row)


def _merge(base: Any, override: Any) -> Any:
    if not (isinstance(base, dict) and isinstance(override, dict)):
        return copy.deepcopy(override)
    out = copy.deepcopy(base)
    for key, value in override.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = _merge(base.get(key), value) if key in base else copy.deepcopy(value)
    return out


def render(template: Any, row: dict[str, str]) -> Any:
    """Fill `{{column}}` placeholders; a bare `"{{answer_options}}"` becomes a list."""
    options = split_options(row.get("answer_options", ""))

    def fill(value: Any) -> Any:
        if isinstance(value, dict):
            out = {k: fill(v) for k, v in value.items()}
            return {k: v for k, v in out.items() if v is not _DROP}
        if isinstance(value, list):
            return [v for v in (fill(v) for v in value) if v is not _DROP]
        if not isinstance(value, str):
            return value
        if value.strip() == OPTIONS_PLACEHOLDER:
            return list(options) if options else _DROP

        def sub(match: re.Match[str]) -> str:
            key = match.group(1)
            if key == "answer_options":
                return "\n".join(options)
            if key not in row:
                raise UserError(
                    f"config uses {{{{{key}}}}} but the CSV has no {key!r} column"
                )
            return row[key]

        if not PLACEHOLDER.search(value):
            return value
        # Trimmed so an empty {{answer_options}} leaves no trailing blank lines.
        return PLACEHOLDER.sub(sub, value).strip()

    return fill(copy.deepcopy(template))


# ------------------------------------------------------------------ output


class RunLog:
    """Append-only raw.jsonl; every write is flushed to disk before returning."""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()

    def append(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def records(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        out = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # a line cut short by a crash mid-write
        return out


def latest_by_question(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        latest[record["question_id"]] = check_basis(record)
    return latest


def check_basis(record: dict[str, Any]) -> dict[str, Any]:
    """An answer without reasoning or citations is not complete, so --resume re-runs it."""
    if record["status"] != "completed":
        return record
    basis = ((record.get("result") or {}).get("output") or {}).get("basis") or []
    if any(b.get("reasoning") or b.get("citations") for b in basis):
        return record
    return {**record, "status": "completed_no_basis",
            "error": "the API returned the answer without reasoning or citations"}


def summarize(record: dict[str, Any]) -> dict[str, str]:
    result = record.get("result") or {}
    run = result.get("run") or record.get("run") or {}
    output = result.get("output") or {}
    content = output.get("content")
    if isinstance(content, dict) and set(content) == {"answer"}:
        answer = content["answer"]
    elif content is None:
        answer = ""
    else:
        answer = content
    if isinstance(answer, list) and all(isinstance(a, str) for a in answer):
        answer = " | ".join(answer)
    elif not isinstance(answer, str):
        answer = json.dumps(answer, ensure_ascii=False)

    basis = output.get("basis") or []
    many = len(basis) > 1

    def per_field(key: str) -> str:
        parts = [
            (f"{b.get('field')}: " if many else "") + str(b.get(key) or "")
            for b in basis
            if b.get(key)
        ]
        return "\n".join(parts)

    sources: dict[str, tuple[str, list[str]]] = {}
    for b in basis:
        for c in b.get("citations") or []:
            url = c.get("url")
            if not url:
                continue
            title = (c.get("title") or "").strip()
            _, excerpts = sources.setdefault(url, (title, []))
            for e in c.get("excerpts") or []:
                e = str(e).strip()
                if e and e not in excerpts:
                    excerpts.append(e)
    citations = []
    for url, (title, excerpts) in sources.items():
        lines = [f"{title} — {url}" if title else url]
        lines += [f"  - {e}" for e in excerpts]
        citations.append("\n".join(lines))

    return {
        "config_name": record.get("config_name", ""),
        "config_version": record.get("config_version", ""),
        "run_id": run.get("run_id", record.get("run_id", "")),
        "status": record["status"],
        "processor": run.get("processor", ""),
        "latency_s": _fmt(record.get("latency_s")),
        "server_latency_s": _fmt(_server_latency(run)),
        "answer": answer,
        "confidence": per_field("confidence"),
        "reasoning": per_field("reasoning"),
        "citations": _excel_cell("\n\n".join(citations)),
        "error": record.get("error") or "",
    }


EXCEL_CELL_LIMIT = 32767


def _excel_cell(text: str) -> str:
    # Excel splits longer cells across rows, shifting every column after it.
    note = "\n[truncated; full text in raw.jsonl]"
    if len(text) <= EXCEL_CELL_LIMIT:
        return text
    return text[: EXCEL_CELL_LIMIT - len(note)] + note


def _server_latency(run: dict[str, Any]) -> float | None:
    try:
        start = datetime.datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(run["modified_at"].replace("Z", "+00:00"))
    except (KeyError, AttributeError, TypeError, ValueError):
        return None
    return (end - start).total_seconds()


def _fmt(seconds: float | None) -> str:
    return "" if seconds is None else f"{seconds:.1f}"


def write_results_csv(
    path: Path,
    columns: list[str],
    rows: list[dict[str, str]],
    latest: dict[str, dict[str, Any]],
) -> None:
    # utf-8-sig so Excel opens non-ASCII text correctly.
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns + RESULT_COLUMNS)
        writer.writeheader()
        for row in rows:
            record = latest.get(row["question_id"])
            summary = summarize(record) if record else {"status": "not_run"}
            writer.writerow({**{c: row.get(c, "") for c in columns}, **summary})


# -------------------------------------------------------------------- API


class TaskApi:
    def __init__(self, api_key: str, headers: dict[str, str], transport=None):
        self.client = httpx.Client(
            base_url=API_BASE,
            headers={**headers, "x-api-key": api_key},
            transport=transport,
            # OS trust store, so corporate TLS-inspection proxies are trusted.
            verify=truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT),
            timeout=httpx.Timeout(60.0, read=RESULT_HTTP_TIMEOUT_S),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=100),
        )

    def run_question(self, body: dict[str, Any], deadline: float) -> dict[str, Any]:
        """Create one run and capture its result with exactly one successful read.

        Under zero data retention the run is deleted by the first /result read
        and ~60s after completion regardless, so /result is already waiting
        when the run finishes and is never retried after a response that may
        have been a 200.
        """
        trace: dict[str, Any] = {"attempts": []}
        started = time.monotonic()

        created = self._create(body, trace)
        if "error" in created:
            return {**trace, "status": "create_failed", "error": created["error"]}
        run = created["run"]
        run_id = run["run_id"]
        trace["run_id"], trace["run"] = run_id, run

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {**trace, "status": "timed_out",
                        "error": "hit --max-wait before the run finished"}
            wait = int(max(10, min(RESULT_WAIT_S, remaining)))
            attempt: dict[str, Any] = {"call": "result", "wait_s": wait}
            try:
                response = self.client.get(
                    f"/v1/tasks/runs/{run_id}/result", params={"timeout": wait}
                )
            except httpx.HTTPError as e:
                attempt["exception"] = repr(e)
                trace["attempts"].append(attempt)
                return {**trace, "status": "lost",
                        "error": f"connection failed while reading the result: {e!r}"}
            attempt["http_status"] = response.status_code
            body_json = _json_or_text(response)
            if response.status_code == 408:
                trace["attempts"].append(attempt)
                continue
            if response.status_code == 200 and isinstance(body_json, dict):
                trace["result"] = body_json
                trace["latency_s"] = time.monotonic() - started
                trace["attempts"].append(attempt)
                return {**trace, "status": "completed"}
            attempt["body"] = body_json
            trace["attempts"].append(attempt)
            return {**trace, "status": "failed",
                    "error": f"HTTP {response.status_code}: {_error_message(body_json)}"}

    def _create(self, body: dict[str, Any], trace: dict[str, Any]) -> dict[str, Any]:
        for n in range(CREATE_ATTEMPTS):
            attempt: dict[str, Any] = {"call": "create"}
            try:
                response = self.client.post("/v1/tasks/runs", json=body)
            except httpx.HTTPError as e:
                # A lost create response may still have created a run; retrying
                # risks one duplicate run, which is cheaper than a missing answer.
                attempt["exception"] = repr(e)
                trace["attempts"].append(attempt)
                if n == CREATE_ATTEMPTS - 1:
                    return {"error": f"could not create the run: {e!r}"}
                _backoff(n)
                continue
            attempt["http_status"] = response.status_code
            payload = _json_or_text(response)
            attempt["body"] = payload
            trace["attempts"].append(attempt)
            if response.status_code in (200, 201, 202) and isinstance(payload, dict):
                return {"run": payload}
            if response.status_code == 429 or response.status_code >= 500:
                if n < CREATE_ATTEMPTS - 1:
                    _backoff(n, response.headers.get("retry-after"))
                    continue
            return {"error": f"HTTP {response.status_code} creating the run: "
                             f"{_error_message(payload)}"}
        return {"error": "could not create the run"}


def _backoff(attempt: int, retry_after: str | None = None) -> None:
    try:
        delay = float(retry_after) if retry_after else 0.0
    except ValueError:
        delay = 0.0
    time.sleep(max(delay, min(60.0, 2.0 ** attempt) + random.random()))


def _json_or_text(response: httpx.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text[:2000]


def _error_message(payload: Any) -> str:
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict) and error.get("message"):
            return str(error["message"])
        if payload.get("detail"):
            return str(payload["detail"])
    return str(payload)[:500]


# -------------------------------------------------------------------- main


def eastern_now() -> datetime.datetime:
    return datetime.datetime.now(EASTERN)


def slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")


def new_run_dir(runs_root: Path, config: dict[str, Any], now: datetime.datetime) -> Path:
    name = f"{now:%Y-%m-%d_%H%M}ET_{slug(config['name'])}_{slug(config['version'])}"
    path = runs_root / name
    n = 2
    while path.exists():
        path = runs_root / f"{name}_{n}"
        n += 1
    return path


def warn_on_unbumped_version(runs_root: Path, config: dict[str, Any], digest: str) -> None:
    for info_path in runs_root.glob("*/run_info.json"):
        try:
            info = json.loads(info_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if (
            info.get("config_name") == config["name"]
            and info.get("config_version") == config["version"]
            and info.get("config_hash") != digest
        ):
            print(
                f"WARNING: {info_path.parent.name} used a different config with the "
                f"same name and version ({config['name']} {config['version']}). "
                "Update \"version\" when you edit a config so runs stay comparable.\n"
            )
            return


def confirm(prompt: str) -> bool:
    if not sys.stdin.isatty():
        return False
    return input(prompt).strip().lower() in ("y", "yes")


def main(argv: list[str] | None = None, transport=None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a question CSV through the Parallel Task API.",
    )
    parser.add_argument("questions", type=Path, help="question CSV (see README.md)")
    parser.add_argument("--config", type=Path, required=True, help="Task API config JSON")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the first request and the cost estimate; send nothing")
    parser.add_argument("--limit", type=int, help="only run the first N questions")
    parser.add_argument("--concurrency", type=int, default=25,
                        help="questions in flight at once (default 25)")
    parser.add_argument("--max-wait-min", type=float, default=30,
                        help="give up on a question after this many minutes (default 30)")
    parser.add_argument("--resume", type=Path, metavar="RUN_DIR",
                        help="continue an earlier run folder instead of starting a new one")
    parser.add_argument("--runs-dir", type=Path, default=Path("runs"),
                        help="where run folders are created (default ./runs)")
    parser.add_argument("--yes", action="store_true", help="skip the cost confirmation")
    args = parser.parse_args(argv)

    try:
        return _main(args, transport)
    except UserError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2


def _main(args: argparse.Namespace, transport) -> int:
    columns, rows = load_questions(args.questions)
    config = load_config(args.config)
    digest = config_hash(config)
    if args.limit is not None:
        rows = rows[: args.limit]
    requests = {row["question_id"]: build_request(config, row) for row in rows}
    headers = render(config.get("headers") or {}, {})

    if args.resume:
        run_dir = args.resume
        saved = run_dir / "task_config.json"
        if not saved.exists():
            raise UserError(f"{run_dir} is not a run folder (no task_config.json)")
        if config_hash(json.loads(saved.read_text(encoding="utf-8"))) != digest:
            raise UserError(
                f"{run_dir} was run with a different config. Start a new run "
                "(drop --resume) so results from two configs never mix."
            )
    else:
        run_dir = new_run_dir(args.runs_dir, config, eastern_now())

    log = RunLog(run_dir / "raw.jsonl")
    done = {
        qid for qid, r in latest_by_question(log.records()).items() if r["status"] == "completed"
    }
    todo = [row for row in rows if row["question_id"] not in done]

    processor = config["body"]["processor"]
    price = PRICE_PER_RUN.get(processor)
    cost = f"~${price * len(todo):,.2f}" if price is not None else "unknown (not in price list)"
    print(f"Config:     {config['name']} (version {config['version']})")
    print(f"Processor:  {processor}")
    print(f"Questions:  {len(rows)} total, {len(done)} already done, {len(todo)} to run")
    print(f"Est. cost:  {cost}")
    print(f"Output:     {run_dir}\n")

    if args.dry_run:
        first = todo[0] if todo else rows[0]
        print(f"Request for {first['question_id']} (POST {API_BASE}/v1/tasks/runs):")
        print(json.dumps(requests[first["question_id"]], indent=2, ensure_ascii=False))
        print("\nDry run: nothing was sent.")
        return 0
    if not todo:
        print("Nothing left to run.")
        write_results_csv(run_dir / "results.csv", columns, rows,
                          latest_by_question(log.records()))
        return 0

    api_key = os.environ.get("PARALLEL_STRICT_ZDR_API_KEY", "").strip()
    if not api_key:
        raise UserError("set PARALLEL_STRICT_ZDR_API_KEY to your Parallel API key first")
    if not args.yes and not confirm(f"Run {len(todo)} questions for {cost}? [y/N] "):
        print("Cancelled; nothing was sent.")
        return 1

    run_dir.mkdir(parents=True, exist_ok=True)
    if not args.resume:
        warn_on_unbumped_version(args.runs_dir, config, digest)
        (run_dir / "task_config.json").write_text(
            json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    info_path = run_dir / "run_info.json"
    previous = json.loads(info_path.read_text(encoding="utf-8")) if info_path.exists() else {}
    info = {
        "runner_version": RUNNER_VERSION,
        "config_name": config["name"],
        "config_version": config["version"],
        "config_hash": digest,
        "processor": processor,
        "questions_file": str(args.questions),
        "args": {k: str(v) for k, v in vars(args).items()},
        "started_at_et": previous.get(
            "started_at_et", eastern_now().isoformat(timespec="seconds")
        ),
    }
    if previous:
        info["resumed_at_et"] = eastern_now().isoformat(timespec="seconds")

    api = TaskApi(api_key, headers, transport=transport)
    max_wait = args.max_wait_min * 60
    counts: dict[str, int] = {}
    counts_lock = threading.Lock()

    def work(row: dict[str, str]) -> None:
        qid = row["question_id"]
        body = requests[qid]
        started_at = eastern_now().isoformat(timespec="seconds")
        try:
            outcome = api.run_question(body, time.monotonic() + max_wait)
        except Exception as e:  # keep one bad question from stopping the run
            outcome = {"status": "error", "error": repr(e)}
        outcome = check_basis(outcome)
        log.append({
            "question_id": qid,
            "config_name": config["name"],
            "config_version": config["version"],
            "started_at_et": started_at,
            "request": body,
            **outcome,
        })
        with counts_lock:
            counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
            finished = sum(counts.values())
        note = f" ({outcome.get('error')})" if outcome["status"] != "completed" else ""
        print(f"[{finished}/{len(todo)}] {qid}: {outcome['status']}{note}", flush=True)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.concurrency))
    futures = [pool.submit(work, row) for row in todo]
    try:
        concurrent.futures.wait(futures)
    except KeyboardInterrupt:
        # A run already started is still billed, and its result is deleted soon
        # after it finishes, so collect the in-flight ones instead of dropping them.
        print("\nStopping: no new questions will start. Waiting for the ones already "
              "running so their results are saved (Ctrl-C again to abandon them).")
        try:
            pool.shutdown(wait=True, cancel_futures=True)
        except KeyboardInterrupt:
            print("Abandoned in-flight questions; resume later to re-run them.")
            os._exit(130)
    pool.shutdown(wait=True)

    latest = latest_by_question(log.records())
    write_results_csv(run_dir / "results.csv", columns, rows, latest)
    info["finished_at_et"] = eastern_now().isoformat(timespec="seconds")
    statuses = [(latest.get(r["question_id"]) or {}).get("status", "not_run") for r in rows]
    info["status_counts"] = {s: statuses.count(s) for s in sorted(set(statuses))}
    info_path.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")

    print(f"\nDone: {info['status_counts']}")
    print(f"Results: {run_dir / 'results.csv'}")
    if any(s != "completed" for s in info["status_counts"]):
        print(f"To retry the unfinished questions:\n  uv run run_eval.py {args.questions} "
              f"--config {args.config} --resume {run_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
