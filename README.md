# Parallel Task API eval runner

Runs a CSV of underwriting questions through the Parallel Task API and saves every
answer, its sources, and the raw API responses on your machine.

## 1. One-time setup

1. Install `uv` (a Python launcher that installs everything else automatically):
   - macOS / Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Windows (PowerShell): `powershell -c "irm https://astral.sh/uv/install.ps1 | iex"`
2. Get an API key at <https://platform.parallel.ai> and set it in your terminal:
   - macOS / Linux: `export PARALLEL_STRICT_ZDR_API_KEY=your-key`
   - Windows (PowerShell): `$env:PARALLEL_STRICT_ZDR_API_KEY="your-key"`

## 2. Prepare your questions CSV

Copy `questions_template.csv` and fill in one question per row. Keep these exact
column names:

| Column | Required | What goes in it |
| --- | --- | --- |
| `account_name` | yes | Company name, e.g. `Terex Corporation` |
| `account_address` | yes | Headquarters address |
| `question_id` | yes | Any unique ID, e.g. `Q01` |
| `question_type` | yes | `open`, `yes_no`, or `multiple_choice` |
| `question` | yes | The question only. Do not put the answer options here. |
| `answer_options` | for `yes_no` and `multiple_choice` | Options separated by `\|`, e.g. `Yes\|No\|Unknown`. Leave empty for `open`. |

The runner checks the whole file before sending anything and lists any rows that
need fixing.

## 3. Run it

From this folder:

```
uv run run_eval.py my_questions.csv --config configs/chubb-v1.json --dry-run
uv run run_eval.py my_questions.csv --config configs/chubb-v1.json --limit 3
uv run run_eval.py my_questions.csv --config configs/chubb-v1.json
```

1. `--dry-run` shows the exact request for the first question and the estimated
   cost. Nothing is sent and nothing is charged.
2. `--limit 3` runs only the first 3 questions, as a quick paid check.
3. The last command runs everything. It asks you to confirm the estimated cost first.

A question usually takes 1-5 minutes. Questions run in parallel (25 at a time;
change with `--concurrency`), so a full run takes about as long as its slowest
question.

## 4. Results

Each run gets its own folder under `runs/`, named with the start time (US Eastern)
and the config name and version:

| File | Contents |
| --- | --- |
| `results.csv` | Your questions plus `answer`, `confidence`, `reasoning`, `citations` (each source URL followed by the excerpts used from it), `run_id`, `status`, `latency_s` (seconds from submitting to receiving the answer), `server_latency_s`, `processor`, `config_name`, `config_version`, `error` |
| `raw.jsonl` | One line per question: the exact request sent and every API response, unmodified |
| `task_config.json` | An exact copy of the config used |
| `run_info.json` | Start/end time, question counts by status, runner version |

`status` is one of:

- `completed`: answered.
- `failed`: the API could not complete that question.
- `lost`: the network dropped while the answer was being delivered.
- `timed_out`: the question took longer than `--max-wait-min` (default 30).
- `create_failed`: the question could not be submitted.

## If something goes wrong

If `uv` fails with `invalid peer certificate: UnknownIssuer`, your network inspects
HTTPS traffic. Add `--system-certs` right after `uv run` so uv trusts your
computer's certificates, e.g. `uv run --system-certs run_eval.py ...`.


To re-run only the questions that did not complete, use the command the runner
prints at the end:

```
uv run run_eval.py my_questions.csv --config configs/chubb-v1.json --resume runs/<run folder>
```

Completed answers are kept and not re-run (or re-charged).

Pressing Ctrl-C stops new questions from starting and waits for those already
running, so their answers are saved. Pressing Ctrl-C a second time abandons them.

## Zero data retention

Under zero data retention, Parallel deletes each answer as soon as it is delivered,
and deletes any undelivered answer about a minute after it finishes, so it can be
retrieved only once. The runner is built for this:

- It is already waiting for each answer when it finishes.
- It writes the answer to disk before doing anything else.
- It never re-requests an answer that may have been delivered.

The one consequence for you: if a run is interrupted, a question that was in flight
cannot be fetched later. `--resume` re-runs it instead.

## New configs from Parallel

Parallel may send you a new config file, e.g. `chubb-v2.json`. Save it in
`configs/` and run the same three commands with `--config configs/chubb-v2.json`.
Every row of `results.csv` records which config produced it, so results from
different configs can be compared side by side.
