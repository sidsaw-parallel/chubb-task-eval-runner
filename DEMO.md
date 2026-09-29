# Live demo script

A walkthrough of the runner, one step at a time. Steps 1–3 and 5 are free; step 4 runs
one real question (~$0.05) and the optional step 6 runs three (~$0.15).

**Before the meeting:** make sure `PARALLEL_STRICT_ZDR_API_KEY` is set in your
terminal (so the key never appears on screen). Then start in this folder:

```
cd chubb-task-eval-runner
```

## 1. Automated checks (free, no network)

Runs the runner against a simulated zero-data-retention API: dropped connections,
answers that can be read only once, resuming a run.

```
uv run --with pytest --with httpx --with truststore --with tzdata pytest test_run_eval.py -q
```

Expect: `10 passed`.

## 2. Dry run (free, nothing sent)

Shows exactly what would be sent for the first question and what the run would cost.

```
uv run run_eval.py questions_template.csv --config configs/chubb-v1.json --dry-run
```

Expect:
- `Questions: 3 total` and `Est. cost: ~$0.15`
- the full request for Q01: company, address, question, and the answer format
- `Dry run: nothing was sent.` at the end, and no `runs/` folder created

## 3. A bad spreadsheet is rejected before anything is sent (free)

Makes a CSV with three mistakes and runs it.

```
printf 'account_name,account_address,question_id,question_type,question,answer_options\nAcme,1 Main St,Q1,yes_no,Is it big?,\nAcme,1 Main St,Q1,maybe,Is it red?,\n' > /tmp/bad.csv
uv run run_eval.py /tmp/bad.csv --config configs/chubb-v1.json --dry-run
```

Expect `ERROR:` listing all three problems at once:
- the yes_no question has no answer options
- `Q1` is used twice
- `'maybe'` is not a valid question type

## 4. One real question (~$0.05)

```
uv run run_eval.py questions_template.csv --config configs/chubb-v1.json --limit 1
```

Type `y` at the cost prompt. After 1–5 minutes, expect `[1/1] Q01: completed`, then
`Done: {'completed': 1}` and the path to the results.

Open the results:

```
open "$(ls -td runs/*/ | head -1)results.csv"
```

Expect the Q01 row filled in: `answer`, `reasoning`, `citations` (source links),
`status` = `completed`, `processor` = `core2x`, and `config_name` / `config_version`.

The run folder also holds `raw.jsonl` (every request and response, unmodified),
`task_config.json` (the config used), and `run_info.json` (timing and status counts):

```
ls "$(ls -td runs/*/ | head -1)"
```

## 5. Resuming never re-runs or re-charges finished questions (free)

```
uv run run_eval.py questions_template.csv --config configs/chubb-v1.json --limit 1 --resume "$(ls -td runs/*/ | head -1)"
```

Expect `1 already done, 0 to run`, then `Nothing left to run.` There is no cost prompt.

## 6. Optional: Ctrl-C keeps answers already in progress (~$0.15)

```
uv run run_eval.py questions_template.csv --config configs/chubb-v1.json --limit 3
```

Type `y`, wait about 10 seconds, then press Ctrl-C **once**. Expect
`Stopping: no new questions will start...`. The runner keeps waiting until all three
finish and saves them, so nothing that was paid for is lost.

## Afterwards

Clear the demo output:

```
rm -rf runs /tmp/bad.csv
```
