import csv
import json
import threading
from pathlib import Path

import httpx
import pytest

import run_eval

HERE = Path(__file__).parent
CONFIG = HERE / "configs" / "chubb-v1.json"
TEMPLATE = HERE / "questions_template.csv"


class FakeTaskApi:
    """Mimics strict ZDR: a result can be read once, then the run is gone."""

    def __init__(self, result_script=None, create_script=None):
        self.result_script = result_script or {}
        self.create_script = list(create_script or [])
        self.lock = threading.Lock()
        self.runs: dict[str, dict] = {}
        self.result_calls: dict[str, int] = {}
        self.creates = 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        with self.lock:
            if request.method == "POST":
                self.creates += 1
                if self.create_script:
                    return httpx.Response(self.create_script.pop(0))
                body = json.loads(request.content)
                run_id = f"trun_{self.creates}"
                self.runs[run_id] = body
                return httpx.Response(202, json=_run(run_id, "queued"))
            run_id = request.url.path.split("/")[4]
            n = self.result_calls.get(run_id, 0)
            self.result_calls[run_id] = n + 1
            if run_id not in self.runs:
                return httpx.Response(404, json={"error": {"message": f"{run_id} not found"}})
            qid = self.runs[run_id]["input"]["question"][:3]
            step = self.result_script.get(qid, ["ok"])
            action = step[min(n, len(step) - 1)]
            if action == "still_running":
                return httpx.Response(408, json={"error": {"message": "Run still active."}})
            if action == "drop":
                del self.runs[run_id]  # the server's read succeeded; the reply never arrives
                raise httpx.ReadError("connection reset")
            if action == "fail":
                return httpx.Response(404, json={"error": {"message": "Run failed."}})
            body = self.runs.pop(run_id)
            return httpx.Response(200, json=_result(run_id, body))


def _run(run_id, status):
    return {
        "run_id": run_id,
        "status": status,
        "processor": "core2x",
        "created_at": "2026-09-27T19:00:00Z",
        "modified_at": "2026-09-27T19:01:30Z",
    }


def _result(run_id, body):
    schema = body["task_spec"]["output_schema"]["json_schema"]["properties"]["answer"]
    if schema.get("type") == "array":
        answer = schema["items"]["enum"][:2]
    else:
        answer = schema["enum"][0] if "enum" in schema else "Prose answer."
    return {
        "run": _run(run_id, "completed"),
        "output": {
            "type": "json",
            "content": {"answer": answer},
            "basis": [{
                "field": "answer",
                "reasoning": "Because.",
                "confidence": "high",
                "citations": [
                    {"url": "https://a.example", "title": "A", "excerpts": ["x"]},
                    {"url": "https://a.example", "title": "A dup", "excerpts": ["x", "y"]},
                    {"url": "https://b.example"},
                ],
            }],
        },
    }


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("PARALLEL_STRICT_ZDR_API_KEY", "test-key")
    monkeypatch.setattr(run_eval, "_backoff", lambda *a, **k: None)


def _questions(tmp_path, n_copies=1):
    with TEMPLATE.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    out = []
    for i in range(n_copies):
        for row in rows:
            out.append({**row, "question_id": f"{row['question_id']}_{i}", "question":
                        f"{row['question_id']} {row['question']}"})
    path = tmp_path / "q.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(out)
    return path


def _run_main(tmp_path, fake, *extra, questions=None):
    args = [str(questions or _questions(tmp_path)), "--config", str(CONFIG),
            "--runs-dir", str(tmp_path / "runs"), "--yes", *extra]
    code = run_eval.main(args, transport=httpx.MockTransport(fake))
    (run_dir,) = [p for p in (tmp_path / "runs").iterdir() if p.is_dir()]
    return code, run_dir


def _results(run_dir):
    with (run_dir / "results.csv").open(encoding="utf-8-sig") as f:
        return {r["question_id"]: r for r in csv.DictReader(f)}


def test_full_run_writes_results_raw_log_and_config(tmp_path):
    fake = FakeTaskApi(result_script={"Q41": ["still_running", "still_running", "ok"]})
    code, run_dir = _run_main(tmp_path, fake)

    assert code == 0
    results = _results(run_dir)
    assert {r["status"] for r in results.values()} == {"completed"}
    assert results["Q41_0"]["answer"] == "Yes"
    assert results["Q01_0"]["answer"] == "Prose answer."
    assert results["Q01_0"]["citations"] == "A — https://a.example\n  - x\n  - y\n\nhttps://b.example"
    assert results["Q01_0"]["server_latency_s"] == "90.0"
    assert results["Q01_0"]["config_name"] == "chubb-v1"
    raw = [json.loads(line) for line in (run_dir / "raw.jsonl").read_text().splitlines()]
    q41 = next(r for r in raw if r["question_id"] == "Q41_0")
    assert [a["http_status"] for a in q41["attempts"]] == [202, 408, 408, 200]
    assert json.loads((run_dir / "task_config.json").read_text()) == json.loads(CONFIG.read_text())
    assert json.loads((run_dir / "run_info.json").read_text())["status_counts"] == {"completed": 3}


def test_dropped_result_is_lost_not_reread_and_resume_reruns_only_it(tmp_path):
    fake = FakeTaskApi(result_script={"Q81": ["drop"]})
    questions = _questions(tmp_path)
    _, run_dir = _run_main(tmp_path, fake, questions=questions)

    assert _results(run_dir)["Q81_0"]["status"] == "lost"
    assert max(fake.result_calls.values()) == 1

    fake.result_script = {}
    run_eval.main([str(questions), "--config", str(CONFIG), "--yes", "--resume", str(run_dir)],
                  transport=httpx.MockTransport(fake))

    assert fake.creates == 4
    assert {r["status"] for r in _results(run_dir).values()} == {"completed"}


def test_failed_run_and_create_retry(tmp_path):
    fake = FakeTaskApi(result_script={"Q01": ["fail"]}, create_script=[429, 503])
    _, run_dir = _run_main(tmp_path, fake, "--concurrency", "1")

    results = _results(run_dir)
    assert results["Q01_0"]["status"] == "failed"
    assert "Run failed." in results["Q01_0"]["error"]
    assert results["Q41_0"]["status"] == "completed"


def test_many_questions_in_parallel(tmp_path):
    fake = FakeTaskApi()
    _, run_dir = _run_main(tmp_path, fake, questions=_questions(tmp_path, n_copies=40))

    results = _results(run_dir)
    assert len(results) == 120
    assert all(r["status"] == "completed" for r in results.values())
    assert max(fake.result_calls.values()) == 1


def test_resume_refuses_a_different_config(tmp_path):
    _, run_dir = _run_main(tmp_path, FakeTaskApi(), "--limit", "1")
    other = tmp_path / "other.json"
    config = json.loads(CONFIG.read_text())
    config["body"]["processor"] = "pro"
    other.write_text(json.dumps(config))

    code = run_eval.main([str(_questions(tmp_path)), "--config", str(other), "--yes",
                          "--resume", str(run_dir)])

    assert code == 2


def test_question_type_override_allows_several_answers(tmp_path):
    config = json.loads(CONFIG.read_text())
    config["question_type_overrides"] = {"multiple_choice": {"task_spec": {"output_schema": {
        "json_schema": {"properties": {"answer": {
            "type": "array", "items": {"type": "string", "enum": "{{answer_options}}"},
            "enum": None,
        }}}}}}}
    path = tmp_path / "multi.json"
    path.write_text(json.dumps(config))
    fake = FakeTaskApi()
    code = run_eval.main([str(_questions(tmp_path)), "--config", str(path), "--runs-dir",
                          str(tmp_path / "runs"), "--yes"], transport=httpx.MockTransport(fake))
    (run_dir,) = (tmp_path / "runs").iterdir()

    assert code == 0
    results = _results(run_dir)
    assert results["Q81_0"]["answer"].count(" | ") == 1
    assert results["Q41_0"]["answer"] == "Yes"
    raw = [json.loads(line) for line in (run_dir / "raw.jsonl").read_text().splitlines()]
    q81 = next(r for r in raw if r["question_id"] == "Q81_0")
    assert "enum" not in q81["request"]["task_spec"]["output_schema"]["json_schema"]["properties"]["answer"]


def test_render_drops_options_placeholder_for_open_questions():
    template = {"enum": "{{answer_options}}", "q": "{{question}}\n\n{{answer_options}}"}

    assert run_eval.render(template, {"question": "Why?", "answer_options": ""}) == {"q": "Why?"}
    assert run_eval.render(template, {"question": "Which?", "answer_options": "A|B"}) == {
        "enum": ["A", "B"], "q": "Which?\n\nA\nB"}
    with pytest.raises(run_eval.UserError, match="no 'industry' column"):
        run_eval.render({"x": "{{industry}}"}, {"question": "q"})


@pytest.mark.parametrize(
    ("row", "problem"),
    [
        ({"question_type": "open", "answer_options": "Yes|No"}, "must leave answer_options empty"),
        ({"question_type": "yes_no", "answer_options": ""}, "needs at least 2"),
        ({"question_type": "essay"}, "must be one of"),
        ({"question_id": "Q41"}, "duplicate question_id"),
    ],
)
def test_csv_validation(tmp_path, row, problem):
    with TEMPLATE.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    rows[0].update(row)
    path = tmp_path / "bad.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    with pytest.raises(run_eval.UserError, match=problem):
        run_eval.load_questions(path)
