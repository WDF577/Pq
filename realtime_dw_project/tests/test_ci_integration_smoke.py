from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
REPOSITORY_DIR = PROJECT_DIR.parent


def test_workflow_keeps_fast_ci_and_gates_heavy_smoke_to_manual_dispatch():
    workflow = (REPOSITORY_DIR / ".github/workflows/project-ci.yml").read_text(
        encoding="utf-8"
    )

    assert "push:" in workflow
    assert "pull_request:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "if: github.event_name == 'workflow_dispatch'" in workflow
    assert "timeout-minutes: 40" in workflow
    assert "bash scripts/ci_integration_smoke.sh" in workflow


def test_integration_job_preserves_diagnostics_and_always_cleans_up():
    workflow = (REPOSITORY_DIR / ".github/workflows/project-ci.yml").read_text(
        encoding="utf-8"
    )

    assert "docker compose logs --no-color --timestamps" in workflow
    assert "integration-smoke-evidence-${{ github.run_id }}" in workflow
    assert "realtime_dw_project/artifacts/ci/quality-report.md" in workflow
    assert "retention-days: 30" in workflow
    assert "flink-jobs-overview.json" in workflow
    assert "quality-report-at-failure.md" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "if: always()" in workflow
    assert "docker compose down -v --remove-orphans" in workflow


def test_smoke_script_checks_nine_jobs_quality_and_tombstones():
    script = (PROJECT_DIR / "scripts/ci_integration_smoke.sh").read_text(
        encoding="utf-8"
    )

    assert "len(jobs) == 9" in script
    assert "nine non-empty serving outputs" in script
    assert "delete_tombstone_drill.py" in script
    assert "verify_result.sh" in script
    assert "quality_report.py" in script
