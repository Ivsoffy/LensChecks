import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from start import run_checks  # noqa: E402


@pytest.fixture(autouse=True)
def isolated_workdir(tmp_path, monkeypatch):
    src_module2_dir = REPO_ROOT / "src" / "modules" / "module_2"
    dst_module2_dir = tmp_path / "modules" / "module_2"
    dst_module2_dir.mkdir(parents=True, exist_ok=True)

    src_weights_dir = src_module2_dir / "function_model_weights"
    dst_weights_dir = dst_module2_dir / "function_model_weights"

    # Module 2 loads model weights by a relative path from CWD.
    # Provide that path in tmp workspace without duplicating large files.
    if not dst_weights_dir.exists():
        linked = False
        try:
            os.symlink(src_weights_dir, dst_weights_dir, target_is_directory=True)
            linked = True
        except OSError:
            try:
                subprocess.run(
                    [
                        "cmd",
                        "/c",
                        "mklink",
                        "/J",
                        str(dst_weights_dir),
                        str(src_weights_dir),
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                linked = True
            except Exception:
                linked = False

        if not linked:
            shutil.copytree(
                src_weights_dir,
                dst_weights_dir,
                ignore=shutil.ignore_patterns("checkpoint-*"),
            )

    shutil.copy2(
        REPO_ROOT / "src" / "modules" / "funcs_2026.parquet",
        dst_module2_dir / "funcs_2026.parquet",
    )
    monkeypatch.chdir(tmp_path)


def test_run_checks_module2_basic_questionnaire(capsys):
    input_folder = REPO_ROOT / "tests" / "data" / "module2_basic"
    config = {
        "input_folder": str(input_folder),
        "module": 2,
        "after_fix": False,
        "folder_past_year": "missing_folder_for_tests",
    }

    run_checks(config)
    captured = capsys.readouterr()

    assert Path("modules/module_2/output/require_check").is_dir()
    assert "Traceback" not in (captured.out + captured.err)


# def test_run_checks_module2_after_fix_questionnaire(capsys):
#     input_folder = REPO_ROOT / "tests" / "data" / "module2_after_fix"
#     config = {
#         "input_folder": str(input_folder),
#         "module": 2,
#         "after_fix": True,
#         "folder_past_year": "missing_folder_for_tests",
#     }

#     run_checks(config)
#     captured = capsys.readouterr()

#     assert Path("modules/module_2/output").is_dir()
#     assert "Traceback" not in (captured.out + captured.err)


# def test_run_checks_module3_basic_questionnaire(capsys):
#     input_folder = REPO_ROOT / "tests" / "data" / "module3_basic"
#     config = {
#         "input_folder": str(input_folder),
#         "module": 3,
#         "after_fix": False,
#         "save_to_parquet": False,
#     }

#     run_checks(config)
#     captured = capsys.readouterr()

#     assert Path("modules/module_3/output").is_dir()
#     assert "Traceback" not in (captured.out + captured.err)
