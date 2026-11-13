"""C2: run the Mage pipeline for max_depth = 1..5 and print the results table.

    python scripts/run_max_depth_experiment.py        # run from the repo root

For each depth: delete iris_predictions.csv, run the pipeline, record the test
accuracy printed by train_model, whether the @test gate passed, and whether the
CSV was created. Requires `pip install mage-ai`.
"""
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "iris_predictions.csv"
CMD = [sys.executable, "-m", "mage_ai.cli.main", "run", "iris_project", "iris_classifier", "--test"]


def run(depth: int) -> str:
    """Mage versions differ in how --runtime-vars is parsed; try JSON, then key/value."""
    out = ""
    for extra in (["--runtime-vars", json.dumps({"max_depth": depth})],
                  ["--runtime-vars", "max_depth", str(depth)]):
        res = subprocess.run(CMD + extra, cwd=ROOT, capture_output=True, text=True)
        out = res.stdout + res.stderr
        if re.search(r"Test accuracy\s*=", out):
            return out
    return out


def main():
    rows = []
    for depth in range(1, 6):
        CSV.unlink(missing_ok=True)
        out = run(depth)
        m = re.search(r"Test accuracy\s*=\s*([0-9.]+)", out)
        acc = float(m.group(1)) if m else None
        rows.append((depth, acc, acc is not None and acc >= 0.8, CSV.exists()))
        print(f"--- max_depth={depth} done", file=sys.stderr)
    print("\n| max_depth | Test accuracy | test_accuracy passed? | iris_predictions.csv created? |")
    print("|---|---|---|---|")
    for d, a, p, c in rows:
        print(f"| {d} | {'n/a' if a is None else a} | {'yes' if p else 'no'} | {'yes' if c else 'no'} |")
    if any(a is None for _, a, _, _ in rows):
        print("\nSome accuracies could not be parsed: run `mage run` by hand and read the output.")


if __name__ == "__main__":
    main()
