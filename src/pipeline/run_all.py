from __future__ import annotations

import subprocess
import sys


STEPS = [
    # Bronze (adjust names if yours differ)
    ["python", "-m", "src.bronze.run_phase1_1_public_power"],
    ["python", "-m", "src.bronze.run_phase1_2_prices"],

    # Silver
    ["python", "-m", "src.silver.run_phase2_2_public_power"],
    ["python", "-m", "src.silver.run_phase2_3_prices"],

    # Gold
    ["python", "-m", "src.gold.run_phase3_1_daily_prices"],
    ["python", "-m", "src.gold.run_phase3_2_power_mix"],
    ["python", "-m", "src.gold.run_phase3_3_price_vs_renewables"],

    # Quality
    ["python", "-m", "src.quality.run_quality_checks"],
]


def main() -> None:
    for cmd in STEPS:
        print("\n" + "=" * 80)
        print("Running:", " ".join(cmd))
        print("=" * 80)
        r = subprocess.run(cmd)
        if r.returncode != 0:
            print(f"❌ Failed step: {' '.join(cmd)}")
            sys.exit(r.returncode)

    print("\n✅ Pipeline completed successfully.")


if __name__ == "__main__":
    main()
