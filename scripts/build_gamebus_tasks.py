from __future__ import annotations

import argparse
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lsmbench.sources.gamebus.descriptor_to_task import build_task_bundle_from_profile


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate benchmark task scaffolds from GameBus descriptor profile JSON files."
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="One or more descriptor profile JSON files",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    for raw_path in args.input:
        path = Path(raw_path).resolve()
        outputs = build_task_bundle_from_profile(path)

        print("=" * 80)
        print(f"PROFILE: {path}")
        for kind, out_path in outputs.items():
            print(f"{kind.upper()}: {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())