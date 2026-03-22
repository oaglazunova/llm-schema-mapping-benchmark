from __future__ import annotations

import argparse
from pathlib import Path

from lsmbench.sources.gamebus.profile_gamebus_descriptor import (
    profile_descriptor_file,
    save_profile,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Profile a GameBus descriptor JSON file and write a descriptor profile JSON report."
    )
    parser.add_argument(
        "input",
        nargs="+",
        help="One or more GameBus descriptor JSON files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Optional output directory. Default: data/interim_private/descriptor_profiles",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    output_dir = Path(args.output_dir).resolve() if args.output_dir else None

    for raw_path in args.input:
        path = Path(raw_path).resolve()
        profile = profile_descriptor_file(path)

        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"{profile.descriptor_name}.profile.json"
        else:
            output_path = None

        written = save_profile(profile, output_path=output_path)

        print("=" * 80)
        print(f"INPUT:  {path}")
        print(f"OUTPUT: {written}")
        print(f"DESCRIPTOR: {profile.descriptor_name}")
        print(f"RECORDS: {profile.record_count}")
        print(f"FIELDS: {profile.field_count}")
        print(f"FAMILY_HINT: {profile.benchmark_family_hint}")
        print("FIELD SUMMARY:")
        for fp in profile.field_profiles:
            print(
                f"  - {fp.field_name}: "
                f"types={fp.raw_type_counts}, "
                f"hints={fp.logical_type_hints}, "
                f"ops={fp.recommended_operations}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())