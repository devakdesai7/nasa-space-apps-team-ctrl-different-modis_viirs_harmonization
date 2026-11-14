
import argparse
from pathlib import Path

from silver_transformation import transform_file


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
DEFAULT_SILVER_ROOT = PROJECT_ROOT / "data" / "silver"

SOURCE_GROUPS = ("MODIS", "NOAA20", "NOAA21", "SNPP")


def run_batch(
    bronze_root: Path,
    silver_root: Path,
    groups: list[str],
    overwrite: bool = False,
):
    successful = []
    skipped = []
    failed = []

    # Discover all Bronze files first.
    files_to_process = []

    for group in groups:
        group_dir = bronze_root / group

        if not group_dir.exists():
            print(f"WARNING: Bronze directory not found: {group_dir}")
            continue

        bronze_files = sorted(group_dir.glob("*.parquet"))

        for bronze_file in bronze_files:
            silver_file = silver_root / group / bronze_file.name
            files_to_process.append(
                (group, bronze_file, silver_file)
            )

    if not files_to_process:
        print("No Bronze Parquet files found.")
        return False

    print("=" * 65)
    print("SILVER BATCH TRANSFORMATION")
    print("=" * 65)
    print(f"Bronze root: {bronze_root}")
    print(f"Silver root: {silver_root}")
    print(f"Source groups: {', '.join(groups)}")
    print(f"Files discovered: {len(files_to_process)}")
    print(f"Overwrite existing: {overwrite}")
    print("=" * 65)

    for index, (group, bronze_file, silver_file) in enumerate(
        files_to_process,
        start=1,
    ):
        print(
            f"\n[{index}/{len(files_to_process)}] "
            f"{group}/{bronze_file.name}"
        )

        if silver_file.exists() and not overwrite:
            print("SKIPPED: Silver output already exists.")
            skipped.append(silver_file.name)
            continue

        try:
            transform_file(
                input_file=bronze_file,
                output_file=silver_file,
                source_group=group,
            )

            successful.append(silver_file.name)

        except Exception as error:
            print(f"FAILED: {error}")
            failed.append(
                (silver_file.name, str(error))
            )

    # Final summary.
    print("\n" + "=" * 65)
    print("BATCH TRANSFORMATION SUMMARY")
    print("=" * 65)

    print(f"Files discovered: {len(files_to_process)}")
    print(f"Successful:      {len(successful)}")
    print(f"Skipped:         {len(skipped)}")
    print(f"Failed:          {len(failed)}")

    if successful:
        print("\nSuccessful files:")
        for filename in successful:
            print(f"  PASS: {filename}")

    if skipped:
        print("\nSkipped files:")
        for filename in skipped:
            print(f"  SKIP: {filename}")

    if failed:
        print("\nFailed files:")
        for filename, error in failed:
            print(f"  FAIL: {filename}")
            print(f"        {error}")

    print("=" * 65)

    if failed:
        print("BATCH COMPLETED WITH FAILURES.")
        return False

    print("BATCH COMPLETED SUCCESSFULLY.")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Batch-transform Bronze Parquet files into Silver."
    )

    parser.add_argument(
        "--group",
        choices=SOURCE_GROUPS,
        help="Process only one source group. Default: all groups.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-transform files even if Silver outputs already exist.",
    )

    parser.add_argument(
        "--bronze-root",
        type=Path,
        default=DEFAULT_BRONZE_ROOT,
    )

    parser.add_argument(
        "--silver-root",
        type=Path,
        default=DEFAULT_SILVER_ROOT,
    )

    args = parser.parse_args()

    groups = [args.group] if args.group else list(SOURCE_GROUPS)

    success = run_batch(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        groups=groups,
        overwrite=args.overwrite,
    )

    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
