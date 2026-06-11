import argparse
import sys
from loguru import logger
from src.bronze.raw_loader import load_bronze
from src.silver.xml_parser import process_silver
from src.gold.aggregator import process_gold


def main():
    parser = argparse.ArgumentParser(
        description="Apple Watch Medallion Data Pipeline CLI (Bronze, Silver, Gold)"
    )
    parser.add_argument(
        "--stage",
        type=str,
        required=True,
        choices=["bronze", "silver", "gold", "all"],
        help="Pipeline stage to execute: 'bronze', 'silver', 'gold', or 'all'",
    )
    parser.add_argument(
        "--user",
        type=str,
        required=True,
        help="User identifier to isolate health data.",
    )
    parser.add_argument(
        "--zip-path",
        type=str,
        required=False,
        default=None,
        help="Optional path to the source ZIP data file.",
    )

    args = parser.parse_args()

    logger.info(f"Launching Apple Watch Medallion Pipeline | User: {args.user} | Stage: {args.stage.upper()}")

    try:
        if args.stage == "bronze":
            load_bronze(args.user, args.zip_path)
        elif args.stage == "silver":
            process_silver(args.user)
        elif args.stage == "gold":
            process_gold(args.user)
        elif args.stage == "all":
            logger.info("Executing Bronze Loader (1/3)...")
            load_bronze(args.user, args.zip_path)

            logger.info("Executing Silver XML Parser (2/3)...")
            process_silver(args.user)

            logger.info("Executing Gold Aggregator (3/3)...")
            process_gold(args.user)

        logger.info("Pipeline Execution Completed Successfully.")

    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
