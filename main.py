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
        help="Pipeline stage to execute: 'bronze' (ingestion), 'silver' (xml to parquet), 'gold' (duckdb aggregates to postgres), or 'all' for full E2E run.",
    )

    args = parser.parse_args()

    logger.info(f"Launching Apple Watch Medallion Pipeline | Stage: {args.stage.upper()}")

    try:
        if args.stage == "bronze":
            load_bronze()
        elif args.stage == "silver":
            process_silver()
        elif args.stage == "gold":
            process_gold()
        elif args.stage == "all":
            logger.info("Executing Bronze Loader (1/3)...")
            load_bronze()
            
            logger.info("Executing Silver XML Parser (2/3)...")
            process_silver()
            
            logger.info("Executing Gold Aggregator (3/3)...")
            process_gold()
            
        logger.info("Pipeline Execution Completed Successfully.")
        
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
