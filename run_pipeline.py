import logging
import time
from ingest import ingest_data
from transform import transform_data
from publish import publish_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    start_time = time.time()
    logging.info("🏁 SALES DATA PIPELINE STARTED")
    logging.info("="*30)

    # Step 1: Ingest
    if not ingest_data():
        logging.error("🛑 Pipeline Stopped: Ingest Failed")
        return

    # Step 2: Transform
    if not transform_data():
        logging.error("🛑 Pipeline Stopped: Transform Failed")
        return

    # Step 3: Publish
    if not publish_data():
        logging.error("🛑 Pipeline Stopped: Publish Failed")
        return

    logging.info("="*30)
    duration = time.time() - start_time
    logging.info(f"🎉 PIPELINE COMPLETED SUCCESSFULLY in {duration:.2f} seconds.")

if __name__ == "__main__":
    main()