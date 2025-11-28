import pandas as pd
from sqlalchemy import create_engine
import os
import logging

# ตั้งค่า Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def ingest_data():
    try:
        logging.info("🚀 Starting Ingestion Process...")

        # เชื่อมต่อ Database
        db_conn = 'postgresql://postgres:mysecretpassword@localhost:5432/sales_db'
        engine = create_engine(db_conn)
        
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        csv_path = os.path.join(project_root, 'data', '100000 Sales Records.csv')

        # ---------------------------------------------------------

        if not os.path.exists(csv_path):
            logging.error(f"❌ File not found: {csv_path}")
            logging.error(f"   (Looking at: {csv_path})")
            return False

        df = pd.read_csv(csv_path)
        logging.info(f"📄 Read {len(df)} rows from CSV.")

        # แก้ชื่อคอลัมน์ (ลบช่องว่าง)
        df.columns = [c.strip().replace(' ', '_') for c in df.columns]

        # นำเข้าข้อมูลสู่ Table 'raw_sales'
        # if_exists='replace' คือถ้ามีตารางเก่าให้ลบทิ้งแล้วสร้างใหม่
        df.to_sql('raw_sales', engine, if_exists='replace', index=False, chunksize=5000)
        
        logging.info("✅ Ingestion Successful: Data saved to 'raw_sales' table.")
        return True

    except Exception as e:
        logging.error(f"❌ Error in Ingestion: {e}")
        return False

if __name__ == "__main__":
    ingest_data()