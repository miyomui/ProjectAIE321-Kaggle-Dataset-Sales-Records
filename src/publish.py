import pandas as pd
from sqlalchemy import create_engine
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# --- ชื่อ Google Sheet ของคุณ ---
SHEET_NAME = 'My Sales Dashboard' 

def publish_data():
    try:
        logging.info("☁️ Starting Publish Process...")

        # 1. เชื่อมต่อ Database
        db_conn = 'postgresql://postgres:mysecretpassword@localhost:5432/sales_db'
        engine = create_engine(db_conn)

        # 2. อ่านข้อมูลจาก Schema 'production'
        logging.info("📥 Fetching cleaned data from production.sales_data...")
        
        # ดึงมาทุกแถว
        query = "SELECT * FROM production.sales_data" 
        df = pd.read_sql(query, engine)

        if df.empty:
            logging.warning("⚠️ No data found in production table!")
            return False

        # 3. แปลงวันที่เป็น String
        if 'Order_Date' in df.columns:
            df['Order_Date'] = df['Order_Date'].astype(str)
        if 'Ship_Date' in df.columns:
            df['Ship_Date'] = df['Ship_Date'].astype(str)

        # 4. เชื่อมต่อ Google Sheets
        logging.info(f"🔗 Connecting to Google Sheet: {SHEET_NAME}")
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
    
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        creds_path = os.path.join(project_root, 'credentials.json')

        if not os.path.exists(creds_path):
             logging.error(f"❌ Credentials file not found at: {creds_path}")
             return False
             
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
        client = gspread.authorize(creds)

        # ---------------------------------------------------------

        # 5. อัปโหลดข้อมูล
        sheet = client.open(SHEET_NAME).sheet1
        sheet.clear()
        
        # เตรียมข้อมูล (Header + Rows)
        data = [df.columns.values.tolist()] + df.values.tolist()
        sheet.update(data)

        logging.info(f"✅ Publish Successful: Uploaded {len(df)} rows to Google Sheets.")
        return True

    except Exception as e:
        logging.error(f"❌ Error in Publish: {e}")
        return False

if __name__ == "__main__":
    publish_data()