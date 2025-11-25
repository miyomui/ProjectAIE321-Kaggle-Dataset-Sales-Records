import pandas as pd
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def transform_data():
    try:
        logging.info("🔄 Starting Transformation Process...")

        # 1. เชื่อมต่อ Database
        db_conn = 'postgresql://postgres:mysecretpassword@localhost:5432/sales_db'
        engine = create_engine(db_conn)

        # 2. สร้าง Schema 'production'
        with engine.connect() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS production;"))
            conn.commit()

        # 3. อ่านข้อมูลดิบ
        df = pd.read_sql("SELECT * FROM raw_sales", engine)
        if df.empty:
            logging.warning("⚠️ No data in raw_sales!")
            return False

        # --- Data Cleansing ---
        logging.info("🧹 Cleaning data...")
        df = df.drop_duplicates() # ลบข้อมูลซ้ำ
        
        # แปลงวันที่
        df['Order_Date'] = pd.to_datetime(df['Order_Date'], errors='coerce')
        df['Ship_Date'] = pd.to_datetime(df['Ship_Date'], errors='coerce')
        df = df.dropna(subset=['Order_Date', 'Ship_Date']) # ลบแถวที่วันที่พัง
        df = df[df['Ship_Date'] >= df['Order_Date']] # กรองวันที่ส่งต้องไม่มาก่อนสั่ง

        # แปลงตัวเลข
        numeric_cols = ['Units_Sold', 'Unit_Price', 'Total_Revenue', 'Total_Profit']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=numeric_cols) # ลบแถวที่ตัวเลขพัง

        # ---------------------------------------------------------
        # FEATURE ENGINEERING (สร้างคอลัมน์ใหม่ตาม KPI)
        # ---------------------------------------------------------
        logging.info("🛠️ Creating new features based on KPIs...")

        # 1. สร้าง 'Order_Year' (สำหรับ KPI: ดูยอดขายรายปี 2010-2017)
        df['Order_Year'] = df['Order_Date'].dt.year

        # 2. สร้าง 'Days_to_Ship' (สำหรับ KPI: ระยะเวลาจัดส่ง)
        df['Days_to_Ship'] = (df['Ship_Date'] - df['Order_Date']).dt.days

        # 3. สร้าง 'Delivery_Speed' (จัดกลุ่มความเร็วการส่ง)
        # เช่น: ส่งภายใน 3 วัน = Fast, 3-7 วัน = Normal, เกิน 7 วัน = Slow
        def categorize_speed(days):
            if days <= 3: return 'Fast'
            elif days <= 7: return 'Normal'
            else: return 'Slow'
        
        df['Delivery_Speed'] = df['Days_to_Ship'].apply(categorize_speed)

        # ---------------------------------------------------------
        # 📊 KPI INSIGHTS (คำนวณโชว์ใน Terminal เลย)
        # ---------------------------------------------------------
        logging.info("\n" + "="*50)
        logging.info("📊 PRELIMINARY KPI INSIGHTS")
        logging.info("="*50)

        # KPI 1: สินค้าประเภทใดมียอดขายรวมมากที่สุด?
        top_item = df.groupby('Item_Type')['Total_Revenue'].sum().idxmax()
        top_item_val = df.groupby('Item_Type')['Total_Revenue'].sum().max()
        logging.info(f"🏆 Top Item Type: {top_item} (Total Revenue: {top_item_val:,.2f})")

        # KPI 2: ประเทศใดมียอดขายมากที่สุด?
        top_country = df.groupby('Country')['Total_Revenue'].sum().idxmax()
        logging.info(f"🌍 Top Country (Sales): {top_country}")

        # KPI 3: ช่องทางขายใดทำกำไรรวมสูงสุด?
        top_channel = df.groupby('Sales_Channel')['Total_Profit'].sum().idxmax()
        logging.info(f"💰 Top Sales Channel (Profit): {top_channel}")

        # KPI 4: ประเทศใดมียอดขาย 'เฉลี่ย' สูงที่สุด?
        top_avg_country = df.groupby('Country')['Total_Revenue'].mean().idxmax()
        logging.info(f"📈 Top Country (Avg Sales): {top_avg_country}")

        # KPI 5: Correlation ระหว่าง วันส่งของ กับ ยอดขาย
        correlation = df['Days_to_Ship'].corr(df['Total_Revenue'])
        logging.info(f"🔗 Correlation (Days vs Revenue): {correlation:.4f} (ใกล้ 0 แปลว่าไม่เกี่ยวกัน)")
        
        logging.info("="*50 + "\n")

        # ---------------------------------------------------------
        
        # 4. บันทึกลง Database
        logging.info("💾 Saving to 'production.sales_data'...")
        df.to_sql('sales_data', engine, schema='production', if_exists='replace', index=False, chunksize=5000)

        logging.info("✅ Transformation Complete!")
        return True

    except Exception as e:
        logging.error(f"❌ Error in Transformation: {e}")
        return False

if __name__ == "__main__":
    transform_data()