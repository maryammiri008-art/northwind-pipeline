#!/usr/bin/env python3
"""
استخراج تصاویر کارمندان از PostgreSQL (Northwind)
"""

import sys
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def extract_photos():
    """استخراج تصاویر از PostgreSQL"""
    
    print("=" * 60)
    print("🔄 اتصال به PostgreSQL برای استخراج تصاویر...")
    print("=" * 60)
    
    # اتصال به PostgreSQL
    try:
        conn = psycopg2.connect(
            host=env("OP_HOST", "localhost"),
            port=int(env("OP_PORT", "25432")),
            dbname=env("OP_DB", "northwind"),
            user=env("OP_USER", "admin"),
            password=env("OP_PASSWORD", "admin123"),
        )
        print("✅ اتصال به PostgreSQL برقرار شد")
    except Exception as e:
        print(f"❌ خطا در اتصال: {e}")
        return
    
    output_dir = PROJECT_HOME / "data_lake" / "employees"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 پوشه خروجی: {output_dir}")
    
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # پیدا کردن ستون تصویر
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'employees' 
            AND data_type IN ('bytea')
        """)
        col_info = cur.fetchone()
        
        if not col_info:
            print("❌ هیچ ستون bytea در جدول employees پیدا نشد!")
            print("🔍 ستون‌های موجود:")
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'employees'
                ORDER BY ordinal_position
            """)
            for col in cur.fetchall():
                print(f"   - {col['column_name']}: {col['data_type']}")
            conn.close()
            return
        
        photo_column = col_info['column_name']
        print(f"✅ ستون تصویر پیدا شد: {photo_column}")
        
        # استخراج تصاویر
        cur.execute(f"""
            SELECT employee_id, {photo_column} 
            FROM employees 
            WHERE {photo_column} IS NOT NULL
            ORDER BY employee_id
        """)
        
        rows = cur.fetchall()
        if not rows:
            print("❌ هیچ تصویری در دیتابیس پیدا نشد!")
            conn.close()
            return
        
        print(f"✅ {len(rows)} تصویر پیدا شد")
        
        saved_count = 0
        for row in rows:
            employee_id = row['employee_id']
            photo_data = row[photo_column]
            
            if photo_data is None:
                continue
            
            # حذف هدر 78 بایتی (OLE Objects)
            image_data = photo_data
            if len(image_data) > 78:
                # بررسی هدر استاندارد تصویر
                header = image_data[:4].hex()
                if not (header.startswith('ffd8') or header.startswith('8950') or header.startswith('4749')):
                    # حذف 78 بایت اول (هدر OLE)
                    image_data = image_data[78:]
            
            # تشخیص فرمت
            header = image_data[:8].hex().lower()
            if header.startswith('ffd8ffe0') or header.startswith('ffd8ffe1'):
                ext = '.jpg'
            elif header.startswith('89504e47'):
                ext = '.png'
            elif header.startswith('47494638'):
                ext = '.gif'
            elif header.startswith('424d'):
                ext = '.bmp'
            else:
                ext = '.jpg'
            
            # ذخیره
            file_path = output_dir / f"{employee_id}{ext}"
            with open(file_path, 'wb') as f:
                f.write(image_data)
            saved_count += 1
            print(f"✅ Employee {employee_id}: {len(image_data)} bytes -> {file_path.name}")
        
    conn.close()
    print("=" * 60)
    print(f"🎉 استخراج کامل شد! {saved_count} تصویر ذخیره شد.")
    print("=" * 60)

if __name__ == "__main__":
    extract_photos()
