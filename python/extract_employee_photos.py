#!/usr/bin/env python3
"""
استخراج تصاویر واقعی کارمندان از دیتابیس Northwind
حذف هدر 78 بایتی OLE Object (اگر وجود داشته باشد)
"""

import os
import sys
import psycopg2
from pathlib import Path

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def extract_photos():
    """استخراج تصاویر کارمندان از دیتابیس و ذخیره در data_lake"""
    
    print("=" * 60)
    print("🔄 شروع استخراج تصاویر کارمندان از دیتابیس...")
    print("=" * 60)
    
    # اتصال به دیتابیس OP
    try:
        conn = psycopg2.connect(
            host=env("OP_HOST", "localhost"),
            port=int(env("OP_PORT", "25432")),
            dbname=env("OP_DB", "northwind"),
            user=env("OP_USER", "admin"),
            password=env("OP_PASSWORD", "admin123"),
        )
        print("✅ اتصال به دیتابیس برقرار شد")
    except Exception as e:
        print(f"❌ خطا در اتصال به دیتابیس: {e}")
        return
    
    # پوشه ذخیره تصاویر
    output_dir = PROJECT_HOME / "data_lake" / "employees"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 پوشه ذخیره تصاویر: {output_dir}")
    
    with conn.cursor() as cur:
        # پیدا کردن ستون تصویر
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'employees' 
            AND data_type IN ('bytea', 'BYTEA', 'oid')
        """)
        photo_columns = cur.fetchall()
        
        if not photo_columns:
            print("❌ هیچ ستون تصویری در جدول employees پیدا نشد!")
            print("🔍 ستون‌های موجود در جدول employees:")
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'employees'
            """)
            for col, dtype in cur.fetchall():
                print(f"   - {col}: {dtype}")
            conn.close()
            return
        
        photo_column = photo_columns[0][0]
        print(f"✅ ستون تصویر پیدا شد: {photo_column}")
        
        # استخراج تصاویر
        print("🔄 در حال استخراج تصاویر...")
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
        for employee_id, photo_data in rows:
            if photo_data is None:
                continue
                
            # حذف هدر 78 بایتی (مخصوص OLE Objects)
            if isinstance(photo_data, bytes):
                image_data = photo_data
                
                # حذف هدر 78 بایتی اگر وجود داشته باشد
                if len(image_data) > 78:
                    # بررسی هدر استاندارد تصویر (بدون هدر OLE)
                    if image_data[:4].hex().startswith('ffd8ffe0') or \
                       image_data[:4].hex().startswith('89504e47') or \
                       image_data[:3].hex().startswith('474946'):
                        # تصویر با هدر استاندارد شروع می‌شود
                        pass
                    else:
                        # حذف 78 بایت اول (هدر OLE)
                        image_data = image_data[78:]
                
                # تشخیص فرمت تصویر
                file_ext = '.jpg'
                header_hex = image_data[:8].hex()
                
                if header_hex.startswith('ffd8ffe0') or header_hex.startswith('ffd8ffe1'):
                    file_ext = '.jpg'
                elif header_hex.startswith('89504e47'):
                    file_ext = '.png'
                elif header_hex.startswith('47494638'):
                    file_ext = '.gif'
                elif header_hex.startswith('424d'):
                    file_ext = '.bmp'
                elif header_hex.startswith('49492a00') or header_hex.startswith('4d4d002a'):
                    file_ext = '.tif'
                else:
                    # اگر ناشناخته بود، به عنوان jpg ذخیره کن
                    file_ext = '.jpg'
                
                # ذخیره فایل
                file_path = output_dir / f"{employee_id}{file_ext}"
                with open(file_path, 'wb') as f:
                    f.write(image_data)
                saved_count += 1
                print(f"✅ ذخیره شد: {file_path.name} ({len(image_data)} bytes)")
                
            else:
                print(f"⚠️  داده‌های کارمند {employee_id} از نوع bytea نیست: {type(photo_data)}")
    
    conn.close()
    print("=" * 60)
    print(f"🎉 استخراج تصاویر کامل شد! {saved_count} تصویر ذخیره شد.")
    print(f"📁 مسیر: {output_dir}")
    print("=" * 60)

if __name__ == "__main__":
    extract_photos()
