#!/usr/bin/env python3
import os
import sys
import psycopg2
from pathlib import Path

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def extract_photos():
    conn = psycopg2.connect(
        host=env("OP_HOST", "localhost"),
        port=int(env("OP_PORT", "25432")),
        dbname=env("OP_DB", "northwind"),
        user=env("OP_USER", "admin"),
        password=env("OP_PASSWORD", "admin123"),
    )
    
    output_dir = PROJECT_HOME / "data_lake" / "employees"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"📁 ذخیره در: {output_dir}")
    
    with conn.cursor() as cur:
        # پیدا کردن ستون تصویر
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'employees' 
            AND data_type IN ('bytea', 'BYTEA')
        """)
        col = cur.fetchone()
        if not col:
            print("❌ ستون bytea پیدا نشد!")
            return
        photo_column = col[0]
        print(f"✅ ستون: {photo_column}")
        
        # استخراج
        cur.execute(f"""
            SELECT employee_id, {photo_column} 
            FROM employees 
            WHERE {photo_column} IS NOT NULL
            ORDER BY employee_id
        """)
        
        rows = cur.fetchall()
        print(f"✅ {len(rows)} تصویر پیدا شد")
        
        for employee_id, data in rows:
            if not data:
                continue
            
            # حذف 78 بایت اول (هدر OLE)
            # این روش استاندارد برای Northwind است
            offset = 0
            if len(data) > 78:
                # هدر OLE معمولاً با 0x00 0x00 0x01 0x00 شروع می‌شود
                if data[:4].hex() == '00000100':
                    offset = 78
                else:
                    # شاید هدر 78 بایتی نباشد
                    for i in range(0, 100):
                        if data[i:i+2].hex() in ['ffd8', '8950', '4749', '424d']:
                            offset = i
                            break
            
            image_data = data[offset:]
            
            # تشخیص فرمت
            ext = '.jpg'
            if image_data[:2].hex() == 'ffd8':
                ext = '.jpg'
            elif image_data[:4].hex() == '89504e47':
                ext = '.png'
            elif image_data[:3].hex() == '474946':
                ext = '.gif'
            elif image_data[:2].hex() == '424d':
                ext = '.bmp'
            
            # ذخیره
            file_path = output_dir / f"{employee_id}{ext}"
            with open(file_path, 'wb') as f:
                f.write(image_data)
            print(f"✅ {employee_id}: {len(image_data)} bytes -> {file_path.name}")
    
    conn.close()
    print("🎉 کامل شد!")

if __name__ == "__main__":
    extract_photos()
