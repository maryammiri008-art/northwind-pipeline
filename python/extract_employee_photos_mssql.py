#!/usr/bin/env python3
"""
استخراج تصاویر کارمندان از SQL Server (Northwind)
"""

import os
import sys
import pymssql
from pathlib import Path

PROJECT_HOME = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_HOME / "python"))
from common import env

def extract_photos():
    """استخراج تصاویر از SQL Server"""
    
    print("=" * 60)
    print("🔄 اتصال به SQL Server برای استخراج تصاویر...")
    print("=" * 60)
    
    # اتصال به SQL Server
    try:
        conn = pymssql.connect(
            server=env("OP_HOST", "localhost"),
            port=int(env("OP_PORT", "25432")),
            database=env("OP_DB", "northwind"),
            user=env("OP_USER", "admin"),
            password=env("OP_PASSWORD", "admin123"),
        )
        print("✅ اتصال به SQL Server برقرار شد")
    except Exception as e:
        print(f"❌ خطا در اتصال: {e}")
        print("⚠️  ممکن است نیاز به تنظیمات ODBC داشته باشید")
        return
    
    output_dir = PROJECT_HOME / "data_lake" / "employees"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 پوشه خروجی: {output_dir}")
    
    with conn.cursor(as_dict=True) as cur:
        cur.execute("""
            SELECT EmployeeID, FirstName, LastName, Photo
            FROM Employees
            WHERE Photo IS NOT NULL
            ORDER BY EmployeeID
        """)
        
        rows = cur.fetchall()
        if not rows:
            print("❌ هیچ تصویری در دیتابیس پیدا نشد!")
            conn.close()
            return
        
        print(f"✅ {len(rows)} تصویر پیدا شد")
        
        saved_count = 0
        for row in rows:
            employee_id = row['EmployeeID']
            photo_data = row['Photo']
            
            if photo_data is None:
                continue
            
            # حذف هدر 78 بایتی (اگر وجود داشته باشد)
            image_data = photo_data
            
            # حذف هدر OLE (78 بایت اول)
            if len(image_data) > 78:
                # بررسی هدر استاندارد تصویر
                if image_data[:4].hex().lower().startswith('ffd8') or \
                   image_data[:4].hex().lower().startswith('8950') or \
                   image_data[:3].hex().lower().startswith('4749'):
                    # تصویر با هدر استاندارد شروع می‌شود
                    pass
                else:
                    # حذف 78 بایت اول (هدر OLE)
                    image_data = image_data[78:]
            
            # تشخیص فرمت تصویر
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
                ext = '.jpg'  # پیش‌فرض
            
            # ذخیره فایل
            file_path = output_dir / f"{employee_id}{ext}"
            with open(file_path, 'wb') as f:
                f.write(image_data)
            saved_count += 1
            print(f"✅ Employee {employee_id}: ذخیره شد -> {file_path.name} ({len(image_data)} bytes)")
        
    conn.close()
    print("=" * 60)
    print(f"🎉 استخراج کامل شد! {saved_count} تصویر ذخیره شد.")
    print("=" * 60)

if __name__ == "__main__":
    extract_photos()
