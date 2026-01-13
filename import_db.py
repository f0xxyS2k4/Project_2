import os
import json
import glob
import psycopg2
from psycopg2 import extras
from psycopg2.extras import execute_values  # <-- Dùng hàm chuyên dụng này

# --- CẤU HÌNH DATABASE ---
DB_CONFIG = {
    "dbname": "tiki",
    "user": "cahara",
    "password": "Lam1239032",
    "host": "localhost",
    "port": "5432"
}

DATA_DIR = 'tiki_data'


def create_table_if_not_exists(cursor):
    """Tạo bảng products nếu chưa có."""
    sql = """
    CREATE TABLE IF NOT EXISTS products (
        id VARCHAR(50) PRIMARY KEY,
        name TEXT,
        url_key TEXT,
        price NUMERIC,
        description TEXT,
        images JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(sql)
    print("✅ Đã kiểm tra/tạo bảng 'products'.")


def get_json_files():
    pattern = os.path.join(DATA_DIR, '*.json')
    files = glob.glob(pattern)
    return files


def process_import():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        print("🔌 Đã kết nối thành công đến PostgreSQL!")
    except Exception as e:
        print(f"❌ Lỗi kết nối Database: {e}")
        return

    create_table_if_not_exists(cur)
    conn.commit()

    json_files = get_json_files()
    print(f"📂 Tìm thấy {len(json_files)} file JSON để xử lý.")

    total_inserted = 0

    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

                if not data:
                    continue

                values = []
                for p in data:
                    p_id = str(p.get('id'))
                    p_name = p.get('name')
                    p_url = p.get('url_key')
                    p_price = p.get('price')
                    p_desc = p.get('description')
                    p_images = json.dumps(p.get('images', []))

                    values.append((p_id, p_name, p_url, p_price, p_desc, p_images))

                # --- SỬA LẠI ĐOẠN INSERT TẠI ĐÂY ---

                # Câu lệnh SQL cho execute_values (chỉ dùng 1 %s đại diện cho cả nhóm VALUES)
                query = """
                INSERT INTO products (id, name, url_key, price, description, images)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
                """

                # Dùng execute_values thay vì execute_batch
                # Nó sẽ tự động xử lý các ký tự đặc biệt như % trong mô tả sản phẩm
                execute_values(cur, query, values)
                conn.commit()

                total_inserted += len(values)
                # In ra số lượng để biết tiến độ, dùng \r để ghi đè dòng cũ cho gọn terminal
                print(f" -> Đã nhập file {os.path.basename(file_path)} ({len(values)} SP)", end='\r')

        except Exception as e:
            print(f"\n⚠️ Lỗi khi đọc file {file_path}: {e}")
            conn.rollback()

    cur.close()
    conn.close()
    print("\n" + "-" * 30)
    print(f"🎉 HOÀN TẤT! Tổng cộng đã xử lý {total_inserted} dòng dữ liệu.")


if __name__ == "__main__":
    process_import()