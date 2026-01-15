import aiohttp
import asyncio
import json
import os
import time
import csv
import glob
import socket
import sys
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

# --- CẤU HÌNH ---
INPUT_FILE = '/home/cahara/Downloads/products-0-200000.csv'

# Tự động xác định thư mục project cha
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)  # Lùi ra khỏi folder src

OUTPUT_DIR = os.path.join(project_root, 'data', 'json_result')
ERROR_FILE = os.path.join(project_root, 'data', 'logs', 'tiki_errors.csv')

BATCH_SIZE = 1000
CONCURRENT_LIMIT = 20
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://tiki.vn/'
}

# Tạo thư mục nếu chưa có
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(ERROR_FILE), exist_ok=True)
except:
    pass


# --- 1. HÀM CHECK TIẾN ĐỘ CŨ (RESUME) ---
def get_completed_batch_count():
    files = glob.glob(os.path.join(OUTPUT_DIR, "batch_*.json"))
    if not files:
        return 0
    max_batch = 0
    for f in files:
        try:
            basename = os.path.basename(f)
            num = int(basename.replace('batch_', '').replace('.json', ''))
            if num > max_batch:
                max_batch = num
        except:
            continue
    return max_batch


# --- 2. HÀM XỬ LÝ TEXT ---
def clean_description(html_content):
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ")
        return " ".join(text.split())
    except Exception:
        return str(html_content)


# --- 3. HÀM GỌI API ---
async def fetch_product(session, product_id):
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
    try:
        timeout = aiohttp.ClientTimeout(total=20)
        async with session.get(url, headers=HEADERS, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                extracted_data = {
                    "id": data.get("id"),
                    "name": data.get("name"),
                    "url_key": data.get("url_key"),
                    "price": data.get("price"),
                    "description": clean_description(data.get("description")),
                    "images": [img.get("base_url") for img in data.get("images", []) if img.get("base_url")]
                }
                return {"status": "success", "data": extracted_data}
            else:
                return {"status": "error", "id": product_id, "reason": f"HTTP {response.status}"}
    except asyncio.TimeoutError:
        return {"status": "error", "id": product_id, "reason": "Timeout"}
    except Exception as e:
        return {"status": "error", "id": product_id, "reason": str(e)}


# --- 4. HÀM GHI LỖI ---
def append_errors_to_csv(error_list):
    file_exists = os.path.isfile(ERROR_FILE)
    try:
        with open(ERROR_FILE, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['id', 'timestamp', 'reason'])
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for err in error_list:
                writer.writerow([err['id'], now, err['reason']])
    except Exception as e:
        print(f"Lỗi ghi file log: {e}")


# --- 5. XỬ LÝ BATCH ---
async def process_batch(session, batch_ids, batch_index, semaphore):
    tasks = []
    for pid in batch_ids:
        async def sem_task(pid):
            async with semaphore:
                return await fetch_product(session, pid)

        tasks.append(sem_task(pid))

    results = await asyncio.gather(*tasks)
    success_items = [r["data"] for r in results if r["status"] == "success"]
    error_items = [r for r in results if r["status"] == "error"]

    if success_items:
        success_file = os.path.join(OUTPUT_DIR, f"batch_{batch_index}.json")
        with open(success_file, 'w', encoding='utf-8') as f:
            json.dump(success_items, f, ensure_ascii=False, indent=4)

    if error_items:
        append_errors_to_csv(error_items)

    # In ra terminal (flush=True để bot đọc được ngay)
    print(f"--> Batch {batch_index}: Xong {len(success_items)} sản phẩm. (Lỗi: {len(error_items)})", flush=True)


# --- 6. MAIN ---
async def main():

    print(f"Đang đọc file: {INPUT_FILE} ...", flush=True)
    try:
        df = pd.read_csv(INPUT_FILE)
        all_ids = df.iloc[:, 0].astype(str).tolist()
        all_ids = [pid for pid in all_ids if pid.isdigit()]
    except Exception as e:
        print(f"LỖI ĐỌC FILE: {e}", flush=True)
        sys.exit(1)  # Báo lỗi cho Bot biết

    total_products = len(all_ids)
    print(f"Tổng số ID tìm thấy: {total_products}", flush=True)

    completed_batches = get_completed_batch_count()
    if completed_batches > 0:
        print(f"🔄 PHÁT HIỆN: Đã xong {completed_batches} batch. Tiếp tục từ batch {completed_batches + 1}...",
              flush=True)
    else:
        print("🚀 Chạy mới từ đầu...", flush=True)

    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, total_products, BATCH_SIZE)]

    # FIX IPv4 CHO MÁY ẢO
    connector = aiohttp.TCPConnector(limit=CONCURRENT_LIMIT, family=socket.AF_INET, ssl=False)
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.time()
        for index, batch_ids in enumerate(chunks):
            current_batch_num = index + 1

            # Nếu batch này đã làm rồi thì bỏ qua
            if current_batch_num <= completed_batches:
                continue

            print(f"⏳ Đang chạy Batch {current_batch_num}/{len(chunks)}...", flush=True)
            await process_batch(session, batch_ids, current_batch_num, semaphore)
            await asyncio.sleep(1)

        print(f"\n✅ HOÀN THÀNH! Tổng thời gian: {time.time() - start_time:.2f}s", flush=True)


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Dừng bởi người dùng.", flush=True)
        sys.exit(0)
    except Exception as e:
        # Bắt buộc in ra lỗi và exit(1) để Bot Discord biết là có biến
        print(f"CRASH: {e}", file=sys.stderr)
        sys.exit(1)