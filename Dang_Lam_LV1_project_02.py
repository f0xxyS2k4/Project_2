import aiohttp
import asyncio
import json
import os
import time
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

# --- config ---
INPUT_FILE = '/home/cahara/Downloads/products-0-200000.csv'

OUTPUT_DIR = 'tiki_data'  # Thư mục lưu data
ERROR_FILE = 'tiki_errors.csv'  # Tên file lưu lỗi
BATCH_SIZE = 1000  # Số lượng ID/file json
CONCURRENT_LIMIT = 20  # Số luồng chạy song song
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://tiki.vn/'
}

# Tạo thư mục output
os.makedirs(OUTPUT_DIR, exist_ok=True)


def clean_description(html_content):
    """Chuẩn hoá description."""
    if not html_content:
        return ""
    try:
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator=" ")
        return " ".join(text.split())
    except Exception:
        return str(html_content)


async def fetch_product(session, product_id):
    """Tải thông tin 1 sản phẩm."""
    url = f"https://api.tiki.vn/product-detail/api/v1/products/{product_id}"
    try:
        async with session.get(url, headers=HEADERS, timeout=20) as response:
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


def append_errors_to_csv(error_list):
    """Ghi nối tiếp danh sách lỗi vào file CSV duy nhất."""
    file_exists = os.path.isfile(ERROR_FILE)

    with open(ERROR_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Nếu file chưa tồn tại thì ghi dòng tiêu đề trước
        if not file_exists:
            writer.writerow(['id', 'timestamp', 'reason'])

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for err in error_list:
            writer.writerow([err['id'], now, err['reason']])


async def process_batch(session, batch_ids, batch_index, semaphore):
    """Xử lý 1 lô, lưu thành công ra JSON riêng, lỗi ghi vào CSV chung."""
    tasks = []
    for pid in batch_ids:
        async def sem_task(pid):
            async with semaphore:
                return await fetch_product(session, pid)

        tasks.append(sem_task(pid))

    results = await asyncio.gather(*tasks)

    success_items = [r["data"] for r in results if r["status"] == "success"]
    error_items = [r for r in results if r["status"] == "error"]

    # 1. Lưu file thành công (nếu có)
    if success_items:
        success_file = f"{OUTPUT_DIR}/batch_{batch_index}.json"
        with open(success_file, 'w', encoding='utf-8') as f:
            json.dump(success_items, f, ensure_ascii=False, indent=4)

    # 2. Ghi lỗi vào file chung (nếu có)
    if error_items:
        append_errors_to_csv(error_items)
        print(f"  [!] Batch {batch_index}: Có {len(error_items)} lỗi -> Đã ghi vào {ERROR_FILE}")

    print(f"--> Batch {batch_index}: Xong {len(success_items)} sản phẩm.")


async def main():
    # --- ĐỌC FILE CSV ---
    print(f"Đang đọc file: {INPUT_FILE} ...")
    try:
        df = pd.read_csv(INPUT_FILE)
        # Lấy cột đầu tiên làm ID
        all_ids = df.iloc[:, 0].astype(str).tolist()
        # Lọc ID chỉ chứa số
        all_ids = [pid for pid in all_ids if pid.isdigit()]
    except Exception as e:
        print(f"LỖI ĐỌC FILE: {e}")
        print("Hãy kiểm tra lại đường dẫn file INPUT_FILE trong code.")
        return

    total_products = len(all_ids)
    print(f"Tổng số ID tìm thấy: {total_products}")

    # Xoá file lỗi cũ nếu muốn chạy lại từ đầu (để tránh ghi đè lẫn lộn)
    if os.path.exists(ERROR_FILE):
        print(f"Lưu ý: File {ERROR_FILE} cũ sẽ được ghi nối tiếp.")

    chunks = [all_ids[i:i + BATCH_SIZE] for i in range(0, total_products, BATCH_SIZE)]
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

    async with aiohttp.ClientSession() as session:
        start_time = time.time()
        for index, batch_ids in enumerate(chunks):
            print(f"Đang chạy Batch {index + 1}/{len(chunks)}...")
            await process_batch(session, batch_ids, index + 1, semaphore)
            await asyncio.sleep(1)  # Nghỉ nhẹ

        print(f"\nHOÀN THÀNH! Tổng thời gian: {time.time() - start_time:.2f}s")


if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())