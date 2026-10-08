---
title: Book Crypto Data — Product Card
doc_id: CARD-book-crypto-data
doc_type: product-card
verdict: showcase
version: 0.1
date: 2026-10-08
status: ร่างเพื่อพิจารณา
owner: Product owner (Book Dev)
---

# Book Crypto Data — Product Card

## 1. สรุปคุณค่าในหนึ่งประโยค

ให้ข้อมูลราคาและเทรนด์คริปโตที่ตรวจย้อนได้ (landing → Bronze Parquet → DuckDB API) สำหรับ dashboard และ synthesis ภายใน Solo Empire

## 2. ปัญหาและผู้ใช้เป้าหมาย

- ต้องการข้อมูลตลาดที่มี lineage และ replay ได้ แทน CSV ที่เขียนทับไปเรื่อย ๆ
- ผู้ใช้: เจ้าของและระบบภายใน; ถูกใช้โดย `book-opportunity-intelligence` (synthesis) และ `solo-empire-insights` (หน้าเว็บ)

## 3. สิ่งที่ทำได้จริงตอนนี้

- `ingest.py` ดึงจาก CoinGecko (และ endpoint Binance) เก็บ bytes ดิบที่ landing แล้วเขียน Bronze Parquet ก่อน CSV
- `api.py` เป็น HTTP API อ่านอย่างเดียว: `/healthz`, `/v1/metadata`, `/v1/records`, `/v1/records/{id}`, `/v1/history` (port 8101)
- `ohlcv.py` และ `lake.py` ใช้ shared adapter `data_lake.product_adapter`
- test 10 ไฟล์

## 4. หลักฐานสถานะ

- Stack: Python
- Commits: 17 (แรก 2026-08-08 → ล่าสุด 2026-10-01), branch `main`; working tree สะอาด
- Source: 21 ไฟล์ / 4,090 บรรทัด; test 10 ไฟล์
- CI: 1 workflow
- Deploy config: ไม่มี
- Live URL: ไม่พบใน repo

## 5. คุณค่าและจุดต่าง

- เป็น reference implementation ของ lake-first data product ที่ตัวอื่นในตระกูลลอกตาม
- ข้อมูลเองไม่มีจุดต่าง (API ฟรีที่ใครก็ดึงได้) จึงไม่มีมุมรายได้โดยตรง

## 6. ช่องว่างสำคัญ

- ไม่มี deploy config หรือ public endpoint; ใช้งานผ่าน API ในเครื่องเท่านั้น (ค่าเริ่มต้น bind 127.0.0.1)
- ไม่มี scheduler ที่ยืนยันใน repo **[ต้องยืนยัน]**
- ไม่มีผู้ใช้นอกระบบของเจ้าของ

## 7. คำตัดสิน

**showcase** — เป็นหลักฐานงาน data engineering ที่ครบวงจรและมีผู้บริโภคภายใน แต่ข้อมูลเป็นสินค้าโภคภัณฑ์และไม่ใช่ธุรกิจ

จะเป็น `invest` ก็ต่อเมื่อมีผลิตภัณฑ์ปลายทางที่มีผู้ใช้ภายนอกพึ่งข้อมูลนี้

## 8. ขั้นต่อไป

1. คงเป็น reference ของตระกูลและรักษา README ให้ตรงกับโค้ด
2. พิจารณารวม 6 repo ตลาด (crypto/stock/fx/defi/flight/seo) เป็น monorepo เดียวเพื่อลดต้นทุนดูแล

## การอนุมัติ

| บทบาท | สถานะ | วันที่ | ลงนาม |
|---|---|---|---|
| Product owner (Book Dev) | รอพิจารณา | — | ________ |

## แหล่งข้อมูล

- `README.md`
- `src/book_crypto/api.py`
- `src/book_crypto/ingest.py`
- `src/book_crypto/lake.py`
- `tests/`
