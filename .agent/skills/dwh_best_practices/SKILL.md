# Skill: Data Engineering & DWH Best Practices (Senior Level)

Hướng dẫn chuyên sâu về xây dựng và duy trì hệ thống Data Warehouse hiện đại. AI phải tuân thủ nghiêm ngặt các quy chuẩn này để đảm bảo code "production-ready".

## 1. Medallion Architecture Standards
- **Bronze (Raw):** Immutable. Chỉ Append. Metadata là bắt buộc.
- **Silver (Standardized):** 
  - Kiểu dữ liệu phải tường minh (Explicit casting).
  - Khử trùng (Deduplication) dựa trên Business Key và thời gian ingestion.
  - Phải có cột `is_valid` và `validation_errors` để theo dõi chất lượng.
- **Gold (Business):** 
  - Star Schema/Snowflake Schema.
  - Surrogate Keys nên được ưu tiên cho Dimensions.
  - SCD Type 2 cho các thông tin định danh (Tên sinh viên, Trạng thái tốt nghiệp).

## 2. Spark Engineering (The "Clean Code" Way)
- **Modularity:** Sử dụng `common.spark_utils` cho mọi I/O và Cleaning. Không viết lại logic kết nối DB.
- **Error Handling:** 
  - Sử dụng block `try-except-finally` chuẩn hóa.
  - Luôn gọi `spark.stop()` trong khối `finally`.
  - Ghi log rõ ràng về số lượng record đầu vào/đầu ra.
- **Performance:** 
  - Tránh `collect()` trên tập dữ liệu lớn.
  - Sử dụng Broadcast Join cho các bảng tham chiếu nhỏ (dưới 100MB).
  - Tối ưu hóa số lượng partition trước khi ghi vào Database.

## 3. dbt & Data Modeling
- **Layering:** staging -> intermediate -> marts.
- **Quality:** Mọi model phải có ít nhất 2 tests: `not_null` và `unique`.
- **Docs:** Cung cấp tài liệu cho 100% cột trong layer Marts.

## 4. Workflows & Automation
- Tận dụng file trong `.agent/workflows` để thực hiện tác vụ theo SOP.
- Luôn cập nhật `Makefile` khi thêm các module hoặc dịch vụ mới.
- Ưu tiên "Idempotency": Chạy lại pipeline 10 lần kết quả vẫn phải như 1.
