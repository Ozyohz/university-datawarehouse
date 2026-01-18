---
description: Quy trình thêm mới một thực thể dữ liệu vào Silver Layer (Spark)
---

Workflow này giúp bạn và AI phối hợp để đưa dữ liệu từ Bronze (Raw) lên Silver (Standardized).

### Các bước thực hiện:

1. Phân tích schema của bảng nguồn trong Bronze layer.
2. Tạo file Spark job mới trong `spark_jobs/silver/transform_[entity].py`.
   - Sử dụng `common.spark_utils` để đảm bảo code sạch.
   - Áp dụng các rules định dạng (trim, date parse) từ Skill `dwh_best_practices`.
3. Đăng ký task mới vào Airflow DAG (`dags/etl_full_pipeline.py`).
4. Chạy kiểm tra dữ liệu sau khi biến đổi.

// turbo
5. Chạy lệnh smoke test để đảm bảo script không lỗi cú pháp:
```bash
python3 spark_jobs/silver/transform_[entity].py --dry-run
```

6. Cập nhật tài liệu kiến trúc `docs/architecture.md` nếu có thay đổi lớn về flow.
