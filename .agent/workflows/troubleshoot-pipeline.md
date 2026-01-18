---
description: Quy trình xử lý lỗi và khắc phục sự cố Pipeline (Troubleshooting)
---

Khi nhận được thông báo lỗi từ Airflow hoặc Spark, hãy thực hiện theo SOP này:

### Bước 1: Thu thập thông tin Log
- Kiểm tra Airflow Task Logs để tìm `Traceback` hoặc lỗi `Java/Python`.
- Chạy lệnh sau để xem log của container Spark nếu cần:
  ```bash
  docker logs spark-master
  ```

### Bước 2: Phân tích nguyên nhân
- **Dữ liệu nguồn**: Kiểm tra bảng Bronze/Raw có bị thiếu cột hoặc sai định dạng không?
- **Kết nối**: Kiểm tra PostgreSQL/MinIO có đang online không thông qua `make ps`.
- **Logic**: Kiểm tra logic biến đổi trong code Spark thông qua smoke test.

### Bước 3: Khắc phục và Thử lại
1. Sửa lỗi trong code hoặc cấu hình.
2. Chạy thử thủ công bằng Makefile (VD: `make spark-students`).
3. Nếu thành công, tiến hành `Clear` task trên Airflow UI để chạy lại.

// turbo
### Lệnh kiểm tra nhanh hệ thống:
```bash
make ps
docker-compose logs --tail=20 airflow-scheduler
```

### Bước 4: Tự động hóa phòng ngừa
- Cập nhật thêm validation rule vào code Silver layer nếu lỗi do dữ liệu xấu gây ra.
- Thêm dbt test nếu lỗi liên quan đến tính đúng đắn của logic Gold layer.
