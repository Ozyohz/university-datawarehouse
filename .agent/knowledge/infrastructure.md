# Knowledge: Infrastructure & Networking

Tài liệu này cung cấp cái nhìn kỹ thuật về cách hệ thống được triển khai để AI có thể debug và tối ưu hóa hạ tầng.

## 1. Network Topology (Docker)
Toàn bộ dịch vụ chạy trong mạng nội bộ Docker (thường là `bridge` hoặc `custom network`).
- **Postgres**: Đóng vai trò vừa là metastore cho Airflow, vừa là Database chính cho DW.
- **MinIO**: S3-Compatible storage. Dữ liệu Spark thường được lưu dưới dạng Parquet/Delta tại đây.
- **Spark Master/Worker**: Truy cập MinIO qua giao thức `s3a://`.

## 2. Dòng chảy dữ liệu (Data Ports)
| Service | Internal Port | External Port | Communication |
|---------|---------------|---------------|---------------|
| Postgres| 5432          | 5432          | Metadata & SQL Storage |
| Airflow | 8080          | 8080          | Orchestrator UI |
| MinIO   | 9000/9001     | 9000/9001     | Object Storage (S3 API) |
| Spark   | 7077/8081     | -             | Master-Worker RPC |

## 3. Quản lý Môi trường (Environment Variables)
Mọi cấu hình nhạy cảm được quản lý trong file `.env`. 
Ai cần chú ý các biến:
- `DW_HOST`, `DW_DATABASE`: Cấu hình kết nối DB.
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: Key truy cập MinIO.

## 4. Bảo mật & Sao lưu
- Dữ liệu trong `silver` và `gold` layer được khuyến nghị sao lưu hàng ngày.
- Pipeline Airflow sử dụng `Secrets Management` để tránh lộ mật khẩu trong log.
