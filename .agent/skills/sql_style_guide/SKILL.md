# Skill: SQL & Data Modeling Style Guide

Quy chuẩn này giúp các câu lệnh SQL trở nên nhất quán, dễ đọc và dễ bảo trì.

## 1. Định dạng Code (Formatting)
- **Từ khóa**: LUÔN LUÔN VIẾT HOA (VD: `SELECT`, `FROM`, `WHERE`).
- **Thụt lề**: Sử dụng 4 khoảng trắng (4 spaces).
- **Phẩy (Commas)**: Đặt dấu phẩy ở **đầu dòng** để dễ dàng comment/uncomment các cột.
  ```sql
  SELECT
      student_id
      , full_name
      , email
  FROM silver.stg_students
  ```
- **Aliasing**: Luôn sử dụng từ khóa `AS` khi đặt alias cho cột hoặc bảng.

## 2. Quy chuẩn Đặt tên (Naming)
- **Snake Case**: Luôn sử dụng `snake_case` cho tên bảng và tên cột.
- **Tiền tố bảng**:
  - `stg_`: Bảng staging (Silver).
  - `int_`: Bảng trung gian (Intermediate).
  - `dim_`: Bảng chiều (Gold - Dimension).
  - `fact_`: Bảng sự kiện (Gold - Fact).

## 3. Best Practices
- **CTE (Common Table Expressions)**: Sử dụng CTE thay vì subqueries để logic trở nên rõ ràng.
- **SELECT***: Tuyệt đối không sử dụng `SELECT *` trong code production. Luôn liệt kê rõ tên cột.
- **Joins**: Luôn ghi rõ loại Join (`INNER JOIN`, `LEFT JOIN`) và đi kèm alias bảng.

## 4. dbt Specifics
- Luôn sử dụng hàm `ref()` hoặc `source()` thay vì viết cứng tên cluster/database.
- Mỗi model chỉ nên giải quyết một logic nghiệp vụ cụ thể.
