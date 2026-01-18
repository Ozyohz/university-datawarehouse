# Knowledge: University DWH Business Logic

Tài liệu này giải thích các khái niệm nghiệp vụ cốt lõi để AI áp dụng chính xác khi viết code biến đổi dữ liệu.

## 1. Trạng thái Sinh viên (Student Status)
- **Active**: Đang theo học chính thức, có đăng ký tín chỉ trong kỳ hiện tại.
- **Suspended**: Tạm dừng học tập (bảo lưu) nhưng vẫn còn trong danh sách quản lý.
- **Graduated**: Đã hoàn thành chương trình và nhận bằng.
- **Dropped**: Thôi học hoặc bị buộc thôi học.

## 2. Logic Học tập & Điểm số
- **Tín chỉ (Credits)**: Đơn vị đo lường khối lượng học tập. Một sinh viên cần tối thiểu 120-140 tín chỉ để tốt nghiệp (tùy ngành).
- **GPA (Grade Point Average)**: 
  - Thang điểm 4.0: A(4), B(3), C(2), D(1), F(0).
  - Công thức: `SUM(GradePoints * Credits) / SUM(TotalCredits)`.

## 3. Quản lý Tài chính (Tuition)
- Học phí được tính dựa trên số tín chỉ đăng ký nhân với đơn giá của ngành học.
- Trạng thái học phí: `Paid`, `Unpaid`, `Partially Paid`.

## 4. Năm học & Học kỳ
- Một năm học có 3 học kỳ: `Semester 1` (Thu), `Semester 2` (Xuân), `Semester 3` (Hè - không bắt buộc).
