# 🚀 Dự Án Cask Insight: Phân Tích Thông Tin Khách Hàng và Đơn Hàng

📚 Tổng Quan Dự Án

Dự án này xây dựng quy trình ETL (Extract - Transform - Load) từ đầu đến cuối nhằm thu thập, xử lý và phân tích dữ liệu khách hàng và đơn hàng. Hệ thống sử dụng các dịch vụ dữ liệu như Azure Data Factory, Azure Data Lake Gen2, Databricks, và Azure Synapse Analytics.

![Extract Pipeline](./image/background.png)

## 1. Kiến Trúc Quy Trình Dữ Liệu

📤 **Bước 1: Trích Xuất Dữ Liệu (Extract)**
**Dữ liệu khách hàng:** Truy xuất từ **Snowflake Cloud** và lưu trữ vào **Azure Blob Storage**.
**Dữ liệu đơn hàng:** Lấy từ **GitHub repo** cá nhân qua **HTTP** và lưu dưới dạng CSV vào **Azure Data Lake Storage Gen2**.

🛠️ **Bước 2: Xử Lý Dữ Liệu (Transform)**
Sử dụng Databricks Notebook để làm sạch, chuẩn hóa và biến đổi dữ liệu.
Kết hợp dữ liệu khách hàng và đơn hàng thành bảng dữ liệu tổng hợp.

📥 **Bước 3: Tải Dữ Liệu (Load)**
Dữ liệu đã xử lý được tải vào **Azure Synapse Analytics** để phục vụ mục đích phân tích chuyên sâu.