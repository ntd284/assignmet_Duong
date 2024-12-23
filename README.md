# 🚀 Dự Án Cask Insight: Phân Tích Thông Tin Khách Hàng và Đơn Hàng

# 📚 **Tổng Quan Dự Án**  
Tài liệu này mô tả **quy trình tích hợp dữ liệu từ đầu đến cuối** dành cho **Nền tảng Xử lý và Phân tích Dữ liệu Khách hàng**. Hệ thống được thiết kế để tích hợp và xử lý dữ liệu từ nhiều nguồn thông qua các luồng xử lý theo lô (**Batch Processing**) nhằm cung cấp báo cáo và phân tích chi tiết.

---

## 🛠️ **1. Kiến Trúc Quy Trình Tích Hợp**

### 📊 **Sơ Đồ Tích Hợp Dữ Liệu**  
![Quy trình tích hợp dữ liệu](./image/background.png)

### **Quy trình tích hợp bao gồm các bước chính:**
1. **Thu thập dữ liệu:** Dữ liệu khách hàng từ **Snowflake Cloud** và dữ liệu đơn hàng từ **GitHub repository** qua API và HTTP.  
2. **Lưu trữ dữ liệu:** Lưu trữ vào **Azure Blob Storage** và **Azure Data Lake Storage Gen2**.  
3. **Xử lý dữ liệu:** Dữ liệu được làm sạch và chuẩn hóa trên **Databricks**.  
4. **Tải dữ liệu:** Dữ liệu đã xử lý được tải vào **Azure Synapse Analytics**.  
5. **Phân tích dữ liệu:** Dữ liệu được truy vấn và phân tích sâu qua **Apache Superset**.  

---






