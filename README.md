# 🧠 Databricks & Azure Data Engineering Project

## 🚀 Project Overview
This project demonstrates how to build an **end-to-end data engineering pipeline** using **Azure Databricks**, **Delta Live Tables (DLT)**, and **Azure Data Lake** following the **Medallion Architecture** (Bronze → Silver → Gold).  

The pipeline simulates an **e-commerce analytics system**, transforming raw sales, customer, and product data into **business-ready insights** for dashboards and reporting.

---

## 🧩 Architecture Overview
```
           ┌────────────────────────────┐
           │  Raw Data (Azure Data Lake) │
           └──────────────┬──────────────┘
                          │
                    Bronze Layer
                (Raw Ingestion + Cleaning)
                          │
                    Silver Layer
        (Transformations, Joins, Business Logic)
                          │
                    Gold Layer
     (Fact & Dimension Tables, SCD Implementation)
                          │
                  Visualization Layer
           (Power BI / Databricks SQL / Tableau)
```

---

## ⚙️ Technologies Used
| Category | Tools / Technologies |
|-----------|----------------------|
| Cloud Platform | Azure |
| Compute | Azure Databricks |
| Framework | Delta Live Tables (DLT) |
| Language | Python, PySpark |
| Storage | Azure Data Lake Gen2 |
| Orchestration | Databricks Workflows |
| Data Modeling | Medallion Architecture |
| Visualization | Power BI |
| Version Control | Git & GitHub |

---

## 🧱 Project Structure
```
Databricks & Azure Data Engineering Project/
├── notebooks/                     # Databricks Notebooks
├── scripts/                       # ETL / Transformation scripts
├── configs/                       # Environment & connection configs
├── datasets/                      # Sample data (Products, Customers, Sales)
├── utils/                         # Helper functions and reusable modules
└── README.md                      # Project documentation
```

---

## 🔁 Pipeline Flow
1. **Bronze Layer – Raw Data Ingestion**
   - Load raw data from source files (CSV, Parquet, APIs).  
   - Store in Delta format for reliability and schema evolution.  

2. **Silver Layer – Data Transformation**
   - Apply joins, filters, and enrichments using PySpark.  
   - Standardize formats and ensure referential integrity.  

3. **Gold Layer – Business & Analytics**
   - Create **Fact** and **Dimension** tables.  
   - Implement **SCD Type 1 & Type 2** for historical tracking.  
   - Prepare data for BI tools like Power BI and Databricks SQL.  

---

## 📊 Real-World Use Case
This project replicates how large-scale e-commerce platforms manage and analyze data:
- **Sales analytics** (top products, daily revenue, regional trends)
- **Customer behavior tracking**
- **Product performance insights**
- **Automated, version-controlled ETL workflows**

---

## 🔍 Key Features
✅ End-to-end pipeline using **Databricks DLT**  
✅ Handles **batch & streaming** ingestion  
✅ Implements **SCD Type 1 & 2** logic  
✅ Follows **Medallion Architecture** principles  
✅ Supports **Power BI integration**  
✅ Automated data lineage & monitoring  

---

## 🧪 How to Run the Project
1. Clone the repository:
   ```bash
   git clone https://github.com/rahulmandaviya123/databricks_azure_data_engineering_project.git
   ```
2. Import the notebooks and scripts into your Databricks workspace.  
3. Configure the Azure Data Lake connection.  
4. Create a **Delta Live Tables pipeline** in Databricks:
   - Source: repository path  
   - Target: your database name  
   - Storage Location: ADLS Gen2 path  
5. Run the pipeline and visualize lineage & metrics.

---

## 🧰 Future Enhancements
- Add **data quality checks** using DLT expectations  
- Integrate **Databricks Workflows** for orchestration  
- Implement **CI/CD with GitHub Actions**  
- Add **Kafka streaming source** for real-time ingestion  

---

## 👨‍💻 Author
**Rahul Mandaviya**  
📍 Data Engineer | Azure | Databricks | Power BI  
🔗 [LinkedIn](https://www.linkedin.com/in/rahul-mandaviya/)  
💻 [GitHub](https://github.com/rahulmandaviya123)
