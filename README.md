[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)

# Proposal: Scalable Data Ecosystem for ETL/ELT

## **Background**

As the company continues to grow rapidly, its operations generate a significant and increasing volume of diverse data, including structured data (relational databases), semi-structured data (JSON, XML), and unstructured data (emails, documents, images). This data arrives at a high velocity, with approximately **10 TB processed daily**, and updates occurring every six hours. The organization’s reliance on this data for business insights, client deliverables, and operational decisions underscores the need for a robust and scalable data ecosystem.

### **Challenges**
1. **Latency Issues**: Business-critical decisions require near-real-time access to data, and delays in processing and transforming this data hinder timely insights.
2. **Scalability Constraints**: Existing systems struggle to efficiently handle the growing volume and complexity of data, resulting in performance bottlenecks.
3. **Fragmented Data Sources**: Data originates from multiple systems and formats, making integration and normalization resource-intensive and error-prone.
4. **Limited Automation**: Manual processes for data ingestion and transformation reduce efficiency and increase the risk of inconsistencies or errors.

To address these challenges, the company requires a robust data ecosystem to manage its complex and rapidly expanding data infrastructure. This solution must ensure scalability, minimize latency, automate data processes, and provide seamless integration of diverse data sources.

---

## **Objective**

This proposal outlines the design and implementation of a **scalable data ecosystem** capable of solving the aforementioned challenges. The objectives include:

1. **Improving Data Latency**: Enable near-real-time access to data by streamlining ingestion and transformation processes.
2. **Scalability**: Build a system capable of dynamically handling increasing data volumes and complex workloads.
3. **Simplifying Data Integration**: Centralize and normalize data from diverse sources, ensuring consistency and reliability.
4. **Automating Processes**: Reduce manual intervention in data ingestion, transformation, and monitoring, enabling a more efficient pipeline.
5. **Empowering Analytics**: Deliver clean, analytics-ready data to support decision-making, reporting, and business intelligence.

---

## **Proposed Architecture**

### **1. Data Ingestion Layer and Staging**
- **Tools**: Airbyte and Snowflake
- **Description**:
  - Airbyte extracts data from diverse sources (e.g., APIs, Google Sheets, relational databases) and loads raw data into staging tables in Snowflake.
  - Snowflake provides a scalable staging environment, storing structured, semi-structured, and unstructured data for further processing.
- **Key Features**:
  - **Addresses Latency**: Incremental updates reduce processing delays, enabling near-real-time availability.
  - **Enhances Scalability**: Prebuilt connectors simplify onboarding new sources, while Snowflake’s scalable storage handles increasing data volumes.
  - **Simplifies Integration**: Centralizes raw data from fragmented sources into a single staging environment.

### **2. Data Transformation Layer**
- **Tools**: dbt (Data Build Tool), Python (with Snowpark), and Kubernetes
- **Description**:
  - For structured data: Use dbt to perform modular SQL-based transformations within Snowflake.
  - For semi-structured and unstructured data: Use Python with Snowpark and Snowflake SQL functions to parse and process data directly in the warehouse.
  - Kubernetes dynamically orchestrates transformation workloads.
- **Key Features**:
  - **Solves Fragmentation**: Normalizes diverse data types into a consistent schema.
  - **Improves Automation**: Automates retries for failed jobs using dbt artifacts and Kubernetes.
  - **Enhances Scalability**: Parallel execution using Kubernetes enables efficient handling of complex data workflows.

### **3. Data Storage Layer**
- **Tools**: Snowflake and Amazon S3
- **Description**:
  - Snowflake serves as the centralized data warehouse for structured and analytics-ready data.
  - Amazon S3 acts as a data lake, storing raw and semi-structured data for backup or future processing.
- **Key Features**:
  - **Scalability**: Snowflake dynamically scales compute and storage to meet workload demands.
  - **Resilience**: S3 provides cost-efficient long-term storage and disaster recovery options.

### **4. Orchestration and Monitoring**
- **Tools**: Kubernetes, Terraform, Prometheus/Grafana
- **Description**:
  - Kubernetes orchestrates data ingestion and transformation processes, enabling fault tolerance and load balancing.
  - Terraform provisions and manages cloud resources, ensuring consistent infrastructure deployments.
  - Prometheus and Grafana monitor pipeline health, track resource utilization, and trigger alerts for failures.
- **Key Features**:
  - **Improves Automation**: Fully automates orchestration and infrastructure provisioning.
  - **Enhances Resilience**: Proactive monitoring ensures system reliability and quick resolution of issues.

---

### **How the Architecture Addresses Challenges**

| **Challenge**               | **Solution in Architecture**                                                                                  |
|-----------------------------|--------------------------------------------------------------------------------------------------------------|
| **Latency Issues**           | Incremental updates with Airbyte and dbt ensure near-real-time access to data, reducing delays in decision-making. |
| **Scalability Constraints**  | Snowflake and Kubernetes dynamically scale resources, handling increasing data volumes and workloads efficiently. |
| **Fragmented Data Sources**  | Centralized staging in Snowflake unifies data from multiple sources, simplifying integration and normalization.     |
| **Limited Automation**       | Kubernetes and Terraform automate ingestion, transformations, and infrastructure provisioning, reducing manual effort. |

---

## Workflow Diagram

![Alt text](/screenshots/etl%20process.png?=5x2 "Example ETL workflow diagram")

```plaintext
Data Sources → Ingestion (Airbyte) → Staging (Snowflake) → Transformation (Python and dbt) 
→ Data Warehouse (Snowflake) → Data Marts → Analytics Tools
```
---

## **Implementation Plan**

### **Phase 1: Infrastructure Setup**
- Provision Kubernetes clusters and Snowflake environments using Terraform.
- Deploy Airbyte connectors for priority data sources.

### **Phase 2: Data Pipeline Development**
- Develop Python scripts for automating data ingestion into Snowflake.
- Build dbt models to cleanse, normalize, and transform raw data.
- Configure dbt threading and Kubernetes pod scaling for optimal performance.

### **Phase 3: Testing and Validation**
- Perform end-to-end testing of ETL/ELT pipelines for accuracy, scalability, and fault tolerance.
- Use dbt’s `run_results.json` and Great Expectations for data quality validation.

### **Phase 4: Deployment**
- Schedule ETL/ELT jobs using Kubernetes CronJobs in Terraform.
- Implement monitoring dashboards with Grafana for real-time observability.

### **Phase 5: Documentation and Handover**
- Provide detailed documentation for pipelines, models, and infrastructure.
- Train users on accessing data and maintaining the ecosystem.

---

## **Benefits**

1. **Efficiency**: Automates ingestion and transformation, reducing manual effort.
2. **Scalability**: Handles growing datasets dynamically with Kubernetes and Snowflake autoscaling.
3. **Data Governance**: Ensures centralized access control and enhances data visibility with dbt.
4. **Resilience**: Provides fault-tolerant pipelines with automatic retries and proactive monitoring.
5. **Future-Proof**: Utilizes modern, cloud-native tools to adapt to evolving business needs.
6. **Cost Efficiency**: ELT minimizes pre-processing costs by leveraging Snowflake for transformations on demand.

---

## **Conclusion**

This data ecosystem combines best-in-class tools like Airbyte, dbt, Snowflake, and Kubernetes to address the challenges of latency, scalability, and data fragmentation. By automating ingestion, transformation, and scaling, the system empowers data-driven decision-making while remaining adaptable to future growth.
___

This project tried to implement the following core tasks
- A “data warehouse” (PostgreSQL): Used to store raw and transformed data during the ETL/ELT process.
- An orchestration service (Airflow): Automates, schedules, and monitors the ETL/ELT workflows.
- An ELT tool (dbt): Performs transformations on data stored in the data warehouse, creating analytics-ready datasets.
- A data ingestion tool (Airbyte): Connects to various data sources and automates the extraction and loading of raw data into the target systems (e.g., PostgreSQL or Snowflake) as part of the ETL/ELT pipeline.

___
<div style="background-color: lightgreen; padding: 10px; border-radius: 5px;">
<strong>Green Box:</strong> ## **Example project**
This [Project](/snowflake/snowflake/README.md) showcases a modern ETL workflow for integrating client engagement data from Google Sheets into Snowflake for analysis. The process begins with data ingestion into PostgreSQL using Python, where mapping tables are created to standardize inconsistencies. dbt transforms the raw data into a clean staging table, which is then transferred to Snowflake using Airbyte. Finally, the data is joined with the `DIM_PROJECT` table to create a comprehensive `customer_engagement_summary` table, ready for actionable insights
</div>

> [!NOTE]
> Highlights information that users should take into account, even when skimming.

> [!TIP]
> Optional information to help a user be more successful.

> [!IMPORTANT]
> Crucial information necessary for users to succeed.

> [!WARNING]
> Critical content demanding immediate user attention due to potential risks.

> [!CAUTION]
> Negative potential consequences of an action.

