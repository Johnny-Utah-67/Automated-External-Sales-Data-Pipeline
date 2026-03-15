### Automated First‑Party Sales Data Pipeline (Cloud → Analytics)

🔍 Problem
Business teams rely on timely and trustworthy sales data from external e‑commerce platforms.
Manually downloading reports or relying on ad‑hoc processes leads to:
  *   Delayed insights
  *   Data inconsistencies
  *   High operational risk
  *   Limited scalability across teams
This project solves that problem by fully automating the data flow, from API extraction to analytics‑ready tables.

🎯 Objective
Build a fully automated, weekly data pipeline that:
  *   Pulls first‑party sales data from an external Selling Partner API
  *   Stores raw files in cloud object storage
  *   Ingests, standardizes, and validates data in a cloud data warehouse
  *   Makes the data immediately available for centralized analytics (BI, reporting, monitoring)
✅ No manual steps
✅ No duplicate loads
✅ Designed for multiple business users

🧠 High‑Level Solution
  1. API Extraction
      A Python service securely authenticates to an external API and requests weekly sales reports.
  2. Cloud Storage Landing Zone
      Raw data files are stored in cloud object storage for traceability and reprocessing.
  3. Automated Warehouse Ingestion
      A scheduled warehouse task:
      *  Detects new files
      *  Loads only unprocessed data
      *  Applies schema standardization
      *  Persists data into analytics‑ready tables
  4. Operational Monitoring
      System task execution and failures are tracked and exposed to a BI tool for visibility.

🏗 Architecture Overview
            External API
                 ↓
            Python Automation (Cloud VM / Data Factory)
                 ↓
            Cloud Object Storage (Raw Files)
                 ↓
            Cloud Data Warehouse (Tasks + Procedures)
                 ↓
            Standardized Analytics Tables
                 ↓
            BI Monitoring & Reporting

📈 Business Impact
⏱ Eliminates manual reporting effort
✅ Improves data reliability and trust
🔁 Enables repeatable, scalable ingestion
📊 Supports multiple departments from one source of truth
🔍 Provides operational transparency via monitoring dashboards

🧪 Key Design Principles
       *  Idempotent loads (no duplicate data)
       *  File‑level traceability
       *  Separation of raw vs curated data
       *  Fail‑safe automation
       *  Enterprise‑grade scheduling and monitoring

🛠 Skills Demonstrated
       *  Data engineering & automation
       *  API integration & authentication
       *  Cloud data architecture
       *  Data governance & reliability
       *  Analytics platform operations
       *  Cross‑functional enablement

⚙️ Technologies Used
       *  Python
       *  REST APIs
       *  Cloud Object Storage
       *  Cloud Data Warehouse
       *  SQL / Stored Procedures
       *  Scheduled Tasks
       *  BI Monitoring

🔒 Notes on Security & Privacy
       * No real credentials are stored in code
       * Secrets managed via environment variables
       * Project structure follows enterprise security best practices
       * Sample data is excluded or anonymized 

Important to note : All identifiers, credentials, and company‑specific details have been anonymized.
