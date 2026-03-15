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

     
