🧩 Multi-Stack Full Script

🐍 Python Script loaded into Visual Studio Code with Azure VM or Azure Data Factory 
(both implemented and tested, automated weekly ADF connection to be retained eventually after testing period)  :

```Python
import requests
import time
from datetime import datetime, timedelta, timezone
import os
import pandas as pd
import gzip
import json
from io import BytesIO, StringIO
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


# === Define Auto Previous Week ===
def get_previous_retail_week():
    today = datetime.now(timezone.utc)

    # Find the most recent Saturday before today
    days_since_saturday = (today.weekday() - 5) % 7
    last_saturday = today - timedelta(days=days_since_saturday)

    # The Sunday before that Saturday
    second_last_sunday = last_saturday - timedelta(days=6)

    # Format as ISO 8601 strings with 'Z' suffix
    start_date_pw = second_last_sunday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat().replace('+00:00', 'Z')
    end_date_pw = last_saturday.replace(hour=23, minute=59, second=59, microsecond=0).isoformat().replace('+00:00', 'Z')

    return start_date_pw, end_date_pw

# === Get dynamic previous week start-end dates ===
start_date_pw, end_date_pw = get_previous_retail_week()

# === Report Parameters ===
marketplace_id = "XXXXXXXXXXXXXX"  # Canada
report_type = "GET_VENDOR_SALES_REPORT"
start_date = start_date_pw    # <-- 2nd last Saturday from today
end_date = end_date_pw        # <-- last Sunday from today


# === Azure Blob Storage settings ===
AZURE_CONTAINER_NAME = "yyyyyyyyy"
AZURE_BLOB_FOLDER = "Canada/XXXXXXX/XXXXX/XXXXXXXX"
AZURE_STORAGE_ACCOUNT = "mdifrimexplrdheunoadlmminh"  # <-- Replace with your storage account name

# === Step 1: Get LWA Access Token ===
def get_access_token():
    load_dotenv(dotenv_path="C:/Python/.venv/.env")
    print("LWA_REFRESH_TOKEN:", os.getenv("LWA_REFRESH_TOKEN"))
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("LWA_REFRESH_TOKEN"),
        "client_id": os.getenv("LWA_CLIENT_ID"),
        "client_secret": os.getenv("LWA_CLIENT_SECRET")
    }
    response = requests.post(token_url, data=payload)
    response.raise_for_status()
    access_token = response.json()["access_token"]
    print("✅ Access token retrieved.")
    return access_token

# === Step 2: Request the Report ===
def request_vendor_sales_report(access_token):
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    payload = {
        "reportType": report_type,
        "marketplaceIds": [marketplace_id],
        "reportOptions": {
            "reportPeriod": "DAY",
            "distributorView": "MANUFACTURING",
            "sellingProgram": "RETAIL"
        },
        "dataStartTime": start_date,
        "dataEndTime": end_date
    }
    response = requests.post(
        "https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports",
        headers=headers,
        json=payload
    )
    response.raise_for_status()
    report_id = response.json()["reportId"]
    print(f"✅ Report requested. Report ID: {report_id}")
    return report_id

# === Step 3: Pull for Report Status ===
def wait_for_report(report_id, access_token):
    headers = {
        "x-amz-access-token": access_token,
        "Accept": "application/json"
    }
    status_url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/reports/{report_id}"
    while True:
        response = requests.get(status_url, headers=headers)
        response.raise_for_status()
        status_data = response.json()
        status = status_data["processingStatus"]
        print(f"⏳ Report status: {status}")
        if status == "DONE":
            return status_data["reportDocumentId"]
        elif status in ["CANCELLED", "FATAL"]:
            print("🔍 Full status response:", status_data)
            raise Exception(f"❌ Report failed with status: {status}")
        time.sleep(30)

# === Step 4: Download, Process, and Upload salesByAsin ===
def download_report(document_id, access_token):
    headers = {
        "x-amz-access-token": access_token,
        "Accept": "application/json"
    }
    doc_url = f"https://sellingpartnerapi-na.amazon.com/reports/2021-06-30/documents/{document_id}"
    response = requests.get(doc_url, headers=headers)
    response.raise_for_status()
    download_url = response.json()["url"]
    print("📥 Download URL retrieved.")

 # Download the compressed file content into memory
    report_file = requests.get(download_url)
    report_file.raise_for_status()

    # Decompress and process only salesByAsin

    try:
        # Wrap the downloaded bytes in a BytesIO buffer
        compressed_stream = BytesIO(report_file.content)

        # Decompress and read as text
        with gzip.open(compressed_stream, mode='rt', encoding='utf-8') as gz_file:
            raw_json = json.load(gz_file)

        # Extract and flatten salesByAsin
        sales_asin = raw_json.get("salesByAsin", [])
        df_asin = pd.json_normalize(sales_asin)
        output_csv_name = f"AmzDailySalesSkuMfg_{start_date[:10]}_to_{end_date[:10]}.csv"

        # Convert DataFrame to CSV in memory
        csv_buffer = StringIO()
        df_asin.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Upload to Azure Blob Storage using browser authentication
        credential = InteractiveBrowserCredential()
        blob_service_client = BlobServiceClient(
            account_url=f"https://{AZURE_STORAGE_ACCOUNT}.blob.core.windows.net",
            credential=credential
        )
        blob_client = blob_service_client.get_blob_client(
            container=AZURE_CONTAINER_NAME,
            blob=f"{AZURE_BLOB_FOLDER}/{output_csv_name}"
        )
        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
        print(f"✅ Uploaded to Azure Blob: {AZURE_BLOB_FOLDER}/{output_csv_name}")

    except Exception as e:
        print(f"❌ Failed to parse and export salesByAsin: {e}")


# === Main Execution ===
if __name__ == "__main__":
    try:
        token = get_access_token()
        report_id = request_vendor_sales_report(token)
        document_id = wait_for_report(report_id, token)
        download_report(document_id, token)
    except Exception as e:
        print(f"❌ An error occurred: {e}")


Amazon credentials in .env :
# Amazon LWA Credentials
LWA_CLIENT_ID=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
LWA_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
LWA_REFRESH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Snowflake task and procedure code :

Create table first :
CREATE OR REPLACE TABLE NN_EXPL.CA_TRADE.F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG_DISSAS (
  Source_File            STRING,
  Calendar_Date          DATE,
  Amazon_Asin            STRING,
  Customer_Returns       INTEGER,
  Ordered_Units          INTEGER,
  Shipped_Units          INTEGER,
  Shipped_COGS_CAD       FLOAT,
  Shipped_Revenue_CAD    FLOAT,
  Ordered_Revenue_CAD    FLOAT
  )

file format creation for parquet :
   CREATE OR REPLACE FILE FORMAT NN_EXPL.CA_TRADE.FF_PARQUET
  TYPE = PARQUET

Procedure to load weekly new files (SQL + Javascript for loading logic) :
CREATE OR REPLACE PROCEDURE NN_EXPL.CA_TRADE.UPDATE_F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
    // 1. List files in the stage
    var stage_path = '@NN_EXPL.CA_TRADE.STG_CA_TRADE/AMAZON_SP_API/GET_VENDOR_SALES_REPORT/RETAIL_MANUFACTURING/';
    var file_list_sql = `LIST ${stage_path}`;
    var file_list = [];
    var stmt = snowflake.createStatement({sqlText: file_list_sql});
    var rs = stmt.execute();
    while (rs.next()) {
        var full_path = rs.getColumnValue(1);
        var file_name = full_path.split('/').pop();
        if (file_name.startsWith('GET_VENDOR_SALES_REPORT')) {
            file_list.push(file_name);
        }
    }

    // 2. Get already loaded files from the table
    var loaded_files = [];
    var loaded_sql = `SELECT DISTINCT Source_File FROM NN_EXPL.CA_TRADE.F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG_DISSAS`;
    var stmt2 = snowflake.createStatement({sqlText: loaded_sql});
    var rs2 = stmt2.execute();
    while (rs2.next()) {
        loaded_files.push(rs2.getColumnValue(1));
    }

    // 3. Find new files
    var new_files = file_list.filter(f => loaded_files.indexOf(f) === -1);

    if (new_files.length === 0) {
        return 'no new data or conditions not met';
    }

    // 4. For each new file, insert its content
    for (var i = 0; i < new_files.length; i++) {
        var file = new_files[i];
        var insert_sql = `
       COPY INTO NN_EXPL.CA_TRADE.F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG_DISSAS
        FROM (
            SELECT
                SPLIT_PART(METADATA$FILENAME, '/', -1) AS SOURCE_FILE,
                $1:"STARTDATE"::DATE AS CALENDAR_DATE,
                $1:"ASIN"::STRING AS AMAZON_ASIN,
                $1:"CUSTOMERRETURNS"::INTEGER AS CUSTOMER_RETURNS,
                $1:"ORDEREDUNITS"::INTEGER AS ORDERED_UNITS,
                $1:"SHIPPEDUNITS"::INTEGER AS SHIPPED_UNITS,
                $1:"SHIPPEDCOGS_AMOUNTCAD"::FLOAT AS SHIPPED_COGS_CAD,
                $1:"SHIPPEDREVENUE_AMOUNTCAD"::FLOAT AS SHIPPED_REVENUE_CAD,
                $1:"ORDEREDREVENUE_AMOUNTCAD"::FLOAT AS ORDERED_REVENUE_CAD
            FROM    @NN_EXPL.CA_TRADE.STG_CA_TRADE/AMAZON_SP_API/GET_VENDOR_SALES_REPORT/RETAIL_MANUFACTURING/${file}) FILE_FORMAT = (FORMAT_NAME = 'NN_EXPL.CA_TRADE.FF_PARQUET')
        `;
        var insert_stmt = snowflake.createStatement({sqlText: insert_sql});
        insert_stmt.execute();
    }

    return 'Loaded new files: ' + new_files.join(', ');
$$;


Weekly Task :

CREATE OR REPLACE TASK NN_EXPL.CA_TRADE.W_UPDT_F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG
WAREHOUSE=NN_USERS_VW2
SCHEDULE='USING CRON 00 13 * * THU UTC'
COMMENT='call procedure to update api amazon canada daily sales (mfg) \ schedule explained : every thursday at 8AM EST (1PM UTC)'
AS
CALL NN_EXPL.CA_TRADE.UPDATE_F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG()

START THE TASK:
ALTER TASK NN_EXPL.CA_TRADE.W_UPDT_F_CA_AMZ_NES_COFF_ACC_DAILY_SALES_MFG resume


finally creation of a sql script load into Power BI to monitor task failures : 

SELECT 
    NAME,
    SCHEMA_NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    TIMESTAMPDIFF('SECOND', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SECONDS,
    ERROR_CODE,
    ERROR_MESSAGE,
    QUERY_ID
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE SCHEMA_NAME in ('USR_NNJOHNSELO','CA_COMMON','CA_CRM','CA_TRADE')
  AND DATABASE_NAME = 'NN_EXPL'
ORDER BY SCHEDULED_TIME DESC
