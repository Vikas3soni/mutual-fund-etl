# file for daily/incremental data load of data
# lookup with bq table to get last upload date (max date) and refer delta records to uplaod
from pyspark.sql import SparkSession, column
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType
from pyspark.sql.functions import *
from utils import upload_blob_gcs, read_file_blob, write_to_bq ,get_data
from fund_extract import extract_data
import datetime

#setup spark session
spark = SparkSession.builder\
        .master("local")\
        .appName("db_transform")\
        .config('spark.ui.port', '4050')\
        .getOrCreate()

# defining schema for table
c_schema = StructType([
            StructField("Scheme Code", StringType(), True),
            StructField("Scheme Name", StringType(), True),
            StructField("ISIN Div Payout", StringType(), True),
            StructField("ISIN Div Reinvestment", StringType(), False),
            StructField("Net Asset Value", StringType(), False),
            StructField("Repurchase Price", StringType(), False),
            StructField("Sale Price", StringType(), False),
            StructField("Date", StringType(), True)
            ])

# get latest date upload and max date from bq table for incremental load
CONTROL_TABLE = "mf_dataset.control_table"
query_string = f""" SELECT
                        max(upload_date),
                        FROM `{CONTROL_TABLE}`
                        LIMIT 1"""
lst_date = get_data(query_string,"my-id")
if lst_date:
    start_date = lst_date
    end_date = datetime.today()
mutual_find_type ="53"
url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf={2}&frmdt={0}&todt={1}"\
    .format(start_date, end_date,mutual_find_type)
data = extract_data()

#reading the spark-data
df = spark.read.format("csv").option("header", "true")\
            .option("delimiter", ";")\
            .schema(c_schema)\
            .load("/content/spark_input.txt")

#remove null un-necessary lines
df = df.filter(df['Scheme Name'].isNotNull())

#update date datatype
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
df = df.withColumn("date",to_date(col('Date'),'dd-MMM-yyyy'))


# write to bq table
bq_table_id = "mf-dataset.mf_fund_53_table"
write_to_bq(df,bq_table_id)

