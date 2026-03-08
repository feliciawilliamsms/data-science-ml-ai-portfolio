################# Use this line to pull the data in Domino Jobs Using PySpark ####################
""" spark-submit --packages com.oracle.database.jdbc:ojdbc8:21.11.0.0 --conf spark.sql.shuffle.partitions=200 /mnt/code/Benchmarking/pyspark_table_pull.py --which UB --single_csv --out_root /mnt/data/Fee_Recovery_2024/test/
"""
#######################################################################################################
# pyspark_table_pull.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
import argparse, os, time

def spark_session():
    return (
        SparkSession.builder
        .appName("single_table_oracle_pull")
        # tune to your cluster size:
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

def oracle_jdbc_options():
    # ---- Credentials from environment variables (your names) ----
    host        = os.environ["host_archive"]
    service     = os.environ["service_archive"]
    port        = os.environ["port_archive"]
    user        = os.environ["user_archive"]
    password    = os.environ["password_archive"]

    # Oracle thin URL
    url = f"jdbc:oracle:thin:@//{host}:{port}/{service}"

    opts = {
        "url": url,
        "user": user,
        "password": password,
        "driver": "oracle.jdbc.OracleDriver",
        "fetchsize": "10000",
        # ensure dates behave as expected for pushdown predicates
        "sessionInitStatement": "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"
    }
    return opts

def load_query(spark, dbtable_or_query, jdbc_opts, is_query=True, predicates=None):
    reader = spark.read.format("jdbc")
    for k, v in jdbc_opts.items():
        reader = reader.option(k, v)
    if is_query:
        reader = reader.option("dbtable", f"({dbtable_or_query}) T")
    else:
        reader = reader.option("dbtable", dbtable_or_query)
    if predicates:
        reader = reader.option("predicates", predicates)
    return reader.load()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--which", choices=["UB","HCFA"], default="UB",
                        help="Pick which test table/query to run.")
    parser.add_argument("--out_root", default="/mnt/data/Fee_Recovery_2024/test/",
                        help="Base output directory.")
    parser.add_argument("--single_csv", action="store_true",
                        help="If set, also emits a single compressed CSV for quick inspection.")
    args = parser.parse_args()

    spark = spark_session()
    jdbc = oracle_jdbc_options()

    # --- Your two test queries (kept as close to your originals as possible) ---
    ub_claim_query = """
      SELECT
            clm.CLAIM_TYPE,
            clm.CLAIM_TOTAL_CHARGE,
            clm.PROVIDER_TAX_ID,
            clm.STATUS_CODE,
            clm.RIMS_CCODE,
            clm.RIMS_CLAIM_NUMBER,
            clm.ALLOWEDAMOUNT as ALLOWED_AMT,
            clm.SAVINGS,
            clm.MISC_TEXT,
            clm.ORIGINAL_PROVIDER_TAX_ID,
            clm.MPI_NETWORK_CODE,
            clm.LOAD_DATE,
            clm.REASONABLECUSTOMARY,
            clm.EXTERNAL_NETWORK_ID,
            clm.MEDICARE_PRICE,
            clm.TARGET_AMT,
            clm.BENCHMARK_PRICE,
            clm.CLAIM_INDICATOR,
            clm.MPI_TARGET_AMT,
            clm.CLOSURE_CODE,
            clm.MPI_CLAIM_ID,
            clm.PRICEDPRODUCTCODE,
            clm.CLIENT_ADJUSTMENT_CLAIM_NUMBER,
            clm.CLIENT_ADJUSTMENT_FLAG,
            clm.MPI_ADJUSTMENT_FLAG,
            clm.ADJUSTED_BY_MPI_CLAIM_ID,
            clm.BASE_MPI_CLAIM_ID,
            clm.MPI_OUTBOUND_CLAIM_ID,
            clm.CLIENT_SURPRISE_BILL_INDICATOR,
            clm.MPI_SURPRISE_BILL_INDICATOR,
            clm.COVERED_CHARGES,
            clm.DROP_DATE,
            clm.PROVIDER_TAX_ID_RENDERING,
            clm.PROVIDER_STATE_RENDERING,
            clm.PROVIDER_ZIP_CODE_RENDERING
        FROM MPIEFP.EDP_UB_CLAIM clm
        WHERE clm.MPI_CLAIM_ID >= 240100000000000
          AND clm.RIMS_CCODE in ( 'NALC','USNAS',
            'MEDHS CRCHSTR','MEDHS MAYO2','SIS MPCOMP','SIS MPNCN','ETFS NFNX',
            'MAGNA BRKL1','PSE BRW','PSE H2143','PSE H50008','PSE MPCOMP',
            'FCP H1848','FCP H2260','AHNJ HMO',
            'ISM CRAC','ISM GTL','ISM PATP','ISM PPOMPL','ISM RNDLHD','ISM S071000PRM',
            'ISM S433433','ISM S436033','ISM S576000','ISM SGOLD01HD','ISM SPHYNET',
            'ISM SWT0906','ISM TXTABSA','ISM TXTLHD','ISM TXTPBHD','ISMPO WATT',
            '90DB 11142','90DB 219102','WHS','BCIH BYCNU',
            'GEMDB GUWS','GEMDB IPDH','GEMDB MDPS','GEMDB RGLCR',
            'HLNK HDMU','HLNK MU','XGHCW GHCSCWI','XGHCW PHCS',
            'TBGC TBG955','TBGC TRAVEL','TBGCPO TBG269')
    """

    hcfa_claim_query = """
      SELECT
            clm.CLAIM_TYPE,
            clm.CLAIM_TOTAL_CHARGE,
            clm.PROVIDER_TAX_ID,
            clm.STATUS_CODE,
            clm.RIMS_CCODE,
            clm.RIMS_CLAIM_NUMBER,
            clm.ALLOWED_AMT,
            clm.SAVINGS,
            clm.MISC_TEXT,
            clm.ORIGINAL_PROVIDER_TAX_ID,
            clm.MPI_NETWORK_CODE,
            clm.LOAD_DATE,
            clm.REASONABLECUSTOMARY,
            clm.EXTERNAL_NETWORK_ID,
            clm.MEDICARE_PRICE,
            clm.TARGET_AMT,
            clm.BENCHMARK_PRICE,
            clm.CLAIM_INDICATOR,
            clm.MPI_TARGET_AMT,
            clm.CLOSURE_CODE,
            clm.MPI_CLAIM_ID,
            clm.PRICEDPRODUCTCODE,
            clm.CLIENT_ADJUSTMENT_CLAIM_NUMBER,
            clm.CLIENT_ADJUSTMENT_FLAG,
            clm.MPI_ADJUSTMENT_FLAG,
            clm.ADJUSTED_BY_MPI_CLAIM_ID,
            clm.BASE_MPI_CLAIM_ID,
            clm.MPI_OUTBOUND_CLAIM_ID,
            clm.CLIENT_SURPRISE_BILL_INDICATOR,
            clm.MPI_SURPRISE_BILL_INDICATOR,
            clm.COVERED_CHARGES,
            clm.DROP_DATE,
            clm.PROVIDER_TAX_ID_RENDERING,
            clm.PROVIDER_STATE_RENDERING,
            clm.PROVIDER_ZIP_CODE_RENDERING
        FROM MPIEFP.EDP_HCFA_CLAIM clm
        WHERE clm.MPI_CLAIM_ID >= 240100000000000
          AND clm.RIMS_CCODE in ( 'NALC','USNAS',
            'MEDHS CRCHSTR','MEDHS MAYO2','SIS MPCOMP','SIS MPNCN','ETFS NFNX',
            'MAGNA BRKL1','PSE BRW','PSE H2143','PSE H50008','PSE MPCOMP',
            'FCP H1848','FCP H2260','AHNJ HMO',
            'ISM CRAC','ISM GTL','ISM PATP','ISM PPOMPL','ISM RNDLHD','ISM S071000PRM',
            'ISM S433433','ISM S436033','ISM S576000','ISM SGOLD01HD','ISM SPHYNET',
            'ISM SWT0906','ISM TXTABSA','ISM TXTLHD','ISM TXTPBHD','ISMPO WATT',
            '90DB 11142','90DB 219102','WHS','BCIH BYCNU',
            'GEMDB GUWS','GEMDB IPDH','GEMDB MDPS','GEMDB RGLCR',
            'HLNK HDMU','HLNK MU','XGHCW GHCSCWI','XGHCW PHCS',
            'TBGC TBG955','TBGC TRAVEL','TBGCPO TBG269')
    """

    query = ub_claim_query if args.which == "UB" else hcfa_claim_query

    # Load via Spark JDBC (pushdown stays in Oracle)
    df = load_query(spark, query, jdbc, is_query=True)

    # Light cleanup (match your pandas steps)
    # remove '|' in PROVIDERGROUPNAME if it exists in this projection
    if "PROVIDERGROUPNAME" in df.columns:
        df = df.withColumn("PROVIDERGROUPNAME", regexp_replace(col("PROVIDERGROUPNAME"), r"\|", ""))

    # Write outputs
    ts = str(int(time.time()))
    out_base = args.out_root.rstrip("/")
    out_parquet = f"{out_base}/{args.which}_pull_{ts}.parquet"
    (df.write.mode("overwrite").parquet(out_parquet))
    print(f"PARQUET -> {out_parquet}")

    if args.single_csv:
        # (small samples only) emit a single csv for inspection
        sample = df.limit(100000)  # avoid giant single files
        out_csv = f"{out_base}/{args.which}_pull_{ts}.csv.bz2"
        (sample.coalesce(1)
               .write.mode("overwrite")
               .option("header", "true")
               .option("sep", "|")
               .option("compression", "bzip2")
               .csv(out_csv))
        print(f"SAMPLE CSV -> {out_csv}")

    spark.stop()

if __name__ == "__main__":
    main()
