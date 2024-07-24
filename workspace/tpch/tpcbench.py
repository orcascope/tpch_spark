# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse

#import datafusion
#from datafusion import SessionContext
from datetime import datetime
import json
import time

def main(spark, benchmark: str, data_path: str, query_path: str, run_id):

    # Register the tables
    if benchmark == "tpch":
        num_queries = 22
        table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    elif benchmark == "tpcds":
        num_queries = 99
        table_names = ["call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
           "customer_address", "customer_demographics", "date_dim", "time_dim", "household_demographics",
           "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store", "store_returns",
           "store_sales", "warehouse", "web_page", "web_returns", "web_sales", "web_site"]
    else:
        raise "invalid benchmark"

    

    for table in table_names:
        path = f"{data_path}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        spark.read.format("parquet").load(path).createOrReplaceTempView(table)

    results = {
        'engine': 'spark',
        'benchmark': benchmark,
        'data_path': data_path,
        'query_path': query_path,
        'run_id': run_id
    }

    for query in range(1, num_queries + 1):
        # read text file
        path = f"{query_path}/q{query}.sql"
        print(f"Reading query {query} using path {path}")
        with open(path, "r") as f:
            text = f.read()
            # each file can contain multiple queries
            queries = text.split(";")

            start_time = time.time()
            for sql in queries:
                sql = sql.strip()
                if len(sql) > 0:
                    print(f"Executing: {sql}")
                    df = spark.sql(sql)
                    rows = df.collect()

                    print(f"Query {query} returned {len(rows)} rows")
            end_time = time.time()
            print(f"Query {query} took {end_time - start_time} seconds")

            # store timings in list and later add option to run > 1 iterations
            results[query] = [end_time - start_time]

    str = json.dumps(results, indent=4)
    current_time_millis = int(datetime.now().timestamp() * 1000)
    results_path = f"python-{run_id}-{benchmark}-{current_time_millis}.json"
    print(f"Writing results to {results_path}")
    with open(results_path, "w") as f:
        f.write(str)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DataFusion benchmark derived from TPC-H / TPC-DS")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--run_id", required=True, help="Test identifier")
    args = parser.parse_args()
    from pyspark.sql.session import SparkSession
    maven_coords = [
    "org.apache.spark:spark-avro_2.12:3.2.1",
    "io.delta:delta-core_2.12:2.3.0"
    ]
    spark = (SparkSession.builder.appName("MyDelta_App")
        .config("spark.jars.packages", ",".join(maven_coords))  
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config('spark.ui.port', '4040')
        .getOrCreate()
        )
    spark
    
    main(spark, args.benchmark, args.data, args.queries, args.run_id)