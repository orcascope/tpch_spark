import argparse
from datetime import datetime
import json
import time
import duckdb

def main(con, benchmark: str, data_path: str, query_path: str, run_id: str):

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
            # Read Parquet files and register them as tables
        con.execute("CREATE TABLE {table} AS SELECT * FROM read_parquet({path})")

    results = {
        'engine': 'duckdb',
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
                    df = con.execute(sql).fetchall()
                    rows = len(df)

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
    parser = argparse.ArgumentParser(description="TPC-H / TPC-DS")
    parser.add_argument("--benchmark", required=True, help="Benchmark to run (tpch or tpcds)")
    parser.add_argument("--data", required=True, help="Path to data files")
    parser.add_argument("--queries", required=True, help="Path to query files")
    parser.add_argument("--run_id", required=True, help="Test identifier")
    args = parser.parse_args()
    
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    
    main(con, args.benchmark, args.data, args.queries, args.run_id)
