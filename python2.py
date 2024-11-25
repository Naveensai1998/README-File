import pandas as pd
import sqlite3

# Extract data
def extract_data(file_path, password):
    df = pd.read_csv(file_path)  # Assuming decryption is already handled
    return df

# Transform data
def transform_data(df, region):
    df['region'] = region
    df['total_sales'] = df['QuantityOrdered'] * df['ItemPrice']
    df['net_sale'] = df['total_sales'] - df['PromotionDiscount']
    df = df[df['net_sale'] > 0]
    df = df.drop_duplicates(subset=['OrderId'])
    return df

# Load data
def load_data_to_db(df, db_name):
    conn = sqlite3.connect(db_name)
    df.to_sql('sales_data', conn, if_exists='replace', index=False)
    conn.close()

# Validation queries
def validate_data(db_name):
    conn = sqlite3.connect(db_name)
    queries = {
        "total_records": "SELECT COUNT(*) FROM sales_data",
        "sales_by_region": "SELECT region, SUM(total_sales) FROM sales_data GROUP BY region",
        "avg_sales_per_transaction": "SELECT AVG(net_sale) FROM sales_data",
        "duplicate_order_ids": "SELECT OrderId, COUNT(*) FROM sales_data GROUP BY OrderId HAVING COUNT(*) > 1"
    }
    results = {key: pd.read_sql_query(query, conn) for key, query in queries.items()}
    conn.close()
    return results

# Main function
if __name__ == "__main__":
    file_a = "order_region_a.csv"
    file_b = "order_region_b.csv"
    db_name = "sales_data.db"
    
    data_a = extract_data(file_a, "order_region_a")
    data_b = extract_data(file_b, "order_region_b")
    
    transformed_a = transform_data(data_a, "A")
    transformed_b = transform_data(data_b, "B")
    
    combined_data = pd.concat([transformed_a, transformed_b], ignore_index=True)
    load_data_to_db(combined_data, db_name)
    
    results = validate_data(db_name)
    for key, result in results.items():
        print(f"{key}:\n{result}\n")
