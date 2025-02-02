import psycopg2
from faker import Faker
import random

fake = Faker()

# Database connection
DB_NAME = "Retail_Store"
DB_USER = "postgres"
DB_PASSWORD = "12345"
DB_HOST = "localhost"
DB_PORT = "5432"

# Connection with PostgreSQL DB
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Database connection successful.")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        exit()

# Bulk insert function
def bulk_insert(conn, query, data):
    try:
        cur = conn.cursor()
        cur.executemany(query, data)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error executing bulk insert: {e}")
        conn.rollback()

# Insert categories
def insert_categories(conn, num_categories=100):
    categories = [(fake.unique.word().capitalize(), fake.sentence()) for _ in range(num_categories)]
    query = "INSERT INTO category (category_name, category_desc) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    bulk_insert(conn, query, categories)
    print(f"{num_categories} categories inserted successfully.")

# Insert products
def insert_products(conn, num_products=2000):
    cur = conn.cursor()
    cur.execute("SELECT category_id FROM category")
    category_ids = [row[0] for row in cur.fetchall()]
    cur.close()

    products = [(fake.word().capitalize(), fake.company(), random.randint(1, 100), round(random.uniform(10, 500), 2), random.choice(category_ids)) for _ in range(num_products)]
    query = "INSERT INTO products (product_name, product_brand, product_quantity, product_price, product_cat_id) VALUES (%s, %s, %s, %s, %s)"
    bulk_insert(conn, query, products)
    print(f"{num_products} products inserted successfully.")

# Insert vendors
def insert_vendors(conn, num_vendors=500):
    cur = conn.cursor()
    cur.execute("SELECT category_id FROM category")
    category_ids = [row[0] for row in cur.fetchall()]
    cur.close()

    vendors = [(fake.company(), round(random.uniform(50, 1000), 2), random.choice(category_ids)) for _ in range(num_vendors)]
    query = "INSERT INTO vendors (vendor_name, vendor_price, vendor_prod_id) VALUES (%s, %s, %s)"
    bulk_insert(conn, query, vendors)
    print(f"{num_vendors} vendors inserted successfully.")

# Insert customers
def insert_customers(conn, num_customers=2000):
    customers = [(random.choice(["Active", "Inactive"]), fake.first_name(), fake.last_name(), fake.address(), fake.random_number(10), fake.credit_card_number()) for _ in range(num_customers)]
    query = "INSERT INTO customer (cust_status, cust_firstname, cust_lastname, cust_address, cust_contact, cust_payment_details) VALUES (%s, %s, %s, %s, %s, %s)"
    bulk_insert(conn, query, customers)
    print(f"{num_customers} customers inserted successfully.")

# Insert orders
def insert_orders(conn, num_orders=5000):
    cur = conn.cursor()
    cur.execute("SELECT cust_id FROM customer")
    customer_ids = [row[0] for row in cur.fetchall()]
    cur.close()

    orders = [(fake.date(), round(random.uniform(100, 2000), 2), random.choice([True, False]), random.choice(customer_ids)) for _ in range(num_orders)]
    bulk_insert(conn, query, orders)
    print(f"{num_orders} orders inserted successfully.")
    query = "INSERT INTO orders (order_date, order_total_amount, order_status, order_cust_id) VALUES (%s, %s, %s, %s)"

# Insert order items
def insert_order_items(conn, num_order_items=5000):
    cur = conn.cursor()
    cur.execute("SELECT order_id FROM orders")
    order_ids = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT product_id FROM products")
    product_ids = [row[0] for row in cur.fetchall()]
    cur.close()

    order_items = [(random.choice(order_ids), random.choice(product_ids), random.randint(1, 10), round(random.uniform(10, 1000), 2)) for _ in range(num_order_items)]
    query = "INSERT INTO order_items (order_item_order_id, order_item_product_id, order_item_quantity, order_item_price) VALUES (%s, %s, %s, %s)"
    bulk_insert(conn, query, order_items)
    print(f"{num_order_items} order items inserted successfully.")

# Insert reviews
def insert_reviews(conn, num_reviews=5000):
    reviews = [(fake.sentence(), f"{random.randint(1, 10)} minutes", random.randint(1, 2000), fake.word().capitalize()) for _ in range(num_reviews)]
    query = "INSERT INTO reviews (review_desc, review_time_spent, review_customer_id, review_product_name) VALUES (%s, %s, %s, %s)"
    bulk_insert(conn, query, reviews)
    print(f"{num_reviews} reviews inserted successfully.")

# Run main function
def main():
    conn = connect_to_db()
    try:
        insert_categories(conn, 100)
        insert_products(conn, 2000)
        insert_vendors(conn, 500)
        insert_customers(conn, 2000)
        insert_orders(conn, 5000)
        insert_order_items(conn, 5000)
        insert_reviews(conn, 5000)
    finally:
        conn.close()
        print("Database connection closed.")

if __name__ == "__main__":
    main()
