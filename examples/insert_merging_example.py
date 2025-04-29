#!/usr/bin/env python3
"""
Insert Merging Example for SQL Batcher.

This example demonstrates how to use SQL Batcher's insert merging feature
to reduce the number of database calls when performing bulk inserts.

Requirements:
- PostgreSQL database
- psycopg2-binary package: pip install psycopg2-binary
- sql-batcher: pip install sql-batcher
"""

import os
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import psycopg2
from psycopg2.extensions import connection as PgConnection

from sql_batcher import SQLBatcher


def get_connection_params() -> Dict[str, str]:
    """
    Get PostgreSQL connection parameters from environment or defaults.

    Returns:
        Dictionary of connection parameters
    """
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "database": os.environ.get("PGDATABASE", "postgres"),
    }


def setup_database(conn: PgConnection) -> None:
    """
    Set up test tables for the example.

    Args:
        conn: PostgreSQL connection object
    """
    with conn.cursor() as cursor:
        # Drop tables if they exist
        cursor.execute("DROP TABLE IF EXISTS order_items")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS products")
        cursor.execute("DROP TABLE IF EXISTS categories")
        cursor.execute("DROP TABLE IF EXISTS customers")

        # Create tables
        cursor.execute(
            """
            CREATE TABLE categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                description TEXT,
                price NUMERIC(10, 2) NOT NULL,
                category_id INTEGER REFERENCES categories(id),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE orders (
                id SERIAL PRIMARY KEY,
                customer_id INTEGER REFERENCES customers(id),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'pending',
                order_reference VARCHAR(36) NOT NULL UNIQUE
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE order_items (
                id SERIAL PRIMARY KEY,
                order_id INTEGER REFERENCES orders(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER NOT NULL,
                price_at_order NUMERIC(10, 2) NOT NULL
            )
            """
        )

        conn.commit()


def generate_product_data(num_products: int) -> List[Tuple]:
    """
    Generate sample product data.

    Args:
        num_products: Number of products to generate

    Returns:
        List of product data tuples
    """
    categories = [
        (1, "Electronics", "Electronic devices and gadgets"),
        (2, "Clothing", "Apparel and accessories"),
        (3, "Home & Garden", "Products for the home and garden"),
        (4, "Books", "Books and publications"),
        (5, "Toys", "Toys and games"),
    ]

    product_templates = [
        ("Laptop {}", "High-performance laptop with SSD", 899.99, 1),
        ("Smartphone {}", "Latest smartphone with advanced camera", 699.99, 1),
        ("T-shirt {}", "Comfortable cotton t-shirt", 19.99, 2),
        ("Jeans {}", "Classic denim jeans", 49.99, 2),
        ("Sofa {}", "Comfortable 3-seater sofa", 549.99, 3),
        ("Plant {}", "Indoor ornamental plant", 24.99, 3),
        ("Novel {}", "Bestselling fiction novel", 14.99, 4),
        ("Cookbook {}", "Collection of gourmet recipes", 29.99, 4),
        ("Action Figure {}", "Collectable action figure", 12.99, 5),
        ("Board Game {}", "Family board game", 34.99, 5),
    ]

    # Generate category INSERT statements
    cat_inserts = [f"INSERT INTO categories (id, name, description) VALUES {cat}" for cat in categories]

    # Generate product INSERT statements
    prod_data = []
    for i in range(1, num_products + 1):
        template_idx = (i - 1) % len(product_templates)
        name_template, desc, base_price, cat_id = product_templates[template_idx]

        # Add some variation
        price_variation = random.uniform(0.8, 1.2)
        price = round(base_price * price_variation, 2)

        name = name_template.format(f"Model {i}")
        prod_data.append((i, name, desc, price, cat_id))

    return cat_inserts + [f"INSERT INTO products (id, name, description, price, category_id) VALUES {p}" for p in prod_data]


def generate_order_data(num_orders: int, max_product_id: int) -> List[str]:
    """
    Generate sample order INSERT statements.

    Args:
        num_orders: Number of orders to generate
        max_product_id: Maximum product ID to reference

    Returns:
        List of SQL INSERT statements for orders
    """
    # Generate customer data
    customers = [(i, f"Customer {i}", f"customer{i}@example.com") for i in range(1, 21)]

    customer_inserts = [f"INSERT INTO customers (id, name, email) VALUES {c}" for c in customers]

    # Generate order data
    order_inserts = []
    order_item_inserts = []

    for order_id in range(1, num_orders + 1):
        customer_id = random.randint(1, len(customers))
        order_date = datetime.now() - timedelta(days=random.randint(0, 30))
        status = random.choice(["pending", "processing", "shipped", "delivered"])
        order_reference = str(uuid.uuid4())

        order_inserts.append(
            f"INSERT INTO orders (id, customer_id, order_date, status, order_reference) "
            f"VALUES ({order_id}, {customer_id}, '{order_date}', '{status}', '{order_reference}')"
        )

        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        for item_idx in range(num_items):
            product_id = random.randint(1, max_product_id)
            quantity = random.randint(1, 3)
            price_at_order = round(random.uniform(10, 1000), 2)

            item_id = len(order_item_inserts) + 1

            order_item_inserts.append(
                f"INSERT INTO order_items (id, order_id, product_id, quantity, price_at_order) "
                f"VALUES ({item_id}, {order_id}, {product_id}, {quantity}, {price_at_order})"
            )

    return customer_inserts + order_inserts + order_item_inserts


def run_example_with_merging() -> None:
    """Run the example with insert merging."""
    conn_params = get_connection_params()

    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False

        # Set up test database
        print("Setting up test database...")
        setup_database(conn)

        # Generate data
        num_products = 50
        num_orders = 30

        print(f"Generating product data ({num_products} products)...")
        product_statements = generate_product_data(num_products)

        print(f"Generating order data ({num_orders} orders)...")
        order_statements = generate_order_data(num_orders, num_products)

        all_statements = product_statements + order_statements
        print(f"Total statements to execute: {len(all_statements)}")

        # Initialize SQL Batcher with insert merging enabled
        batcher = SQLBatcher(merge_inserts=True)

        # Define execute function
        executed_count = 0

        def execute_fn(sql: str) -> None:
            nonlocal executed_count
            executed_count += 1
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

        # Execute statements
        print("\nExecuting statements with insert merging...")
        start_time = time.time()
        batcher.process_statements(all_statements, execute_fn)
        duration = time.time() - start_time

        # Count rows
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM categories")
            cat_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM products")
            prod_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM customers")
            cust_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM orders")
            order_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM order_items")
            item_count = cursor.fetchone()[0]

        print("\nResults with insert merging:")
        print(f"Execution time: {duration:.6f} seconds")
        print(f"Total statements: {len(all_statements)}")
        print(f"Database calls: {executed_count}")
        print(
            f"Rows inserted: {cat_count} categories, {prod_count} products, {cust_count} customers, {order_count} orders, {item_count} order items"
        )
        print(f"Total rows: {cat_count + prod_count + cust_count + order_count + item_count}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "conn" in locals() and conn:
            conn.close()


def run_example_without_merging() -> None:
    """Run the example without insert merging."""
    conn_params = get_connection_params()

    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = False

        # Set up test database
        print("Setting up test database...")
        setup_database(conn)

        # Generate data
        num_products = 50
        num_orders = 30

        print(f"Generating product data ({num_products} products)...")
        product_statements = generate_product_data(num_products)

        print(f"Generating order data ({num_orders} orders)...")
        order_statements = generate_order_data(num_orders, num_products)

        all_statements = product_statements + order_statements
        print(f"Total statements to execute: {len(all_statements)}")

        # Initialize SQL Batcher without insert merging
        batcher = SQLBatcher(merge_inserts=False)

        # Define execute function
        executed_count = 0

        def execute_fn(sql: str) -> None:
            nonlocal executed_count
            executed_count += 1
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

        # Execute statements
        print("\nExecuting statements without insert merging...")
        start_time = time.time()
        batcher.process_statements(all_statements, execute_fn)
        duration = time.time() - start_time

        # Count rows
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM categories")
            cat_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM products")
            prod_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM customers")
            cust_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM orders")
            order_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM order_items")
            item_count = cursor.fetchone()[0]

        print("\nResults without insert merging:")
        print(f"Execution time: {duration:.6f} seconds")
        print(f"Total statements: {len(all_statements)}")
        print(f"Database calls: {executed_count}")
        print(
            f"Rows inserted: {cat_count} categories, {prod_count} products, {cust_count} customers, {order_count} orders, {item_count} order items"
        )
        print(f"Total rows: {cat_count + prod_count + cust_count + order_count + item_count}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "conn" in locals() and conn:
            conn.close()


def main() -> None:
    """Run both examples and compare the results."""
    print("SQL Batcher Insert Merging Example")
    print("==================================")

    print("\nRUNNING EXAMPLE WITHOUT INSERT MERGING")
    print("---------------------------------------")
    run_example_without_merging()

    print("\n\nRUNNING EXAMPLE WITH INSERT MERGING")
    print("-----------------------------------")
    run_example_with_merging()


if __name__ == "__main__":
    main()
