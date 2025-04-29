"""
Comprehensive Trino example for SQL Batcher.

This example demonstrates all the key features of SQL Batcher with Trino:
- Async execution
- Insert merging
- Batch size optimization
- Transaction management
- Savepoints
- Retry mechanism
- Circuit breaker pattern
- Performance monitoring

Requirements:
- Trino server
- aiotrino package: pip install aiotrino
- sql-batcher[trino]: pip install "sql-batcher[trino]"
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List

import asyncio

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter
from sql_batcher.async_query_collector import AsyncListQueryCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("trino_example")


def get_connection_params() -> Dict[str, str]:
    """
    Get Trino connection parameters from environment or defaults.

    Returns:
        Dictionary of connection parameters
    """
    return {
        "host": os.environ.get("TRINO_HOST", "localhost"),
        "port": int(os.environ.get("TRINO_PORT", "8080")),
        "user": os.environ.get("TRINO_USER", "trino"),
        "catalog": os.environ.get("TRINO_CATALOG", "memory"),
        "schema": os.environ.get("TRINO_SCHEMA", "default"),
        "role": os.environ.get("TRINO_ROLE", None),  # Optional Trino role
    }


async def setup_database(adapter: AsyncTrinoAdapter) -> None:
    """
    Set up the example database structure asynchronously.

    Args:
        adapter: Async Trino adapter instance
    """
    logger.info("Setting up database schema...")

    # Create products table
    await adapter.execute(
        """
    CREATE TABLE IF NOT EXISTS products (
        id BIGINT,
        sku VARCHAR,
        name VARCHAR,
        category VARCHAR,
        price DECIMAL(10, 2),
        stock INTEGER,
        attributes JSON,
        created_at TIMESTAMP
    )
    """
    )

    # Create orders table
    await adapter.execute(
        """
    CREATE TABLE IF NOT EXISTS orders (
        id BIGINT,
        order_number VARCHAR,
        customer_name VARCHAR,
        customer_email VARCHAR,
        status VARCHAR,
        total_amount DECIMAL(12, 2),
        order_date TIMESTAMP,
        metadata JSON
    )
    """
    )

    # Create order_items table
    await adapter.execute(
        """
    CREATE TABLE IF NOT EXISTS order_items (
        id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity INTEGER,
        unit_price DECIMAL(10, 2),
        total_price DECIMAL(10, 2)
    )
    """
    )

    logger.info("Database schema created successfully")


def generate_product_data(num_products: int) -> List[str]:
    """
    Generate sample product INSERT statements.

    Args:
        num_products: Number of products to generate

    Returns:
        List of INSERT statements
    """
    logger.info(f"Generating {num_products} sample products...")

    categories = ["Electronics", "Books", "Clothing", "Home", "Food", "Toys"]
    attributes = [
        {"color": "Red", "weight": "1.5kg", "dimensions": "10x15x5cm"},
        {"color": "Blue", "material": "Cotton", "size": "M"},
        {"color": "Black", "connectivity": "Bluetooth", "battery": "10h"},
        {"material": "Wood", "finish": "Matte", "assembly_required": True},
        {"pages": 350, "language": "English", "format": "Hardcover"},
        {"ingredients": ["Sugar", "Flour", "Butter"], "allergens": ["Gluten", "Dairy"]},
    ]

    statements = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        sku = f"{category[0:3].upper()}{i:06d}"
        name = f"{category} Product {i}"
        price = round(random.uniform(9.99, 999.99), 2)
        stock = random.randint(0, 1000)

        # Select attributes relevant to the category
        category_index = categories.index(category) % len(attributes)
        product_attributes = attributes[category_index].copy()

        # Add some random attributes
        if random.random() > 0.5:
            product_attributes["featured"] = random.random() > 0.8
        if random.random() > 0.7:
            product_attributes["rating"] = round(random.uniform(1, 5), 1)

        # Convert to JSON string
        json_attributes = json.dumps(product_attributes)

        # Create timestamp
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Create INSERT statement
        statement = f"""
        INSERT INTO products (
            id, sku, name, category, price, stock, attributes, created_at
        ) VALUES (
            {i}, '{sku}', '{name}', '{category}', {price}, {stock},
            JSON '{json_attributes}', TIMESTAMP '{created_at}'
        )
        """
        statements.append(statement)

    return statements


async def generate_order_data(num_orders: int, max_product_id: int) -> List[str]:
    """
    Generate sample order SQL statements asynchronously.

    Args:
        num_orders: Number of orders to generate
        max_product_id: Maximum product ID (to reference existing products)

    Returns:
        List of SQL INSERT statements for orders
    """
    logger.info(f"Generating {num_orders} sample orders...")

    order_statements = []
    order_item_statements = []
    now = datetime.now()

    for i in range(1, num_orders + 1):
        # Order details
        order_number = f"ORD-{i:06d}"
        customer_name = f"Customer {i}"
        customer_email = f"customer{i}@example.com"
        status = random.choice(["pending", "processing", "shipped", "delivered", "cancelled"])

        # Create order date within the last 30 days
        days_ago = random.randint(0, 30)
        order_date = (now - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")

        # Generate 1-5 items per order
        num_items = random.randint(1, 5)
        total_amount = 0

        # Create order items
        for j in range(1, num_items + 1):
            product_id = random.randint(1, max_product_id)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(9.99, 99.99), 2)
            total_price = round(quantity * unit_price, 2)
            total_amount += total_price

            # Create order item INSERT statement
            item_statement = f"""
            INSERT INTO order_items (
                id, order_id, product_id, quantity, unit_price, total_price
            ) VALUES (
                {(i-1)*10 + j}, {i}, {product_id}, {quantity}, {unit_price}, {total_price}
            )
            """
            order_item_statements.append(item_statement)

        # Order metadata
        metadata = {
            "items_count": num_items,
            "source": random.choice(["web", "mobile", "store", "phone"]),
            "shipping_method": random.choice(["standard", "express", "pickup"]),
            "notes": f"Test order {i}" if random.random() > 0.8 else "",
        }

        # Create order INSERT statement
        order_stmt = f"""
        INSERT INTO orders (
            id, order_number, customer_name, customer_email,
            status, total_amount, order_date, metadata
        ) VALUES (
            {i}, '{order_number}', '{customer_name}', '{customer_email}',
            '{status}', {total_amount:.2f}, TIMESTAMP '{order_date}',
            JSON '{json.dumps(metadata)}'
        )
        """
        order_statements.append(order_stmt)

    # Combine all statements
    all_statements = order_statements + order_item_statements
    return all_statements


async def run_comprehensive_trino_example(products: int, orders: int) -> None:
    """
    Run the comprehensive Trino example with SQL Batcher.

    Args:
        products: Number of products to generate
        orders: Number of orders to generate
    """
    logger.info("SQL Batcher - Comprehensive Trino Example")
    logger.info("=========================================")

    # Get connection parameters
    connection_params = get_connection_params()
    logger.info(f"Connecting to Trino at {connection_params['host']}:{connection_params['port']}...")

    # Create async Trino adapter with retry and circuit breaker
    adapter = AsyncTrinoAdapter(
        host=connection_params["host"],
        port=connection_params["port"],
        user=connection_params["user"],
        catalog=connection_params["catalog"],
        schema=connection_params["schema"],
        role=connection_params["role"],  # Optional Trino role
        max_query_size=600_000,  # 600KB per batch
        retry_attempts=3,
        retry_delay=0.5,
        retry_max_delay=10.0,
        retry_backoff_factor=2.0,
        retry_jitter=True,
        circuit_breaker_enabled=True,
        circuit_breaker_failure_threshold=5,
        circuit_breaker_recovery_timeout=30.0,
    )

    # Create a query collector for monitoring
    query_collector = AsyncListQueryCollector()

    # Connect to database
    await adapter.connect()

    try:
        # Set up database
        await setup_database(adapter)

        # Create SQL batcher with appropriate settings
        batcher = AsyncSQLBatcher(
            adapter=adapter,
            max_bytes=100_000,  # 100KB per batch (Trino has lower limits)
            auto_adjust_for_columns=True,  # Adjust batch size based on column count
            merge_inserts=True,  # Enable insert merging
        )

        # Insert products using batched statements
        if products > 0:
            logger.info(f"\nInserting {products} products using batched statements...")
            start_time = time.time()

            # Generate product INSERT statements
            product_statements = generate_product_data(products)

            # Begin a transaction
            await adapter.begin_transaction()

            try:
                # Create a savepoint before inserting products
                await adapter.create_savepoint("before_products")

                # Process all statements in batches
                # Use async context manager for clean resource management
                async with batcher as b:
                    # Process statements in batches with query collection
                    await b.process_statements(product_statements, adapter.execute, query_collector)

                # Commit the transaction
                await adapter.commit_transaction()

                elapsed = time.time() - start_time
                logger.info(f"Inserted {products} products in {elapsed:.2f} seconds")

                # Display query statistics
                queries = query_collector.get_all()
                logger.info(f"Executed {len(queries)} batches for product insertion")
                total_size = sum(len(q["query"].encode("utf-8")) for q in queries)
                logger.info(f"Total query size: {total_size / 1024:.2f} KB")
                logger.info(f"Average batch size: {total_size / len(queries) / 1024:.2f} KB")

            except Exception as e:
                # Rollback to savepoint on error
                await adapter.rollback_to_savepoint("before_products")
                # Then rollback the entire transaction
                await adapter.rollback_transaction()
                logger.error(f"Error inserting products: {e}")
                raise

        # Reset query collector for orders
        query_collector.clear()

        # Insert orders using batched statements
        if orders > 0:
            logger.info(f"\nInserting {orders} orders using batched statements...")
            start_time = time.time()

            # Begin a transaction
            await adapter.begin_transaction()

            try:
                # Create a savepoint before inserting orders
                await adapter.create_savepoint("before_orders")

                # Generate order INSERT statements
                order_statements = await generate_order_data(orders, products)

                # Process all statements in batches
                # Use async context manager for clean resource management
                async with batcher as b:
                    # Process statements in batches with query collection
                    await b.process_statements(order_statements, adapter.execute, query_collector)

                # Commit the transaction
                await adapter.commit_transaction()

                elapsed = time.time() - start_time
                logger.info(f"Inserted {orders} orders in {elapsed:.2f} seconds")

                # Display query statistics
                queries = query_collector.get_all()
                logger.info(f"Executed {len(queries)} batches for order insertion")
                total_size = sum(len(q["query"].encode("utf-8")) for q in queries)
                logger.info(f"Total query size: {total_size / 1024:.2f} KB")
                logger.info(f"Average batch size: {total_size / len(queries) / 1024:.2f} KB")

            except Exception as e:
                # Rollback to savepoint on error
                await adapter.rollback_to_savepoint("before_orders")
                # Then rollback the entire transaction
                await adapter.rollback_transaction()
                logger.error(f"Error inserting orders: {e}")
                raise

        # Run some example queries to demonstrate Trino-specific features
        logger.info("\nRunning example queries...")

        # Query using JSON functions
        logger.info("\n1. Find products with specific attributes using JSON functions:")
        results = await adapter.execute(
            """
        SELECT id, name, category, JSON_EXTRACT_SCALAR(attributes, '$.color') as color
        FROM products
        WHERE JSON_EXTRACT_SCALAR(attributes, '$.color') = 'Red'
        LIMIT 5
        """
        )

        for row in results:
            logger.info(f"  Product {row[0]}: {row[1]} ({row[2]}) - Color: {row[3]}")

        # Query with aggregation and filtering
        logger.info("\n2. Order statistics by status:")
        results = await adapter.execute(
            """
        SELECT
            status,
            COUNT(*) as order_count,
            SUM(total_amount) as total_sales,
            AVG(total_amount) as avg_order_value
        FROM orders
        GROUP BY status
        ORDER BY total_sales DESC
        """
        )

        for row in results:
            status, count, total, avg = row
            logger.info(f"  {status}: {count} orders, ${total:.2f} total, ${avg:.2f} average")

        # Query with JOIN and JSON extraction
        logger.info("\n3. Top selling products by order source:")
        results = await adapter.execute(
            """
        SELECT
            p.category,
            JSON_EXTRACT_SCALAR(o.metadata, '$.source') as order_source,
            COUNT(*) as order_count
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.id
        JOIN products p ON oi.product_id = p.id
        GROUP BY p.category, JSON_EXTRACT_SCALAR(o.metadata, '$.source')
        ORDER BY order_count DESC
        LIMIT 10
        """
        )

        for row in results:
            category, source, count = row
            logger.info(f"  {category} via {source}: {count} orders")

        # Demonstrate EXPLAIN
        logger.info("\n4. Query execution plan:")
        explain_results = await adapter.explain(
            """
        SELECT
            p.category,
            COUNT(*) as product_count,
            AVG(p.price) as avg_price
        FROM products p
        GROUP BY p.category
        ORDER BY product_count DESC
        """
        )

        logger.info("  Execution plan:")
        for line in explain_results[:5]:  # Show first 5 lines
            logger.info(f"  {line}")
        if len(explain_results) > 5:
            logger.info("  ...")

        # Demonstrate catalog and schema operations
        logger.info("\n5. Available catalogs:")
        catalogs = await adapter.get_catalogs()
        for catalog in catalogs:
            logger.info(f"  {catalog}")

        logger.info("\n6. Available schemas in current catalog:")
        schemas = await adapter.get_schemas()
        for schema in schemas:
            logger.info(f"  {schema}")

        logger.info("\n7. Available tables in current schema:")
        tables = await adapter.get_tables()
        for table in tables:
            logger.info(f"  {table}")

        # Demonstrate retry mechanism
        logger.info("\n8. Demonstrating retry mechanism:")
        try:
            # This query has a syntax error and will fail
            await adapter.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            logger.info(f"  Expected error: {str(e)}")

        # Demonstrate circuit breaker
        logger.info("\n9. Demonstrating circuit breaker:")
        try:
            # Force multiple failures to trigger circuit breaker
            for i in range(6):
                try:
                    await adapter.execute("SELECT * FROM non_existent_table")
                except Exception:
                    logger.info(f"  Failure {i+1} recorded")
        except Exception as e:
            logger.info(f"  Circuit breaker opened: {str(e)}")

    finally:
        # Close the connection
        await adapter.close()
        logger.info("\nExample completed successfully!")


async def main():
    """Run the comprehensive Trino example."""
    import argparse

    parser = argparse.ArgumentParser(description="SQL Batcher Comprehensive Trino Example")
    parser.add_argument(
        "--products",
        type=int,
        default=100,
        help="Number of products to generate (default: 100)",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=50,
        help="Number of orders to generate (default: 50)",
    )

    args = parser.parse_args()
    await run_comprehensive_trino_example(args.products, args.orders)


if __name__ == "__main__":
    asyncio.run(main())
