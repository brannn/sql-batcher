"""Example of using the plugin system with sql-batcher.

This example demonstrates how to use plugins to extend sql-batcher functionality.
"""

import asyncio
import logging
from typing import List

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.hooks import MetricsCollector, QueryLogger, SQLPreprocessor


async def main() -> None:
    """Run the plugin example."""
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create PostgreSQL adapter
    adapter = AsyncPostgreSQLAdapter(
        dsn="postgresql://postgres:postgres@localhost:5432/testdb",
        min_size=5,
        max_size=10,
    )

    try:
        # Connect to database
        await adapter.connect()
        logger.info("Connected to database")

        # Create batcher
        batcher = AsyncSQLBatcher(
            adapter=adapter,
            max_bytes=1000,
            auto_adjust_for_columns=True,
        )

        # Register plugins
        # 1. SQL Preprocessor - Add schema prefix to table names
        def preprocess_sql(sql: str) -> str:
            return sql.replace("test_table", "public.test_table")

        batcher.register_plugin(SQLPreprocessor(preprocess_sql))

        # 2. Metrics Collector - Track execution metrics
        metrics_collector = MetricsCollector()
        batcher.register_plugin(metrics_collector)

        # 3. Query Logger - Log SQL queries
        batcher.register_plugin(QueryLogger(logger.info))

        # Generate some test data
        statements = [
            "INSERT INTO test_table (id, name) VALUES (1, 'test1')",
            "INSERT INTO test_table (id, name) VALUES (2, 'test2')",
            "INSERT INTO test_table (id, name) VALUES (3, 'test3')",
        ]

        # Process statements
        async def execute_sql(sql: str) -> None:
            logger.info(f"Executing: {sql}")
            # In a real scenario, this would execute the SQL
            await asyncio.sleep(0.1)  # Simulate database operation

        processed = await batcher.process_statements(statements, execute_sql)
        logger.info(f"Processed {processed} statements")

        # Get metrics
        metrics = metrics_collector.get_metrics()
        logger.info("Metrics:")
        for key, value in metrics.items():
            logger.info(f"  {key}: {value}")

        # Unregister a plugin
        batcher.unregister_plugin("query_logger")
        logger.info("Unregistered query logger plugin")

        # Process more statements without logging
        statements = [
            "INSERT INTO test_table (id, name) VALUES (4, 'test4')",
            "INSERT INTO test_table (id, name) VALUES (5, 'test5')",
        ]

        processed = await batcher.process_statements(statements, execute_sql)
        logger.info(f"Processed {processed} more statements")

        # Get updated metrics
        metrics = metrics_collector.get_metrics()
        logger.info("Updated metrics:")
        for key, value in metrics.items():
            logger.info(f"  {key}: {value}")

    finally:
        # Disconnect from database
        await adapter.disconnect()
        logger.info("Disconnected from database")


if __name__ == "__main__":
    asyncio.run(main())
