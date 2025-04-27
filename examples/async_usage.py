"""Example of async usage with sql-batcher.

This example demonstrates how to use sql-batcher with async/await syntax.
"""

import asyncio
import logging
from typing import List

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter


async def main() -> None:
    """Run the async example."""
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

        # Generate some test data
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')" for i in range(100)
        ]

        # Process statements
        async def execute_callback(sql: str) -> None:
            logger.info(f"Executing: {sql}")
            # In a real scenario, this would execute the SQL
            await asyncio.sleep(0.1)  # Simulate database operation

        processed = await batcher.process_statements(statements, execute_callback)
        logger.info(f"Processed {processed} statements")

        # Process in batches
        batch_results = await batcher.process_batch(statements[:10])
        logger.info(f"Processed batch of {len(batch_results)} statements")

        # Process in stream
        stream_results = await batcher.process_stream(statements[10:20])
        logger.info(f"Processed stream of {len(stream_results)} statements")

        # Process in chunks
        chunk_results = await batcher.process_chunk(statements[20:30])
        logger.info(f"Processed chunk of {len(chunk_results)} statements")

    finally:
        # Disconnect from database
        await adapter.disconnect()
        logger.info("Disconnected from database")


if __name__ == "__main__":
    asyncio.run(main())
