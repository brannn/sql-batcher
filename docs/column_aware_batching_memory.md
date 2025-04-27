# Memory Usage Considerations for Column-Aware Batching

This document provides detailed information about memory usage considerations when using SQL Batcher's Column-Aware Batching feature. Understanding these considerations is crucial for optimizing performance and preventing memory-related issues in production environments.

## Batch Size Impact on Memory

### Per-Row Memory Usage
The memory required for each row in a batch depends on several factors:

1. **Column Count**: More columns generally mean more memory per row
2. **Data Types**: Different data types have varying memory footprints:
   - INTEGER: 4 bytes
   - BIGINT: 8 bytes
   - VARCHAR(n): n bytes + 1-4 bytes overhead
   - TEXT: Variable length + 1-4 bytes overhead
   - JSON/JSONB: Variable length + overhead
   - ARRAY: Variable length + overhead

### Batch Size Calculation
The total memory required for a batch can be estimated as:
```
Memory = (Rows × Columns × Avg_Column_Size) + Batch_Overhead
```

Where:
- `Rows`: Number of rows in the batch
- `Columns`: Number of columns per row
- `Avg_Column_Size`: Average size of each column in bytes
- `Batch_Overhead`: Additional memory for batch management

## Memory Optimization Strategies

### Setting Appropriate Parameters

1. **min_adjustment_factor**:
   - Prevents batches from becoming too small
   - Helps maintain efficient memory usage
   - Recommended: 0.5 (50% of reference size)

2. **max_adjustment_factor**:
   - Limits maximum batch size
   - Prevents memory spikes
   - Recommended: 2.0 (200% of reference size)

3. **reference_column_count**:
   - Should be set based on your typical table structure
   - Affects batch size calculations
   - Recommended: Median number of columns in your tables

### Database-Specific Optimizations

#### PostgreSQL
- Use COPY for large batches (more memory efficient)
- Consider using prepared statements for repeated batches
- Monitor shared_buffers usage

#### Snowflake
- Adjust warehouse size based on batch requirements
- Consider using bulk loading for large datasets
- Monitor query memory usage

#### BigQuery
- Use streaming inserts for real-time data
- Consider partitioning for large tables
- Monitor slot usage and memory limits

## Memory Monitoring and Tuning

### Monitoring Tools
1. **System Monitoring**:
   - Memory usage graphs
   - GC activity
   - Swap usage

2. **Database Monitoring**:
   - Query memory usage
   - Connection memory
   - Cache hit rates

3. **Application Monitoring**:
   - Batch processing times
   - Memory allocation patterns
   - Error rates

### Tuning Process
1. **Baseline Measurement**:
   - Measure current memory usage
   - Identify peak usage patterns
   - Document performance metrics

2. **Parameter Adjustment**:
   - Adjust batch size parameters
   - Monitor impact on memory usage
   - Document changes and results

3. **Validation**:
   - Test with production-like data
   - Verify memory usage improvements
   - Ensure stability under load

## Practical Examples

### Example 1: Small Table (5 columns)
```python
# Table with 5 columns, average 50 bytes per row
batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=1_000_000,
    auto_adjust_for_columns=True,
    reference_column_count=10,
)
# Adjustment factor = 10/5 = 2.0
# Effective batch size = 1_000_000 * 2.0 = 2_000_000 bytes
# Estimated rows per batch = 2_000_000 / (5 * 50) = 8,000 rows
```

### Example 2: Large Table (20 columns)
```python
# Table with 20 columns, average 100 bytes per row
batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=1_000_000,
    auto_adjust_for_columns=True,
    reference_column_count=10,
)
# Adjustment factor = 10/20 = 0.5
# Effective batch size = 1_000_000 * 0.5 = 500_000 bytes
# Estimated rows per batch = 500_000 / (20 * 100) = 250 rows
```

### Example 3: Mixed Column Counts
```python
# Tables with varying column counts
batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=1_000_000,
    auto_adjust_for_columns=True,
    reference_column_count=10,
    min_adjustment_factor=0.5,
    max_adjustment_factor=2.0,
)
# Adjustment factor will be clamped between 0.5 and 2.0
# Effective batch size will be between 500,000 and 2,000,000 bytes
```

## Best Practices

1. **Start Conservative**:
   - Begin with smaller batch sizes
   - Monitor memory usage
   - Gradually increase if performance allows

2. **Regular Monitoring**:
   - Set up memory usage alerts
   - Track batch processing times
   - Monitor error rates

3. **Adapt to Workload**:
   - Adjust parameters based on data patterns
   - Consider time-of-day variations
   - Account for seasonal changes

4. **Documentation**:
   - Keep track of parameter changes
   - Document performance improvements
   - Share knowledge with team members

## Troubleshooting

### Common Issues

1. **Memory Spikes**:
   - Symptom: Sudden increase in memory usage
   - Solution: Reduce max_adjustment_factor
   - Prevention: Set appropriate limits

2. **Slow Processing**:
   - Symptom: Long batch processing times
   - Solution: Increase min_adjustment_factor
   - Prevention: Monitor performance metrics

3. **Connection Issues**:
   - Symptom: Database connection timeouts
   - Solution: Reduce batch size
   - Prevention: Set appropriate timeouts

### Diagnostic Steps

1. **Memory Analysis**:
   - Check system memory usage
   - Monitor GC activity
   - Analyze heap dumps if needed

2. **Performance Analysis**:
   - Measure batch processing times
   - Check database performance
   - Monitor network usage

3. **Parameter Review**:
   - Verify current settings
   - Check for recent changes
   - Compare with baseline

## Conclusion

Effective memory management is crucial for successful batch processing. By understanding these considerations and following the best practices outlined in this document, you can optimize your SQL Batcher configuration for both performance and stability.

Remember to:
- Monitor memory usage regularly
- Adjust parameters based on actual usage patterns
- Document changes and their effects
- Share knowledge with your team

For more information, refer to the [main SQL Batcher documentation](../README.md). 