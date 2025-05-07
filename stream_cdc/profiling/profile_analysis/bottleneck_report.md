## Performance Bottlenecks Report

### Slowest Methods

1. **Dynamodb._ensure_table_exists**
   - Average time: 0.016000s
   - Maximum time: 0.016000s
   - Call count: 1

2. **Coordinator.stop**
   - Average time: 0.014320s
   - Maximum time: 0.200300s
   - Call count: 5174035

3. **MySQLDataSource.disconnect**
   - Average time: 0.000000s
   - Maximum time: 0.023710s
   - Call count: 5153045

### Highest Cumulative Time

1. **app_main**
   - Cumulative time: 305.468655s

2. **tracked_method**
   - Cumulative time: 300.059754s

3. **stop**
   - Cumulative time: 300.004547s

4. **stop_tracking**
   - Cumulative time: 12.491388s

5. **start_tracking**
   - Cumulative time: 10.691903s

### Most Frequently Called Methods

1. **Coordinator.stop**
   - Call count: 5174035
   - Average time: 0.014320s
   - Total estimated time: 74092.181200s

2. **MySQLDataSource.disconnect**
   - Call count: 5153045
   - Average time: 0.000000s
   - Total estimated time: 0.000000s

3. **Dynamodb._ensure_table_exists**
   - Call count: 1
   - Average time: 0.016000s
   - Total estimated time: 0.016000s

