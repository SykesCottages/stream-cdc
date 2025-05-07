## Performance Bottlenecks Report

### Slowest Methods

1. **Dynamodb._ensure_table_exists**
   - Average time: 0.014810s
   - Maximum time: 0.014810s
   - Call count: 1

2. **Coordinator.stop**
   - Average time: 0.000030s
   - Maximum time: 44.094840s
   - Call count: 10077693

3. **MySQLDataSource.disconnect**
   - Average time: 0.000000s
   - Maximum time: 0.000000s
   - Call count: 1

### Highest Cumulative Time

1. **app_main**
   - Cumulative time: 281.381363s

2. **tracked_method**
   - Cumulative time: 271.882344s

3. **stop**
   - Cumulative time: 240.632975s

4. **stop_tracking**
   - Cumulative time: 12.102335s

5. **start_tracking**
   - Cumulative time: 9.373247s

### Most Frequently Called Methods

1. **Coordinator.stop**
   - Call count: 10077693
   - Average time: 0.000030s
   - Total estimated time: 302.330790s

2. **Dynamodb._ensure_table_exists**
   - Call count: 1
   - Average time: 0.014810s
   - Total estimated time: 0.014810s

3. **MySQLDataSource.disconnect**
   - Call count: 1
   - Average time: 0.000000s
   - Total estimated time: 0.000000s

