# Memory Core Dump Feature for Thor Slaves

This document describes the new configurable memory monitoring and core dump feature for Thor slave processes.

## Overview

The memory core dump feature allows Thor slave processes to monitor their total memory usage and automatically generate core dumps when memory consumption exceeds a configured threshold. After generating the core dump, the process continues running normally.

## Configuration Options

The following configuration options are available in the Thor configuration:

### memoryCoreDumpEnabled
- **Type**: boolean
- **Default**: false
- **Description**: Enables or disables the memory core dump monitoring feature

### memoryCoreDumpThresholdMB
- **Type**: integer
- **Default**: 0
- **Description**: Memory threshold in megabytes. When the slave process uses more than this amount of memory, a core dump will be generated

### memoryCoreDumpIntervalSecs
- **Type**: integer
- **Default**: 60
- **Description**: Interval in seconds between memory usage checks

## Example Configuration

```yaml
thor:
  daliServers: dali
  memoryCoreDumpEnabled: true
  memoryCoreDumpThresholdMB: 8192    # 8GB threshold
  memoryCoreDumpIntervalSecs: 30     # Check every 30 seconds
  watchdogEnabled: true
  logging:
    detail: 50
```

## How It Works

1. When a Thor slave process starts, it checks if `memoryCoreDumpEnabled` is true and `memoryCoreDumpThresholdMB` is greater than 0
2. If enabled, a background monitoring thread is started
3. The monitoring thread periodically checks the process's resident memory usage using `ProcessInfo::getActiveResidentMemory()`
4. If memory usage exceeds the threshold, the monitor:
   - Forks a child process
   - The child process generates a core dump using `SIGABRT`
   - The parent process waits for the child to complete and continues running
   - Logs the core dump generation event

## Implementation Details

- The feature is implemented in `thorlcr/slave/slavmain.cpp` as the `CMemoryCoreDumpMonitor` class
- Uses the existing ProcessInfo infrastructure from `system/jlib/jdebug.hpp`
- Core dump generation uses fork/signal approach similar to existing HPCC core dump functionality
- The monitoring thread is automatically stopped when the slave process shuts down

## Logging

The feature produces the following log messages:

- `Memory core dump monitor enabled: threshold=X MB, interval=Y seconds` - Feature startup
- `Memory usage (X MB) exceeded threshold (Y MB) - generating core dump` - Threshold breach
- `Memory threshold exceeded - generating core dump using child process PID` - Core dump start
- `Core dump generation completed, continuing operation` - Core dump complete
- `Memory core dump monitor thread stopped` - Feature shutdown

## Safety Considerations

- Core dumps are generated in child processes to avoid terminating the main slave process
- The main process continues operation after core dump generation
- Memory monitoring has minimal performance impact with configurable check intervals
- Feature is disabled by default for backward compatibility