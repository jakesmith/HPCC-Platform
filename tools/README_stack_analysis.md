# Stack Trace Analysis Tool

The `analyze_stacks.py` script helps developers analyze stack traces to identify interesting ones and filter out mundane (idle) ones. This is particularly useful when debugging multi-threaded applications where many threads may be idle in thread pools or waiting on synchronization primitives.

## Overview

The script parses stack traces from various debugging tools (gdb, eu-stack, etc.) and categorizes them as either:
- **Mundane/Idle**: Stacks showing threads waiting on synchronization primitives, thread pools, or other idle states
- **Interesting**: Stacks showing active work or potentially problematic states

## Usage

```bash
# Analyze stacks from a file
python3 tools/analyze_stacks.py stacktrace.txt

# Show verbose analysis with pattern matches
python3 tools/analyze_stacks.py stacktrace.txt --verbose

# Show filtered out (mundane) stacks instead of interesting ones
python3 tools/analyze_stacks.py stacktrace.txt --show-filtered

# Read from stdin (useful with pipes)
cat stacktrace.txt | python3 tools/analyze_stacks.py -

# Use custom filter configuration
python3 tools/analyze_stacks.py stacktrace.txt --config my_filters.json

# Create sample configuration file
python3 tools/analyze_stacks.py --create-sample-config
```

## Filtering Methodology

The script identifies idle patterns by looking for common function names in stack traces:

### Default Idle Patterns

1. **Futex Waits**: `__futex_abstimed_wait*`, `do_futex_wait`
   - Low-level synchronization primitive waits
   
2. **Semaphore Waits**: `sem_wait*`, `__new_sem_wait*`
   - Semaphore synchronization waits
   
3. **Thread Pool Waits**: `CPooledThreadWrapper::run`, `ThreadPool*::wait`
   - Thread pool workers waiting for tasks
   
4. **Thread Management**: `start_thread`, `clone*`, `Thread::_threadmain`
   - Generic thread startup and management
   
5. **Condition Variables**: `pthread_cond_wait*`
   - Condition variable waits
   
6. **Mutex Locks**: `__lll_lock_wait`, `pthread_mutex_lock*`
   - Mutex locking operations
   
7. **I/O Waits**: `epoll_wait`, `poll`, `select`, `read`, `write`
   - I/O waiting operations

### Custom Filters

You can extend the filtering with custom patterns by creating a JSON configuration file:

```json
{
  "idle_patterns": [
    {
      "name": "custom_wait",
      "description": "Custom waiting pattern",
      "patterns": [
        "MyCustomWait",
        "CustomThreadPool::wait"
      ]
    }
  ]
}
```

Patterns are regular expressions that match against function names in the stack trace.

## Integration with Existing Tools

This script complements the existing HPCC Platform debugging tools:

- **doperf**: Use `analyze_stacks.py` to post-process stack traces collected by doperf
- **eu-stack**: Direct analysis of eu-stack output
- **gdb**: Analysis of gdb backtrace output

## Examples

### Example 1: Basic Analysis

```bash
$ python3 tools/analyze_stacks.py stacks.txt
=== Stack Trace Analysis Summary ===
Total stacks: 10
Mundane/Idle stacks: 7 (70.0%)
Interesting stacks: 3 (30.0%)

=== Interesting Stacks ===

--- Stack 1 ---
#0  malloc () at malloc.c:123
#1  allocateMemory (size=1024) at memory_manager.cpp:56
#2  processQuery (query=0x123) at query_processor.cpp:42
```

### Example 2: Verbose Analysis

```bash
$ python3 tools/analyze_stacks.py stacks.txt --verbose
=== Stack Trace Analysis Summary ===
Total stacks: 10
Mundane/Idle stacks: 7 (70.0%)
Interesting stacks: 3 (30.0%)

=== Idle Pattern Matches ===
  futex_wait: 4
  thread_pool_wait: 2
  semaphore_wait: 1

=== Interesting Stacks ===
...
```

## Tips

1. **Focus on Percentages**: In healthy systems, 70-90% of stacks may be idle. Focus on the interesting ones.

2. **Use with Performance Tools**: Combine with tools like doperf for comprehensive performance analysis.

3. **Custom Patterns**: Add application-specific idle patterns to reduce noise for your specific use case.

4. **Batch Processing**: Process multiple stack files to identify patterns across different time periods or scenarios.