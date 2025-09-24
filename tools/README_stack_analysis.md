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

The script uses a sophisticated two-tier filtering approach to accurately distinguish between interesting and mundane stacks:

### 1. Contextual Patterns (Advanced)

These patterns require **BOTH** low-level wait primitives AND high-level idle context to avoid false positives. This prevents marking interesting stacks as mundane just because they happen to be waiting on a mutex or condition variable.

**Example**: A stack with `pthread_mutex_lock` is only considered mundane if it also contains `CPooledThreadWrapper::run` or similar thread pool context.

#### Default Contextual Patterns

1. **Thread Pool Futex Waits**: 
   - Low-level: `__futex_abstimed_wait*`, `do_futex_wait`, `sem_wait*`
   - High-level: `CPooledThreadWrapper::run`, `ThreadPool*::wait`
   
2. **Thread Pool Mutex Waits**:
   - Low-level: `__lll_lock_wait`, `pthread_mutex_lock*`
   - High-level: `CPooledThreadWrapper::run`, `Waiter::wait`
   
3. **Roxie Worker Idle**:
   - Low-level: `pthread_cond_wait*`, `InterruptableSemaphore::wait`
   - High-level: `RoxieQueue::wait`, `CRoxieWorker*threadmain`
   
4. **Roxie Cache Waiting**:
   - Low-level: `pthread_cond_wait`, `InterruptableSemaphore::wait`
   - High-level: `RoxieFileCache`

### 2. Simple Patterns (Basic)

These patterns indicate idle state regardless of context:

1. **Thread Management**: `start_thread`, `clone*`, `Thread::_threadmain` - Always idle
2. **I/O Waits**: `epoll_wait`, `poll`, `select` - Always waiting
3. **Performance Tracing**: `PerfTracer` - Always diagnostic

### Why This Approach Works

Consider these examples:

```bash
# MUNDANE: Thread pool worker waiting on mutex
#0  __lll_lock_wait (futex=0x...) at lowlevellock.c:52
#1  pthread_mutex_lock (mutex=0x...) at pthread_mutex_lock.c:81  
#2  Waiter::wait() from libjlib.so
#3  CPooledThreadWrapper::run() from libjlib.so  ← Idle context

# INTERESTING: Important processing waiting on mutex  
#0  __lll_lock_wait (futex=0x...) at lowlevellock.c:52
#1  pthread_mutex_lock (mutex=0x...) at pthread_mutex_lock.c:81
#2  acquireLock() from libecl.so
#3  processImportantData() from libecl.so  ← Active work context
```

The first stack matches the contextual pattern (mutex wait + thread pool context) and is filtered as mundane. The second has the same low-level wait but lacks the idle context, so it's preserved as interesting.

### Custom Filters

You can extend the filtering with custom patterns using both simple and contextual approaches:

```json
{
  "idle_patterns": [
    {
      "name": "custom_simple_wait",
      "description": "Simple pattern - any function match indicates idle",
      "type": "simple",
      "patterns": [
        "MyCustomWait",
        "CustomIOWait"
      ]
    },
    {
      "name": "custom_contextual_wait", 
      "description": "Contextual pattern - requires both low-level wait AND idle context",
      "type": "contextual",
      "low_level_patterns": [
        "pthread_mutex_lock",
        "__lll_lock_wait"
      ],
      "high_level_patterns": [
        "MyApplicationWorker::wait",
        "MyThreadPool::run" 
      ]
    }
  ]
}
```

- **Simple patterns**: Match any function name anywhere in the stack
- **Contextual patterns**: Require low-level wait primitives (top ~4 frames) AND high-level idle context (anywhere in stack)
- All patterns are regular expressions

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