#!/usr/bin/env python3
"""
Stack Trace Analyzer

This script analyzes stack traces to help identify interesting ones and filter out
mundane (idle) stacks. It's designed to work with stack traces from debugging tools
like gdb, eu-stack, or similar.

Usage:
    python3 analyze_stacks.py [options] <input_file>

Options:
    --show-filtered    Show filtered out (mundane) stacks instead of interesting ones
    --verbose         Show detailed analysis information
    --config <file>   Use custom filter configuration file
    --help            Show this help message

The script identifies common idle patterns such as:
- Futex waits and synchronization primitives
- Thread pool worker waits
- Semaphore and mutex waits  
- Generic thread startup/cleanup
"""

import re
import sys
import argparse
import json
from typing import List, Set, Dict, Tuple, Optional
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class StackFrame:
    """Represents a single stack frame"""
    frame_num: int
    address: Optional[str]
    function: str
    args: str
    location: str


@dataclass
class StackTrace:
    """Represents a complete stack trace"""
    frames: List[StackFrame]
    raw_text: str
    
    def __post_init__(self):
        self.signature = self._create_signature()
    
    def _create_signature(self) -> str:
        """Create a signature for this stack trace based on function names"""
        functions = []
        for frame in self.frames:
            # Extract just the function name, clean up C++ mangling indicators
            func = frame.function.strip()
            if func and func != "[unknown]":
                # Remove template parameters and arguments for cleaner signatures
                func = re.sub(r'<[^>]*>', '', func)
                func = re.sub(r'\([^)]*\)', '', func)
                functions.append(func)
        return " -> ".join(functions[:5])  # Use top 5 frames for signature


class StackFilter:
    """Handles filtering logic for stack traces"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.idle_patterns = self._load_default_patterns()
        if config_file:
            self._load_config_file(config_file)
    
    def _load_default_patterns(self) -> List[Dict]:
        """Load default patterns for identifying idle/mundane stacks"""
        return [
            # Contextual patterns - require BOTH low-level wait primitives AND high-level idle context
            {
                "name": "thread_pool_futex_wait",
                "description": "Thread pool workers waiting on futex",
                "type": "contextual",
                "low_level_patterns": [
                    r"__futex_abstimed_wait",
                    r"__GI___futex_abstimed_wait",
                    r"do_futex_wait",
                    r"__new_sem_wait",
                    r"sem_wait",
                ],
                "high_level_patterns": [
                    r"CPooledThreadWrapper::run",
                    r"ThreadPool.*::wait",
                    r"WorkerThread.*::wait",
                ]
            },
            {
                "name": "thread_pool_mutex_wait",
                "description": "Thread pool workers waiting on mutex/locks",
                "type": "contextual", 
                "low_level_patterns": [
                    r"__lll_lock_wait",
                    r"pthread_mutex_lock",
                    r"__pthread_mutex_lock",
                ],
                "high_level_patterns": [
                    r"CPooledThreadWrapper::run",
                    r"Waiter::wait",
                    r"ThreadPool.*::wait",
                ]
            },
            {
                "name": "roxie_worker_idle",
                "description": "Roxie workers waiting for work",
                "type": "contextual",
                "low_level_patterns": [
                    r"pthread_cond_wait",
                    r"pthread_cond_timedwait", 
                    r"InterruptableSemaphore::wait",
                ],
                "high_level_patterns": [
                    r"RoxieQueue::wait",
                    r"CRoxieWorker.*threadmain",
                ]
            },
            {
                "name": "roxie_cache_wait",
                "description": "Roxie file cache waiting",
                "type": "contextual",
                "low_level_patterns": [
                    r"pthread_cond_wait",
                    r"InterruptableSemaphore::wait",
                ],
                "high_level_patterns": [
                    r"RoxieFileCache",
                ]
            },
            # Simple patterns - just presence of these functions indicates idle state
            {
                "name": "thread_management",
                "description": "Generic thread startup/cleanup",
                "type": "simple",
                "patterns": [
                    r"start_thread",
                    r"clone[0-9]*\s*\(",
                    r"Thread::_threadmain",
                    r"Thread::begin",
                ]
            },
            {
                "name": "io_wait",
                "description": "I/O waiting operations",
                "type": "simple",
                "patterns": [
                    r"epoll_wait",
                    r"poll\s*\(",
                    r"select\s*\(",
                    r"read\s*\(",
                    r"write\s*\(",
                ]
            },
            # Performance tracing - always idle
            {
                "name": "perf_tracer",
                "description": "Performance tracing code",
                "type": "simple", 
                "patterns": [
                    r"PerfTracer",
                ]
            }
        ]
    
    def _load_config_file(self, config_file: str):
        """Load custom filter patterns from JSON config file"""
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                if 'idle_patterns' in config:
                    self.idle_patterns.extend(config['idle_patterns'])
        except Exception as e:
            print(f"Warning: Could not load config file {config_file}: {e}", file=sys.stderr)
    
    def is_mundane_stack(self, stack: StackTrace) -> Tuple[bool, List[str]]:
        """
        Determine if a stack trace represents mundane/idle activity
        Returns (is_mundane, matching_pattern_names)
        """
        matching_patterns = []
        
        # Get all function names from the stack for matching
        all_functions = " ".join(frame.function for frame in stack.frames)
        
        for pattern_group in self.idle_patterns:
            pattern_type = pattern_group.get("type", "simple")
            
            if pattern_type == "contextual":
                # Contextual patterns require BOTH low-level waits AND high-level idle context
                if self._matches_contextual_pattern(stack, pattern_group):
                    matching_patterns.append(pattern_group["name"])
                    
            elif pattern_type == "simple":
                # Simple patterns just require any function match
                for pattern in pattern_group["patterns"]:
                    if re.search(pattern, all_functions, re.IGNORECASE):
                        matching_patterns.append(pattern_group["name"])
                        break
        
        # Consider it mundane if it matches any idle pattern
        is_mundane = len(matching_patterns) > 0
        return is_mundane, matching_patterns
    
    def _matches_contextual_pattern(self, stack: StackTrace, pattern_group: Dict) -> bool:
        """
        Check if a stack matches a contextual pattern requiring both low-level and high-level matches
        """
        low_level_patterns = pattern_group.get("low_level_patterns", [])
        high_level_patterns = pattern_group.get("high_level_patterns", [])
        
        if not low_level_patterns or not high_level_patterns:
            return False
        
        # Check for low-level wait patterns (typically in first few frames)
        low_level_match = False
        top_frames = stack.frames[:4]  # Check top 4 frames for low-level waits
        top_functions = " ".join(frame.function for frame in top_frames)
        
        for pattern in low_level_patterns:
            if re.search(pattern, top_functions, re.IGNORECASE):
                low_level_match = True
                break
        
        if not low_level_match:
            return False
        
        # Check for high-level idle context patterns (anywhere in stack)
        all_functions = " ".join(frame.function for frame in stack.frames)
        for pattern in high_level_patterns:
            if re.search(pattern, all_functions, re.IGNORECASE):
                return True
        
        return False


class StackParser:
    """Parses stack traces from various formats"""
    
    def parse_stack_traces(self, content: str) -> List[StackTrace]:
        """Parse stack traces from input content"""
        stacks = []
        current_stack_lines = []
        
        lines = content.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                if current_stack_lines:
                    stack = self._parse_single_stack(current_stack_lines)
                    if stack:
                        stacks.append(stack)
                    current_stack_lines = []
                continue
            
            # Check if this is a stack frame line
            if re.match(r'^#\d+', line):
                current_stack_lines.append(line)
            elif current_stack_lines:
                # Non-frame line after we started collecting frames - end of stack
                stack = self._parse_single_stack(current_stack_lines)
                if stack:
                    stacks.append(stack)
                current_stack_lines = []
        
        # Handle last stack if file doesn't end with empty line
        if current_stack_lines:
            stack = self._parse_single_stack(current_stack_lines)
            if stack:
                stacks.append(stack)
        
        return stacks
    
    def _parse_single_stack(self, lines: List[str]) -> Optional[StackTrace]:
        """Parse a single stack trace from lines"""
        if not lines:
            return None
            
        frames = []
        raw_text = '\n'.join(lines)
        
        for line in lines:
            frame = self._parse_frame_line(line)
            if frame:
                frames.append(frame)
        
        if not frames:
            return None
            
        return StackTrace(frames=frames, raw_text=raw_text)
    
    def _parse_frame_line(self, line: str) -> Optional[StackFrame]:
        """Parse a single frame line"""
        # Pattern for frame lines like:
        # #0  __futex_abstimed_wait_common64 (private=<optimized out>, ...) at ./nptl/futex-internal.c:57
        # #3  0x00007fa3f3a61bdf in do_futex_wait (sem=sem@entry=0x...) at ./nptl/sem_waitcommon.c:111
        
        match = re.match(r'^#(\d+)\s+(?:0x[0-9a-f]+\s+in\s+)?([^(]+)(\([^)]*\))?\s*(?:at\s+(.+))?', line)
        if not match:
            return None
        
        frame_num = int(match.group(1))
        function = match.group(2).strip()
        args = match.group(3) or ""
        location = match.group(4) or ""
        
        # Extract address if present
        address_match = re.search(r'0x[0-9a-f]+', line)
        address = address_match.group(0) if address_match else None
        
        return StackFrame(
            frame_num=frame_num,
            address=address,
            function=function,
            args=args,
            location=location
        )


class StackAnalyzer:
    """Main analyzer class"""
    
    def __init__(self, config_file: Optional[str] = None):
        self.parser = StackParser()
        self.filter = StackFilter(config_file)
    
    def analyze_file(self, filename: str, show_filtered: bool = False, verbose: bool = False):
        """Analyze stack traces from a file"""
        try:
            with open(filename, 'r') as f:
                content = f.read()
        except Exception as e:
            print(f"Error reading file {filename}: {e}", file=sys.stderr)
            return
        
        stacks = self.parser.parse_stack_traces(content)
        self.analyze_stacks(stacks, show_filtered, verbose)
    
    def analyze_stacks(self, stacks: List[StackTrace], show_filtered: bool = False, verbose: bool = False):
        """Analyze a list of stack traces"""
        if not stacks:
            print("No stack traces found in input")
            return
        
        mundane_stacks = []
        interesting_stacks = []
        pattern_counts = defaultdict(int)
        
        for stack in stacks:
            is_mundane, patterns = self.filter.is_mundane_stack(stack)
            if is_mundane:
                mundane_stacks.append((stack, patterns))
                for pattern in patterns:
                    pattern_counts[pattern] += 1
            else:
                interesting_stacks.append(stack)
        
        # Print summary
        total = len(stacks)
        mundane_count = len(mundane_stacks)
        interesting_count = len(interesting_stacks)
        
        print(f"=== Stack Trace Analysis Summary ===")
        print(f"Total stacks: {total}")
        print(f"Mundane/Idle stacks: {mundane_count} ({mundane_count/total*100:.1f}%)")
        print(f"Interesting stacks: {interesting_count} ({interesting_count/total*100:.1f}%)")
        
        if verbose and pattern_counts:
            print(f"\n=== Idle Pattern Matches ===")
            for pattern, count in sorted(pattern_counts.items()):
                print(f"  {pattern}: {count}")
        
        # Show requested stacks
        if show_filtered:
            print(f"\n=== Filtered Out (Mundane) Stacks ===")
            self._print_stacks([(stack, patterns) for stack, patterns in mundane_stacks], verbose)
        else:
            print(f"\n=== Interesting Stacks ===")
            self._print_stacks([(stack, []) for stack in interesting_stacks], verbose)
    
    def _print_stacks(self, stacks_with_patterns: List[Tuple[StackTrace, List[str]]], verbose: bool):
        """Print stack traces with optional pattern information"""
        if not stacks_with_patterns:
            print("  (none)")
            return
        
        for i, (stack, patterns) in enumerate(stacks_with_patterns, 1):
            print(f"\n--- Stack {i} ---")
            if verbose and patterns:
                print(f"Matched patterns: {', '.join(patterns)}")
            if verbose:
                print(f"Signature: {stack.signature}")
            print(stack.raw_text)


def create_sample_config():
    """Create a sample configuration file"""
    config = {
        "idle_patterns": [
            {
                "name": "custom_simple_wait",
                "description": "Custom simple waiting pattern",
                "type": "simple",
                "patterns": [
                    r"MyCustomWait",
                    r"CustomThreadPool::wait"
                ]
            },
            {
                "name": "custom_contextual_wait",
                "description": "Custom contextual waiting pattern requiring both low-level wait and high-level context",
                "type": "contextual",
                "low_level_patterns": [
                    r"pthread_mutex_lock",
                    r"__lll_lock_wait"
                ],
                "high_level_patterns": [
                    r"MyApplicationWorker::wait",
                    r"MyThreadPool::run"
                ]
            }
        ]
    }
    
    with open("stack_filter_config.json", 'w') as f:
        json.dump(config, f, indent=2)
    
    print("Created sample configuration file: stack_filter_config.json")
    print("This demonstrates both 'simple' and 'contextual' pattern types.")
    print("Contextual patterns require BOTH low-level wait primitives AND high-level idle context.")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze stack traces to identify interesting vs mundane (idle) stacks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('input_file', nargs='?', 
                       help='Input file containing stack traces (use - for stdin)')
    parser.add_argument('--show-filtered', action='store_true',
                       help='Show filtered out (mundane) stacks instead of interesting ones')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show detailed analysis information')
    parser.add_argument('--config', metavar='FILE',
                       help='Use custom filter configuration file')
    parser.add_argument('--create-sample-config', action='store_true',
                       help='Create a sample configuration file and exit')
    
    args = parser.parse_args()
    
    if args.create_sample_config:
        create_sample_config()
        return
    
    if not args.input_file:
        parser.print_help()
        return
    
    analyzer = StackAnalyzer(args.config)
    
    if args.input_file == '-':
        content = sys.stdin.read()
        stacks = analyzer.parser.parse_stack_traces(content)
        analyzer.analyze_stacks(stacks, args.show_filtered, args.verbose)
    else:
        analyzer.analyze_file(args.input_file, args.show_filtered, args.verbose)


if __name__ == '__main__':
    main()