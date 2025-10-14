# Thor Config Monitoring Implementation

## Overview
This implementation enables configuration monitoring for Thor (manager and workers) in containerized deployments, allowing "soft" configuration changes (e.g., logging levels) to be applied without restarting Thor.

## Problem Solved
Previously, Thor components loaded configuration without monitoring (monitor=false), meaning any configuration changes required a restart. While other components could auto-reload config, Thor couldn't because:
- The manager sends additional settings to workers during registration
- If workers auto-reloaded config, they would lose these manager-provided settings

## Solution
The solution reorganizes how Thor handles configuration in containerized mode:

1. **Separate additional settings**: Manager-specific settings are extracted into a dedicated IPropertyTree
2. **Send settings separately**: Only additional settings are sent to workers (not the full merged config)
3. **Re-merge on refresh**: Both manager and workers install hooks to re-merge additional settings when config is refreshed
4. **Enable monitoring**: Config monitoring is enabled for both manager and workers in containerized mode

## Technical Details

### Containerized Mode Changes

#### Manager (thmastermain.cpp)
- Enables config monitoring: `loadConfiguration(..., monitor=true)`
- Creates `managerAdditionalSettings` tree with manager-specific settings:
  - `@masterBuildTag`, `@channelsPerWorker`, `@name`, `@nodeGroup`
  - `@masterTotalMem`, `@thorPath`, `@query_so_dir`, `@dllsToSlaves`, `@thorTempDirectory`
  - `logging/@thorworkerdetail`
  - `workerMemory/*`, `managerMemory/*`
- Sends only `managerAdditionalSettings` to workers (not full globals)
- Installs `ConfigModifyFunc` hook to re-merge settings on config refresh

#### Worker (thslavemain.cpp)
- Enables config monitoring: `loadConfiguration(..., monitor=true)`
- Receives `managerAdditionalSettings` from manager
- Merges additional settings into its own config
- Stores additional settings for re-use
- Installs `ConfigModifyFunc` hook to re-merge settings on config refresh

### Bare-Metal Mode (Unchanged)
- Config monitoring disabled: `loadConfiguration(..., monitor=false)`
- Manager sends full merged globals to workers
- Workers use master config as before
- Complete backward compatibility

## Files Modified
- `thorlcr/master/thmastermain.cpp` (+83 lines)
- `thorlcr/slave/thslavemain.cpp` (+49 lines)

## Benefits
1. **No restart required**: Soft config changes applied automatically
2. **Preserved settings**: Manager-provided settings maintained across refreshes
3. **Backward compatible**: Bare-metal mode unchanged
4. **Clean design**: Clear separation of base config and additional settings

## Related Code
- Config update hooks: `system/jlib/jptree.cpp` (ConfigModifyFunc, ConfigUpdateFunc)
- Config loading: `system/jlib/jptree.cpp` (loadConfiguration)
- Config merging: `system/jlib/jptree.cpp` (mergeConfiguration)
