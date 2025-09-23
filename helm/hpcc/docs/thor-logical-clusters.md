# Thor Logical Clusters

This document describes the logical Thor cluster feature, which allows you to define a single logical Thor configuration that gets expanded into multiple physical Thor instances.

## Problem Statement

Previously, to create multiple Thor targets that share a common queue (using auxQueues), you needed to:

1. Define multiple Thor configurations manually
2. Manually calculate and divide maxJobs and maxGraphs between instances
3. Ensure each instance had the same configuration except for limits

This was error-prone and required conscious configuration steps to subdivide the total resources.

## Solution: Logical Clusters

The logical cluster feature allows you to define a single Thor configuration that automatically expands into multiple physical instances with properly divided limits.

## Configuration

### Basic Example

```yaml
thor:
- name: thorcluster
  cluster:
    instances: 3
    instanceTemplate: "{name}-az{instance}"
  numWorkers: 2
  maxJobs: 6      # Will be divided: 2 per instance
  maxGraphs: 3    # Will be divided: 1 per instance
```

This configuration will generate:
- `thorcluster-az1` with maxJobs=2, maxGraphs=1, auxQueues=[thorcluster]
- `thorcluster-az2` with maxJobs=2, maxGraphs=1, auxQueues=[thorcluster]  
- `thorcluster-az3` with maxJobs=2, maxGraphs=1, auxQueues=[thorcluster]
- A logical queue named `thorcluster` that routes to all instances

### Configuration Options

#### cluster.instances (required)
- Type: integer
- Minimum: 2
- Description: Number of physical Thor instances to create

#### cluster.instanceTemplate (optional)
- Type: string
- Default: "{name}-{instance}"
- Description: Template for generating instance names
- Placeholders:
  - `{name}`: Replaced with the logical cluster name
  - `{instance}`: Replaced with instance number (1, 2, 3, ...)

### Validation Rules

1. **maxJobs** and **maxGraphs** must be evenly divisible by the number of instances
2. All other configuration (numWorkers, resources, etc.) is copied to each instance
3. Instance names must not conflict with existing Thor names
4. The `cluster` configuration is removed from generated instances

## Use Cases

### Multi-AZ Deployment
```yaml
thor:
- name: thor-prod
  cluster:
    instances: 3
    instanceTemplate: "{name}-az{instance}"
  numWorkers: 4
  maxJobs: 12
  maxGraphs: 6
  # Each instance gets maxJobs=4, maxGraphs=2
```

### Custom Instance Naming with Regions
```yaml
thor:
- name: thor-global
  cluster:
    instances: 4
    instanceTemplate: "thor-region-{instance}-cluster"
  maxJobs: 16
  maxGraphs: 8
  # Generates: thor-region-1-cluster, thor-region-2-cluster, 
  #           thor-region-3-cluster, thor-region-4-cluster
  # This pattern is useful when instances need specific naming for
  # integration with external systems or monitoring tools
```

## Generated Queue Configuration

When a logical cluster is expanded:

1. Each physical instance gets its own individual queue
2. Each instance is configured with `auxQueues: [<logical-name>]`
3. An auxiliary queue with the logical cluster name is created
4. Jobs submitted to the logical queue are distributed among all instances

## Backward Compatibility

The logical cluster feature is fully backward compatible:
- Existing Thor configurations continue to work unchanged
- Existing auxQueues configurations work as before
- No changes to existing templates or configurations are required

## Migration from Manual auxQueues

### Before (Manual Configuration)
```yaml
thor:
- name: thor-az1
  auxQueues: [thor-prod]
  maxJobs: 2
  maxGraphs: 1
- name: thor-az2  
  auxQueues: [thor-prod]
  maxJobs: 2
  maxGraphs: 1
- name: thor-az3
  auxQueues: [thor-prod]
  maxJobs: 2
  maxGraphs: 1
```

### After (Logical Cluster)
```yaml
thor:
- name: thor-prod
  cluster:
    instances: 3
    instanceTemplate: "{name}-az{instance}"
  maxJobs: 6
  maxGraphs: 3
```

## Error Handling

The system validates configurations and provides clear error messages:

```
Thor cluster 'thorcluster': maxJobs (5) and maxGraphs (3) must be evenly divisible by instances (3)
```

## Implementation Details

The logical cluster expansion happens during Helm template processing:
1. Templates detect Thor configurations with `cluster` properties
2. Validation ensures proper divisibility
3. Multiple Thor instances are generated with appropriate limits
4. Queue configurations are updated to include both individual and auxiliary queues