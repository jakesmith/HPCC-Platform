# Dali Graceful Shutdown

## Overview

Dali now supports graceful shutdown to prevent client disconnection issues during pod termination in Kubernetes environments. When a HPCC helm chart is removed, Kubernetes may terminate pods in an unordered fashion, which can cause Dali to shut down before its clients, leaving them in a hanging state.

## Configuration

The graceful shutdown behavior can be configured using the `shutdownGracePeriod` parameter in the Dali configuration.

### Configuration Parameter

- **`shutdownGracePeriod`**: Time in seconds to wait for clients to disconnect before forcing shutdown
  - **Type**: Integer
  - **Default**: 60 seconds
  - **Range**: 0 or greater
  - **Location**: Dali configuration section

### Example Configuration

#### Helm Values (values.yaml)

```yaml
dali:
  - name: mydali
    shutdownGracePeriod: 120  # Wait up to 120 seconds for clients to disconnect
```

#### Legacy Configuration (daliconf.xml)

```xml
<dali shutdownGracePeriod="120">
  <!-- other configuration -->
</dali>
```

### Disabling Graceful Shutdown

To disable the graceful shutdown wait and proceed immediately with shutdown:

```yaml
dali:
  - name: mydali
    shutdownGracePeriod: 0  # Disable graceful shutdown wait
```

## Behavior

### During Shutdown

1. When Dali receives a shutdown signal (e.g., SIGTERM from Kubernetes):
   - The `suspend()` phase begins
   - Dali waits for all connected clients to disconnect
   - Progress is logged every 5 seconds showing the number of remaining clients
   
2. If all clients disconnect before the timeout:
   - A log message confirms all clients have disconnected
   - Shutdown proceeds immediately
   
3. If the timeout is reached with clients still connected:
   - A warning is logged listing the remaining connected clients
   - Shutdown proceeds regardless

### Log Messages

During graceful shutdown, you'll see log messages like:

```
Dali shutdown: waiting for clients to disconnect (timeout: 60000 ms)
Dali shutdown: waiting for 5 clients to disconnect (55 seconds remaining)
Dali shutdown: waiting for 3 clients to disconnect (50 seconds remaining)
Dali shutdown: all clients disconnected
```

Or if timeout is reached:

```
Dali shutdown: timeout reached with 2 clients still connected:
1234567890ABCDEF: 10.0.1.15:7100, role=ThorMaster
234567890ABCDEF0: 10.0.1.16:7100, role=EclAgent
```

## Kubernetes Integration

### Termination Grace Period

The Kubernetes `terminationGracePeriodSeconds` should be set to a value greater than the Dali `shutdownGracePeriod` to allow enough time for the graceful shutdown process.

Default helm chart configuration:
- `terminationGracePeriodSeconds`: 3600 seconds (1 hour)
- `shutdownGracePeriod`: 60 seconds

### Recommended Settings

For production environments:
- Set `shutdownGracePeriod` to a value appropriate for your workload (e.g., 60-300 seconds)
- Ensure `terminationGracePeriodSeconds` > `shutdownGracePeriod` + buffer (e.g., +60 seconds)

## Client Behavior

Clients do not need to be modified to benefit from graceful shutdown. They will:
1. Continue normal operation during the grace period
2. Disconnect naturally when their work completes
3. Avoid attempting to reconnect to a shutting-down Dali instance

## Troubleshooting

### Clients Not Disconnecting

If clients consistently remain connected until timeout:
- Check client logs for stuck operations or long-running queries
- Consider increasing the `shutdownGracePeriod`
- Investigate why clients are not detecting the pending shutdown

### Timeout Too Short

Symptoms:
- Warning messages showing clients still connected at timeout
- Clients experiencing connection errors during shutdown

Solution:
- Increase the `shutdownGracePeriod` value
- Monitor typical client session durations to determine appropriate timeout

### Shutdown Takes Too Long

If the shutdown process is taking longer than desired:
- Reduce the `shutdownGracePeriod` value
- Set to 0 to disable graceful shutdown wait (not recommended for production)

## Implementation Details

The graceful shutdown is implemented in the Dali session server (`CDaliSessionServer`) and session manager (`CCovenSessionManager`). During the `suspend()` phase, Dali:

1. Checks the configuration for `shutdownGracePeriod`
2. Waits for the client process lookup table to become empty
3. Logs progress periodically
4. Exits the wait loop when all clients disconnect or timeout is reached
5. Proceeds with normal shutdown sequence

This approach ensures that connected clients have time to complete their operations and disconnect gracefully before Dali shuts down completely.
