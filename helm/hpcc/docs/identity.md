# Identity Management Integration

## Overview

The HPCC Helm chart supports integration with cloud provider identity management systems through ServiceAccount annotations and pod labels. This enables secure, credential-free access to cloud resources using workload identity features.

## ServiceAccount Configuration

The HPCC platform creates several ServiceAccounts with different permission levels:

- **hpcc-default**: Used by components that don't need API access to launch child jobs
- **hpcc-agent**: Used by components that need API access to launch child jobs (e.g., eclagent, eclccserver when not using child processes)
- **hpcc-thoragent**: Used by Thor manager for launching child jobs and managing NetworkPolicies
- **hpcc-esp-service**: Used by ESP components that need to check service status
- **hpcc-dali**: Used by Dali components to find external directio dafilesrv services

### Adding Annotations and Labels

You can configure annotations and pod labels for each ServiceAccount through the `global.serviceAccounts` section in your values file:

```yaml
global:
  serviceAccounts:
    <service-account-name>:
      annotations:
        <key>: <value>
      podLabels:
        <key>: <value>
```

- **annotations**: Added to the ServiceAccount resource itself
- **podLabels**: Added to all pods that use this ServiceAccount

## Azure Workload Identity

Azure Workload Identity allows pods to authenticate to Azure services using managed identities without storing credentials in Kubernetes secrets.

### Prerequisites

1. An Azure Kubernetes Service (AKS) cluster with Workload Identity enabled
2. Azure managed identities created for your HPCC components
3. Federated identity credentials configured for the managed identities

### Configuration Example

```yaml
global:
  serviceAccounts:
    default:
      annotations:
        azure.workload.identity/client-id: "00000000-0000-0000-0000-000000000000"
      podLabels:
        azure.workload.identity/use: "true"
    
    agent:
      annotations:
        azure.workload.identity/client-id: "11111111-1111-1111-1111-111111111111"
      podLabels:
        azure.workload.identity/use: "true"
    
    thoragent:
      annotations:
        azure.workload.identity/client-id: "22222222-2222-2222-2222-222222222222"
      podLabels:
        azure.workload.identity/use: "true"
    
    esp-service:
      annotations:
        azure.workload.identity/client-id: "33333333-3333-3333-3333-333333333333"
      podLabels:
        azure.workload.identity/use: "true"
    
    dali:
      annotations:
        azure.workload.identity/client-id: "44444444-4444-4444-4444-444444444444"
      podLabels:
        azure.workload.identity/use: "true"
```

### Setup Steps

1. **Create Managed Identities** in Azure for each ServiceAccount that needs access to Azure resources

2. **Configure Federated Identity Credentials** for each managed identity:
   ```bash
   az identity federated-credential create \
     --name hpcc-default-federated-id \
     --identity-name hpcc-default-identity \
     --resource-group myResourceGroup \
     --issuer "${OIDC_ISSUER}" \
     --subject system:serviceaccount:${NAMESPACE}:hpcc-default
   ```

3. **Grant Azure Permissions** to the managed identities (e.g., Storage Blob Data Contributor for Azure Storage access)

4. **Deploy HPCC** with the ServiceAccount configuration shown above

### Verification

After deployment, verify that:

1. ServiceAccounts have the correct annotations:
   ```bash
   kubectl get serviceaccount hpcc-default -o yaml
   ```

2. Pods have the correct labels:
   ```bash
   kubectl get pods -l azure.workload.identity/use=true
   ```

3. Pods can access Azure resources using the managed identity

## AWS IAM Roles for Service Accounts (IRSA)

Similar to Azure Workload Identity, AWS IRSA allows pods to assume IAM roles.

### Configuration Example

```yaml
global:
  serviceAccounts:
    default:
      annotations:
        eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/hpcc-default-role"
    
    agent:
      annotations:
        eks.amazonaws.com/role-arn: "arn:aws:iam::123456789012:role/hpcc-agent-role"
```

## Google Cloud Workload Identity

For Google Kubernetes Engine (GKE), use Workload Identity:

### Configuration Example

```yaml
global:
  serviceAccounts:
    default:
      annotations:
        iam.gke.io/gcp-service-account: "hpcc-default@project-id.iam.gserviceaccount.com"
    
    agent:
      annotations:
        iam.gke.io/gcp-service-account: "hpcc-agent@project-id.iam.gserviceaccount.com"
```

## Backward Compatibility

This feature is fully backward compatible. If no ServiceAccount configuration is provided, the system behaves exactly as before, with no annotations or extra labels added.
