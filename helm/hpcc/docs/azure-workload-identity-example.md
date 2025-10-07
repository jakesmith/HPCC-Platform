# HPCC Azure Workload Identity Example

This example demonstrates how to configure the HPCC Helm chart to use Azure Workload Identity for secure, credential-free access to Azure resources.

## Prerequisites

1. An Azure Kubernetes Service (AKS) cluster with Workload Identity enabled
2. Azure managed identities created for your HPCC components
3. Federated identity credentials configured for the managed identities
4. Appropriate Azure permissions granted to the managed identities

## Configuration

Add the following to your `values.yaml`:

```yaml
global:
  serviceAccounts:
    # For components using hpcc-default ServiceAccount (roxie, sasha, etc.)
    default:
      annotations:
        azure.workload.identity/client-id: "00000000-0000-0000-0000-000000000000"
      podLabels:
        azure.workload.identity/use: "true"
    
    # For components using hpcc-agent ServiceAccount (eclagent, eclccserver)
    agent:
      annotations:
        azure.workload.identity/client-id: "11111111-1111-1111-1111-111111111111"
      podLabels:
        azure.workload.identity/use: "true"
    
    # For Thor agent components
    thoragent:
      annotations:
        azure.workload.identity/client-id: "22222222-2222-2222-2222-222222222222"
      podLabels:
        azure.workload.identity/use: "true"
    
    # For ESP components
    esp-service:
      annotations:
        azure.workload.identity/client-id: "33333333-3333-3333-3333-333333333333"
      podLabels:
        azure.workload.identity/use: "true"
    
    # For Dali components
    dali:
      annotations:
        azure.workload.identity/client-id: "44444444-4444-4444-4444-444444444444"
      podLabels:
        azure.workload.identity/use: "true"
```

## Setup Steps

### 1. Create Managed Identities

Create a managed identity for each ServiceAccount:

```bash
az identity create --name hpcc-default-identity --resource-group myResourceGroup
az identity create --name hpcc-agent-identity --resource-group myResourceGroup
az identity create --name hpcc-thoragent-identity --resource-group myResourceGroup
az identity create --name hpcc-esp-service-identity --resource-group myResourceGroup
az identity create --name hpcc-dali-identity --resource-group myResourceGroup
```

### 2. Get OIDC Issuer URL

```bash
export OIDC_ISSUER=$(az aks show --name myAKSCluster --resource-group myResourceGroup --query "oidcIssuerProfile.issuerUrl" -o tsv)
```

### 3. Configure Federated Identity Credentials

For each ServiceAccount, create a federated identity credential:

```bash
export NAMESPACE="default"  # or your HPCC namespace

# For hpcc-default
az identity federated-credential create \
  --name hpcc-default-federated-id \
  --identity-name hpcc-default-identity \
  --resource-group myResourceGroup \
  --issuer "${OIDC_ISSUER}" \
  --subject system:serviceaccount:${NAMESPACE}:hpcc-default

# For hpcc-agent
az identity federated-credential create \
  --name hpcc-agent-federated-id \
  --identity-name hpcc-agent-identity \
  --resource-group myResourceGroup \
  --issuer "${OIDC_ISSUER}" \
  --subject system:serviceaccount:${NAMESPACE}:hpcc-agent

# Repeat for other ServiceAccounts...
```

### 4. Grant Azure Permissions

Grant the necessary Azure permissions to each managed identity. For example, to grant Storage Blob Data Contributor access:

```bash
export STORAGE_ACCOUNT="mystorageaccount"
export STORAGE_ACCOUNT_ID=$(az storage account show --name ${STORAGE_ACCOUNT} --query id -o tsv)

az role assignment create \
  --role "Storage Blob Data Contributor" \
  --assignee "00000000-0000-0000-0000-000000000000" \
  --scope "${STORAGE_ACCOUNT_ID}"
```

### 5. Deploy HPCC

Deploy the HPCC cluster with your updated values.yaml:

```bash
helm install myhpcc hpcc/hpcc -f values.yaml
```

## Verification

### Verify ServiceAccount Annotations

```bash
kubectl get serviceaccount hpcc-default -o yaml
```

Expected output should include:
```yaml
metadata:
  annotations:
    azure.workload.identity/client-id: 00000000-0000-0000-0000-000000000000
```

### Verify Pod Labels

```bash
kubectl get pods -l azure.workload.identity/use=true
```

This should list all HPCC pods that are configured with Azure Workload Identity.

### Test Azure Access

You can test that pods can access Azure resources by execing into a pod and using Azure SDK or CLI:

```bash
kubectl exec -it <pod-name> -- bash
# Inside the pod, Azure SDK will automatically use the workload identity
```

## Troubleshooting

1. **Pods cannot access Azure resources**: 
   - Verify the managed identity has the necessary permissions
   - Check that federated identity credentials are correctly configured
   - Ensure the OIDC issuer URL is correct

2. **Labels/annotations not appearing**:
   - Verify the values.yaml syntax is correct
   - Check the Helm chart was deployed with the updated values

3. **Multiple managed identities needed**:
   - Different components may need access to different Azure resources
   - Create separate managed identities with appropriate permissions for each ServiceAccount

## References

- [Azure Workload Identity Documentation](https://azure.github.io/azure-workload-identity/)
- [HPCC Identity Management Documentation](../docs/identity.md)
