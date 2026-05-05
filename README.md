# Fabric-CICD

This repository implements a CI/CD pipeline for deploying Microsoft Fabric items (Notebooks, Lakehouses, Reports, etc.) across environments (Dev → Test → Prod) using [`microsoft/fabric-cicd`](https://github.com/microsoft/fabric-cicd).

## How it works

When code is pushed to `main`, GitHub Actions:
1. Deploys all Fabric items to the **Test** workspace.
2. After manual approval, deploys to the **Prod** workspace.

## Notebook–Lakehouse Association

A key challenge in multi-environment Fabric deployments is that notebooks store their **default Lakehouse binding** as hard-coded IDs in `notebook-content.py` metadata:

```python
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "<DEV-LAKEHOUSE-ID>",
# META       "default_lakehouse_name": "my_lakehouse",
# META       "default_lakehouse_workspace_id": "<DEV-WORKSPACE-ID>",
# META       ...
# META     }
```

Without rebinding, a notebook deployed to Test will still reference the Dev Lakehouse — visible in Fabric lineage as cross-workspace dependencies.

### How the fix works (`parameter.yml`)

`parameter.yml` instructs `fabric-cicd` to perform find/replace substitutions on notebook content at deploy time, using built-in variables that resolve to the **target** workspace and item IDs:

| Variable | Resolves to |
|---|---|
| `$workspace.$id` | The target workspace ID (Test or Prod) |
| `$items.Lakehouse.<name>.$id` | The ID of the Lakehouse named `<name>` in the target workspace |

**Example — `test-CICD-05052026-14h24` notebook bound to `demo_cicd_lkh`:**

```yaml
# Replace Dev workspace ID with target workspace ID
- find_value: "04e2a7df-2bff-4029-922b-ec4facdecbf8"   # Dev workspace ID
  replace_value:
    test: "$workspace.$id"
    prod: "$workspace.$id"
  item_type: "Notebook"
  file_path: "test-CICD-05052026-14h24.Notebook/notebook-content.py"

# Replace Dev lakehouse ID with target lakehouse ID (resolved by name)
- find_value: "2f40a2b0-44be-4ecb-89fa-f8120d810d9f"   # demo_cicd_lkh ID in Dev
  replace_value:
    test: "$items.Lakehouse.demo_cicd_lkh.$id"
    prod: "$items.Lakehouse.demo_cicd_lkh.$id"
  item_type: "Notebook"
  file_path: "test-CICD-05052026-14h24.Notebook/notebook-content.py"
```

After deployment, the notebook in Test/Prod will reference the `demo_cicd_lkh` Lakehouse that lives in *that* workspace, not the Dev one.

### Adding a new notebook with a Lakehouse binding

For each new notebook that has a Lakehouse dependency, add two entries to `parameter.yml`:

1. **Workspace ID replacement** — replace the Dev workspace ID with `$workspace.$id`.
2. **Lakehouse ID replacement** — replace the Dev lakehouse ID with `$items.Lakehouse.<lakehouse-name>.$id`.

The Dev workspace and lakehouse IDs are visible in the `notebook-content.py` metadata block under `default_lakehouse_workspace_id` and `default_lakehouse` respectively.

## Required GitHub Secrets

| Secret | Description |
|---|---|
| `AZURE_TENANT_ID` | Azure AD tenant ID for the Service Principal |
| `AZURE_CLIENT_ID` | Service Principal (app registration) client ID |
| `AZURE_CLIENT_SECRET` | Service Principal client secret |
| `FABRIC_WS_TEST` | Fabric workspace ID for the Test environment |
| `FABRIC_WS_PROD` | Fabric workspace ID for the Prod environment |

## Optional Environment Variables

| Variable | Default | Description |
|---|---|---|
| `FABRIC_CLEANUP_ORPHANS` | `true` | Remove items from the target workspace that no longer exist in the repo |
| `FABRIC_PUBLISH_SEMANTIC_MODELS` | `false` | Also deploy Semantic Models (disabled by default to avoid import errors) |
| `FABRIC_ITEMS_IN_SCOPE` | *(all supported types)* | Comma-separated list of Fabric item types to deploy |
| `FABRIC_MAX_RETRIES` | `3` | Number of retry attempts on transient failures |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

