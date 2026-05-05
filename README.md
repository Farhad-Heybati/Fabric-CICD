# Fabric-CICD

Automated CI/CD pipeline for Microsoft Fabric workspaces using [fabric-cicd](https://github.com/microsoft/fabric-cicd).

## Promotion flow

Every push to `main` triggers a **gated, three-stage pipeline**:

```
deploy-test  →  validate-test  →  deploy-prod
```

| Stage | What it does | Gating |
|-------|-------------|--------|
| **deploy-test** | Publishes Fabric items to the **test** workspace | Runs on every push to `main` |
| **validate-test** | Connects to the test workspace and verifies the deployment (item count, reachability) | Runs only if `deploy-test` succeeds |
| **deploy-prod** | Publishes Fabric items to the **production** workspace | Runs only if `validate-test` succeeds; optionally gated by manual approval (see below) |

> **Production is never deployed if validation fails.**  The `validate-test` job exits with a non-zero code on failure, and `deploy-prod` has `needs: validate-test`, so it is automatically skipped.

### Manual-approval gate (optional)

The `deploy-prod` job targets the GitHub Actions **`production` environment**.  
To require a human reviewer before each production deployment:

1. Go to **Settings → Environments → production** in this repository.
2. Enable **Required reviewers** and add the desired users/teams.

## Required secrets

Configure these in **Settings → Secrets and variables → Actions**:

| Secret | Description |
|--------|-------------|
| `FABRIC_WS_TEST` | Workspace GUID of the **test** Fabric workspace |
| `FABRIC_WS_PROD` | Workspace GUID of the **production** Fabric workspace |
| `AZURE_TENANT_ID` | Azure AD tenant ID for the service principal |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Service principal client secret |

## Scripts

| Script | Purpose |
|--------|---------|
| `deploy_test.py` | Deploy to the test workspace (`FABRIC_WS_TEST`) |
| `validate_test.py` | Smoke-test the test workspace after deployment |
| `deploy_prod.py` | Deploy to the production workspace (`FABRIC_WS_PROD`) |

### Production guardrails

`deploy_prod.py` enforces two hard checks at runtime even when run directly:

- `FABRIC_PROMOTE_TO_PROD=true` — explicit opt-in flag (only set by the workflow after validation)
- `FABRIC_ENV=prod` — environment marker

Both must be present; if either is missing the script exits immediately without touching the production workspace.

## Optional environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FABRIC_CLEANUP_ORPHANS` | `true` | Remove items from workspace that no longer exist in the repo |
| `FABRIC_ITEMS_IN_SCOPE` | *(all)* | Comma-separated list of item types to publish |
| `FABRIC_MIN_ITEMS` | `0` | Minimum items expected in test workspace during validation |
| `FABRIC_MAX_RETRIES` | `3` | Retry attempts on transient failures |
| `FABRIC_RETRY_SLEEP` | `10` | Seconds between retries |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

