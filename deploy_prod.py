#!/usr/bin/env python3
"""
deploy_prod.py – Deploy Fabric items to the PRODUCTION workspace.

This script must ONLY be executed after validate_test.py has exited
successfully (exit code 0).  Two hard guardrails are enforced at runtime:

  1. FABRIC_PROMOTE_TO_PROD=true   – explicit opt-in flag (set in workflow)
  2. FABRIC_ENV=prod               – environment marker (set in workflow)

These checks prevent accidental production deployments even when the
script is run outside the normal CI/CD pipeline.

Environment variables:
  FABRIC_WS_PROD          – Fabric workspace GUID for production (required)
  FABRIC_PROMOTE_TO_PROD  – Must be "true" to allow prod deploy (required)
  FABRIC_ENV              – Must be "prod" / "prd" / "production" (required)
  AZURE_TENANT_ID         – Azure AD tenant ID (required)
  AZURE_CLIENT_ID         – Service-principal client ID (required)
  AZURE_CLIENT_SECRET     – Service-principal client secret (required)
  FABRIC_REPO_DIR         – Path to the repo root (optional)
  FABRIC_CLEANUP_ORPHANS  – Remove orphaned items from workspace (default: true)
  FABRIC_ITEMS_IN_SCOPE   – Comma-separated item types to publish (optional)
  LOG_LEVEL               – Logging level, default INFO (optional)
  FABRIC_MAX_RETRIES      – Number of retry attempts on failure (default: 3)
  FABRIC_RETRY_SLEEP      – Seconds between retries (default: 10)
"""
import logging
import os
import sys
import time
from pathlib import Path

from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

ENV_NAME = "prod"


def setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise ValueError(f"Missing required environment variable: {name}")
    return v


def get_workspace_id() -> str:
    return os.getenv("FABRIC_WS_PROD", "").strip()


def get_repo_dir() -> Path:
    repo_dir = Path(os.getenv("FABRIC_REPO_DIR", Path(__file__).resolve().parents[0])).resolve()
    if not repo_dir.exists():
        raise ValueError(f"Repository directory not found: {repo_dir}")
    return repo_dir


def token_credential() -> ClientSecretCredential:
    return ClientSecretCredential(
        tenant_id=required("AZURE_TENANT_ID"),
        client_id=required("AZURE_CLIENT_ID"),
        client_secret=required("AZURE_CLIENT_SECRET"),
    )


def assert_prod_allowed() -> None:
    """
    Hard guardrails that prevent accidental production deployments:
      - FABRIC_PROMOTE_TO_PROD must be exactly "true"
      - FABRIC_ENV must indicate a production environment
    Both must be set explicitly in the GitHub Actions workflow AFTER
    the validate-test job has succeeded.
    """
    promote = os.getenv("FABRIC_PROMOTE_TO_PROD", "false").strip().lower()
    if promote != "true":
        raise PermissionError(
            "Refusing PROD deploy: FABRIC_PROMOTE_TO_PROD must be 'true'. "
            "This flag is only set by the workflow after validation passes."
        )

    env_marker = os.getenv("FABRIC_ENV", "").strip().lower()
    if env_marker not in ("prod", "prd", "production"):
        raise PermissionError(
            f"Refusing PROD deploy: FABRIC_ENV must indicate prod (got '{env_marker}')"
        )

    logging.info("PROD guardrails satisfied: FABRIC_PROMOTE_TO_PROD=true, FABRIC_ENV=%s", env_marker)


def run_deploy() -> None:
    assert_prod_allowed()

    ws_id = get_workspace_id()
    if not ws_id:
        raise ValueError("FABRIC_WS_PROD is empty/missing.")

    repo_dir = get_repo_dir()

    # PROD: strongly recommended true to avoid drift
    cleanup_orphans = os.getenv("FABRIC_CLEANUP_ORPHANS", "true").lower() == "true"
    items_raw = os.getenv("FABRIC_ITEMS_IN_SCOPE", "").strip()
    items_in_scope = [x.strip() for x in items_raw.split(",") if x.strip()] if items_raw else None

    logging.info("PROD deploy starting workspace_id=%s repo_dir=%s", ws_id, repo_dir)

    ws = FabricWorkspace(
        workspace_id=ws_id,
        repository_directory=str(repo_dir),
        environment=ENV_NAME,
        item_type_in_scope=items_in_scope,
        token_credential=token_credential(),
    )

    publish_all_items(ws)
    logging.info("PROD publish completed")

    if cleanup_orphans:
        unpublish_all_orphan_items(ws)
        logging.info("PROD orphan cleanup completed")


def main() -> int:
    setup_logging()
    retries = int(os.getenv("FABRIC_MAX_RETRIES", "3"))
    sleep_s = int(os.getenv("FABRIC_RETRY_SLEEP", "10"))

    for attempt in range(1, retries + 2):
        try:
            logging.info("PROD attempt %d/%d", attempt, retries + 1)
            run_deploy()
            return 0
        except Exception as e:
            logging.error("PROD deployment failed: %s", e, exc_info=True)
            if attempt >= retries + 1:
                return 1
            logging.warning("Retrying PROD in %ds...", sleep_s)
            time.sleep(sleep_s)

    return 1  # should never reach here


if __name__ == "__main__":
    raise SystemExit(main())

