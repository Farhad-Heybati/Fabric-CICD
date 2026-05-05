#!/usr/bin/env python3
"""
validate_test.py – Smoke-test / validation script for the TEST workspace.

Run after deploy_test.py succeeds.  Exits with code 0 on success and
code 1 on any failure, which causes the GitHub Actions pipeline to stop
and prevents the production deployment from running.

Environment variables (same as deploy_test.py):
  FABRIC_WS_TEST        – Fabric workspace GUID for the test workspace (required)
  AZURE_TENANT_ID       – Azure AD tenant ID (required)
  AZURE_CLIENT_ID       – Service-principal client ID (required)
  AZURE_CLIENT_SECRET   – Service-principal client secret (required)
  FABRIC_REPO_DIR       – Path to the repo root used for context (optional)
  LOG_LEVEL             – Logging level, default INFO (optional)
  FABRIC_MIN_ITEMS      – Minimum number of items expected in the workspace (optional, default 0)
"""
import logging
import os
import sys
import time
from pathlib import Path

from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace


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
    return os.getenv("FABRIC_WS_TEST", "").strip()


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


def run_validation() -> None:
    """Connect to the test workspace and verify it is reachable and has items."""
    ws_id = get_workspace_id()
    if not ws_id:
        raise ValueError("FABRIC_WS_TEST is empty/missing – cannot validate.")

    repo_dir = get_repo_dir()
    min_items = int(os.getenv("FABRIC_MIN_ITEMS", "0"))

    logging.info("VALIDATION starting against test workspace_id=%s", ws_id)

    ws = FabricWorkspace(
        workspace_id=ws_id,
        repository_directory=str(repo_dir),
        environment="test",
        token_credential=token_credential(),
    )

    # Fetch the list of deployed items from the workspace.
    # FabricWorkspace exposes `workspace_items` which is populated on first access.
    items = ws.workspace_items
    item_count = sum(len(v) for v in items.values()) if isinstance(items, dict) else len(items)

    logging.info("VALIDATION found %d item(s) in test workspace", item_count)

    if item_count < min_items:
        raise AssertionError(
            f"VALIDATION FAILED: expected at least {min_items} item(s), found {item_count}. "
            "The test deployment may not have succeeded."
        )

    logging.info(
        "VALIDATION PASSED: workspace %s is reachable and contains %d item(s). "
        "Proceeding to production is safe.",
        ws_id,
        item_count,
    )


def main() -> int:
    setup_logging()
    retries = int(os.getenv("FABRIC_MAX_RETRIES", "2"))
    sleep_s = int(os.getenv("FABRIC_RETRY_SLEEP", "10"))

    for attempt in range(1, retries + 2):
        try:
            logging.info("VALIDATION attempt %d/%d", attempt, retries + 1)
            run_validation()
            return 0
        except Exception as e:
            logging.error("VALIDATION failed: %s", e, exc_info=True)
            if attempt >= retries + 1:
                logging.error(
                    "VALIDATION FAILED after %d attempt(s). "
                    "Production deployment will NOT proceed.",
                    attempt,
                )
                return 1
            logging.warning("Retrying VALIDATION in %ds...", sleep_s)
            time.sleep(sleep_s)

    return 1  # should never reach here, but be explicit


if __name__ == "__main__":
    raise SystemExit(main())
