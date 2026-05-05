#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import time
import urllib.request
from pathlib import Path

from azure.identity import ClientSecretCredential
from fabric_cicd import FabricWorkspace, publish_all_items, unpublish_all_orphan_items

ENV_NAME = "test"


def setup_logging():
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
    # Uses your variable directly
    return os.getenv("FABRIC_WS_TEST", "").strip()


def get_repo_dir() -> Path:
    """Return the repository root directory.

    In GitHub Actions, the checked-out repository root is typically:
      $GITHUB_WORKSPACE == /home/runner/work/<repo>/<repo>

    This script lives in the repo root, so the safest default is the directory
    containing this file, not a parent directory.

    Users can override via FABRIC_REPO_DIR.
    """

    # Prefer explicit override.
    override = os.getenv("FABRIC_REPO_DIR", "").strip()
    if override:
        repo_dir = Path(override).expanduser().resolve()
    else:
        repo_dir = Path(__file__).resolve().parent

    if not repo_dir.exists():
        raise ValueError(f"Repository directory not found: {repo_dir}")
    return repo_dir


def token_credential() -> ClientSecretCredential:
    return ClientSecretCredential(
        tenant_id=required("AZURE_TENANT_ID"),
        client_id=required("AZURE_CLIENT_ID"),
        client_secret=required("AZURE_CLIENT_SECRET"),
    )


def resolve_lakehouse_ids(ws_id: str, credential: ClientSecretCredential) -> dict:
    """Query the Fabric REST API and return a mapping of Lakehouse name -> item id
    for every Lakehouse deployed in the target workspace.

    Logs each resolved mapping so the exact target IDs are visible in CI output.
    Raises RuntimeError if no Lakehouses are found.
    """
    token = credential.get_token("https://api.fabric.microsoft.com/.default").token
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items?type=Lakehouse"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        raise RuntimeError(
            f"Fabric API returned HTTP {exc.code} when listing Lakehouses in workspace {ws_id}: {body}"
        ) from exc

    mapping: dict = {}
    for item in data.get("value", []):
        name = item.get("displayName", "")
        item_id = item.get("id", "")
        if name and item_id:
            mapping[name] = item_id
            logging.info(
                "Resolved Lakehouse '%s' -> id=%s in target workspace %s",
                name,
                item_id,
                ws_id,
            )

    if not mapping:
        raise RuntimeError(
            f"No Lakehouses found in target workspace {ws_id} after Lakehouse publish phase. "
            "Ensure at least one Lakehouse item exists in the repository and was published successfully."
        )
    return mapping


def run_deploy(parameter_file: str | None = None):
    ws_id = get_workspace_id()
    if not ws_id:
        raise ValueError("FABRIC_WS_TEST is empty/missing.")

    repo_dir = get_repo_dir()

    # TEST: recommended to keep envs aligned; default cleanup_orphans to true
    cleanup_orphans = os.getenv("FABRIC_CLEANUP_ORPHANS", "true").lower() == "true"

    # Allow scoping via env var (comma-separated item types)
    items_raw = os.getenv("FABRIC_ITEMS_IN_SCOPE", "").strip()
    items_in_scope = [x.strip() for x in items_raw.split(",") if x.strip()] if items_raw else None

    # Skip a known-bad semantic model by restricting the scope to everything *except*
    # semantic models. This prevents the pipeline from failing on import errors like:
    # Dataset_Import_FailedToImportDataset for 'poc-aas-direct-lake-adb'.
    #
    # If you later fix the model, set FABRIC_PUBLISH_SEMANTIC_MODELS=true to re-enable.
    publish_semantic_models = os.getenv("FABRIC_PUBLISH_SEMANTIC_MODELS", "false").lower() == "true"
    if not publish_semantic_models:
        # Only override when user didn't already set scope explicitly.
        if items_in_scope is None:
            # Typical Fabric item type names used by fabric-cicd are like 'Report', 'Notebook', 'SemanticModel', etc.
            items_in_scope = [
                "Report",
                "Notebook",
                "Lakehouse",
                "Warehouse",
                "DataPipeline",
                "Environment",
                "KQLDatabase",
            ]
            logging.warning(
                "Semantic model publishing is disabled (FABRIC_PUBLISH_SEMANTIC_MODELS=false). "
                "Deploying only item types: %s",
                items_in_scope,
            )

    # Optional safety: require explicit marker
    expected = os.getenv("FABRIC_ENV", ENV_NAME).strip().lower()
    if expected not in ("test", "tst", "qa", "uat"):
        raise PermissionError(f"Refusing TEST deploy: FABRIC_ENV must indicate test (got '{expected}')")

    logging.info(
        "TEST deploy starting workspace_id=%s repo_dir=%s parameter_file=%s",
        ws_id,
        repo_dir,
        parameter_file,
    )

    # Create a single credential instance shared across all API calls.
    cred = token_credential()

    # Base kwargs shared by both deploy phases.
    base_kwargs = dict(
        workspace_id=ws_id,
        repository_directory=str(repo_dir),
        environment=ENV_NAME,
        token_credential=cred,
    )

    ws_kwargs = dict(base_kwargs)
    ws_kwargs["item_type_in_scope"] = items_in_scope

    # Pass parameter file through to fabric-cicd if provided.
    if parameter_file:
        ws_kwargs["parameter_file"] = parameter_file

    # ---------------------------------------------------------------------------
    # Phase 1: Publish Lakehouses first so they exist in the target workspace
    # before notebooks (which reference them) are published.
    # ---------------------------------------------------------------------------
    scope_includes_lakehouse = items_in_scope is None or "Lakehouse" in items_in_scope
    if scope_includes_lakehouse:
        logging.info("Phase 1: Publishing Lakehouses to target workspace...")
        lh_ws = FabricWorkspace(
            **base_kwargs,
            item_type_in_scope=["Lakehouse"],
        )
        publish_all_items(lh_ws)
        logging.info("Phase 1: Lakehouses published successfully.")

        # Resolve deployed Lakehouse IDs from the target workspace and log them
        # so the mapping is visible in CI output and references can be verified.
        lh_mapping = resolve_lakehouse_ids(ws_id, cred)
        logging.info(
            "Lakehouse ID mapping resolved (%d Lakehouse(s) available for notebook references):",
            len(lh_mapping),
        )
        for lh_name, lh_id in lh_mapping.items():
            logging.info("  '%s' -> %s", lh_name, lh_id)
    else:
        logging.info(
            "Lakehouse not in items_in_scope (%s); skipping Phase 1 Lakehouse pre-publish.",
            items_in_scope,
        )

    # ---------------------------------------------------------------------------
    # Phase 2: Publish all items in scope.  Lakehouses published in Phase 1 will
    # be detected as already-current and skipped or updated idempotently.
    # Notebooks will now find their referenced Lakehouses in the workspace.
    # ---------------------------------------------------------------------------
    logging.info("Phase 2: Publishing all items in scope...")
    ws = FabricWorkspace(**ws_kwargs)
    publish_all_items(ws)
    logging.info("TEST publish completed")

    if cleanup_orphans:
        unpublish_all_orphan_items(ws)
        logging.info("TEST orphan cleanup completed")


def main() -> int:
    setup_logging()

    parser = argparse.ArgumentParser(description="Deploy Fabric items to TEST workspace")
    parser.add_argument(
        "--parameter_file",
        default=os.getenv("FABRIC_PARAMETER_FILE", "").strip() or None,
        help="Path to parameter YAML file (e.g., parameter.yml) used for find/replace during publish.",
    )
    args = parser.parse_args()

    retries = int(os.getenv("FABRIC_MAX_RETRIES", "3"))
    sleep_s = int(os.getenv("FABRIC_RETRY_SLEEP", "10"))

    for attempt in range(1, retries + 2):
        try:
            logging.info("TEST attempt %d/%d", attempt, retries + 1)
            run_deploy(parameter_file=args.parameter_file)
            return 0
        except Exception as e:
            logging.error("TEST deployment failed: %s", e, exc_info=True)
            if attempt >= retries + 1:
                return 1
            logging.warning("Retrying TEST in %ds...", sleep_s)
            time.sleep(sleep_s)


if __name__ == "__main__":
    raise SystemExit(main())
