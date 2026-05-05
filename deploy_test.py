#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
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
    repo_dir = Path(os.getenv("FABRIC_REPO_DIR", Path(__file__).resolve().parents[1])).resolve()
    if not repo_dir.exists():
        raise ValueError(f"Repository directory not found: {repo_dir}")
    return repo_dir


def token_credential() -> ClientSecretCredential:
    return ClientSecretCredential(
        tenant_id=required("AZURE_TENANT_ID"),
        client_id=required("AZURE_CLIENT_ID"),
        client_secret=required("AZURE_CLIENT_SECRET"),
    )


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

    ws_kwargs = dict(
        workspace_id=ws_id,
        repository_directory=str(repo_dir),
        environment=ENV_NAME,
        item_type_in_scope=items_in_scope,
        token_credential=token_credential(),
    )

    # Pass parameter file through to fabric-cicd if provided.
    if parameter_file:
        ws_kwargs["parameter_file"] = parameter_file

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
