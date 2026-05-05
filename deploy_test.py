#!/usr/bin/env python3
import argparse
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import yaml
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


def get_repo_lakehouses(repo_dir: Path) -> set[str]:
    """Scan the repository for Lakehouse item definitions.

    Returns the set of display names for every Lakehouse found under a
    ``*.Lakehouse/.platform`` file in the repository.
    """
    result: set[str] = set()
    for platform_file in repo_dir.rglob(".platform"):
        if not platform_file.parent.name.endswith(".Lakehouse"):
            continue
        try:
            data = json.loads(platform_file.read_text(encoding="utf-8"))
            if data.get("metadata", {}).get("type") != "Lakehouse":
                continue
            name = data["metadata"]["displayName"]
            result.add(name)
            logging.info("Repo lakehouse discovered: displayName=%s", name)
        except (KeyError, json.JSONDecodeError) as exc:
            logging.warning("Could not parse lakehouse platform file %s: %s", platform_file, exc)
    return result


def parse_notebook_meta(content: str) -> dict:
    """Parse the first ``# META { ... # META }`` block from a Fabric notebook file.

    Returns the parsed metadata as a dict, or an empty dict if the block is
    absent or cannot be parsed as JSON.
    """
    lines = content.splitlines()
    meta_lines: list[str] = []
    in_meta = False
    for line in lines:
        stripped = line.strip()
        if not in_meta:
            if stripped == "# META {":
                in_meta = True
                meta_lines = []
            continue
        # Root-level closing brace ends the block.
        if stripped == "# META }":
            meta_lines.append("}")
            break
        if stripped.startswith("# META "):
            meta_lines.append(stripped[7:])
        elif stripped == "# META":
            meta_lines.append("")
    if not meta_lines:
        return {}
    try:
        return json.loads("{\n" + "\n".join(meta_lines))
    except json.JSONDecodeError as exc:
        logging.debug("Could not parse notebook META block: %s", exc)
        return {}


def build_lakehouse_mapping(repo_dir: Path) -> tuple[dict[str, str], set[str]]:
    """Scan all notebooks in the repo and build a lakehouse ID → display-name mapping.

    For every notebook that declares a ``default_lakehouse`` dependency the
    function verifies that the referenced lakehouse exists as a Lakehouse item
    in the repository.  It also collects the DEV workspace IDs that appear in
    notebook metadata so they can be replaced with the target workspace ID.

    Returns:
        id_to_name: mapping of DEV lakehouse item ID → display name for every
            mappable lakehouse reference found across all notebooks.
        workspace_ids: set of DEV workspace IDs found in notebook metadata.

    Raises:
        ValueError: if a notebook's ``default_lakehouse_name`` does not match
            any Lakehouse item in the repository, or if the same lakehouse ID
            is associated with conflicting names across notebooks.
    """
    repo_lakehouses = get_repo_lakehouses(repo_dir)
    logging.info("Repo lakehouses available: %s", sorted(repo_lakehouses))

    id_to_name: dict[str, str] = {}
    workspace_ids: set[str] = set()
    unknown_ids: set[str] = set()

    for notebook_dir in sorted(repo_dir.rglob("*.Notebook")):
        if not notebook_dir.is_dir():
            continue
        notebook_content_file = notebook_dir / "notebook-content.py"
        if not notebook_content_file.exists():
            continue

        rel_path = notebook_content_file.relative_to(repo_dir)
        content = notebook_content_file.read_text(encoding="utf-8")
        meta = parse_notebook_meta(content)

        lakehouse_info = meta.get("dependencies", {}).get("lakehouse", {})
        if not lakehouse_info:
            logging.info("Notebook '%s': no lakehouse dependency found", rel_path)
            continue

        default_id: str | None = lakehouse_info.get("default_lakehouse")
        default_name: str | None = lakehouse_info.get("default_lakehouse_name")
        default_ws: str | None = lakehouse_info.get("default_lakehouse_workspace_id")
        known: list[str] = [k["id"] for k in lakehouse_info.get("known_lakehouses", []) if "id" in k]

        if default_ws:
            workspace_ids.add(default_ws)

        if default_id and default_name:
            if default_name not in repo_lakehouses:
                raise ValueError(
                    f"Notebook '{rel_path}' references default lakehouse '{default_name}' "
                    f"(id={default_id}) which is not found as a Lakehouse item in the "
                    f"repository.  Available repo lakehouses: {sorted(repo_lakehouses)}"
                )
            existing_name = id_to_name.get(default_id)
            if existing_name and existing_name != default_name:
                raise ValueError(
                    f"Conflicting lakehouse name for id={default_id}: previously mapped to "
                    f"'{existing_name}', but notebook '{rel_path}' maps it to '{default_name}'."
                )
            id_to_name[default_id] = default_name
            logging.info(
                "Notebook '%s' -> default_lakehouse: %s (id=%s)",
                rel_path,
                default_name,
                default_id,
            )
        elif default_id:
            logging.warning(
                "Notebook '%s' has default_lakehouse id=%s but no default_lakehouse_name; "
                "cannot map to a repo lakehouse.",
                rel_path,
                default_id,
            )

        for known_id in known:
            if known_id == default_id or known_id in id_to_name:
                continue
            unknown_ids.add(known_id)

    if unknown_ids:
        logging.warning(
            "The following lakehouse IDs appear in 'known_lakehouses' entries across notebooks "
            "but cannot be mapped to any Lakehouse item in the repository (they will not be "
            "replaced in the target workspace): %s",
            sorted(unknown_ids),
        )

    logging.info("Lakehouse ID → name mapping: %s", id_to_name)
    logging.info("DEV workspace IDs found in notebooks: %s", sorted(workspace_ids))
    return id_to_name, workspace_ids


def generate_parameter_yaml(
    id_to_name: dict[str, str],
    workspace_ids: set[str],
    env: str,
) -> str:
    """Generate a ``parameter.yml``-compatible YAML string.

    Produces ``find_replace`` rules that:

    * Replace each DEV workspace ID with ``$workspace.$id`` (the target workspace ID).
    * Replace each DEV lakehouse item ID with ``$items.Lakehouse.<display_name>.$id``
      so that ``fabric-cicd`` resolves the newly-created target lakehouse ID at
      publish time.

    Returns an empty string when there are no rules to generate.
    """
    rules: list[dict] = []

    for ws_id in sorted(workspace_ids):
        rules.append(
            {
                "find_value": ws_id,
                "replace_value": {env: "$workspace.$id"},
                "item_type": "Notebook",
            }
        )

    for src_id, display_name in sorted(id_to_name.items()):
        rules.append(
            {
                "find_value": src_id,
                "replace_value": {env: f"$items.Lakehouse.{display_name}.$id"},
                "item_type": "Notebook",
            }
        )

    if not rules:
        return ""
    return yaml.dump({"find_replace": rules}, default_flow_style=False, allow_unicode=True, sort_keys=False)


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

    # Auto-detect lakehouse bindings from notebooks and generate parameter replacements.
    # Lakehouses are published before Notebooks in fabric-cicd's default ordering
    # (position 4 vs 10), so $items.Lakehouse.<name>.$id references will always
    # resolve correctly within a single publish_all_items() call.
    id_to_name, workspace_ids = build_lakehouse_mapping(repo_dir)
    param_yaml = generate_parameter_yaml(id_to_name, workspace_ids, ENV_NAME)

    auto_param_path: str | None = None
    try:
        if param_yaml:
            fd, auto_param_path = tempfile.mkstemp(suffix=".yml", prefix="fabric_params_")
            os.close(fd)
            os.chmod(auto_param_path, 0o600)
            Path(auto_param_path).write_text(param_yaml, encoding="utf-8")
            logging.info(
                "Auto-generated parameter file written to %s:\n%s",
                auto_param_path,
                param_yaml,
            )

        logging.info(
            "TEST deploy starting workspace_id=%s repo_dir=%s auto_param=%s",
            ws_id,
            repo_dir,
            auto_param_path,
        )

        ws_kwargs = dict(
            workspace_id=ws_id,
            repository_directory=str(repo_dir),
            environment=ENV_NAME,
            item_type_in_scope=items_in_scope,
            token_credential=token_credential(),
        )

        # Prefer the auto-generated parameter file; fall back to any user-supplied file.
        if auto_param_path:
            ws_kwargs["parameter_file"] = auto_param_path
        elif parameter_file:
            ws_kwargs["parameter_file"] = parameter_file

        ws = FabricWorkspace(**ws_kwargs)

        publish_all_items(ws)
        logging.info("TEST publish completed")

        if cleanup_orphans:
            unpublish_all_orphan_items(ws)
            logging.info("TEST orphan cleanup completed")

    finally:
        if auto_param_path:
            try:
                Path(auto_param_path).unlink(missing_ok=True)
            except OSError:
                pass


def main() -> int:
    setup_logging()

    parser = argparse.ArgumentParser(description="Deploy Fabric items to TEST workspace")
    parser.add_argument(
        "--parameter_file",
        default=os.getenv("FABRIC_PARAMETER_FILE", "").strip() or None,
        help=(
            "Path to an optional parameter YAML file for additional find/replace overrides. "
            "Lakehouse bindings and workspace ID replacements are now auto-detected from the "
            "repository and do not need to be specified here."
        ),
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
