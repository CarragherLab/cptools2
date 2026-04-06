"""
Container path resolution and validation for cptools2.

Resolves the path to Singularity .sif container images using a safe
precedence chain. No site-specific paths are hardcoded — all paths come
from user configuration.

Resolution order:
    1. YAML config ``container_path`` key   — explicit per-experiment override
    2. ``CPTOOLS2_CONTAINER_DIR`` env var    — site-wide directory
    3. Neither set                           — returns None (legacy conda mode)

The env var points to a single directory containing all .sif files for the
lab's pipelines (CellProfiler, DeepProfiler, Cellpose, etc.). Individual
containers are looked up by role name (e.g., "cellprofiler", "deepprofiler")
using either the manifest file or the default image name.

A ``cptools2_containers.json`` manifest file may exist alongside the .sif
files. It records which containers are available, their versions, and
whether they have been validated on the cluster. This provides a runtime
safety check before generating hundreds of job scripts.

Safety model (three layers):
    1. Installer discovers group spaces and finds existing containers,
       then sets CPTOOLS2_CONTAINER_DIR in ~/.bashrc.
    2. Manifest file lives alongside .sif files recording what's available.
    3. cptools2 validates at generate-time before producing job scripts.
"""

import json
import os

from cptools2.colours import pretty_print, yellow, red, green


# ─── Default image names per role ───────────────────────────────────────────────
# These are the expected filenames within the container directory when no
# manifest is present. The manifest overrides these if available.
DEFAULT_CONTAINERS = {
    "cellprofiler": "cellprofiler_4.2.8.sif",
    "deepprofiler": "deepprofiler_1.0.sif",
    "cellpose": "cellpose_sam_1.0.sif",
}

# Manifest filename — lives alongside .sif files in the container directory
MANIFEST_FILENAME = "cptools2_containers.json"


def resolve_container_dir(yaml_dict=None):
    """
    Resolve the container directory from config or environment.

    Parameters
    ----------
    yaml_dict : dict, optional
        Parsed YAML config. If it contains a ``container_path`` key pointing
        to a specific .sif file, the parent directory is used. If it points
        to a directory, it is used directly.

    Returns
    -------
    str or None
        Path to the container directory, or None if not configured.
    """
    # Priority 1: explicit path in YAML config
    if yaml_dict and "container_path" in yaml_dict:
        path = yaml_dict["container_path"]
        if isinstance(path, list):
            path = path[0]
        path = os.path.expandvars(str(path))
        if path.endswith(".sif"):
            return os.path.dirname(path)
        return path

    # Priority 2: environment variable
    container_dir = os.environ.get("CPTOOLS2_CONTAINER_DIR")
    if container_dir:
        return os.path.expandvars(container_dir)

    return None


def resolve_container_path(yaml_dict=None, role="cellprofiler"):
    """
    Resolve the full path to a container image by its role.

    The role determines which .sif file to look for. For the main cptools2
    pipeline this is always "cellprofiler", but the illumination correction
    and downstream stages use "deepprofiler" and "cellpose" as well.

    Resolution:
        1. If YAML has a direct .sif path, return it (ignores role).
        2. Read the manifest to find the image name for the given role.
        3. Fall back to the DEFAULT_CONTAINERS mapping if no manifest.

    Parameters
    ----------
    yaml_dict : dict, optional
        Parsed YAML config.
    role : str
        Container role to look up. One of: "cellprofiler", "deepprofiler",
        "cellpose". Defaults to "cellprofiler".

    Returns
    -------
    str or None
        Full path to the .sif file, or None if not configured.
    """
    # If YAML has a direct .sif path, return it (role-agnostic override)
    if yaml_dict and "container_path" in yaml_dict:
        path = yaml_dict["container_path"]
        if isinstance(path, list):
            path = path[0]
        path = os.path.expandvars(str(path))
        if path.endswith(".sif"):
            return path

    # Resolve directory
    container_dir = resolve_container_dir(yaml_dict)
    if container_dir is None:
        return None

    # Determine image name: manifest first, then defaults
    image_name = _image_name_for_role(container_dir, role)
    return os.path.join(container_dir, image_name)


def _image_name_for_role(container_dir, role):
    """
    Look up the image filename for a given role.

    Checks the manifest first (authoritative), then falls back to the
    hardcoded defaults in DEFAULT_CONTAINERS.

    Parameters
    ----------
    container_dir : str
        Path to the container directory.
    role : str
        Container role (e.g., "cellprofiler").

    Returns
    -------
    str
        Image filename (e.g., "cellprofiler_4.2.8.sif").
    """
    # Try manifest
    manifest = read_manifest(container_dir)
    if manifest is not None:
        containers = manifest.get("containers", {})
        if role in containers:
            return containers[role]["image"]

    # Fall back to defaults
    if role in DEFAULT_CONTAINERS:
        return DEFAULT_CONTAINERS[role]

    raise ValueError(
        f"Unknown container role: '{role}'. "
        f"Known roles: {', '.join(DEFAULT_CONTAINERS.keys())}. "
        f"Check the manifest at {container_dir}/{MANIFEST_FILENAME} "
        f"or update DEFAULT_CONTAINERS in containers.py."
    )


def read_manifest(container_dir):
    """
    Read the container manifest from a directory.

    Parameters
    ----------
    container_dir : str
        Path to the container directory.

    Returns
    -------
    dict or None
        Parsed manifest, or None if not found or unreadable.
    """
    manifest_path = os.path.join(container_dir, MANIFEST_FILENAME)
    if not os.path.isfile(manifest_path):
        return None
    try:
        with open(manifest_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        pretty_print(f"Warning: could not read manifest at {manifest_path}: {e}")
        return None


def validate_container_path(resolved_path):
    """
    Quick format validation of a resolved container path.

    Lightweight check used during YAML parsing — validates the path format
    (.sif extension) without touching the filesystem.

    Parameters
    ----------
    resolved_path : str or None
        Container path from ``resolve_container_path()``.

    Returns
    -------
    str or None
        The path, unchanged.

    Raises
    ------
    ValueError
        If the path doesn't end with .sif.
    """
    if resolved_path is None:
        return None
    if not resolved_path.endswith(".sif"):
        raise ValueError(
            f"Container path does not end with .sif: '{resolved_path}'\n"
            f"Check your YAML 'container_path' or CPTOOLS2_CONTAINER_DIR."
        )
    return resolved_path


def validate_container_setup(yaml_dict=None, role="cellprofiler", require_container=False):
    """
    Full validation of container configuration with filesystem and manifest checks.

    Call at the start of ``cptools2 generate`` to catch problems before
    generating job scripts.

    Parameters
    ----------
    yaml_dict : dict, optional
        Parsed YAML config.
    role : str
        Container role to validate (default: "cellprofiler").
    require_container : bool
        If True, raise error when not configured. If False, return None.

    Returns
    -------
    str or None
        Validated container path, or None if not configured.
    """
    container_path = resolve_container_path(yaml_dict, role=role)

    if container_path is None:
        if require_container:
            raise ValueError(
                "No container path configured. Set one of:\n"
                "  1. 'container_path' in your YAML config\n"
                "  2. CPTOOLS2_CONTAINER_DIR environment variable\n"
                "     (set by the cptools2 installer, or add to ~/.bashrc)\n"
                "\n"
                "Run the installer or see .env.example for details."
            )
        return None

    # Format check
    validate_container_path(container_path)

    # Filesystem check (only if path is accessible from this machine)
    container_dir = os.path.dirname(container_path)
    if os.path.isdir(container_dir):
        if not os.path.isfile(container_path):
            raise FileNotFoundError(
                f"Container not found: {container_path}\n"
                f"Directory exists ({container_dir}) but .sif is missing.\n"
                f"Build it first — see Guide 04 in dev_docs."
            )

        # Manifest status reporting
        manifest = read_manifest(container_dir)
        if manifest is not None:
            _report_manifest_status(manifest, container_path)

        pretty_print(f"Container validated: {green(os.path.basename(container_path))} "
                     f"at {container_dir}")
    else:
        # Generating locally for Eddie — can't check filesystem
        pretty_print(
            f"Container path resolved: {yellow(container_path)}\n"
            f"  (not accessible from this machine — validated at runtime)"
        )

    return container_path


def is_gpu_container(container_dir, role):
    """
    Check whether a container for a given role requires GPU resources.

    Reads the ``gpu`` field from the manifest entry for the specified role.
    If no manifest exists or the role is not listed, returns False (assume CPU).

    Parameters
    ----------
    container_dir : str
        Path to the container directory.
    role : str
        Container role to check (e.g., "cellprofiler", "deepprofiler").

    Returns
    -------
    bool
        True if the manifest indicates GPU is required, False otherwise.
    """
    manifest = read_manifest(container_dir)
    if manifest is None:
        return False
    containers = manifest.get("containers", {})
    if role not in containers:
        return False
    return containers[role].get("gpu", False)


def list_available_containers(yaml_dict=None):
    """
    List all containers available in the configured container directory.

    Useful for diagnostic output and for the `illum` CLI to determine
    which pipeline stages can run.

    Parameters
    ----------
    yaml_dict : dict, optional
        Parsed YAML config.

    Returns
    -------
    dict
        Mapping of role → {path, version, verified, exists}.
        Empty dict if no container directory configured.
    """
    container_dir = resolve_container_dir(yaml_dict)
    if container_dir is None:
        return {}

    manifest = read_manifest(container_dir)
    result = {}

    for role, default_image in DEFAULT_CONTAINERS.items():
        # Get image name from manifest if available
        image_name = default_image
        version = "unknown"
        verified = False

        if manifest:
            containers = manifest.get("containers", {})
            if role in containers:
                image_name = containers[role].get("image", default_image)
                version = containers[role].get("version", "unknown")
                verified = containers[role].get("verified", False)

        full_path = os.path.join(container_dir, image_name)
        exists = os.path.isfile(full_path) if os.path.isdir(container_dir) else None

        result[role] = {
            "path": full_path,
            "image": image_name,
            "version": version,
            "verified": verified,
            "exists": exists,  # None = can't check, True/False = checked
        }

    return result


def _report_manifest_status(manifest, container_path):
    """Print manifest-based status information for a specific container."""
    containers = manifest.get("containers", {})
    image_name = os.path.basename(container_path)

    for key, entry in containers.items():
        if entry.get("image") == image_name:
            version = entry.get("version", "unknown")
            verified = entry.get("verified", False)
            if verified:
                pretty_print(f"  Manifest: {image_name} v{version} — {green('verified')}")
            else:
                pretty_print(
                    f"  Manifest: {image_name} v{version} — {yellow('not yet verified')}\n"
                    f"  Verify with: qlogin -l h_rss=8G && module load singularity && "
                    f"singularity exec {container_path} cellprofiler --version"
                )
            return

    pretty_print(
        f"  Warning: {image_name} not listed in manifest. "
        f"May have been built manually."
    )
