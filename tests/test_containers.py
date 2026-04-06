"""Tests for cptools2.containers module."""

import json
import os

import pytest

from cptools2.containers import (
    DEFAULT_CONTAINERS,
    MANIFEST_FILENAME,
    _image_name_for_role,
    is_gpu_container,
    list_available_containers,
    read_manifest,
    resolve_container_dir,
    resolve_container_path,
    validate_container_path,
    validate_container_setup,
)


# --- Fixtures ----------------------------------------------------------------


@pytest.fixture()
def container_dir(tmp_path):
    """Create a container directory with default .sif files."""
    for name in DEFAULT_CONTAINERS.values():
        (tmp_path / name).write_text("fake sif")
    return str(tmp_path)


@pytest.fixture()
def manifest_data():
    """Canonical manifest dict for testing."""
    return {
        "containers": {
            "cellprofiler": {
                "image": "cellprofiler_4.2.8.sif",
                "version": "4.2.8",
                "verified": True,
            },
            "deepprofiler": {
                "image": "deepprofiler_1.0.sif",
                "version": "1.0",
                "verified": False,
            },
        }
    }


@pytest.fixture()
def container_dir_with_manifest(container_dir, manifest_data):
    """Container directory that also has a manifest file."""
    manifest_path = os.path.join(container_dir, MANIFEST_FILENAME)
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f)
    return container_dir


# --- resolve_container_dir ---------------------------------------------------


class TestResolveContainerDir:
    def test_yaml_with_sif_file_returns_parent(self, tmp_path):
        sif = tmp_path / "cellprofiler_4.2.8.sif"
        sif.write_text("fake")
        yaml_dict = {"container_path": str(sif)}
        result = resolve_container_dir(yaml_dict)
        assert result == str(tmp_path)

    def test_yaml_with_directory(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        result = resolve_container_dir(yaml_dict)
        assert result == container_dir

    def test_yaml_with_list_container_path(self, tmp_path):
        sif = tmp_path / "cp.sif"
        sif.write_text("fake")
        yaml_dict = {"container_path": [str(sif)]}
        result = resolve_container_dir(yaml_dict)
        assert result == str(tmp_path)

    def test_env_var_fallback(self, monkeypatch, container_dir):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        monkeypatch.setenv("CPTOOLS2_CONTAINER_DIR", container_dir)
        result = resolve_container_dir()
        assert result == container_dir

    def test_env_var_used_when_no_yaml(self, monkeypatch, container_dir):
        monkeypatch.setenv("CPTOOLS2_CONTAINER_DIR", container_dir)
        result = resolve_container_dir(yaml_dict={})
        assert result == container_dir

    def test_yaml_takes_priority_over_env(self, monkeypatch, container_dir):
        monkeypatch.setenv("CPTOOLS2_CONTAINER_DIR", "/some/other/dir")
        yaml_dict = {"container_path": container_dir}
        result = resolve_container_dir(yaml_dict)
        assert result == container_dir

    def test_returns_none_when_nothing_set(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = resolve_container_dir()
        assert result is None

    def test_returns_none_with_empty_yaml(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = resolve_container_dir({})
        assert result is None

    def test_none_yaml_dict(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = resolve_container_dir(None)
        assert result is None


# --- resolve_container_path --------------------------------------------------


class TestResolveContainerPath:
    def test_direct_sif_in_yaml(self, tmp_path):
        sif = tmp_path / "my_container.sif"
        sif.write_text("fake")
        yaml_dict = {"container_path": str(sif)}
        result = resolve_container_path(yaml_dict)
        assert result == str(sif)

    def test_direct_sif_ignores_role(self, tmp_path):
        sif = tmp_path / "my_container.sif"
        sif.write_text("fake")
        yaml_dict = {"container_path": str(sif)}
        result = resolve_container_path(yaml_dict, role="deepprofiler")
        assert result == str(sif)

    def test_directory_with_manifest(self, container_dir_with_manifest):
        yaml_dict = {"container_path": container_dir_with_manifest}
        result = resolve_container_path(yaml_dict, role="cellprofiler")
        expected = os.path.join(container_dir_with_manifest, "cellprofiler_4.2.8.sif")
        assert result == expected

    def test_directory_with_manifest_different_role(self, container_dir_with_manifest):
        yaml_dict = {"container_path": container_dir_with_manifest}
        result = resolve_container_path(yaml_dict, role="deepprofiler")
        expected = os.path.join(container_dir_with_manifest, "deepprofiler_1.0.sif")
        assert result == expected

    def test_directory_with_defaults(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        result = resolve_container_path(yaml_dict, role="cellprofiler")
        expected = os.path.join(container_dir, DEFAULT_CONTAINERS["cellprofiler"])
        assert result == expected

    def test_unknown_role_raises(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        with pytest.raises(ValueError, match="Unknown container role"):
            resolve_container_path(yaml_dict, role="nonexistent")

    def test_returns_none_when_not_configured(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = resolve_container_path()
        assert result is None

    def test_env_var_directory(self, monkeypatch, container_dir):
        monkeypatch.setenv("CPTOOLS2_CONTAINER_DIR", container_dir)
        result = resolve_container_path(role="cellpose")
        expected = os.path.join(container_dir, DEFAULT_CONTAINERS["cellpose"])
        assert result == expected

    def test_list_container_path_sif(self, tmp_path):
        sif = tmp_path / "custom.sif"
        sif.write_text("fake")
        yaml_dict = {"container_path": [str(sif)]}
        result = resolve_container_path(yaml_dict)
        assert result == str(sif)


# --- read_manifest -----------------------------------------------------------


class TestReadManifest:
    def test_valid_manifest(self, container_dir_with_manifest, manifest_data):
        result = read_manifest(container_dir_with_manifest)
        assert result == manifest_data

    def test_missing_manifest_returns_none(self, container_dir):
        result = read_manifest(container_dir)
        assert result is None

    def test_invalid_json_returns_none(self, container_dir, capsys):
        manifest_path = os.path.join(container_dir, MANIFEST_FILENAME)
        with open(manifest_path, "w") as f:
            f.write("{not valid json!!!")
        result = read_manifest(container_dir)
        assert result is None
        captured = capsys.readouterr()
        assert "Warning" in captured.out

    def test_nonexistent_directory_returns_none(self):
        result = read_manifest("/nonexistent/path/that/does/not/exist")
        assert result is None


# --- validate_container_path -------------------------------------------------


class TestValidateContainerPath:
    def test_valid_sif_path(self):
        path = "/containers/cellprofiler_4.2.8.sif"
        assert validate_container_path(path) == path

    def test_non_sif_raises(self):
        with pytest.raises(ValueError, match="does not end with .sif"):
            validate_container_path("/containers/cellprofiler.img")

    def test_none_returns_none(self):
        assert validate_container_path(None) is None

    def test_directory_path_raises(self):
        with pytest.raises(ValueError, match="does not end with .sif"):
            validate_container_path("/containers/")


# --- validate_container_setup ------------------------------------------------


class TestValidateContainerSetup:
    def test_not_configured_require_false_returns_none(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = validate_container_setup(require_container=False)
        assert result is None

    def test_not_configured_require_true_raises(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        with pytest.raises(ValueError, match="No container path configured"):
            validate_container_setup(require_container=True)

    def test_valid_setup_returns_path(self, container_dir, capsys):
        sif_name = DEFAULT_CONTAINERS["cellprofiler"]
        yaml_dict = {"container_path": container_dir}
        result = validate_container_setup(yaml_dict, role="cellprofiler")
        assert result == os.path.join(container_dir, sif_name)
        captured = capsys.readouterr()
        assert "validated" in captured.out.lower() or "Container" in captured.out

    def test_missing_sif_file_raises(self, tmp_path):
        # Directory exists but no .sif file
        yaml_dict = {"container_path": str(tmp_path)}
        with pytest.raises(FileNotFoundError, match="Container not found"):
            validate_container_setup(yaml_dict, role="cellprofiler")

    def test_inaccessible_dir_prints_warning(self, capsys):
        yaml_dict = {"container_path": "/nonexistent/remote/path/cp.sif"}
        result = validate_container_setup(yaml_dict)
        assert result == "/nonexistent/remote/path/cp.sif"
        captured = capsys.readouterr()
        assert "not accessible" in captured.out or "runtime" in captured.out

    def test_with_manifest_verified(self, container_dir_with_manifest, capsys):
        yaml_dict = {"container_path": container_dir_with_manifest}
        result = validate_container_setup(yaml_dict, role="cellprofiler")
        assert result.endswith(".sif")
        captured = capsys.readouterr()
        assert "verified" in captured.out.lower()

    def test_with_manifest_unverified(self, container_dir_with_manifest, capsys):
        yaml_dict = {"container_path": container_dir_with_manifest}
        result = validate_container_setup(yaml_dict, role="deepprofiler")
        assert result.endswith(".sif")
        captured = capsys.readouterr()
        assert "not yet verified" in captured.out.lower()


# --- is_gpu_container --------------------------------------------------------


class TestIsGpuContainer:
    def test_gpu_true_in_manifest(self, tmp_path):
        """Container marked as GPU in manifest should return True."""
        manifest = {
            "containers": {
                "deepprofiler": {
                    "image": "deepprofiler_1.0.sif",
                    "version": "1.0",
                    "gpu": True,
                }
            }
        }
        with open(tmp_path / MANIFEST_FILENAME, "w") as f:
            json.dump(manifest, f)
        assert is_gpu_container(str(tmp_path), "deepprofiler") is True

    def test_gpu_false_in_manifest(self, tmp_path):
        """Container explicitly marked as not GPU should return False."""
        manifest = {
            "containers": {
                "cellprofiler": {
                    "image": "cellprofiler_4.2.8.sif",
                    "version": "4.2.8",
                    "gpu": False,
                }
            }
        }
        with open(tmp_path / MANIFEST_FILENAME, "w") as f:
            json.dump(manifest, f)
        assert is_gpu_container(str(tmp_path), "cellprofiler") is False

    def test_gpu_not_specified_defaults_false(self, tmp_path):
        """Container without gpu field should default to False."""
        manifest = {
            "containers": {
                "cellprofiler": {
                    "image": "cellprofiler_4.2.8.sif",
                    "version": "4.2.8",
                }
            }
        }
        with open(tmp_path / MANIFEST_FILENAME, "w") as f:
            json.dump(manifest, f)
        assert is_gpu_container(str(tmp_path), "cellprofiler") is False

    def test_no_manifest_returns_false(self, tmp_path):
        """No manifest file should return False."""
        assert is_gpu_container(str(tmp_path), "cellprofiler") is False

    def test_role_not_in_manifest_returns_false(self, tmp_path):
        """Role not listed in manifest should return False."""
        manifest = {
            "containers": {
                "cellprofiler": {
                    "image": "cellprofiler_4.2.8.sif",
                    "gpu": True,
                }
            }
        }
        with open(tmp_path / MANIFEST_FILENAME, "w") as f:
            json.dump(manifest, f)
        assert is_gpu_container(str(tmp_path), "deepprofiler") is False


# --- list_available_containers -----------------------------------------------


class TestListAvailableContainers:
    def test_no_container_dir_returns_empty(self, monkeypatch):
        monkeypatch.delenv("CPTOOLS2_CONTAINER_DIR", raising=False)
        result = list_available_containers()
        assert result == {}

    def test_with_manifest(self, container_dir_with_manifest):
        yaml_dict = {"container_path": container_dir_with_manifest}
        result = list_available_containers(yaml_dict)
        assert "cellprofiler" in result
        assert "deepprofiler" in result
        assert "cellpose" in result
        # cellprofiler comes from manifest
        assert result["cellprofiler"]["version"] == "4.2.8"
        assert result["cellprofiler"]["verified"] is True
        # cellpose not in manifest, gets defaults
        assert result["cellpose"]["version"] == "unknown"
        assert result["cellpose"]["verified"] is False

    def test_without_manifest(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        result = list_available_containers(yaml_dict)
        assert len(result) == len(DEFAULT_CONTAINERS)
        for role in DEFAULT_CONTAINERS:
            assert result[role]["version"] == "unknown"
            assert result[role]["verified"] is False

    def test_exists_field_when_dir_accessible(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        result = list_available_containers(yaml_dict)
        # All default .sif files were created by the fixture
        for role in DEFAULT_CONTAINERS:
            assert result[role]["exists"] is True

    def test_exists_none_when_dir_inaccessible(self, monkeypatch):
        monkeypatch.setenv("CPTOOLS2_CONTAINER_DIR", "/nonexistent/path")
        result = list_available_containers()
        for role in DEFAULT_CONTAINERS:
            assert result[role]["exists"] is None

    def test_result_has_expected_keys(self, container_dir):
        yaml_dict = {"container_path": container_dir}
        result = list_available_containers(yaml_dict)
        for role, info in result.items():
            assert "path" in info
            assert "image" in info
            assert "version" in info
            assert "verified" in info
            assert "exists" in info


# --- _image_name_for_role ----------------------------------------------------


class TestImageNameForRole:
    def test_from_manifest(self, container_dir_with_manifest):
        name = _image_name_for_role(container_dir_with_manifest, "cellprofiler")
        assert name == "cellprofiler_4.2.8.sif"

    def test_from_defaults_no_manifest(self, container_dir):
        name = _image_name_for_role(container_dir, "cellprofiler")
        assert name == DEFAULT_CONTAINERS["cellprofiler"]

    def test_from_defaults_role_not_in_manifest(self, container_dir_with_manifest):
        # cellpose is not in the manifest fixture, should fall back to defaults
        name = _image_name_for_role(container_dir_with_manifest, "cellpose")
        assert name == DEFAULT_CONTAINERS["cellpose"]

    def test_unknown_role_raises(self, container_dir):
        with pytest.raises(ValueError, match="Unknown container role"):
            _image_name_for_role(container_dir, "unknownrole")

    def test_manifest_overrides_default(self, tmp_path):
        """Manifest image name takes precedence over DEFAULT_CONTAINERS."""
        manifest = {
            "containers": {
                "cellprofiler": {
                    "image": "cellprofiler_custom_5.0.sif",
                    "version": "5.0",
                }
            }
        }
        with open(tmp_path / MANIFEST_FILENAME, "w") as f:
            json.dump(manifest, f)
        name = _image_name_for_role(str(tmp_path), "cellprofiler")
        assert name == "cellprofiler_custom_5.0.sif"
