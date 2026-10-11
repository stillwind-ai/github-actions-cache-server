import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).with_name("release_metadata.py").resolve()
CHART = Path("install/kubernetes/github-actions-cache-server/Chart.yaml")


class ReleaseMetadataTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.cwd = Path(self.directory.name)

    def write(self, package_version, chart_version, app_version):
        (self.cwd / "Cargo.toml").write_text(
            f'[package]\nname = "github-actions-cache-server"\nversion = "{package_version}"\n\n'
            '[dependencies]\nfoo = { version = "1.0.0" }\n'
        )
        (self.cwd / "Cargo.lock").write_text(
            '[[package]]\nname = "foo"\nversion = "1.0.0"\n\n'
            f'[[package]]\nname = "github-actions-cache-server"\nversion = "{package_version}"\n'
        )
        (self.cwd / CHART).parent.mkdir(parents=True)
        (self.cwd / CHART).write_text(f"apiVersion: v2\nversion: {chart_version}\nappVersion: '{app_version}'\n")

    def run_script(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], cwd=self.cwd, capture_output=True, text=True, env=os.environ
        )

    def test_bumps_chart_patch_version_and_syncs_app_version(self):
        self.write("9.4.8", "1.0.3", "9.4.7")
        self.assertEqual(self.run_script("bump", "patch").returncode, 0)
        self.assertEqual((self.cwd / CHART).read_text(), "apiVersion: v2\nversion: 1.0.4\nappVersion: '9.4.8'\n")

    def test_supports_minor_and_major_chart_bumps(self):
        for bump, expected in (("minor", "1.1.0"), ("major", "2.0.0")):
            with self.subTest(bump=bump):
                self.setUp()
                self.write("9.4.8", "1.0.3", "9.4.7")
                self.assertEqual(self.run_script("bump", bump).returncode, 0)
                self.assertIn(f"version: {expected}\n", (self.cwd / CHART).read_text())

    def test_bumps_the_package_version_in_manifest_and_lockfile(self):
        self.write("9.8.0", "1.4.1", "9.8.0")
        result = self.run_script("bump-version", "major")
        self.assertEqual((result.returncode, result.stdout), (0, "10.0.0\n"))
        self.assertIn('version = "10.0.0"', (self.cwd / "Cargo.toml").read_text())
        self.assertIn('foo = { version = "1.0.0" }', (self.cwd / "Cargo.toml").read_text())
        lock = (self.cwd / "Cargo.lock").read_text()
        self.assertIn('name = "github-actions-cache-server"\nversion = "10.0.0"', lock)
        self.assertIn('name = "foo"\nversion = "1.0.0"', lock)
        self.assertEqual(self.run_script("version").stdout, "10.0.0\n")

    def test_accepts_consistent_release_metadata(self):
        self.write("9.4.8", "1.0.4", "9.4.8")
        result = self.run_script("validate", "v9.4.8")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Validated v9.4.8: package and appVersion are 9.4.8; chart version is 1.0.4", result.stdout)

    def test_rejects_a_tag_that_does_not_match_the_metadata(self):
        self.write("9.4.8", "1.0.4", "9.4.7")
        result = self.run_script("validate", "v9.4.8")
        self.assertEqual(result.returncode, 1)
        self.assertIn("must match package version and chart appVersion", result.stderr)

    def test_rejects_invalid_input(self):
        self.write("9.4.8", "1.0.x", "9.4.8")
        for args, message in (
            (("validate", "9.4.8"), 'must start with "v"'),
            (("validate", "v9.4.8"), "chart version must be valid SemVer"),
            (("bump", "huge"), "chart bump must be one of"),
            (("nope",), "expected command"),
        ):
            with self.subTest(args=args):
                result = self.run_script(*args)
                self.assertEqual(result.returncode, 1)
                self.assertIn(message, result.stderr)


if __name__ == "__main__":
    unittest.main()
