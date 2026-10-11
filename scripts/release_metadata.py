#!/usr/bin/env python3
"""Release metadata for the application (Cargo.toml) and its Helm chart.

  version                       print the application version
  bump-version patch|minor|major  bump the application version (Cargo.toml, Cargo.lock)
  bump patch|minor|major        bump the chart version and sync appVersion
  validate vX.Y.Z               check a release tag against the committed metadata

The chart version is independent of the application version (ADR-0003).
"""

import re
import sys
from pathlib import Path

PACKAGE = "github-actions-cache-server"
CHART_PATH = Path("install/kubernetes/github-actions-cache-server/Chart.yaml")
SEMVER = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[a-z-][\da-z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-z-][\da-z-]*))*))?"
    r"(?:\+([\da-z-]+(?:\.[\da-z-]+)*))?$",
    re.IGNORECASE,
)
# The `version` line of the [package] table, which comes first in Cargo.toml.
PACKAGE_VERSION = re.compile(r'(\[package\][^\[]*?^version\s*=\s*")([^"]+)(")', re.MULTILINE | re.DOTALL)
LOCK_VERSION = re.compile(
    rf'(\[\[package\]\]\nname = "{PACKAGE}"\nversion = ")([^"]+)(")', re.MULTILINE
)


class ReleaseMetadataError(Exception):
    pass


def fail(message):
    raise ReleaseMetadataError(f"Release metadata error: {message}")


def parse_semver(version, label):
    match = SEMVER.match(version)
    if not match:
        fail(f'{label} must be valid SemVer, received "{version}"')
    return int(match[1]), int(match[2]), int(match[3])


def bumped(version, bump, label):
    if bump not in ("patch", "minor", "major"):
        fail(f'{label} bump must be one of patch, minor, or major, received "{bump or ""}"')
    major, minor, patch = parse_semver(version, label)
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def package_version():
    match = PACKAGE_VERSION.search(Path("Cargo.toml").read_text())
    if not match:
        fail("Cargo.toml must contain a [package] version")
    return match[2]


def read_chart_field(chart, field):
    match = re.search(rf"^{field}:\s*['\"]?([^'\"\s]+)['\"]?\s*$", chart, re.MULTILINE)
    if not match:
        fail(f"Chart.yaml must contain a single-line {field} field")
    return match[1]


def replace_chart_field(chart, field, value, quoted=False):
    value = f"'{value}'" if quoted else value
    return re.sub(rf"^{field}:.*$", lambda _: f"{field}: {value}", chart, count=1, flags=re.MULTILINE)


def bump_package_version(bump):
    version = bumped(package_version(), bump, "package version")
    for path, pattern in ((Path("Cargo.toml"), PACKAGE_VERSION), (Path("Cargo.lock"), LOCK_VERSION)):
        content, replaced = pattern.subn(lambda m: f"{m[1]}{version}{m[3]}", path.read_text(), count=1)
        if replaced != 1:
            fail(f"{path} has no version for {PACKAGE}")
        path.write_text(content)
    print(version)


def bump_chart_version(bump):
    chart = CHART_PATH.read_text()
    chart_version = bumped(read_chart_field(chart, "version"), bump, "chart")
    version = package_version()
    chart = replace_chart_field(replace_chart_field(chart, "version", chart_version), "appVersion", version, True)
    CHART_PATH.write_text(chart)
    print(f"Updated chart version to {chart_version} and appVersion to {version}")


def validate(tag):
    if not tag or not tag.startswith("v"):
        fail(f'release tag must start with "v", received "{tag or ""}"')
    tag_version = tag[1:]
    parse_semver(tag_version, "release tag version")
    version = package_version()
    parse_semver(version, "package version")
    chart = CHART_PATH.read_text()
    chart_version = read_chart_field(chart, "version")
    app_version = read_chart_field(chart, "appVersion")
    parse_semver(chart_version, "chart version")
    if tag_version != version or tag_version != app_version:
        fail(
            f"release tag {tag} must match package version and chart appVersion; "
            f"received {version} and {app_version}"
        )
    print(f"Validated {tag}: package and appVersion are {tag_version}; chart version is {chart_version}")


def main(argv):
    command, argument = (argv + [None, None])[:2]
    commands = {
        "version": lambda _: print(package_version()),
        "bump-version": bump_package_version,
        "bump": bump_chart_version,
        "validate": validate,
    }
    try:
        if command not in commands:
            fail(f'expected command "version", "bump-version", "bump" or "validate", received "{command or ""}"')
        commands[command](argument)
    except ReleaseMetadataError as err:
        print(err, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
