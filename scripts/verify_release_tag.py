# SPDX-License-Identifier: Apache-2.0
"""Ensure an immutable release tag exactly matches all package version ledgers."""

from __future__ import annotations

import argparse

from meridian_storage.streaming import __version__, compatibility_contract, public_api_contract


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("tag")
    arguments = parser.parse_args()
    versions = {__version__, compatibility_contract()["version"], public_api_contract()["version"]}
    if len(versions) != 1:
        raise AssertionError(f"package version ledgers disagree: {versions!r}")
    expected = f"v{versions.pop()}"
    if arguments.tag != expected:
        raise AssertionError(f"release tag must be {expected!r}, not {arguments.tag!r}")
    print(f"release tag {arguments.tag} verified")


if __name__ == "__main__":
    main()
