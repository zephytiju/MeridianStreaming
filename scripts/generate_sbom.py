# SPDX-License-Identifier: Apache-2.0
"""Generate a deterministic SPDX 2.3 SBOM from the exact validation recipe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from meridian_storage.streaming import __version__, compatibility_contract


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    compatibility = compatibility_contract()
    entries = {
        "Streaming": {"distribution": "meridian-storage-streaming", "version": __version__},
        "Core": compatibility["core"],
        "Semantics": compatibility["semantics"],
    }
    packages = []
    for name, entry in entries.items():
        package = {
            "SPDXID": f"SPDXRef-Package-{name}",
            "name": entry["distribution"],
            "versionInfo": entry["version"],
            "downloadLocation": (
                f"https://pypi.org/project/{entry['distribution']}/{entry['version']}/"
            ),
            "filesAnalyzed": False,
            "licenseConcluded": "Apache-2.0",
            "licenseDeclared": "Apache-2.0",
            "copyrightText": "Copyright 2026 Meridian contributors",
        }
        if "wheelSha256" in entry:
            package["checksums"] = [{"algorithm": "SHA256", "checksumValue": entry["wheelSha256"]}]
        packages.append(package)
    document = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "name": f"meridian-storage-streaming-{__version__}",
        "documentNamespace": (
            f"https://github.com/zephytiju/MeridianStreaming/releases/tag/v{__version__}/sbom"
        ),
        "creationInfo": {
            "created": "2026-09-08T00:00:00Z",
            "creators": ["Tool: MeridianStreaming/scripts/generate_sbom.py"],
            "licenseListVersion": "3.27",
        },
        "packages": packages,
        "relationships": [
            {
                "spdxElementId": "SPDXRef-Package-Streaming",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": f"SPDXRef-Package-{name}",
            }
            for name in ("Core", "Semantics")
        ],
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
