# SPDX-License-Identifier: Apache-2.0
"""Generate a deterministic SPDX 2.3 release SBOM for the pinned distribution set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    document = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "name": "meridian-storage-streaming-1.0.0",
        "documentNamespace": "https://github.com/zephytiju/MeridianStreaming/releases/tag/v1.0.0/sbom",
        "creationInfo": {
            "created": "2026-08-25T00:00:00Z",
            "creators": ["Tool: MeridianStreaming/scripts/generate_sbom.py"],
            "licenseListVersion": "3.27",
        },
        "packages": [
            {
                "SPDXID": "SPDXRef-Package-Streaming",
                "name": "meridian-storage-streaming",
                "versionInfo": "1.0.0",
                "downloadLocation": "https://pypi.org/project/meridian-storage-streaming/1.0.0/",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
                "licenseDeclared": "Apache-2.0",
                "copyrightText": "Copyright 2026 Meridian contributors",
            },
            {
                "SPDXID": "SPDXRef-Package-Core",
                "name": "meridian-storage-core",
                "versionInfo": "1.0.0",
                "downloadLocation": "https://pypi.org/project/meridian-storage-core/1.0.0/",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
                "licenseDeclared": "Apache-2.0",
                "copyrightText": "Copyright 2026 Meridian contributors",
                "checksums": [
                    {
                        "algorithm": "SHA256",
                        "checksumValue": (
                            "6b8ebb70ee1a8467a96d668878a8eebf826c1c4b63b3832ae70f2c630a8ef4a1"
                        ),
                    }
                ],
            },
            {
                "SPDXID": "SPDXRef-Package-Semantics",
                "name": "meridian-storage-semantics",
                "versionInfo": "1.0.0",
                "downloadLocation": "https://pypi.org/project/meridian-storage-semantics/1.0.0/",
                "filesAnalyzed": False,
                "licenseConcluded": "Apache-2.0",
                "licenseDeclared": "Apache-2.0",
                "copyrightText": "Copyright 2026 Meridian contributors",
                "checksums": [
                    {
                        "algorithm": "SHA256",
                        "checksumValue": (
                            "76fced0bc083f145fad1949b85147a3563f3584f99994471693915a8ba1851ec"
                        ),
                    }
                ],
            },
        ],
        "relationships": [
            {
                "spdxElementId": "SPDXRef-Package-Streaming",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": "SPDXRef-Package-Core",
            },
            {
                "spdxElementId": "SPDXRef-Package-Streaming",
                "relationshipType": "DEPENDS_ON",
                "relatedSpdxElement": "SPDXRef-Package-Semantics",
            },
        ],
    }
    arguments.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
