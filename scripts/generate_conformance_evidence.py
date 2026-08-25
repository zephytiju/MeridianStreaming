# SPDX-License-Identifier: Apache-2.0
"""Generate deterministic reference conformance evidence for a release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from meridian_storage.streaming.testing import InMemoryStreamingTarget, run_streaming_conformance


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    report = run_streaming_conformance(InMemoryStreamingTarget())
    document = {
        "report": report.to_dict(),
        "reportFingerprint": report.fingerprint,
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
