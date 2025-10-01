#!/usr/bin/env python3
"""Backward-compatible CLI wrapper for the refactored SFTP tool."""

from __future__ import annotations

import sys

from sftp_tool.main import main


if __name__ == "__main__":
    sys.exit(main())
