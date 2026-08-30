#!/usr/bin/env python3
"""Oracle placeholder.

The 100M fact is already on MinIO. The reference settlement summary is produced
by the salted Spark job (solution/src/main.py) and written to expected/.
Local generation does not recompute it.
"""
from __future__ import annotations
