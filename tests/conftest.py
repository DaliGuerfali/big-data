"""
pytest configuration: make airflow/plugins and the repo root importable.
"""

import sys
from pathlib import Path

# repo root — gives access to batch/, streaming/, producers/, etc.
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# Airflow puts /opt/airflow/plugins on sys.path; mirror that locally.
sys.path.insert(0, str(ROOT / "airflow" / "plugins"))
