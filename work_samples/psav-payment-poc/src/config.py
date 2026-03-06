import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Project root (one level above src/)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Path to default config file
CONFIG_PATH = PROJECT_ROOT / "configs" / "default.yaml"


def load_config():
    """
    Load YAML configuration for the pipeline.
    """
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    return config
