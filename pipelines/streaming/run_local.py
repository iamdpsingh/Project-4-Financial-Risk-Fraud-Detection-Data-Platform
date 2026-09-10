"""
Helper script to run the streaming pipeline locally using the DirectRunner.

Usage:
    python pipelines/streaming/run_local.py
"""

import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s  [%(name)s]  %(message)s")
log = logging.getLogger("streaming_run_local")

def run():
    repo_root = Path(__file__).parent.parent.parent
    pipeline_script = repo_root / "pipelines" / "streaming" / "pipeline.py"
    
    cmd = [
        sys.executable, str(pipeline_script),
        "--runner=DirectRunner",
        "--output_local"
    ]
    
    log.info("Running streaming pipeline locally (mock mode)...")
    log.info(f"Command: {' '.join(cmd)}")
    log.info("Note: Without a real Pub/Sub subscription, this processes a mock stream and exits.")
    
    try:
        subprocess.run(cmd, check=True)
        log.info("Streaming pipeline completed successfully.")
    except subprocess.CalledProcessError as e:
        log.error(f"Pipeline failed with exit code {e.returncode}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    run()
