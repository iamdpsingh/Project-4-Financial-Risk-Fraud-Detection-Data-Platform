"""
Helper script to run the batch pipeline locally using the DirectRunner.

Usage:
    python pipelines/batch/run_local.py
"""

from utils.logger import get_logger
import logging
import subprocess
import sys
from pathlib import Path

log = get_logger("batch_run_local")

def run():
    # Make sure we're at the repo root
    repo_root = Path(__file__).parent.parent.parent
    
    input_dir = repo_root / "data" / "extracted"
    if not input_dir.exists():
        log.warning(f"Extracted data directory {input_dir} not found.")
        log.info("Falling back to raw data directory data/raw/ (Postgres extraction skipped)")
        input_dir = repo_root / "data" / "raw"
        
    pipeline_script = repo_root / "pipelines" / "batch" / "pipeline.py"
    
    cmd = [
        sys.executable, str(pipeline_script),
        "--runner=DirectRunner",
        f"--input_dir={input_dir}",
        "--output_local"
    ]
    
    log.info("Running batch pipeline locally...")
    log.info(f"Command: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True)
        log.info("Batch pipeline completed successfully.")
        log.info("Output files saved to data/staged/")
    except subprocess.CalledProcessError as e:
        log.error(f"Pipeline failed with exit code {e.returncode}")
        sys.exit(e.returncode)

if __name__ == "__main__":
    run()
