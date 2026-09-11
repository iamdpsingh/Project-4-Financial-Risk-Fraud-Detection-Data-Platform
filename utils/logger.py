import logging
from pathlib import Path


def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger configured to write to both local files and Google Cloud Logging (if in GCP).
    """
    logger = logging.getLogger(name)
    
    # If the logger already has handlers, assume it's already configured to prevent duplicate logs.
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.DEBUG)
    
    # 1. Local File Handler
    # Find the root of the project to create the logs directory.
    # We'll just use the current working directory.
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    file_handler = logging.FileHandler(log_dir / f"{name.replace('.', '_')}.log")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    # 2. Console Handler (fallback for local dev)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(file_format)
    logger.addHandler(console_handler)
    
    # 3. Google Cloud Logging Handler
    # We attempt to setup the Cloud Logging handler if google-cloud-logging is available.
    try:
        from google.cloud import logging as cloud_logging
        
        # This will automatically pick up the credentials from the environment
        # (e.g. Workload Identity on Cloud Run, or GOOGLE_APPLICATION_CREDENTIALS locally)
        client = cloud_logging.Client()
        
        # Setup Cloud Logging handler
        cloud_handler = client.get_default_handler()
        cloud_handler.setLevel(logging.DEBUG)
        
        # Add the Cloud Logging handler
        logger.addHandler(cloud_handler)
    except Exception:
        # Ignore errors if GCP credentials aren't set or not running in GCP
        pass

    # Prevent logs from propagating to the root logger which might double print
    logger.propagate = False

    return logger
