FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

# Extract only the dependencies we need for Cloud Run to keep the image small
RUN pip install --no-cache-dir fastapi uvicorn[standard] google-cloud-pubsub pydantic

COPY cloud_run/ /app/cloud_run/

# Environment variables
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8080

# Run the API
CMD ["uvicorn", "cloud_run.main:app", "--host", "0.0.0.0", "--port", "8080"]
