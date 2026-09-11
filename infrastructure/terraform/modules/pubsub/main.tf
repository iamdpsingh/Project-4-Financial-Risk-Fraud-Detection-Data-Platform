resource "google_pubsub_topic" "events" {
  name    = var.topic_name
  project = var.project_id
}

resource "google_pubsub_subscription" "events_sub" {
  name    = "${var.topic_name}-sub"
  topic   = google_pubsub_topic.events.name
  project = var.project_id

  # Retain unacknowledged messages for 7 days
  message_retention_duration = "604800s"

  # Prevent message loss during rapid ingestion bursts
  retain_acked_messages = false
  ack_deadline_seconds  = 20
}
