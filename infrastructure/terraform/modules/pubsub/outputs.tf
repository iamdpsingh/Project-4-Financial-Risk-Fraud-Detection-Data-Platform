output "topic_name" {
  value = google_pubsub_topic.events.name
}

output "subscription_name" {
  value = google_pubsub_subscription.events_sub.name
}
