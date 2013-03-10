# memcache key pattern for requested task status
ATP_REQUESTED_TASK_KEY_RAW = "atp.req.%s.%s"

"""ATP task status code"""
ATP_TASK_STATUS_PENDING = 0
ATP_TASK_STATUS_SUCCESS = 1
ATP_TASK_STATUS_FAILED = -1

JOB_ETA_TYPE_PREVIOUS_ETA = "previous_eta"
JOB_ETA_TYPE_CURRENT_TIME = "current_time"
JOB_ETA_TYPE_NEVER = "never"

ENTITY_PARAM_NAMES = ["profile", "list", "delay", "linkedin_id", "shard_id"]

URL_TO_JOB_NAME = {
                     "/delete_old_canceled_accounts/": "contacts.tasks.delete_old_canceled_accounts",
                     "/digest/email/": "digest.tasks.digest_email",
                     "/digest/purge/": "digest.tasks.digest_purge",
                     "/digest/sync/checkins/": "digest.tasks.digest_checkins",
                     "/digest/test/": "digest.tasks.test",
                     "/digest/update/": "digest.tasks.digest_update",
                     "/digest/update_daily_reports/": "digest.tasks.digest_update_daily_reports",
                     "/email/add_to_autoresponder/": "contacts.tasks.add_to_autoresponder",
                     "/email/daily/": "contacts.tasks.email_daily",
                     "/email/engage_events/": "contacts.tasks.send_engage_events",
                     "/email/generate_trial_expiration_emails/": "contacts.tasks.generate_trial_expiration_emails",
                     "/generate_reminders/": "contacts.tasks.generate_reminders",
                     "/refresh_network_stats/": "contacts.tasks.refresh_network_stats",
                     "/sync/calendar/": "contacts.tasks.sync_calendar_events",
                     "/sync/calendar_events/": "contacts.tasks.sync_calendar_events",
                     "/sync/calls/": "contacts.tasks.sync_calls",
                     "/sync/contacts/": "contacts.tasks.sync_contacts",
                     "/sync/email_files/": "contacts.tasks.sync_email_files",
                     "/sync/email_messages/": "contacts.tasks.sync_email_messages",
                     "/sync/email_previews/": "contacts.tasks.sync_email_previews",
                     "/sync/facebook_birthdays/": "contacts.tasks.sync_facebook_birthdays",
                     "/sync/merge_orphaned_contacts/": "contacts.tasks.merge_orphaned_contacts",
                     "/sync/notes/": "contacts.tasks.sync_notes",
                     "/sync/profiles_from_email_adddresses/": "contacts.tasks.profiles_from_email_addresses",
                     "/sync/profiles_from_email_addresses/": "contacts.tasks.profiles_from_email_addresses",
                     "/sync/profiles_from_email_links/": "contacts.tasks.profiles_from_email_links",
                     "/sync/profiles_from_sites/": "contacts.tasks.profiles_from_sites",
                     "/sync/services/": "contacts.tasks.sync_contacts",
                     "/sync/social_messages/": "contacts.tasks.sync_social_messages",
                     "/update_daily_reports/": "contacts.tasks.update_daily_reports"
                     }
