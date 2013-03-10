"""
This object execute the tasks managed by the ATP.
It is configured in settings.py ATP_TASK_EXECUTOR_CLASS
It must implement these two methods
"""
import contacts.tasks
import digest.tasks
import copy
import time
from django.conf import settings
import django.db
from atp import constants
from shared.autometrics import autometrics_decorators
from shared.contacts_shard_manager import ContactsShardManager

def supports_method(method, params):
    """Indicate if this executor support the particular method with params"""
    return method in AVAILABLE_METHODS

@autometrics_decorators.atp_executor_autometrics
def execute(job, entity_id):
    if job.method in settings.ATP_SKIPPED_JOB_NAMES:
        return no_op()
    if job.params:
        # replace the entity_id value if necessary
        params = copy.deepcopy(job.params)
        shard_manager = ContactsShardManager()
        shard_manager.clear_thread_local()
        for ename in constants.ENTITY_PARAM_NAMES:
            if ename in params:
                params[ename] = entity_id
                if ename == "profile":
                    shard_manager.set_thread_local_profile_id(entity_id)
                elif ename == "linkedin_id":
                    shard_manager.set_thread_local_member_id(entity_id)
                elif ename == "shard_id":
                    shard_manager.set_thread_local_shard_id(entity_id)
                    # shard_id is only used by the executor the set the shard manager thread local
                    # It's not passed to the job.method, so deleting from the params.
                    del params[ename]
                else:
                    # no special handling for other ename values
                    pass
                break
        result = AVAILABLE_METHODS[job.method](**params)
        shard_manager.clear_thread_local()
    else:
        result = AVAILABLE_METHODS[job.method]()
    # We can only do this because we are single threaded application
    django.db.close_connection()
    return result

# using  parameter to indicate delay in seconds
def ping(delay, fail=False):
    if delay > 0:
        time.sleep(delay)
    if fail:
        return {"status": "failed", "ping_delay": delay, "status_details": "ping called with fail = True"}
    else:
        return {"status": "success", "ping_delay": delay}

def no_op(*args, **kwargs):
    return {"status": "success", "status_details": "no operation"}

AVAILABLE_METHODS = {
                     "executor.ping": ping,
                     "digest.tasks.digest_update": digest.tasks.digest_update,
                     "digest.tasks.digest_email": digest.tasks.digest_email,
                     "digest.tasks.digest_update_daily_reports": digest.tasks.digest_update_daily_reports,
                     "digest.tasks.digest_checkins": digest.tasks.digest_checkins,
                     "digest.tasks.digest_purge": digest.tasks.digest_purge,
                     "digest.tasks.test": digest.tasks.test,
                     "contacts.tasks.sync_services": contacts.tasks.sync_services,
                     "contacts.tasks.sync_contacts": contacts.tasks.sync_contacts,
                     "contacts.tasks.sync_email_messages": contacts.tasks.sync_email_messages,
                     "contacts.tasks.sync_social_messages": contacts.tasks.sync_social_messages,
                     "contacts.tasks.sync_facebook_birthdays": contacts.tasks.sync_facebook_birthdays,
                     "contacts.tasks.sync_calendar_events": contacts.tasks.sync_calendar,
                     "contacts.tasks.sync_notes": contacts.tasks.sync_notes,
                     "contacts.tasks.sync_calls": contacts.tasks.sync_calls,
                     "contacts.tasks.sync_email_files": contacts.tasks.sync_email_files,
                     "contacts.tasks.sync_email_previews": contacts.tasks.sync_email_previews,
                     "contacts.tasks.profiles_from_email_links": contacts.tasks.profiles_from_email_links,
                     "contacts.tasks.profiles_from_sites": contacts.tasks.profiles_from_sites,
                     "contacts.tasks.profiles_from_email_addresses": contacts.tasks.profiles_from_email_addresses,
                     "contacts.tasks.merge_orphaned_contacts": contacts.tasks.merge_orphaned_contacts,
                     "contacts.tasks.add_to_autoresponder": contacts.tasks.add_to_autoresponder,
                     "contacts.tasks.email_daily": contacts.tasks.email_daily,
                     "contacts.tasks.generate_trial_expiration_emails": contacts.tasks.generate_trial_expiration_emails,
                     "contacts.tasks.update_daily_reports": contacts.tasks.update_daily_reports,
                     "contacts.tasks.generate_reminders": contacts.tasks.generate_reminders,
                     "contacts.tasks.refresh_network_stats": contacts.tasks.refresh_network_stats,
                     "contacts.tasks.delete_old_canceled_accounts": contacts.tasks.delete_old_canceled_accounts,
                     "contacts.tasks.send_engage_events": contacts.tasks.send_engage_events,
                     "contacts.tasks.delete_test_accounts": contacts.tasks.delete_test_accounts,
                     "contacts.tasks.remove_associated_data": contacts.tasks.remove_associated_data,
                     "contacts.tasks.delete_linkedin_contacts_account": contacts.tasks.delete_linkedin_contacts_account,
                     "contacts.tasks.close_linkedin_contacts_account": contacts.tasks.close_linkedin_contacts_account,
                     "contacts.tasks.reactivate_linkedin_contacts_account": contacts.tasks.reactivate_linkedin_contacts_account,
                     "contacts.tasks.clean_merged_contact_action_table": contacts.tasks.clean_merged_contact_action_table,
                     "contacts.tasks.update_email_address": contacts.tasks.update_email_address,
                     "contacts.tasks.update_profile": contacts.tasks.update_profile,
                     "contacts.tasks.migrate_connections": contacts.tasks.migrate_connections,
                     "contacts.tasks.migrate_data": contacts.tasks.migrate_data,
                     "contacts.tasks.update_timestamp": contacts.tasks.update_timestamp,
                     "contacts.tasks.sync_uploaded_contacts": contacts.tasks.sync_uploaded_contacts,
                     }
