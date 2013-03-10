"""testing atp client"""
from atp import client
from simplequeue import queue_task
from simpleauth.models import UserProfile
from connectme.models import Profile
import global_constants

user = None

def test_get_job_by_url_param():
    job_id = client.get_job_id_by_url_params("/email/daily/", {"profile": 123})
    assert job_id ==  14

def test_add_task_no_profile():
    queue_task.add_task_delete_old_canceled_accounts()
    task = client.get_scheduled_task(67, 0)
    assert not task == None

def test_add_task_profile():
    profile = UserProfile()
    # yuriko's test account for which ATP is enabled.
    profile.id = 15655 
    queue_task.add_task_daily_email(profile)
    task = client.get_scheduled_task(14, profile.id)
    assert not task == None
    assert task.entity_id == profile.id
    assert task.job_id == 14

def test_add_task_profile_disabled():
    profile = UserProfile()
    # disabled account
    profile.id = 888 
    queue_task.add_task_daily_email(profile)
    task = client.get_scheduled_task(14, profile.id)
    assert task == None

def test_sync_contacts_cardmunch():
    job_id = client.get_job_id_by_name_params("contacts.tasks.sync_contacts", {"profile": 0, "service": global_constants.CARDMUNCH})
    assert job_id == 68

test_sync_contacts_cardmunch.integration = True
test_get_job_by_url_param.integration = True
test_add_task_no_profile.integration = True
test_add_task_profile.integration = True
test_add_task_profile_disabled.integration = True

if __name__ == "__main__":
    test_sync_contacts_cardmunch()
    test_get_job_by_url_param()
    test_add_task_no_profile()
    test_add_task_profile()
    test_add_task_profile_disabled()
