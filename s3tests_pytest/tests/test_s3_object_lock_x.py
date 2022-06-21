import datetime

import isodate
import pytest
import pytz

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestObjectLock(TestBaseClass):

    def test_object_lock_put_obj_lock(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with default retention')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 1
                    }
                }}
        response = client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration=conf)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'COMPLIANCE',
                        'Years': 1
                    }
                }}
        response = client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration=conf)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_bucket_versioning(Bucket=bucket_name)  # ?
        self.eq(response['Status'], 'Enabled')

    def test_object_lock_put_obj_lock_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with bucket object lock not enabled')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 409)
        self.eq(error_code, 'InvalidBucketState')

    def test_object_lock_put_obj_lock_with_days_and_years(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with days and years')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 1,
                        'Years': 1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_object_lock_put_obj_lock_invalid_days(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with invalid days')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 0
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRetentionPeriod')

    def test_object_lock_put_obj_lock_invalid_years1(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with invalid years')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Years': -1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRetentionPeriod')

    def test_object_lock_put_obj_lock_invalid_years2(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with invalid mode')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'abc',
                        'Years': 1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'governance',
                        'Years': 1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_object_lock_put_obj_lock_invalid_status(self, s3cfg_global_unique):
        """
        (operation='Test put object lock with invalid status')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Disabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Years': 1
                    }
                }}
        e = assert_raises(ClientError, client.put_object_lock_configuration, Bucket=bucket_name,
                          ObjectLockConfiguration=conf)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_object_lock_suspend_versioning(self, s3cfg_global_unique):
        """
        (operation='Test suspend versioning when object lock enabled')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        e = assert_raises(ClientError, client.put_bucket_versioning, Bucket=bucket_name,
                          VersioningConfiguration={'Status': 'Suspended'})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 409)
        self.eq(error_code, 'InvalidBucketState')

    def test_object_lock_get_obj_lock(self, s3cfg_global_unique):
        """
        (operation='Test get object lock')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 1
                    }
                }}
        client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration=conf)
        response = client.get_object_lock_configuration(Bucket=bucket_name)
        self.eq(response['ObjectLockConfiguration'], conf)

    def test_object_lock_get_obj_lock_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test get object lock with bucket object lock not enabled')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        e = assert_raises(ClientError, client.get_object_lock_configuration, Bucket=bucket_name)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'ObjectLockConfigurationNotFoundError')

    def test_object_lock_put_obj_retention(self, s3cfg_global_unique):
        """
        (operation='Test put object retention')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        response = client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_put_obj_retention_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test put object retention with bucket object lock not enabled')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRequest')

    def test_object_lock_put_obj_retention_invalid_mode(self, s3cfg_global_unique):
        """
        (operation='Test put object retention with invalid mode')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        retention = {'Mode': 'governance', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

        retention = {'Mode': 'abc', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_object_lock_get_obj_retention(self, s3cfg_global_unique):
        """
        (operation='Test get object retention')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.eq(response['Retention'], retention)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_get_obj_retention_iso8601(self, s3cfg_global_unique):
        """
        (operation='Test object retention date formatting')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        date = datetime.datetime.today() + datetime.timedelta(days=365)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': date}

        http_response = None

        def get_http_response(**kwargs):
            nonlocal http_response
            http_response = kwargs['http_response'].__dict__

        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        client.meta.events.register('after-call.s3.HeadObject', get_http_response)
        client.head_object(Bucket=bucket_name, VersionId=version_id, Key=key)
        retain_date = http_response['headers']['x-amz-object-lock-retain-until-date']
        isodate.parse_datetime(retain_date)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_get_obj_retention_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test get object retention with invalid bucket')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        e = assert_raises(ClientError, client.get_object_retention, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRequest')

    def test_object_lock_put_obj_retention_version_id(self, s3cfg_global_unique):
        """
        (operation='Test put object retention with version id')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, VersionId=version_id, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key, VersionId=version_id)
        self.eq(response['Retention'], retention)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_put_obj_retention_override_default_retention(self, s3cfg_global_unique):
        """
        (operation='Test put object retention to override default retention')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        conf = {'ObjectLockEnabled': 'Enabled',
                'Rule': {
                    'DefaultRetention': {
                        'Mode': 'GOVERNANCE',
                        'Days': 1
                    }
                }}
        client.put_object_lock_configuration(
            Bucket=bucket_name,
            ObjectLockConfiguration=conf)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.eq(response['Retention'], retention)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_put_obj_retention_increase_period(self, s3cfg_global_unique):
        """
        (operation='Test put object retention to increase retention period')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention1 = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention1)
        retention2 = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention2)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.eq(response['Retention'], retention2)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_put_obj_retention_shorten_period(self, s3cfg_global_unique):
        """
        (operation='Test put object retention to shorten period')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_put_obj_retention_shorten_period_bypass(self, s3cfg_global_unique):
        """
        (operation='Test put object retention to shorten period with bypass header')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        version_id = response['VersionId']
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 3, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True)
        response = client.get_object_retention(Bucket=bucket_name, Key=key)
        self.eq(response['Retention'], retention)
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id, BypassGovernanceRetention=True)

    def test_object_lock_delete_object_with_retention(self, s3cfg_global_unique):
        """
        (operation='Test delete object with retention')
        (assertion='retention period make effects')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'

        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key,
                          VersionId=response['VersionId'])
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

        response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'],
                                        BypassGovernanceRetention=True)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def test_object_lock_multi_delete_object_with_retention(self, s3cfg_global_unique):
        """
        (operation='Test multi-delete object with retention')
        (assertion='retention period make effects')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key1 = 'file1'
        key2 = 'file2'

        response1 = client.put_object(Bucket=bucket_name, Body='abc', Key=key1)
        response2 = client.put_object(Bucket=bucket_name, Body='abc', Key=key2)

        version_id1 = response1['VersionId']
        version_id2 = response2['VersionId']

        # key1 is under retention, but key2 isn't.
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key1, Retention=retention)

        delete_response = client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {
                        'Key': key1,
                        'VersionId': version_id1
                    },
                    {
                        'Key': key2,
                        'VersionId': version_id2
                    }
                ]
            }
        )

        self.eq(len(delete_response['Deleted']), 1)
        self.eq(len(delete_response['Errors']), 1)

        failed_object = delete_response['Errors'][0]
        self.eq(failed_object['Code'], 'AccessDenied')
        self.eq(failed_object['Key'], key1)
        self.eq(failed_object['VersionId'], version_id1)

        deleted_object = delete_response['Deleted'][0]
        self.eq(deleted_object['Key'], key2)
        self.eq(deleted_object['VersionId'], version_id2)

        delete_response = client.delete_objects(
            Bucket=bucket_name,
            Delete={
                'Objects': [
                    {
                        'Key': key1,
                        'VersionId': version_id1
                    }
                ]
            },
            BypassGovernanceRetention=True
        )

        assert (('Errors' not in delete_response) or (len(delete_response['Errors']) == 0))
        self.eq(len(delete_response['Deleted']), 1)
        deleted_object = delete_response['Deleted'][0]
        self.eq(deleted_object['Key'], key1)
        self.eq(deleted_object['VersionId'], version_id1)

    def test_object_lock_put_legal_hold(self, s3cfg_global_unique):
        """
        (operation='Test put legal hold')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        legal_hold = {'Status': 'ON'}
        response = client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        response = client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_object_lock_put_legal_hold_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test put legal hold with invalid bucket')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        legal_hold = {'Status': 'ON'}
        e = assert_raises(ClientError, client.put_object_legal_hold, Bucket=bucket_name, Key=key, LegalHold=legal_hold)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRequest')

    def test_object_lock_put_legal_hold_invalid_status(self, s3cfg_global_unique):
        """
        (operation='Test put legal hold with invalid status')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        legal_hold = {'Status': 'abc'}
        e = assert_raises(ClientError, client.put_object_legal_hold, Bucket=bucket_name, Key=key, LegalHold=legal_hold)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_object_lock_get_legal_hold(self, s3cfg_global_unique):
        """
        (operation='Test get legal hold')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        legal_hold = {'Status': 'ON'}
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
        response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        self.eq(response['LegalHold'], legal_hold)
        legal_hold_off = {'Status': 'OFF'}
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold_off)
        response = client.get_object_legal_hold(Bucket=bucket_name, Key=key)
        self.eq(response['LegalHold'], legal_hold_off)

    def test_object_lock_get_legal_hold_invalid_bucket(self, s3cfg_global_unique):
        """
        (operation='Test get legal hold with invalid bucket')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        e = assert_raises(ClientError, client.get_object_legal_hold, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRequest')

    def test_object_lock_delete_object_with_legal_hold_on(self, s3cfg_global_unique):
        """
        (operation='Test delete object with legal hold on')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'ON'})
        e = assert_raises(ClientError, client.delete_object, Bucket=bucket_name, Key=key,
                          VersionId=response['VersionId'])
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})

    def test_object_lock_delete_object_with_legal_hold_off(self, s3cfg_global_unique):
        """
        (operation='Test delete object with legal hold off')
        (assertion='fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        response = client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})
        response = client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'])
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)

    def test_object_lock_get_obj_metadata(self, s3cfg_global_unique):
        """
        (operation='Test get object metadata')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key)
        legal_hold = {'Status': 'ON'}
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold=legal_hold)
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC)}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention)
        response = client.head_object(Bucket=bucket_name, Key=key)
        self.eq(response['ObjectLockMode'], retention['Mode'])
        self.eq(response['ObjectLockRetainUntilDate'], retention['RetainUntilDate'])
        self.eq(response['ObjectLockLegalHoldStatus'], legal_hold['Status'])

        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'],
                             BypassGovernanceRetention=True)

    def test_object_lock_uploading_obj(self, s3cfg_global_unique):
        """
        (operation='Test put legal hold and retention when uploading object')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        key = 'file1'
        client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                          ObjectLockRetainUntilDate=datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC),
                          ObjectLockLegalHoldStatus='ON')

        response = client.head_object(Bucket=bucket_name, Key=key)
        self.eq(response['ObjectLockMode'], 'GOVERNANCE')
        self.eq(response['ObjectLockRetainUntilDate'], datetime.datetime(2030, 1, 1, tzinfo=pytz.UTC))
        self.eq(response['ObjectLockLegalHoldStatus'], 'ON')
        client.put_object_legal_hold(Bucket=bucket_name, Key=key, LegalHold={'Status': 'OFF'})
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=response['VersionId'],
                             BypassGovernanceRetention=True)

    def test_object_lock_changing_mode_from_governance_with_bypass(self, s3cfg_global_unique):
        """
        (operation='Test changing object retention mode from GOVERNANCE to COMPLIANCE with bypass')
        (assertion='succeeds')
        """
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client = get_client(s3cfg_global_unique)
        key = 'file1'
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        # upload object with mode=GOVERNANCE
        retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
        client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                          ObjectLockRetainUntilDate=retain_until)
        # change mode to COMPLIANCE
        retention = {'Mode': 'COMPLIANCE', 'RetainUntilDate': retain_until}
        client.put_object_retention(Bucket=bucket_name, Key=key, Retention=retention, BypassGovernanceRetention=True)

    def test_object_lock_changing_mode_from_governance_without_bypass(self, s3cfg_global_unique):
        """
        (operation='Test changing object retention mode from GOVERNANCE to COMPLIANCE without bypass')
        (assertion='fails')
        """
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client = get_client(s3cfg_global_unique)
        key = 'file1'
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        # upload object with mode=GOVERNANCE
        retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
        client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='GOVERNANCE',
                          ObjectLockRetainUntilDate=retain_until)
        # try to change mode to COMPLIANCE
        retention = {'Mode': 'COMPLIANCE', 'RetainUntilDate': retain_until}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    def test_object_lock_changing_mode_from_compliance(self, s3cfg_global_unique):
        """
        (operation='Test changing object retention mode from COMPLIANCE to GOVERNANCE')
        (assertion='fails')
        """
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client = get_client(s3cfg_global_unique)
        key = 'file1'
        client.create_bucket(Bucket=bucket_name, ObjectLockEnabledForBucket=True)
        # upload object with mode=COMPLIANCE
        retain_until = datetime.datetime.now(pytz.utc) + datetime.timedelta(seconds=10)
        client.put_object(Bucket=bucket_name, Body='abc', Key=key, ObjectLockMode='COMPLIANCE',
                          ObjectLockRetainUntilDate=retain_until)
        # try to change mode to GOVERNANCE
        retention = {'Mode': 'GOVERNANCE', 'RetainUntilDate': retain_until}
        e = assert_raises(ClientError, client.put_object_retention, Bucket=bucket_name, Key=key, Retention=retention)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')
