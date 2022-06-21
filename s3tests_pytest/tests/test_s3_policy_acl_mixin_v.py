
import pytest

from s3tests_pytest.functional.policy import make_json_policy
from s3tests_pytest.tests import (
    TestBaseClass, ClientError, assert_raises, get_client, get_alt_client)


class TestPolicyAclMixin(TestBaseClass):

    @pytest.mark.ess
    def test_bucket_policy_acl(self, s3cfg_global_unique):
        """
        测试-验证Policy和ACL共同作用；
        Policy：ListBucket-Deny & ACL：authenticated-read
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2], effect="Deny")

        client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        e = assert_raises(ClientError, alt_client.list_objects, Bucket=bucket_name)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

        client.delete_bucket_policy(Bucket=bucket_name)
        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    @pytest.mark.ess
    def test_bucket_v2_policy_acl(self, s3cfg_global_unique):
        """
        测试-验证Policy和ACL共同作用（使用list_objects_v2）；
        Policy：ListBucket-Deny & ACL：authenticated-read
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2], effect="Deny")

        client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        e = assert_raises(ClientError, alt_client.list_objects_v2, Bucket=bucket_name)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

        client.delete_bucket_policy(Bucket=bucket_name)
        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    @pytest.mark.ess
    def test_bucket_policy_put_obj_grant(self, s3cfg_global_unique):
        """
        测试-验证put obj with amz-grant back to bucket-owner
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)

        # In normal cases a key owner would be the uploader of a key in first case
        # we explicitly require that the bucket owner is granted full control over
        # the object uploaded by any user, the second bucket is where no such
        # policy is enforced meaning that the uploader still retains ownership

        main_user_id = s3cfg_global_unique.main_user_id
        alt_user_id = s3cfg_global_unique.alt_user_id

        owner_id_str = "id=" + main_user_id
        s3_conditional = {"StringEquals": {
            "s3:x-amz-grant-full-control": owner_id_str
        }}

        resource1 = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:PutObject",
                                           resource1,
                                           conditions=s3_conditional)

        resource2 = self.make_arn_resource("{}/{}".format(bucket_name2, "*"))
        policy_document2 = make_json_policy("s3:PutObject", resource2)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document2)

        alt_client = get_alt_client(s3cfg_global_unique)
        key1 = 'key1'

        lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-grant-full-control": owner_id_str}))
        alt_client.meta.events.register('before-call.s3.PutObject', lf)

        response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        def remove_header(**kwargs):
            if "x-amz-grant-full-control" in kwargs['params']['headers']:
                del kwargs['params']['headers']["x-amz-grant-full-control"]

        alt_client.meta.events.register('before-call.s3.PutObject', remove_header)

        key2 = 'key2'
        response = alt_client.put_object(Bucket=bucket_name2, Key=key2, Body=key2)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        acl1_response = client.get_object_acl(Bucket=bucket_name, Key=key1)

        # user 1 is trying to get acl for the object from user2 where ownership wasn't transferred
        self.check_access_denied(client.get_object_acl, Bucket=bucket_name2, Key=key2)

        acl2_response = alt_client.get_object_acl(Bucket=bucket_name2, Key=key2)

        self.eq(acl1_response['Grants'][0]['Grantee']['ID'], main_user_id)
        self.eq(acl2_response['Grants'][0]['Grantee']['ID'], alt_user_id)
