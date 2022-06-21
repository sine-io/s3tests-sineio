import json

import boto3
import pytest
import requests

from s3tests_pytest.functional.policy import Statement, Policy, make_json_policy
from s3tests_pytest.tests import (
    TestBaseClass, assert_raises, ClientError, get_client, get_alt_client, get_v2_client
)


class TestBucketPolicy(TestBaseClass):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/using-iam-policies.html
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/bucket-policies.html
    """

    @pytest.mark.ess
    def test_bucket_policy(self, s3cfg_global_unique):
        """
        测试-验证Policy：给所有用户赋予存储桶ListBucket权限（list_objects）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2])
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects(Bucket=bucket_name)
        self.eq(len(response['Contents']), 1)

    @pytest.mark.ess
    def test_bucket_v2_policy(self, s3cfg_global_unique):
        """
        测试-验证Policy：给所有用户赋予存储桶ListBucket权限（list_objects_v2）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2])
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects_v2(Bucket=bucket_name)
        self.eq(len(response['Contents']), 1)

    @pytest.mark.ess
    def test_bucket_policy_another_bucket(self, s3cfg_global_unique):
        """
        测试-验证从某个桶获取的Policy后给另一个桶设置相同的Policy（使用list_objects进行验证）
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        key2 = 'abcd'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')
        client.put_object(Bucket=bucket_name2, Key=key2, Body='abcd')

        resource1 = "arn:aws:s3:::*"
        resource2 = "arn:aws:s3:::*/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2])

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        response = client.get_bucket_policy(Bucket=bucket_name)
        response_policy = response['Policy']
        client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects(Bucket=bucket_name)
        self.eq(len(response['Contents']), 1)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects(Bucket=bucket_name2)
        self.eq(len(response['Contents']), 1)

    @pytest.mark.ess
    def test_bucket_v2_policy_another_bucket(self, s3cfg_global_unique):
        """
        测试-验证从某个桶获取的Policy后给另一个桶设置相同的Policy（使用list_objects_v2进行验证）
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'asdf'
        key2 = 'abcd'
        client.put_object(Bucket=bucket_name, Key=key, Body='asdf')
        client.put_object(Bucket=bucket_name2, Key=key2, Body='abcd')

        resource1 = "arn:aws:s3:::*"
        resource2 = "arn:aws:s3:::*/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2])

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        response = client.get_bucket_policy(Bucket=bucket_name)
        response_policy = response['Policy']

        client.put_bucket_policy(Bucket=bucket_name2, Policy=response_policy)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects_v2(Bucket=bucket_name)
        self.eq(len(response['Contents']), 1)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.list_objects_v2(Bucket=bucket_name2)
        self.eq(len(response['Contents']), 1)

    @pytest.mark.ess
    def test_bucket_policy_set_condition_operator_end_with_if_exists(self, s3cfg_global_unique):
        """
        测试-验证Policy的Condition中的StringLikeIfExists参数+aws:Referer
        """
        main_client = get_client(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(main_client, s3cfg_global_unique)
        key = 'foo'
        main_client.put_object(Bucket=bucket_name, Key=key)

        arn_resource = self.make_arn_resource(f"{bucket_name}/*")
        condition = {
            "StringLikeIfExists": {
                "aws:Referer": "http://www.example.com/*"
            }
        }
        policy_document = make_json_policy("s3:GetObject", arn_resource, conditions=condition)

        # boto3.set_stream_logger(name='botocore')  # uncomment this line if want to capture more logs.
        main_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        response = alt_client.get_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)  # because of x-IfExists, so PASSED.

        request_headers = {'referer': 'http://www.example.com/'}  # liked referer
        lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
        alt_client.meta.events.register('before-call.s3.GetObject', lf)
        response = alt_client.get_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        request_headers = {'referer': 'http://www.example.com/index.html'}  # liked referer
        lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
        alt_client.meta.events.register('before-call.s3.GetObject', lf)
        response = alt_client.get_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        request_headers = {'referer': 'http://www.example.com'}  # unliked referer, will raise ClientError
        lf = (lambda **kwargs: kwargs['params']['headers'].update(request_headers))
        alt_client.meta.events.register('before-call.s3.GetObject', lf)
        e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        # Test Restful api via requests package.
        url = s3cfg_global_unique.default_endpoint
        response = requests.get(url + f'/{bucket_name}/{key}')
        self.eq(response.status_code, 200)

        response = requests.get(url + f'/{bucket_name}/{key}', headers={'referer': 'http://www.example.com/'})
        self.eq(response.status_code, 200)

        response = requests.get(
            url + f'/{bucket_name}/{key}', headers={'referer': 'http://www.example.com/index.com'})
        self.eq(response.status_code, 200)

        response = requests.get(url + f'/{bucket_name}/{key}', headers={'referer': 'http://www.example.com'})
        self.eq(response.status_code, 403)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        存储桶默认为 not public
        """
        # Retrieves the policy status for an Amazon S3 bucket, indicating whether the bucket is public.
        # In order to use this operation, you must have the s3:GetBucketPolicyStatus permission.
        # IsPublic:
        #   The policy status for this bucket.
        #   TRUE indicates that this bucket is public.
        #   FALSE indicates that the bucket is not public.
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], False)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_public_acl_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        ACL为public-read的存储桶为public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], True)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_auth_public_acl_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        ACL为authenticated-read的存储桶为public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_bucket_acl(Bucket=bucket_name, ACL='authenticated-read')
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], True)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_public_policy_acl_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        Policy为s3:ListBucket的存储桶为public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        # 'PolicyStatus': {}
        self.eq(resp['PolicyStatus']['IsPublic'], False)

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2])

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        # 'PolicyStatus': {}
        self.eq(resp['PolicyStatus']['IsPublic'], True)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_non_public_policy_acl_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        Policy为s3:ListBucket但是限制了IP的存储桶为not public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], False)

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        conditions = {
            "IpAddress": {"aws:SourceIp": "10.0.0.0/32"}
        }
        policy_document = make_json_policy("s3:ListBucket", [resource1, resource2], conditions=conditions)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], False)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：IsPublic为False，但是返回结果为空", run=True, strict=True)
    def test_get_non_public_policy_deny_bucket_policy_status(self, s3cfg_global_unique):
        """
        测试-验证使用get_bucket_policy_status接口验证存储桶是否为public，
        Policy为s3:ListBucket且NotPrincipal为arn:aws:iam::s3tenant1:root的存储桶为public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], False)

        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "NotPrincipal": {"AWS": "arn:aws:iam::s3tenant1:root"},
                    "Action": "s3:ListBucket",
                    "Resource": [
                        "{}".format(resource1),
                        "{}".format(resource2)
                    ],
                }]
            })

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        resp = client.get_bucket_policy_status(Bucket=bucket_name)
        self.eq(resp['PolicyStatus']['IsPublic'], True)

    @pytest.mark.ess
    def test_multipart_upload_on_a_bucket_with_policy(self, s3cfg_global_unique):
        """
        测试-验证往设置policy的存储桶里分段上传对象
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        resource1 = "arn:aws:s3:::" + bucket_name
        resource2 = "arn:aws:s3:::" + bucket_name + "/*"
        policy_document = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",  # 所有用户
                    "Action": "*",  # 所有操作
                    "Resource": [
                        resource1,  # 桶
                        resource2  # 桶内对象
                    ],
                }]
            })
        key = "foo"
        obj_len = 50 * 1024 * 1024
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        (upload_id, data, parts) = self.multipart_upload(config=s3cfg_global_unique, bucket_name=bucket_name, key=key,
                                                         size=obj_len, client=client)
        response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                                    MultipartUpload={'Parts': parts})
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：PublicAccessBlockConfiguration返回4个值，但Ceph返回结果为空", run=True, strict=True)
    def test_get_default_public_block(self, s3cfg_global_unique):
        """
        测试-验证获取存储桶默认的public-access-block状态
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        resp = client.get_public_access_block(Bucket=bucket_name)
        # 'PublicAccessBlockConfiguration': {}
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicAcls'], False)
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'], False)
        self.eq(resp['PublicAccessBlockConfiguration']['IgnorePublicAcls'], False)
        self.eq(resp['PublicAccessBlockConfiguration']['RestrictPublicBuckets'], False)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：PublicAccessBlockConfiguration返回4个值，但Ceph返回结果为空", run=True, strict=True)
    def test_put_public_block(self, s3cfg_global_unique):
        """
        测试-验证对存储桶设置public-access-block
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        access_conf = {'BlockPublicAcls': True,
                       'IgnorePublicAcls': True,
                       'BlockPublicPolicy': True,
                       'RestrictPublicBuckets': False}
        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

        resp = client.get_public_access_block(Bucket=bucket_name)
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicAcls'], access_conf['BlockPublicAcls'])
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'], access_conf['BlockPublicPolicy'])
        self.eq(resp['PublicAccessBlockConfiguration']['IgnorePublicAcls'], access_conf['IgnorePublicAcls'])
        self.eq(resp['PublicAccessBlockConfiguration']['RestrictPublicBuckets'], access_conf['RestrictPublicBuckets'])

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：PublicAccessBlockConfiguration返回4个值，但Ceph返回结果为空", run=True, strict=True)
    def test_block_public_put_bucket_acls(self, s3cfg_global_unique):
        """
        测试-验证将BlockPublicAcls设置为True时，是否可以设置ACL
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        access_conf = {'BlockPublicAcls': True,
                       'IgnorePublicAcls': False,
                       'BlockPublicPolicy': True,
                       'RestrictPublicBuckets': False}
        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

        resp = client.get_public_access_block(Bucket=bucket_name)
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicAcls'], access_conf['BlockPublicAcls'])
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'], access_conf['BlockPublicPolicy'])

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='public-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='public-read-write')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='authenticated-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：PublicAccessBlockConfiguration返回4个值，但Ceph返回结果为空", run=True, strict=True)
    def test_block_public_object_canned_acls(self, s3cfg_global_unique):
        """
        测试-验证BlockPublicAcls为True时，上传对象的时候添加ACL，会失败（403）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        access_conf = {'BlockPublicAcls': True,
                       'IgnorePublicAcls': False,
                       'BlockPublicPolicy': False,
                       'RestrictPublicBuckets': False}

        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

        resp = client.get_public_access_block(Bucket=bucket_name)
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicAcls'], access_conf['BlockPublicAcls'])
        self.eq(resp['PublicAccessBlockConfiguration']['BlockPublicPolicy'], access_conf['BlockPublicPolicy'])

        # FIXME: use empty body until #42208
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo1', Body='', ACL='public-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo2', Body='', ACL='public-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo3', Body='',
                          ACL='authenticated-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="Ceph未实现put_public_access_block接口", run=True, strict=True)
    def test_block_public_policy(self, s3cfg_global_unique):
        """
        测试-验证BlockPublicPolicy为True时，给存储桶添加policy报403
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        access_conf = {'BlockPublicAcls': False,
                       'IgnorePublicAcls': False,
                       'BlockPublicPolicy': True,
                       'RestrictPublicBuckets': False}

        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)
        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObject", resource)

        self.check_access_denied(client.put_bucket_policy, Bucket=bucket_name, Policy=policy_document)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="Ceph未实现put_public_access_block接口", run=True, strict=True)
    def test_ignore_public_acls(self, s3cfg_global_unique):
        """
        测试-验证IgnorePublicAcls为True时，对桶设置ACL是失效状态
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)

        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
        # Public bucket should be accessible
        alt_client.list_objects(Bucket=bucket_name)

        client.put_object(Bucket=bucket_name, Key='key1', Body='abcde', ACL='public-read')
        resp = alt_client.get_object(Bucket=bucket_name, Key='key1')
        self.eq(self.get_body(resp), 'abcde')

        access_conf = {'BlockPublicAcls': False,
                       'IgnorePublicAcls': True,
                       'BlockPublicPolicy': False,
                       'RestrictPublicBuckets': False}

        client.put_public_access_block(Bucket=bucket_name, PublicAccessBlockConfiguration=access_conf)

        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')
        # IgnorePublicACLs is true, so regardless this should behave as a private bucket
        self.check_access_denied(alt_client.list_objects, Bucket=bucket_name)
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key='key1')

    @pytest.mark.ess_maybe
    @pytest.mark.fails_on_ess  # TODO: remove this 'fails_on_rgw' once I get the test passing
    @pytest.mark.xfail(reason="加密传输，现阶段不严重，而且用例也不完善", run=True, strict=True)
    def test_bucket_policy_put_obj_enc(self, s3cfg_global_unique):
        """
        (operation='Deny put obj requests without encryption')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        v2_client = get_v2_client(s3cfg_global_unique)

        deny_incorrect_algo = {
            "StringNotEquals": {
                "s3:x-amz-server-side-encryption": "AES256"
            }
        }

        deny_unencrypted_obj = {
            "Null": {
                "s3:x-amz-server-side-encryption": "true"
            }
        }

        p = Policy()
        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))

        s1 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_incorrect_algo)
        s2 = Statement("s3:PutObject", resource, effect="Deny", condition=deny_unencrypted_obj)
        policy_document = p.add_statement(s1).add_statement(s2).to_json()

        # boto3.set_stream_logger(name='botocore')

        v2_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        key1_str = 'testobj'

        # response = client.get_bucket_policy(Bucket=bucket_name)
        # print response

        self.check_access_denied(v2_client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

        sse_client_headers = {
            'x-amz-server-side-encryption': 'AES256',
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        v2_client.meta.events.register('before-call.s3.PutObject', lf)
        # TODO: why is this a 400 and not passing,
        #       it appears boto3 is not parsing the 200 response the rgw sends back properly
        # DEBUGGING: run the boto2 and compare the requests
        # DEBUGGING: try to run this with v2 auth (figure out why get_v2_client isn't working)
        #            to make the requests similar to what boto2 is doing
        # DEBUGGING: try to add other options to put_object to see if that makes the response better
        v2_client.put_object(Bucket=bucket_name, Key=key1_str)
