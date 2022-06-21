import base64
import datetime
import hashlib
import hmac
import json
import string
from collections import OrderedDict

import pytest
import pytz
import requests

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestTaggingBase(TestBaseClass):

    def make_random_string(self, size):
        return self.gen_rand_string(size, chars=string.ascii_letters)


@pytest.mark.ess
class TestObjectTagging(TestTaggingBase):

    def test_get_obj_tagging(self, s3cfg_global_unique):
        """
        测试-验证设置和获取对象的tagging
        """
        key = 'testputtags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        input_tag_set = self.create_simple_tag_set(2)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

    def test_get_obj_head_tagging(self, s3cfg_global_unique):
        """
        测试-验证head-object里含有设置的tag
        """
        key = 'testputtags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        count = 2
        input_tag_set = self.create_simple_tag_set(count)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.head_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['x-amz-tagging-count'], str(count))

    def test_put_max_tags(self, s3cfg_global_unique):
        """
        测试-验证最大允许设置的tags（10个）
        """
        key = 'testputmaxtags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        input_tag_set = self.create_simple_tag_set(10)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

    def test_put_excess_tags(self, s3cfg_global_unique):
        """
        测试-验证最大允许设置的tags（11个）， failed
        """
        key = 'testputmaxtags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        input_tag_set = self.create_simple_tag_set(11)
        e = assert_raises(
            ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidTag')

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(len(response['TagSet']), 0)

    def test_put_max_kvsize_tags(self, s3cfg_global_unique):
        """
        测试-验证设置tag是key和value的最大字符数：key为128，value为256
        """
        key = 'testputmaxkeysize'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        tag_set = []
        for i in range(10):
            k = self.make_random_string(128)
            v = self.make_random_string(256)
            tag_set.append({'Key': k, 'Value': v})

        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        for kv_pair in response['TagSet']:
            self.eq((kv_pair in input_tag_set['TagSet']), True)

    def test_put_excess_key_tags(self, s3cfg_global_unique):
        """
        测试-验证设置tag是key和value的最大字符数：key为129，value为256，failed
        """
        key = 'testputexcesskeytags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        tag_set = []
        for i in range(10):
            k = self.make_random_string(129)
            v = self.make_random_string(256)
            tag_set.append({'Key': k, 'Value': v})

        input_tag_set = {'TagSet': tag_set}
        e = assert_raises(ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidTag')

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(len(response['TagSet']), 0)

    def test_put_excess_val_tags(self, s3cfg_global_unique):
        """
        测试-验证设置tag是key和value的最大字符数：key为128，value为257，failed
        """
        key = 'testputexcesskeytags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        tag_set = []
        for i in range(10):
            k = self.make_random_string(128)
            v = self.make_random_string(257)
            tag_set.append({'Key': k, 'Value': v})

        input_tag_set = {'TagSet': tag_set}
        e = assert_raises(ClientError, client.put_object_tagging, Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidTag')

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(len(response['TagSet']), 0)

    def test_put_modify_tags(self, s3cfg_global_unique):
        """
        测试-验证修改已存在的tags
        """
        key = 'testputmodifytags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        tag_set = [{'Key': 'key', 'Value': 'val'}, {'Key': 'key2', 'Value': 'val2'}]

        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

        tag_set2 = [{'Key': 'key3', 'Value': 'val3'}]

        input_tag_set2 = {'TagSet': tag_set2}

        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set2)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set2['TagSet'])

    def test_put_delete_tags(self, s3cfg_global_unique):
        """
        测试-验证删除tags
        """
        key = 'testputmodifytags'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        input_tag_set = self.create_simple_tag_set(2)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

        response = client.delete_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(len(response['TagSet']), 0)

    def test_post_object_tags_anonymous_request(self, s3cfg_global_unique):
        """
        测试-验证设置对象tags，通过browser based via POST request
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        key_name = "foo.txt"
        input_tag_set = self.create_simple_tag_set(2)
        # xml_input_tag_set is the same as input_tag_set in xml.
        # There is not a simple way to change input_tag_set to xml like there is in the boto2 tetss
        xml_input_tag_set = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"

        payload = OrderedDict([
            ("key", key_name),
            ("acl", "public-read"),
            ("Content-Type", "text/plain"),
            ("tagging", xml_input_tag_set),
            ('file', 'bar'),
        ])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key=key_name)
        body = self.get_body(response)
        self.eq(body, 'bar')

        response = client.get_object_tagging(Bucket=bucket_name, Key=key_name)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

    def test_post_object_tags_authenticated_request(self, s3cfg_global_unique):
        """
        测试-验证
        (operation='authenticated browser based upload via POST request')
        (assertion='succeeds and returns written data')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024],
                               ["starts-with", "$tagging", ""]
                           ]}

        # xml_input_tag_set is the same as `input_tag_set = self.create_simple_tag_set(2)` in xml
        # There is not a simple way to change input_tag_set to xml like there is in the boto2 tetss
        xml_input_tag_set = "<Tagging><TagSet><Tag><Key>0</Key><Value>0</Value></Tag><Tag><Key>1</Key><Value>1</Value></Tag></TagSet></Tagging>"

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([
            ("key", "foo.txt"),
            ("AWSAccessKeyId", aws_access_key_id),
            ("acl", "private"), ("signature", signature), ("policy", policy),
            ("tagging", xml_input_tag_set),
            ("Content-Type", "text/plain"),
            ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_put_obj_with_tags(self, s3cfg_global_unique):
        """
        (operation='Test PutObj with tagging headers')
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'testtagobj1'
        data = 'A' * 100

        tag_set = [{'Key': 'bar', 'Value': ''}, {'Key': 'foo', 'Value': 'bar'}]

        put_obj_tag_headers = {
            'x-amz-tagging': 'foo=bar&bar'
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(put_obj_tag_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)

        client.put_object(Bucket=bucket_name, Key=key, Body=data)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, data)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        response_tag_set = response['TagSet']
        tag_set = tag_set
        self.eq(response_tag_set, tag_set)


class TestBucketTagging(TestTaggingBase):

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：当没设置桶标签的时候，获取标签返回NoSuchTagSetError", run=True, strict=True)
    def test_set_bucket_tagging(self, s3cfg_global_unique):
        """
        测试-验证设置存储桶的tags
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)  # won't raise ClientError
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)

        # https://github.com/ceph/s3-tests/commit/5b08b26453d8362c87a496b0a9cd448a6c331ddf
        self.eq(error_code, 'NoSuchTagSet')

        tags = {
            'TagSet': [
                {
                    'Key': 'Hello',
                    'Value': 'World'
                },
            ]
        }
        client.put_bucket_tagging(Bucket=bucket_name, Tagging=tags)

        response = client.get_bucket_tagging(Bucket=bucket_name)
        self.eq(len(response['TagSet']), 1)
        self.eq(response['TagSet'][0]['Key'], 'Hello')
        self.eq(response['TagSet'][0]['Value'], 'World')

        client.delete_bucket_tagging(Bucket=bucket_name)
        e = assert_raises(ClientError, client.get_bucket_tagging, Bucket=bucket_name)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchTagSet')
