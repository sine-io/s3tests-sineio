import datetime
import re
import time
from collections import namedtuple

from dateutil import parser

import pytest
from unittest.case import SkipTest

from munch import Munch

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestLifecycleBase(TestBaseClass):

    @staticmethod
    def configured_storage_classes(config):
        sc = ['STANDARD']

        extra_sc = re.split(r"[\b\W\b]+", config.storage_classes)
        sc.extend(extra_sc)
        sc = set(sc)
        sc = [x for x in sc if x]

        # sc_dict = dict(zip([i.lower() for i in sc], sc))
        # print("storage classes configured: " + str(sc))

        return sc

    def setup_lifecycle_with_two_tags(self, client, bucket_name):
        # factor out common setup code
        tom_key = 'days1/tom'
        tom_tag_set = {'TagSet': [{'Key': 'tom', 'Value': 'sawyer'}]}

        client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

        response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                             Tagging=tom_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        huck_key = 'days1/huck'
        huck_tag_set = {
            'TagSet': [
                {'Key': 'tom', 'Value': 'sawyer'},
                {'Key': 'huck', 'Value': 'finn'}
            ]
        }

        client.put_object(Bucket=bucket_name, Key=huck_key, Body='huck_body')

        response = client.put_object_tagging(Bucket=bucket_name, Key=huck_key,
                                             Tagging=huck_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        lifecycle_config = {
            'Rules': [
                {
                    'Expiration': {
                        'Days': 1,
                    },
                    'ID': 'rule_tag1',
                    'Filter': {
                        'Prefix': 'days1/',
                        'Tag': {
                            'Key': 'tom',
                            'Value': 'sawyer'
                        },
                        'And': {
                            'Prefix': 'days1',
                            'Tags': [
                                {
                                    'Key': 'huck',
                                    'Value': 'finn'
                                },
                            ]
                        }
                    },
                    'Status': 'Enabled',
                },
            ]
        }

        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        return response

    def setup_lifecycle_non_cur_tags(self, client, bucket_name, days):
        # setup for scenario based on vidushi mishra's in rhbz#1877737
        # first create and tag the objects (10 versions of 1)
        key = "myobject_"
        tag_set = {'TagSet': [{'Key': 'vidushi', 'Value': 'mishra'}]}

        for ix in range(10):
            body = "%s v%d" % (key, ix)
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
            self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
            response = client.put_object_tagging(Bucket=bucket_name, Key=key,
                                                 Tagging=tag_set)
            self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        lifecycle_config = {
            'Rules': [
                {
                    'NoncurrentVersionExpiration': {
                        'NoncurrentDays': days,
                    },
                    'ID': 'rule_tag1',
                    'Filter': {
                        'Prefix': '',
                        'Tag': {
                            'Key': 'vidushi',
                            'Value': 'mishra'
                        },
                    },
                    'Status': 'Enabled',
                },
            ]
        }

        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        return response

    @staticmethod
    def verify_lifecycle_expiration_non_cur_tags(client, bucket_name, secs):
        time.sleep(secs)
        try:
            response = client.list_object_versions(Bucket=bucket_name)
            objs_list = response['Versions']
        except ClientError:  # noqa.
            objs_list = []
        return len(objs_list)

    def setup_lifecycle_expiration(self, client, bucket_name, rule_id, delta_days,
                                   rule_prefix):
        rules = [{'ID': rule_id,
                  'Expiration': {'Days': delta_days}, 'Prefix': rule_prefix,
                  'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        key = rule_prefix + 'foo'
        body = 'bar'
        response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        return response

    @staticmethod
    def check_lifecycle_expiration_header(response, start_time, rule_id, delta_days):
        expr_exists = ('x-amz-expiration' in response['ResponseMetadata']['HTTPHeaders'])
        if not expr_exists:
            return False
        expr_hdr = response['ResponseMetadata']['HTTPHeaders']['x-amz-expiration']

        m = re.search(r'expiry-date="(.+)", rule-id="(.+)"', expr_hdr)

        expiration = parser.parse(m.group(1))
        days_to_expire = ((expiration.replace(tzinfo=None) - start_time).days == delta_days)
        rule_eq_id = (m.group(2) == rule_id)

        return days_to_expire and rule_eq_id

    def verify_object(self, client, bucket, key, content=None, sc=None):
        response = client.get_object(Bucket=bucket, Key=key)

        if sc is None:
            sc = 'STANDARD'

        if 'StorageClass' in response:
            self.eq(response['StorageClass'], sc)
        else:  # storage class should be STANDARD
            self.eq('STANDARD', sc)

        if content is not None:
            body = self.get_body(response)
            self.eq(body, content)


@pytest.mark.ess
class TestLifecycleLowLevel(TestLifecycleBase):

    def test_lifecycle_set(self, s3cfg_global_unique):
        """
        测试-设置存储桶生命周期；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status': 'Enabled'},
                 {'ID': 'rule2', 'Expiration': {'Days': 2}, 'Prefix': 'test2/', 'Status': 'Disabled'}]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_lifecycle_get(self, s3cfg_global_unique):
        """
        测试-获取存储桶生命周期；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'test1/', 'Expiration': {'Days': 31}, 'Prefix': 'test1/', 'Status': 'Enabled'},
                 {'ID': 'test2/', 'Expiration': {'Days': 120}, 'Prefix': 'test2/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        self.eq(response['Rules'], rules)

    def test_lifecycle_get_no_id(self, s3cfg_global_unique):
        """
        测试-验证设置的生命周期各项是否正确（不包含ID，因为ID是随机的）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        rules = [{'Expiration': {'Days': 31}, 'Prefix': 'test1/', 'Status': 'Enabled'},
                 {'Expiration': {'Days': 120}, 'Prefix': 'test2/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        current_lc = response['Rules']
        """
        [{'Expiration': {'Days': 31},
          'ID': 'c94ovawac1eotwiuvm7j3hvmfkoz5j6xcmif1blqizj3ijku',
          'Prefix': 'test1/',
          'Status': 'Enabled'},
         {'Expiration': {'Days': 120},
          'ID': 'lcol5tgpq0256d138dkshyitqp9zws2vk0f4bb6m0l5ix1oe',
          'Prefix': 'test2/',
          'Status': 'Enabled'}]
        """
        Rule = namedtuple('Rule', ['prefix', 'status', 'days'])
        rules = {'rule1': Rule('test1/', 'Enabled', 31),
                 'rule2': Rule('test2/', 'Enabled', 120)}
        """
        {'rule1': Rule(prefix='test1/', status='Enabled', days=31),
         'rule2': Rule(prefix='test2/', status='Enabled', days=120)}
        """

        for lc_rule in current_lc:
            if lc_rule['Prefix'] == rules['rule1'].prefix:
                self.eq(lc_rule['Expiration']['Days'], rules['rule1'].days)
                self.eq(lc_rule['Status'], rules['rule1'].status)
                assert 'ID' in lc_rule
            elif lc_rule['Prefix'] == rules['rule2'].prefix:
                self.eq(lc_rule['Expiration']['Days'], rules['rule2'].days)
                self.eq(lc_rule['Status'], rules['rule2'].status)
                self.assertion.assertIn('ID', lc_rule)
            else:
                # neither of the rules we supplied was returned, something wrong
                print("rules not right")
                assert False

    def test_lifecycle_id_too_long(self, s3cfg_global_unique):
        """
        测试-生命周期的ID＞255；
        400，InvalidArgument
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 256 * 'a', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidArgument')

    def test_lifecycle_same_id(self, s3cfg_global_unique):
        """
        测试-生命周期设置相同的ID；
        400，InvalidArgument
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status': 'Enabled'},
                 {'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test2/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidArgument')

    def test_lifecycle_invalid_status(self, s3cfg_global_unique):
        """
        测试-生命周期设置无效的Status；
        400， MalformedXML
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status': 'enabled'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

        rules = [{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status': 'disabled'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

        rules = [{'ID': 'rule1', 'Expiration': {'Days': 2}, 'Prefix': 'test1/', 'Status': 'invalid'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_lifecycle_set_date(self, s3cfg_global_unique):
        """
        测试-生命周期设置Date
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Date': '2017-09-27'}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}

        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_lifecycle_set_invalid_date(self, s3cfg_global_unique):
        """
        测试-生命周期设置无效的Date(not iso8601 date);
        400， MalformedXML
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Date': '20200101'}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}

        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        print(status, error_code)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    def test_lifecycle_set_multipart(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期参数：AbortIncompleteMultipartUpload 是否成功
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [
            {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
             'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
            {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled',
             'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 3}}
        ]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)


class TestLifecycleHighLevel(TestLifecycleBase):

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="Expiration的Days=0也能设置成功，不符合S3标准", run=True, strict=True)
    def test_lifecycle_expiration_days0(self, s3cfg_global_unique):
        """
        测试-验证生命周期的过期规则里Days不能设置为0；
        """
        # https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_LifecycleExpiration.html
        # Days: Indicates the lifetime, in days, of the objects that are subject to the rule.
        #   The value must be a non-zero positive integer.

        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['days0/foo', 'days0/bar'])

        rules = [{'Expiration': {'Days': 0}, 'ID': 'rule1', 'Prefix': 'days0/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}

        # days: 0 is legal in a transition rule, but not legal in an expiration rule
        response_code = ""
        try:
            client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        except ClientError as e:
            response_code = e.response['Error']['Code']

        self.eq(response_code, 'InvalidArgument')

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="未返回header：x-amz-expiration，不符合S3标准", run=True, strict=True)
    def test_lifecycle_expiration_header_put(self, s3cfg_global_unique):
        """
        测试-验证生命周期过期规则，put-object的响应Header里面有expiry-date；
        """
        # https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutObject.html
        # Sample Response: Expiration rule created using lifecycle configuration:
        # If an expiration rule that was created on the bucket using lifecycle configuration applies to the object,
        #   you get a response with an x-amz-expiration header, as shown in the following response.
        # For more information, see Transitioning Objects: General Considerations.

        # x-amz-expiration: expiry-date="Fri, 23 Dec 2012 00:00:00 GMT", rule-id="1"

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        now = datetime.datetime.now(None)
        response = self.setup_lifecycle_expiration(client, bucket_name, 'rule1', 1, 'days1/')
        self.eq(self.check_lifecycle_expiration_header(response, now, 'rule1', 1), True)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="未返回header：x-amz-expiration，不符合S3标准", run=True, strict=True)
    def test_lifecycle_expiration_header_head(self, s3cfg_global_unique):
        """
        测试-验证生命周期过期规则，验证head-object的响应Header里面有expiry-date；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        now = datetime.datetime.now(None)
        self.setup_lifecycle_expiration(client, bucket_name, 'rule1', 1, 'days1/')

        key = 'days1/' + 'foo'
        # stat the object, check header
        response = client.head_object(Bucket=bucket_name, Key=key)
        from pprint import pprint
        pprint(response)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(self.check_lifecycle_expiration_header(response, now, 'rule1', 1), True)

    @pytest.mark.ess
    def test_lifecycle_transition_set_invalid_date(self, s3cfg_global_unique):
        """
        测试-设置生命周期的转储日期为无效的Date(not iso8601 date)；
        400，MalformedXML
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'Date': '2023-09-27'},
                  'Transitions': [{'Date': '20220927', 'StorageClass': 'GLACIER'}], 'Prefix': 'test1/',
                  'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        e = assert_raises(ClientError, client.put_bucket_lifecycle_configuration, Bucket=bucket_name,
                          LifecycleConfiguration=lifecycle)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, "MalformedXML")

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="未返回header：x-amz-expiration，不符合S3标准", run=True, strict=True)
    def test_lifecycle_expiration_header_tags_head(self, s3cfg_global_unique):
        """
        测试-验证生命周期过期规则+tags，验证head-object的响应Header里面有expiry-date；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        """
        Tag: This tag must exist in the object's tag set in order for the rule to apply.
            Key: Name of the object key --- maybe the tag name.
            Value: Value of the tag.
        """
        lifecycle = {
            "Rules": [
                {
                    "Filter": {
                        "Tag": {"Key": "key1", "Value": "tag1"}
                    },
                    "Status": "Enabled",
                    "Expiration": {
                        "Days": 1
                    },
                    "ID": "rule1"
                },
            ]
        }
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        key1 = "obj_key1"
        body1 = "obj_key1_body"
        tags1 = {'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
                            {'Key': 'key5', 'Value': 'tag5'}]}
        client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
        client.put_object_tagging(Bucket=bucket_name, Key=key1, Tagging=tags1)

        # stat the object, check header
        response = client.head_object(Bucket=bucket_name, Key=key1)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(self.check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1), True)

        # test that header is not returning when it should not
        lifecycle = {
            "Rules": [
                {
                    "Filter": {
                        "Tag": {"Key": "key2", "Value": "tag1"}
                    },
                    "Status": "Enabled",
                    "Expiration": {
                        "Days": 1
                    },
                    "ID": "rule1"
                },
            ]
        }
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        # stat the object, check header
        response = client.head_object(Bucket=bucket_name, Key=key1)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(self.check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1), False)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="未返回header：x-amz-expiration，不符合S3标准", run=True, strict=True)
    def test_lifecycle_expiration_header_and_tags_head(self, s3cfg_global_unique):
        """
        测试-验证生命周期过期规则+tags（with And），验证head-object的响应Header里面有expiry-date；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        lifecycle = {
            "Rules": [
                {
                    "Filter": {
                        "And": {
                            "Tags": [
                                {
                                    "Key": "key1",
                                    "Value": "tag1"
                                },
                                {
                                    "Key": "key5",
                                    "Value": "tag6"
                                }
                            ]
                        }
                    },
                    "Status": "Enabled",
                    "Expiration": {
                        "Days": 1
                    },
                    "ID": "rule1"
                },
            ]
        }
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        key1 = "obj_key1"
        body1 = "obj_key1_body"
        tags1 = {'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
                            {'Key': 'key5', 'Value': 'tag5'}]}
        client.put_object(Bucket=bucket_name, Key=key1, Body=body1)
        client.put_object_tagging(Bucket=bucket_name, Key=key1, Tagging=tags1)

        # stat the object, check header
        response = client.head_object(Bucket=bucket_name, Key=key1)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(self.check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1), False)

        tags2 = {'TagSet': [{'Key': 'key1', 'Value': 'tag1'},
                            {'Key': 'key5', 'Value': 'tag6'}]}
        client.put_object_tagging(Bucket=bucket_name, Key=key1, Tagging=tags2)

        # stat the object, check header
        response = client.head_object(Bucket=bucket_name, Key=key1)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(self.check_lifecycle_expiration_header(response, datetime.datetime.now(None), 'rule1', 1), True)

    @pytest.mark.ess
    def test_lifecycle_set_non_current(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期时添加NoncurrentVersionExpiration参数；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['past/foo', 'future/bar'])
        rules = [
            {'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}, 'Prefix': 'past/',
             'Status': 'Enabled'},
            {'ID': 'rule2', 'NoncurrentVersionExpiration': {'NoncurrentDays': 3}, 'Prefix': 'future/',
             'Status': 'Enabled'}
        ]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_lifecycle_set_delete_marker(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期时添加ExpiredObjectDeleteMarker参数
        """
        # Indicates whether Amazon S3 will remove a delete marker with no noncurrent versions.
        # If set to true, the delete marker will be expired; if set to false the policy takes no action.
        # This cannot be specified with Days or Date in a Lifecycle Expiration Policy.
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [
            {'ID': 'rule1',
             'Expiration': {'ExpiredObjectDeleteMarker': True},
             'Prefix': 'test1/',
             'Status': 'Enabled'}
        ]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_lifecycle_set_filter(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期中ExpiredObjectDeleteMarker参数+Filter参数
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Filter': {'Prefix': 'foo'},
                  'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_lifecycle_set_empty_filter(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期中ExpiredObjectDeleteMarker参数+空Filter+Status参数
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [{'ID': 'rule1', 'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Filter': {}, 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_lifecycle_set_noncurrent_transition(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期转储参数NoncurrentVersionTransitions和NoncurrentVersionExpiration成功
        """
        client = get_client(s3cfg_global_unique)
        sc = self.configured_storage_classes(s3cfg_global_unique)
        if not len(sc) == 3:
            raise SkipTest

        bucket = self.get_new_bucket(client, s3cfg_global_unique)
        rules = [
            {
                'ID': 'rule1',
                'Prefix': 'test1/',
                'Status': 'Enabled',
                'NoncurrentVersionTransitions': [
                    {
                        'NoncurrentDays': 2,
                        'StorageClass': sc[1]
                    },
                    {
                        'NoncurrentDays': 4,
                        'StorageClass': sc[2]
                    }
                ],
                'NoncurrentVersionExpiration': {
                    'NoncurrentDays': 6
                }
            },
            {'ID': 'rule2', 'Prefix': 'test2/', 'Status': 'Disabled',
             'NoncurrentVersionExpiration': {'NoncurrentDays': 3}}
        ]
        lifecycle = {'Rules': rules}
        response = client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)


@pytest.mark.lifecycle_need_speedup
class TestLifecycleNeedSpeedup(TestLifecycleBase):

    @pytest.mark.ess
    def test_lifecycle_expiration(self, s3cfg_global_unique):
        """
        测试-生命周期-过期规则，需要设置加速（10秒为1天）；
        """
        # The test harness for lifecycle is configured to treat days as 10 second intervals.
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['expire1/foo', 'expire1/bar', 'keep2/foo',
                                                                     'keep2/bar', 'expire3/foo', 'expire3/bar'])

        rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'expire1/', 'Status': 'Enabled'},
                 {'ID': 'rule2', 'Expiration': {'Days': 5}, 'Prefix': 'expire3/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response = client.list_objects(Bucket=bucket_name)
        init_objects = response['Contents']

        lc_interval = s3cfg_global_unique.lc_debug_interval

        time.sleep(3 * lc_interval)
        response = client.list_objects(Bucket=bucket_name)
        expire1_objects = response['Contents']

        time.sleep(lc_interval)
        response = client.list_objects(Bucket=bucket_name)
        keep2_objects = response['Contents']

        time.sleep(3 * lc_interval)
        response = client.list_objects(Bucket=bucket_name)
        expire3_objects = response['Contents']

        self.eq(len(init_objects), 6)
        self.eq(len(expire1_objects), 4)
        self.eq(len(keep2_objects), 4)
        self.eq(len(expire3_objects), 2)

    @pytest.mark.ess
    def test_lifecycle_v2_expiration(self, s3cfg_global_unique):
        """
        测试-生命周期-过期规则，需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.create_objects(s3cfg_global_unique,
                                          keys=['expire1/foo', 'expire1/bar', 'keep2/foo', 'keep2/bar', 'expire3/foo',
                                                'expire3/bar'])
        rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'expire1/', 'Status': 'Enabled'},
                 {'ID': 'rule2', 'Expiration': {'Days': 5}, 'Prefix': 'expire3/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response = client.list_objects_v2(Bucket=bucket_name)
        init_objects = response['Contents']

        lc_interval = s3cfg_global_unique.lc_debug_interval

        time.sleep(3 * lc_interval)
        response = client.list_objects_v2(Bucket=bucket_name)
        expire1_objects = response['Contents']

        time.sleep(lc_interval)
        response = client.list_objects_v2(Bucket=bucket_name)
        keep2_objects = response['Contents']

        time.sleep(3 * lc_interval)
        response = client.list_objects_v2(Bucket=bucket_name)
        expire3_objects = response['Contents']

        self.eq(len(init_objects), 6)
        self.eq(len(expire1_objects), 4)
        self.eq(len(keep2_objects), 4)
        self.eq(len(expire3_objects), 2)

    @pytest.mark.ess
    def test_lifecycle_expiration_versioning_enabled(self, s3cfg_global_unique):
        """
        测试-生命周期过期+多版本对象，需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.create_multiple_versions(client, bucket_name, "test1/a", 1)
        client.delete_object(Bucket=bucket_name, Key="test1/a")

        rules = [{'ID': 'rule1', 'Expiration': {'Days': 1}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        lc_interval = s3cfg_global_unique.lc_debug_interval

        time.sleep(3 * lc_interval)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        delete_markers = response['DeleteMarkers']
        self.eq(len(versions), 1)
        self.eq(len(delete_markers), 1)

    @pytest.mark.ess_maybe
    def test_lifecycle_expiration_with_one_tag(self, s3cfg_global_unique):
        """
        测试-生命周期过期规则+对象tag(1个)，需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        tom_key = 'days1/tom'
        tom_tag_set = {'TagSet': [{'Key': 'tom', 'Value': 'sawyer'}]}

        client.put_object(Bucket=bucket_name, Key=tom_key, Body='tom_body')

        response = client.put_object_tagging(Bucket=bucket_name, Key=tom_key,
                                             Tagging=tom_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        lifecycle_config = {
            'Rules': [
                {
                    'Expiration': {
                        'Days': 1,
                    },
                    'ID': 'rule_tag1',
                    'Filter': {
                        'Prefix': 'days1/',
                        'Tag': {
                            'Key': 'tom',
                            'Value': 'sawyer'
                        },
                    },
                    'Status': 'Enabled',
                },
            ]
        }

        response = client.put_bucket_lifecycle_configuration(Bucket=bucket_name,
                                                             LifecycleConfiguration=lifecycle_config)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        lc_interval = s3cfg_global_unique.lc_debug_interval

        time.sleep(3 * lc_interval)

        try:
            expire_objects = response['Contents']
        except KeyError:
            expire_objects = []

        self.eq(len(expire_objects), 0)

    @pytest.mark.ess_maybe
    def test_lifecycle_expiration_with_two_tags(self, s3cfg_global_unique):
        """
        测试-生命周期过期规则+对象tag（2个），需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.setup_lifecycle_with_two_tags(client, bucket_name)

        lc_interval = s3cfg_global_unique.lc_debug_interval
        time.sleep(3 * lc_interval)

        response = client.list_objects(Bucket=bucket_name)
        expire1_objects = response['Contents']

        self.eq(len(expire1_objects), 1)

    @pytest.mark.ess_maybe
    def test_lifecycle_expiration_versioned_with_two_tags(self, s3cfg_global_unique):
        """
        测试-生命周期过期规则+对象多版本+对象tag（2个），需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        # mixin versioning
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        self.setup_lifecycle_with_two_tags(client, bucket_name)

        lc_interval = s3cfg_global_unique.lc_debug_interval
        time.sleep(3 * lc_interval)
        response = client.list_objects(Bucket=bucket_name)
        expire1_objects = response['Contents']

        self.eq(len(expire1_objects), 1)

    @pytest.mark.ess_maybe
    def test_lifecycle_expiration_non_cur_with_one_tag(self, s3cfg_global_unique):
        """
        测试-生命周期NonCurrent过期规则+对象多版本+对象tag（1个），需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        # create 10 object versions (9 noncurrent) and a tag-filter noncurrent version expiration at 4 "days"
        self.setup_lifecycle_non_cur_tags(client, bucket_name, 4)

        lc_interval = s3cfg_global_unique.lc_debug_interval

        num_objs = self.verify_lifecycle_expiration_non_cur_tags(
            client, bucket_name, 2 * lc_interval)

        # at T+20, 10 objects should exist
        self.eq(num_objs, 10)

        num_objs = self.verify_lifecycle_expiration_non_cur_tags(
            client, bucket_name, 5 * lc_interval)

        # at T+60, only the current object version should exist
        self.eq(num_objs, 1)

    @pytest.mark.ess
    def test_lifecycle_expiration_date(self, s3cfg_global_unique):
        """
        测试-验证生命周期设置过期日期是否真实生效，需要设置加速（10秒为1天）；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['past/foo', 'future/bar'])
        rules = [{'ID': 'rule1', 'Expiration': {'Date': '2015-01-01'}, 'Prefix': 'past/', 'Status': 'Enabled'},
                 {'ID': 'rule2', 'Expiration': {'Date': '2030-01-01'}, 'Prefix': 'future/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        response = client.list_objects(Bucket=bucket_name)
        init_objects = response['Contents']

        lc_interval = s3cfg_global_unique.lc_debug_interval
        # Wait for first expiration (plus fudge to handle the timer window)
        time.sleep(3 * lc_interval)
        response = client.list_objects(Bucket=bucket_name)
        expire_objects = response['Contents']

        self.eq(len(init_objects), 2)
        self.eq(len(expire_objects), 1)

    @pytest.mark.ess_maybe
    def test_lifecycle_non_cur_expiration(self, s3cfg_global_unique):
        """
        测试-验证生命周期的NoncurrentVersionExpiration规则+多版本，需要设置加速（10秒为1天）；
        """
        # Specifies when noncurrent object versions expire.
        # Upon expiration, Amazon S3 permanently deletes the noncurrent object versions.
        # You set this lifecycle configuration action on a bucket that has versioning enabled (or suspended)
        # to request that Amazon S3 delete noncurrent object versions at a specific period in the object's lifetime.

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.create_multiple_versions(client, bucket_name, "test1/a", 3)

        # not checking the object contents on the second run, because the function doesn't support multiple checks
        self.create_multiple_versions(client, bucket_name, "test2/abc", 3, check_versions=False)

        response = client.list_object_versions(Bucket=bucket_name)
        init_versions = response['Versions']

        rules = [{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 2}, 'Prefix': 'test1/',
                  'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        lc_interval = s3cfg_global_unique.lc_debug_interval
        # Wait for first expiration (plus fudge to handle the timer window)
        time.sleep(5 * lc_interval)

        response = client.list_object_versions(Bucket=bucket_name)
        expire_versions = response['Versions']
        self.eq(len(init_versions), 6)
        self.eq(len(expire_versions), 4)

    @pytest.mark.ess_maybe
    def test_lifecycle_delete_marker_expiration(self, s3cfg_global_unique):
        """
        测试-验证生命周期ExpiredObjectDeleteMarker+多版本，需要设置加速（10s为1天）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.create_multiple_versions(client, bucket_name, "test1/a", 1)
        self.create_multiple_versions(client, bucket_name, "test2/abc", 1, check_versions=False)
        client.delete_object(Bucket=bucket_name, Key="test1/a")
        client.delete_object(Bucket=bucket_name, Key="test2/abc")

        response = client.list_object_versions(Bucket=bucket_name)
        init_versions = response['Versions']
        deleted_versions = response['DeleteMarkers']
        total_init_versions = init_versions + deleted_versions

        rules = [{'ID': 'rule1', 'NoncurrentVersionExpiration': {'NoncurrentDays': 1},
                  'Expiration': {'ExpiredObjectDeleteMarker': True}, 'Prefix': 'test1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)
        lc_interval = s3cfg_global_unique.lc_debug_interval
        # Wait for first expiration (plus fudge to handle the timer window)
        time.sleep(7 * lc_interval)

        response = client.list_object_versions(Bucket=bucket_name)
        init_versions = response['Versions']
        deleted_versions = response['DeleteMarkers']
        total_expire_versions = init_versions + deleted_versions

        self.eq(len(total_init_versions), 4)
        self.eq(len(total_expire_versions), 2)

    @pytest.mark.ess
    def test_lifecycle_transition(self, s3cfg_global_unique):
        """
        测试-验证生命周期转储规则生效，需要设置加速（10s为1天）
        """
        # The test harness for lifecycle is configured to treat days as 10 second intervals.
        client = get_client(s3cfg_global_unique)

        sc = self.configured_storage_classes(s3cfg_global_unique)
        if not len(sc) == 3:
            raise SkipTest

        bucket_name = self.create_objects(s3cfg_global_unique, keys=['trans1/foo', 'trans1/bar', 'keep2/foo',
                                                                     'keep2/bar', 'trans3/foo', 'trans3/bar'])
        rules = [{'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': sc[1]}], 'Prefix': 'trans1/',
                  'Status': 'Enabled'},
                 {'ID': 'rule2', 'Transitions': [{'Days': 6, 'StorageClass': sc[2]}], 'Prefix': 'trans3/',
                  'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        # Get list of all keys
        response = client.list_objects(Bucket=bucket_name)
        init_keys = self.get_keys(response)
        self.eq(len(init_keys), 6)

        lc_interval = s3cfg_global_unique.lc_debug_interval

        # Wait for first transition (plus fudge to handle the timer window)
        time.sleep(4 * lc_interval)
        expire1_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(expire1_keys['STANDARD']), 4)
        self.eq(len(expire1_keys[sc[1]]), 2)
        self.eq(len(expire1_keys[sc[2]]), 0)

        # Wait for next transition cycle
        time.sleep(lc_interval)
        keep2_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(keep2_keys['STANDARD']), 4)
        self.eq(len(keep2_keys[sc[1]]), 2)
        self.eq(len(keep2_keys[sc[2]]), 0)

        # Wait for final transition cycle
        time.sleep(5 * lc_interval)
        expire3_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(expire3_keys['STANDARD']), 2)
        self.eq(len(expire3_keys[sc[1]]), 2)
        self.eq(len(expire3_keys[sc[2]]), 2)

    @pytest.mark.ess
    def test_lifecycle_transition_single_rule_multi_trans(self, s3cfg_global_unique):
        """
        测试-验证生命周期转储规则生效，需要设置加速（10s为1天）
        """
        # The test harness for lifecycle is configured to treat days as 10 second intervals.
        client = get_client(s3cfg_global_unique)
        sc = self.configured_storage_classes(s3cfg_global_unique)
        if not len(sc) == 3:
            raise SkipTest

        bucket_name = self.create_objects(s3cfg_global_unique, keys=['trans1/foo', 'trans1/bar', 'keep2/foo',
                                                                     'keep2/bar', 'trans3/foo', 'trans3/bar'])
        rules = [
            {'ID': 'rule1', 'Transitions': [{'Days': 1, 'StorageClass': sc[1]}, {'Days': 7, 'StorageClass': sc[2]}],
             'Prefix': 'trans1/', 'Status': 'Enabled'}]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        # Get list of all keys
        response = client.list_objects(Bucket=bucket_name)
        init_keys = self.get_keys(response)
        self.eq(len(init_keys), 6)
        lc_interval = s3cfg_global_unique.lc_debug_interval

        # Wait for first transition (plus fudge to handle the timer window)
        time.sleep(5 * lc_interval)
        expire1_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(expire1_keys['STANDARD']), 4)
        self.eq(len(expire1_keys[sc[1]]), 2)
        self.eq(len(expire1_keys[sc[2]]), 0)

        # Wait for next transition cycle
        time.sleep(lc_interval)
        keep2_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(keep2_keys['STANDARD']), 4)
        self.eq(len(keep2_keys[sc[1]]), 2)
        self.eq(len(keep2_keys[sc[2]]), 0)

        # Wait for final transition cycle
        time.sleep(6 * lc_interval)
        expire3_keys = self.list_bucket_storage_class(client, bucket_name)
        self.eq(len(expire3_keys['STANDARD']), 4)
        self.eq(len(expire3_keys[sc[1]]), 0)
        self.eq(len(expire3_keys[sc[2]]), 2)

    @pytest.mark.ess_maybe
    def test_lifecycle_noncur_transition(self, s3cfg_global_unique):
        """
        测试-验证生命周期参数NoncurrentVersionTransitions和NoncurrentVersionExpiration生效，需要设置加速（10s为1天）；
        """
        sc = self.configured_storage_classes(s3cfg_global_unique)
        if not len(sc) == 3:
            raise SkipTest

        client = get_client(s3cfg_global_unique)
        bucket = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket, "Enabled", "Enabled")

        rules = [
            {
                'ID': 'rule1',
                'Prefix': 'test1/',
                'Status': 'Enabled',
                'NoncurrentVersionTransitions': [
                    {
                        'NoncurrentDays': 1,
                        'StorageClass': sc[1]
                    },
                    {
                        'NoncurrentDays': 5,
                        'StorageClass': sc[2]
                    }
                ],
                'NoncurrentVersionExpiration': {
                    'NoncurrentDays': 9
                }
            }
        ]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration=lifecycle)

        self.create_multiple_versions(client, bucket, "test1/a", 3)
        self.create_multiple_versions(client, bucket, "test1/b", 3)

        init_keys = self.list_bucket_storage_class(client, bucket)
        self.eq(len(init_keys['STANDARD']), 6)
        lc_interval = s3cfg_global_unique.lc_debug_interval

        time.sleep(4 * lc_interval)
        expire1_keys = self.list_bucket_storage_class(client, bucket)
        self.eq(len(expire1_keys['STANDARD']), 2)
        self.eq(len(expire1_keys[sc[1]]), 4)
        self.eq(len(expire1_keys[sc[2]]), 0)

        time.sleep(4 * lc_interval)
        expire1_keys = self.list_bucket_storage_class(client, bucket)
        self.eq(len(expire1_keys['STANDARD']), 2)
        self.eq(len(expire1_keys[sc[1]]), 0)
        self.eq(len(expire1_keys[sc[2]]), 4)

        time.sleep(6 * lc_interval)
        expire1_keys = self.list_bucket_storage_class(client, bucket)
        self.eq(len(expire1_keys['STANDARD']), 2)
        self.eq(len(expire1_keys[sc[1]]), 0)
        self.eq(len(expire1_keys[sc[2]]), 0)

    @pytest.mark.ess
    def test_lifecycle_multipart_expiration(self, s3cfg_global_unique):
        """
        测试-验证设置生命周期参数：AbortIncompleteMultipartUpload是否符合预期，
        需要设置加速（10秒钟为1天）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        key_names = ['test1/a', 'test2/']
        upload_ids = []

        for key in key_names:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
            upload_ids.append(response['UploadId'])

        response = client.list_multipart_uploads(Bucket=bucket_name)
        init_uploads = response['Uploads']

        rules = [
            {'ID': 'rule1', 'Prefix': 'test1/', 'Status': 'Enabled',
             'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 2}},
        ]
        lifecycle = {'Rules': rules}
        client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lifecycle)

        lc_interval = s3cfg_global_unique.lc_debug_interval

        # Wait for first expiration (plus fudge to handle the timer window)
        time.sleep(5 * lc_interval)

        response = client.list_multipart_uploads(Bucket=bucket_name)
        expired_uploads = response['Uploads']
        self.eq(len(init_uploads), 2)
        self.eq(len(expired_uploads), 1)

