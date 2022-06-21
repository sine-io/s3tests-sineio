import socket
import ssl
import string
import time

import pytz
import datetime
import json
import base64
import hmac
import hashlib
from collections import OrderedDict
import requests
import pytest

from s3tests_pytest.tests import (
    TestBaseClass, ClientError,
    assert_raises, FakeWriteFile,
    FakeReadFile, get_client, get_alt_client, get_unauthenticated_client
)


class TestObjectBase(TestBaseClass):

    @staticmethod
    def simple_http_req_100_cont(host, port, is_secure, method, resource):
        """
        Send the specified request w/expect 100-continue
        and await confirmation.
        """
        req_str = '{method} {resource} HTTP/1.1\r\nHost: {host}\r\nAccept-Encoding: identity\r\nContent-Length: 123\r\nExpect: 100-continue\r\n\r\n'.format(
            method=method,
            resource=resource,
            host=host,
        )

        req = bytes(req_str, 'utf-8')

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if is_secure:
            s = ssl.wrap_socket(s)
        s.settimeout(5)
        s.connect((host, port))
        s.send(req)

        try:
            data = s.recv(1024)
        except socket.error as msg:
            print('got response: ', msg)
            print('most likely server doesn\'t support 100-continue')

        s.close()
        data_str = data.decode()
        l = data_str.split(' ')

        assert l[0].startswith('HTTP')

        return l[1]

    def atomic_write(self, config, file_size):
        """
        Create a file of A's, use it to set_contents_from_file.
        Verify the contents are all A's.
        Create a file of B's, use it to re-set_contents_from_file.
        Before re-set continues, verify content's still A's
        Re-read the contents, and confirm we get B's
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        obj_name = 'testobj'

        # create <file_size> file of A's
        fp_a = FakeWriteFile(file_size, 'A')
        client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_a)

        # verify A's
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'A')

        # create <file_size> file of B's
        # but try to verify the file before we finish writing all the B's
        fp_b = FakeWriteFile(
            file_size, 'B', lambda: self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'A'))

        client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_b)

        # verify B's
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'B')

    def atomic_read(self, config, file_size):
        """
        Create a file of A's, use it to set_contents_from_file.
        Create a file of B's, use it to re-set_contents_from_file.
        Re-read the contents, and confirm we get B's
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)

        fp_a = FakeWriteFile(file_size, 'A')
        client.put_object(Bucket=bucket_name, Key='testobj', Body=fp_a)

        fp_b = FakeWriteFile(file_size, 'B')
        fp_a2 = FakeReadFile(file_size, 'A',
                             lambda: client.put_object(Bucket=bucket_name, Key='testobj', Body=fp_b)
                             )
        read_client = get_client(config)

        read_client.download_fileobj(bucket_name, 'testobj', fp_a2)
        fp_a2.close()

        self.verify_atomic_key_data(client, bucket_name, 'testobj', file_size, 'B')

    def atomic_dual_write(self, config, file_size):
        """
        create an object, two sessions writing different contents
        confirm that it is all one or the other
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        obj_name = 'testobj'
        client.put_object(Bucket=bucket_name, Key=obj_name)

        # write <file_size> file of B's
        # but before we're done, try to write all A's
        fp_a = FakeWriteFile(file_size, 'A')

        def rewind_put_fp_a():
            fp_a.seek(0)
            client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_a)

        fp_b = FakeWriteFile(file_size, 'B', rewind_put_fp_a)
        client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_b)

        # verify the file
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'B')

    def atomic_conditional_write(self, config, file_size):
        """
        Create a file of A's, use it to set_contents_from_file.
        Verify the contents are all A's.
        Create a file of B's, use it to re-set_contents_from_file.
        Before re-set continues, verify content's still A's
        Re-read the contents, and confirm we get B's
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        obj_name = 'testobj'

        # create <file_size> file of A's
        fp_a = FakeWriteFile(file_size, 'A')
        client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_a)

        fp_b = FakeWriteFile(
            file_size, 'B', lambda: self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'A'))

        # create <file_size> file of B's
        # but try to verify the file before we finish writing all the B's
        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_b)

        # verify B's
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'B')

    def atomic_dual_conditional_write(self, config, file_size):
        """
        create an object, two sessions writing different contents
        confirm that it is all one or the other
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        obj_name = 'testobj'

        fp_a = FakeWriteFile(file_size, 'A')
        response = client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_a)
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'A')
        etag_fp_a = response['ETag'].replace('"', '')

        # write <file_size> file of C's
        # but before we're done, try to write all B's
        fp_b = FakeWriteFile(file_size, 'B')
        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': etag_fp_a}))
        client.meta.events.register('before-call.s3.PutObject', lf)

        def rewind_put_fp_b():
            fp_b.seek(0)
            client.put_object(Bucket=bucket_name, Key=obj_name, Body=fp_b)

        fp_c = FakeWriteFile(file_size, 'C', rewind_put_fp_b)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=obj_name, Body=fp_c)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

        # verify the file
        self.verify_atomic_key_data(client, bucket_name, obj_name, file_size, 'B')


class TestObjectOpts(TestObjectBase):

    @pytest.mark.ess
    def test_bucket_list_return_data(self, s3cfg_global_unique):
        """
        测试-验证list_objects返回值中ETag、Size、Owner-DisplayName、Owner-ID、LastModified符合预期
        """
        key_names = ['bar', 'baz', 'foo']
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        data = {}
        for key_name in key_names:
            obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
            acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
            data.update({
                key_name: {
                    'DisplayName': acl_response['Owner']['DisplayName'],
                    'ID': acl_response['Owner']['ID'],
                    'ETag': obj_response['ETag'],
                    'LastModified': obj_response['LastModified'],
                    'ContentLength': obj_response['ContentLength'],
                }
            })

        response = client.list_objects(Bucket=bucket_name)
        objs_list = response['Contents']
        for obj in objs_list:
            key_name = obj['Key']
            key_data = data[key_name]
            self.eq(obj['ETag'], key_data['ETag'])
            self.eq(obj['Size'], key_data['ContentLength'])
            self.eq(obj['Owner']['DisplayName'], key_data['DisplayName'])
            self.eq(obj['Owner']['ID'], key_data['ID'])
            self.compare_dates(obj['LastModified'], key_data['LastModified'])

    @pytest.mark.ess
    def test_object_write_to_non_exist_bucket(self, s3cfg_global_unique):
        """
        测试-验证向不存在的存储桶上传对象，
        404，NoSuchBucket
        """

        client = get_client(s3cfg_global_unique)
        bucket_name = 'whatchutalkinboutwillis'

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='foo')

        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchBucket')

    @pytest.mark.ess
    def test_object_read_not_exist(self, s3cfg_global_unique):
        """
        测试-验证对不存在的对象进行get_object操作，
        404，NoSuchKey
        """

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

    @pytest.mark.ess
    def test_object_request_id_matches_header_on_error(self, s3cfg_global_unique):
        """
        测试-验证错误响应头中会返回RequestId
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        http_response = None

        def get_http_response(**kwargs):
            nonlocal http_response
            http_response = kwargs['http_response'].__dict__

        # get http response after failed request
        client.meta.events.register('after-call.s3.GetObject', get_http_response)
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='bar')

        response_body = http_response['_content']
        resp_body_xml = self.ele_tree.fromstring(response_body)
        request_id = resp_body_xml.find('.//RequestId').text

        assert request_id is not None
        self.eq(request_id, e.response['ResponseMetadata']['RequestId'])

    # ------------------------- DeleteObjects Begin ------------------------------
    # https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_DeleteObjects.html
    # The request contains a list of up to 1000 keys that you want to delete.
    # In the XML, you provide the object key names, and optionally,
    # version IDs if you want to delete a specific version of the object from a versioning-enabled bucket.
    # For each key, Amazon S3 performs a delete action and returns the result of that delete, success, or failure,
    # in the response.
    # Note that if the object specified in the request is not found, Amazon S3 returns the result as deleted.

    @pytest.mark.ess
    def test_multi_object_delete(self, s3cfg_global_unique):
        """
        测试-验证delete_objects 并和 list_objects相结合
        """
        key_names = ['key0', 'key1', 'key2']
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        response = client.list_objects(Bucket=bucket_name)
        self.eq(len(response['Contents']), 3)

        objs_dict = self.make_objs_dict(keys_in=key_names)
        response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
        self.eq(len(response['Deleted']), 3)
        assert 'Errors' not in response

        response = client.list_objects(Bucket=bucket_name)
        assert 'Contents' not in response

        response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
        self.eq(len(response['Deleted']), 3)
        assert 'Errors' not in response

        response = client.list_objects(Bucket=bucket_name)
        assert 'Contents' not in response

    @pytest.mark.ess
    def test_multi_object_v2_delete(self, s3cfg_global_unique):
        """
        测试-验证delete_objects 并和 list_objects_v2相结合
        """
        key_names = ['key0', 'key1', 'key2']
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        response = client.list_objects_v2(Bucket=bucket_name)
        self.eq(len(response['Contents']), 3)

        objs_dict = self.make_objs_dict(keys_in=key_names)
        response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
        self.eq(len(response['Deleted']), 3)
        assert 'Errors' not in response

        response = client.list_objects_v2(Bucket=bucket_name)
        assert 'Contents' not in response

        response = client.delete_objects(Bucket=bucket_name, Delete=objs_dict)
        self.eq(len(response['Deleted']), 3)
        assert 'Errors' not in response

        response = client.list_objects_v2(Bucket=bucket_name)
        assert 'Contents' not in response

    @pytest.mark.ess
    def test_multi_object_delete_key_limit(self, s3cfg_global_unique):
        """
        测试-验证delete_objects每次最多删除1000个对象，并和list_objects结合
        """
        key_names = [f"key-{i}" for i in range(1001)]
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        paginator = client.get_paginator('list_objects')
        pages = paginator.paginate(Bucket=bucket_name)
        num_keys = 0
        for page in pages:
            num_keys += len(page['Contents'])
        self.eq(num_keys, 1001)

        objs_dict = self.make_objs_dict(keys_in=key_names)
        e = assert_raises(ClientError, client.delete_objects, Bucket=bucket_name, Delete=objs_dict)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    @pytest.mark.ess
    def test_multi_object_v2_delete_key_limit(self, s3cfg_global_unique):
        """
        测试-验证delete_objects每次最多删除1000个对象，并和list_objects_v2结合
        """
        key_names = [f"key-{i}" for i in range(1001)]
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        paginator = client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        num_keys = 0
        for page in pages:
            num_keys += len(page['Contents'])
        self.eq(num_keys, 1001)

        objs_dict = self.make_objs_dict(keys_in=key_names)
        e = assert_raises(ClientError, client.delete_objects, Bucket=bucket_name, Delete=objs_dict)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    # ------------------------- DeleteObjects End ------------------------------

    @pytest.mark.ess
    def test_object_head_zero_bytes(self, s3cfg_global_unique):
        """
        测试-验证上传 0 字节的对象，
        响应中的ContentLength=0
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='')

        response = client.head_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ContentLength'], 0)

    @pytest.mark.ess
    def test_object_write_check_etag(self, s3cfg_global_unique):
        """
        测试-验证对象上传并验证ETag只是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(response['ETag'], '"37b51d194a7513e45b56f6524f2d51f2"')

    @pytest.mark.ess
    def test_object_write_cache_control(self, s3cfg_global_unique):
        """
        测试-验证put-object的CacheControl参数
        """
        # Can be used to specify caching behavior along the request/reply chain.
        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.9
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        cache_control = 'public, max-age=14400'
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar', CacheControl=cache_control)

        response = client.head_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['cache-control'], cache_control)

    @pytest.mark.ess
    def test_object_write_expires(self, s3cfg_global_unique):
        """
        测试-验证put-object的Expires参数
        """
        # The date and time at which the object is no longer cacheable.
        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.21
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Expires=expires)

        response = client.head_object(Bucket=bucket_name, Key='foo')
        self.compare_dates(expires, response['Expires'])

    @pytest.mark.ess
    def test_object_write_read_update_read_delete(self, s3cfg_global_unique):
        """
        测试-验证对象基本操作：上传，获取，覆盖写，删除
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        # Write
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        # Read
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')
        # Update
        client.put_object(Bucket=bucket_name, Key='foo', Body='soup')
        # Read
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'soup')
        # Delete
        client.delete_object(Bucket=bucket_name, Key='foo')

    @pytest.mark.ess
    def test_object_metadata_replaced_on_put(self, s3cfg_global_unique):
        """
        测试-验证对象自定义元数据在对象覆盖写的过程中也会被覆盖
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        metadata_dict = {'meta1': 'bar'}
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        got = response['Metadata']
        self.eq(got, {})

    @pytest.mark.ess
    def test_object_write_file(self, s3cfg_global_unique):
        """
        测试-验证put-object方法的Body参数中使用二进制
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data_str = 'bar'
        data = bytes(data_str, 'utf-8')
        client.put_object(Bucket=bucket_name, Key='foo', Body=data)
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_object_anon_put(self, s3cfg_global_unique):
        """
        测试-验证未认证用户对已存在的对象进行覆盖写，
        403，AccessDenied
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        e = assert_raises(ClientError, unauthenticated_client.put_object, Bucket=bucket_name, Key='foo', Body='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @pytest.mark.ess
    def test_object_put_authenticated(self, s3cfg_global_unique):
        """
        测试-验证认证用户上传对象（默认桶ACLs）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        response = client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_put_authenticated_expired(self, s3cfg_global_unique):
        """
        验证预签名时间过期时，是否可以上传对象，
        403错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo')

        params = {'Bucket': bucket_name, 'Key': 'foo'}
        url = client.generate_presigned_url(
            ClientMethod='put_object', Params=params, ExpiresIn=-1000, HttpMethod='PUT')

        # params wouldn't take a 'Body' parameter so we're passing it in here
        res = requests.put(url, data="foo", verify=s3cfg_global_unique.default_ssl_verify).__dict__
        self.eq(res['status_code'], 403)

    @pytest.mark.ess
    def test_100_continue(self, s3cfg_global_unique):
        """
        测试-验证100-continue，
        'succeeds if object is public-read-write'
        """
        # https://docs.aws.amazon.com/zh_cn/zh_cn/AmazonS3/latest/userguide/RESTRedirect.html#RESTRedirectExample
        # https://www.w3.org/Protocols/rfc2616/rfc2616-sec8.html#sec8.2.3

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        obj_name = 'testobj'
        resource = '/{bucket}/{obj}'.format(bucket=bucket_name, obj=obj_name)

        host = s3cfg_global_unique.default_host
        port = s3cfg_global_unique.default_port
        is_secure = s3cfg_global_unique.default_is_secure

        # NOTES: this test needs to be tested when is_secure is True
        status = self.simple_http_req_100_cont(host, port, is_secure, 'PUT', resource)
        self.eq(status, '403')

        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

        status = self.simple_http_req_100_cont(host, port, is_secure, 'PUT', resource)
        self.eq(status, '100')

    @pytest.mark.ess
    def test_ranged_request_response_code(self, s3cfg_global_unique):
        """
        测试-验证get-object的range读取，PASSED
        """
        content = 'testcontent'

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
        response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=4-7')

        fetched_content = self.get_body(response)
        self.eq(fetched_content, content[4:8])
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-range'], 'bytes 4-7/11')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 206)

    @pytest.mark.ess
    def test_ranged_big_request_response_code(self, s3cfg_global_unique):
        """
        测试-验证get-object的range读取（大数据块），PASSED
        """
        content = self.gen_rand_string(8 * 1024 * 1024, chars=string.ascii_letters + string.digits)

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
        response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=3145728-5242880')

        fetched_content = self.get_body(response)
        self.eq(fetched_content, content[3145728:5242881])
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-range'], 'bytes 3145728-5242880/8388608')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 206)

    @pytest.mark.ess
    def test_ranged_request_skip_leading_bytes_response_code(self, s3cfg_global_unique):
        """
        测试-验证Range参数的值为：'bytes=x-'
        """
        content = 'testcontent'

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
        response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=4-')

        fetched_content = self.get_body(response)
        self.eq(fetched_content, content[4:])
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-range'], 'bytes 4-10/11')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 206)

    @pytest.mark.ess
    def test_ranged_request_return_trailing_bytes_response_code(self, s3cfg_global_unique):
        """
        测试-验证Range参数的值为：'bytes=-x'
        """
        content = 'testcontent'

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)
        response = client.get_object(Bucket=bucket_name, Key='testobj', Range='bytes=-7')

        fetched_content = self.get_body(response)
        self.eq(fetched_content, content[-7:])
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-range'], 'bytes 4-10/11')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 206)

    @pytest.mark.ess
    def test_ranged_request_invalid_range(self, s3cfg_global_unique):
        """
        测试-验证Range参数的值为无效的（超过了本身的content长度），
        416，InvalidRange
        """
        content = 'testcontent'

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)

        # test invalid range
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='testobj', Range='bytes=40-50')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 416)
        self.eq(error_code, 'InvalidRange')

    @pytest.mark.ess
    def test_ranged_request_empty_object(self, s3cfg_global_unique):
        """
        测试-验证获取空对象的时候，使用Range参数的值为无效的（超过了本身的content长度），
        416，InvalidRange
        """
        content = ''

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='testobj', Body=content)

        # test invalid range
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='testobj', Range='bytes=40-50')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 416)
        self.eq(error_code, 'InvalidRange')

    @pytest.mark.ess
    @pytest.mark.fails_on_ess  # TODO: results in a 404 instead of 400 on the RGW
    def test_object_read_unreadable(self, s3cfg_global_unique):
        """
        测试-验证下载不存在的对象，
        404，NoSuchKey
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='\xae\x8a-')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')
        # self.eq(status, 400)
        # self.eq(e.response['Error']['Message'], 'Couldn\'t parse the specified URI.')


class TestAtomicWriteAndRead(TestObjectBase):

    @pytest.mark.ess
    def test_atomic_read_1mb(self, s3cfg_global_unique):
        """
        测试-验证原子读取 - 1MB对象
        """
        self.atomic_read(s3cfg_global_unique, 1024 * 1024)

    @pytest.mark.ess
    def test_atomic_read_4mb(self, s3cfg_global_unique):
        """
        测试-验证原子读取 - 4MB对象
        """
        self.atomic_read(s3cfg_global_unique, 1024 * 1024 * 4)

    @pytest.mark.ess
    def test_atomic_read_8mb(self, s3cfg_global_unique):
        """
        测试-验证原子读取 - 8MB对象
        """
        self.atomic_read(s3cfg_global_unique, 1024 * 1024 * 8)

    @pytest.mark.ess
    def test_atomic_write_1mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入 - 1MB对象
        """
        self.atomic_write(s3cfg_global_unique, 1024 * 1024)

    @pytest.mark.ess
    def test_atomic_write_4mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入 - 4MB对象
        """
        self.atomic_write(s3cfg_global_unique, 1024 * 1024 * 4)

    @pytest.mark.ess
    def test_atomic_write_8mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入 - 8MB对象
        """
        self.atomic_write(s3cfg_global_unique, 1024 * 1024 * 8)

    @pytest.mark.ess
    def test_atomic_dual_write_1mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入（同时写） - 1MB对象，
        write one or the other
        """
        self.atomic_dual_write(s3cfg_global_unique, 1024 * 1024)

    @pytest.mark.ess
    def test_atomic_dual_write_4mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入（同时写） - 4MB对象，
        write one or the other
        """
        self.atomic_dual_write(s3cfg_global_unique, 1024 * 1024 * 4)

    @pytest.mark.ess
    def test_atomic_dual_write_8mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入（同时写） - 8MB对象，
        write one or the other
        """
        self.atomic_dual_write(s3cfg_global_unique, 1024 * 1024 * 8)

    @pytest.mark.ess
    def test_atomic_conditional_write_1mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入 - 1MB对象
        """
        self.atomic_conditional_write(s3cfg_global_unique, 1024 * 1024)

    @pytest.mark.ess
    def test_atomic_dual_conditional_write_1mb(self, s3cfg_global_unique):
        """
        测试-验证原子写入 - 1MB对象，
        write one or the other
        """
        self.atomic_dual_conditional_write(s3cfg_global_unique, 1024 * 1024)

    @pytest.mark.fails_on_ess  # TODO: test not passing with SSL, fix this
    @pytest.mark.ess
    def test_atomic_write_bucket_gone(self, s3cfg_global_unique):
        """
        测试-验证往不存在的桶内原子写，
        404，NoSuchBucket
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        def remove_bucket():
            client.delete_bucket(Bucket=bucket_name)

        obj_name = 'foo'
        fp_a = FakeWriteFile(1024 * 1024, 'A', remove_bucket)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=obj_name, Body=fp_a)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchBucket')


class TestObjectNameRules(TestObjectBase):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/object-keys.html

    Object key naming guidelines:
        You can use any UTF-8 character in an object key name.
        However, using certain characters in key names can cause problems with some applications and protocols.
        The following guidelines help you maximize compliance with DNS,
            web-safe characters, XML parsers, and other APIs.

    Safe characters:
        The following character sets are generally safe for use in key names.
        Alphanumeric characters:
            0-9
            a-z
            A-Z
        Special characters:
            Exclamation point (!)
            Hyphen (-)
            Underscore (_)
            Period (.)
            Asterisk (*)
            Single quote (')
            Open parenthesis (()
            Close parenthesis ())

    Characters that might require special handling:
        The following characters in a key name might require additional code handling
            and likely need to be URL encoded or referenced as HEX.
        Some of these are non-printable characters that your browser might not handle,
            which also requires special handling:
                Ampersand ("&")
                Dollar ("$")
                ASCII character ranges 00–1F hex (0–31 decimal) and 7F (127 decimal)
                'At' symbol ("@")
                Equals ("=")
                Semicolon (";")
                Forward slash ("/")
                Colon (":")
                Plus ("+")
                Space – Significant sequences of spaces might be lost in some uses (especially multiple spaces)
                Comma (",")
                Question mark ("?")

    Characters to avoid:
        Avoid the following characters in a key name
            because of significant special handling for consistency across all applications.

            Backslash ("\")
            Left curly brace ("{")
            Non-printable ASCII characters (128–255 decimal characters)
            Caret ("^")
            Right curly brace ("}")
            Percent character ("%")
            Grave accent / back tick ("`")
            Right square bracket ("]")
            Quotation marks
            'Greater Than' symbol (">")
            Left square bracket ("[")
            Tilde ("~")
            'Less Than' symbol ("<")
            'Pound' character ("#")
            Vertical bar / pipe ("|")

    XML related object key constraints:
        As specified by the XML standard on end-of-line handling,
            all XML text is normalized such that single carriage returns (ASCII code 13)
            and carriage returns immediately followed by a line feed (ASCII code 10)
                are replaced by a single line feed character.
            To ensure the correct parsing of object keys in XML requests,
                carriage returns and other special characters must be replaced
                    with their equivalent XML entity code when they are inserted within XML tags.
            The following is a list of such special characters and their equivalent entity codes:

            ' as &apos;
            ” as &quot;
            & as &amp;
            < as &lt;
            > as &gt;
            \r as &#13; or &#x0D;
            \n as &#10; or &#x0A;

        The following example illustrates the use of an XML entity code as a substitution for a carriage return.
        This DeleteObjects request deletes an object with the key parameter:
            /some/prefix/objectwith\rcarriagereturn (where the \r is the carriage return).

            <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              <Object>
                <Key>/some/prefix/objectwith&#13;carriagereturn</Key>
              </Object>
            </Delete>
    """

    @pytest.mark.ess
    def test_bucket_create_special_key_names(self, s3cfg_global_unique):
        """
        测试-验证对象名字使用特殊字符是否成功
        """
        key_names = [
            ' ',
            '"',
            '$',
            '%',
            '&',
            '\'',
            '<',
            '>',
            '_',
            '_ ',
            '_ _',
            '__',
        ]
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=key_names)

        objs_list = self.get_objects_list(client=client, bucket=bucket_name)
        self.eq(key_names, objs_list)

        for name in key_names:
            self.eq((name in objs_list), True)
            response = client.get_object(Bucket=bucket_name, Key=name)
            body = self.get_body(response)
            self.eq(name, body)
            client.put_object_acl(Bucket=bucket_name, Key=name, ACL='private')  # maybe unnecessary, i think.


class TestCopyObject(TestObjectBase):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_CopyObject.html
    """

    @pytest.mark.ess
    def test_object_copy_zero_size(self, s3cfg_global_unique):
        """
        测试-验证在同一个存储桶中拷贝0字节的对象
        """
        key = 'foo123bar'
        bucket_name = self.create_objects(s3cfg_global_unique, keys=[key])
        client = get_client(s3cfg_global_unique)
        fp_a = FakeWriteFile(0, '')
        client.put_object(Bucket=bucket_name, Key=key, Body=fp_a)

        copy_source = {'Bucket': bucket_name, 'Key': key}

        client.copy(copy_source, bucket_name, 'bar321foo')
        response = client.get_object(Bucket=bucket_name, Key='bar321foo')
        self.eq(response['ContentLength'], 0)

    @pytest.mark.ess
    def test_object_copy_same_bucket(self, s3cfg_global_unique):
        """
        测试-验证在同一个存储桶中拷贝对象(非0字节)
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

        client.copy(copy_source, bucket_name, 'bar321foo')

        response = client.get_object(Bucket=bucket_name, Key='bar321foo')
        body = self.get_body(response)
        self.eq('foo', body)

    @pytest.mark.ess
    def test_object_copy_verify_content_type(self, s3cfg_global_unique):
        """
        测试-验证成功拷贝对象的ContentType跟源对象一致
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        content_type = 'text/bla'
        client.put_object(Bucket=bucket_name, ContentType=content_type, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

        client.copy(copy_source, bucket_name, 'bar321foo')

        response = client.get_object(Bucket=bucket_name, Key='bar321foo')
        body = self.get_body(response)
        self.eq('foo', body)
        response_content_type = response['ContentType']
        self.eq(response_content_type, content_type)

    @pytest.mark.ess
    def test_object_copy_to_itself(self, s3cfg_global_unique):
        """
        测试-验证自拷贝对象（不操作元数据）；
        400，InvalidRequest
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}

        e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'foo123bar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidRequest')

    @pytest.mark.ess
    def test_object_copy_to_itself_with_metadata(self, s3cfg_global_unique):
        """
        测试-验证自拷贝对象（覆盖写元数据信息）- PASSED
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')
        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        metadata = {'foo': 'bar'}

        # MetadataDirective: 'COPY'|'REPLACE'
        #   Specifies whether the metadata is copied from the source object
        #   or replaced with metadata provided in the request.
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='foo123bar', Metadata=metadata,
                           MetadataDirective='REPLACE')
        response = client.get_object(Bucket=bucket_name, Key='foo123bar')
        self.eq(response['Metadata'], metadata)

    @pytest.mark.ess
    def test_object_copy_diff_bucket(self, s3cfg_global_unique):
        """
        测试-验证跨桶拷贝对象
        """
        client = get_client(s3cfg_global_unique)
        bucket_name1 = self.get_new_bucket(client, s3cfg_global_unique)
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name1, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name1, 'Key': 'foo123bar'}

        client.copy(copy_source, bucket_name2, 'bar321foo')

        response = client.get_object(Bucket=bucket_name2, Key='bar321foo')
        body = self.get_body(response)
        self.eq('foo', body)

    @pytest.mark.ess
    def test_object_copy_not_owned_bucket(self, s3cfg_global_unique):
        """
        测试-验证跨用户拷贝，
        403错误
        """
        client = get_client(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)
        bucket_name1 = self.get_new_bucket_name(s3cfg_global_unique)
        bucket_name2 = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name1)
        alt_client.create_bucket(Bucket=bucket_name2)

        client.put_object(Bucket=bucket_name1, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name1, 'Key': 'foo123bar'}

        e = assert_raises(ClientError, alt_client.copy, copy_source, bucket_name2, 'bar321foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    def test_object_copy_canned_acl(self, s3cfg_global_unique):
        """
        测试-验证拷贝对象的时候，赋予public-read权限
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo', ACL='public-read')
        # check ACL is applied by doing GET from another user
        alt_client.get_object(Bucket=bucket_name, Key='bar321foo')

        metadata = {'abc': 'def'}
        copy_source = {'Bucket': bucket_name, 'Key': 'bar321foo'}
        client.copy_object(ACL='public-read', Bucket=bucket_name, CopySource=copy_source, Key='foo123bar',
                           Metadata=metadata, MetadataDirective='REPLACE')

        # check ACL is applied by doing GET from another user
        alt_client.get_object(Bucket=bucket_name, Key='foo123bar')

    @pytest.mark.ess
    def test_object_copy_retaining_metadata(self, s3cfg_global_unique):
        """
        测试-验证拷贝对象（保留源对象的metadata）
        """
        for size in [3, 1024 * 1024]:
            client = get_client(s3cfg_global_unique)
            bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
            content_type = 'audio/ogg'

            metadata = {'key1': 'value1', 'key2': 'value2'}
            client.put_object(Bucket=bucket_name, Key='foo123bar', Metadata=metadata, ContentType=content_type,
                              Body=bytearray(size))

            copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
            client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo')

            response = client.get_object(Bucket=bucket_name, Key='bar321foo')
            self.eq(content_type, response['ContentType'])
            self.eq(metadata, response['Metadata'])
            self.eq(size, response['ContentLength'])

    @pytest.mark.ess
    def test_object_copy_replacing_metadata(self, s3cfg_global_unique):
        """
        测试-验证拷贝对象（覆盖写metadata）
        """
        for size in [3, 1024 * 1024]:
            client = get_client(s3cfg_global_unique)
            bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
            content_type = 'audio/ogg'

            metadata = {'key1': 'value1', 'key2': 'value2'}
            client.put_object(Bucket=bucket_name, Key='foo123bar', Metadata=metadata, ContentType=content_type,
                              Body=bytearray(size))

            metadata = {'key3': 'value3', 'key2': 'value2'}
            content_type = 'audio/mpeg'

            copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
            client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='bar321foo', Metadata=metadata,
                               MetadataDirective='REPLACE', ContentType=content_type)

            response = client.get_object(Bucket=bucket_name, Key='bar321foo')
            self.eq(content_type, response['ContentType'])
            self.eq(metadata, response['Metadata'])
            self.eq(size, response['ContentLength'])

    @pytest.mark.ess
    def test_object_copy_bucket_not_found(self, s3cfg_global_unique):
        """
        测试-验证对不存在的桶进行对象拷贝，
        404
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        copy_source = {'Bucket': bucket_name + "-fake", 'Key': 'foo123bar'}
        e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'bar321foo')
        status = self.get_status(e.response)
        self.eq(status, 404)

    @pytest.mark.ess
    def test_object_copy_key_not_found(self, s3cfg_global_unique):
        """
        测试-验证对不存在的对象进行拷贝，
        404
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        e = assert_raises(ClientError, client.copy, copy_source, bucket_name, 'bar321foo')
        status = self.get_status(e.response)
        self.eq(status, 404)

    @pytest.mark.ess
    def test_copy_object_if_match_good(self, s3cfg_global_unique):
        """
        测试-验证copy-object接口的CopySourceIfMatch参数（x-amz-copy-source-if-match: the latest ETag'），
        ceph没做校验，所以是否添加此参数均会返回成功响应。
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        # Copies the object if its entity tag (ETag) matches the specified tag.
        client.copy_object(Bucket=bucket_name, CopySource=bucket_name + '/foo', CopySourceIfMatch=resp['ETag'],
                           Key='bar')
        response = client.get_object(Bucket=bucket_name, Key='bar')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    @pytest.mark.fails_on_ess  # TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
    @pytest.mark.xfail(reason="预期：不匹配的ETag会返回错误，ceph有bug", run=True, strict=True)
    def test_copy_object_if_match_failed(self, s3cfg_global_unique):
        """
        测试-验证copy-object接口的CopySourceIfMatch参数（x-amz-copy-source-if-match: 不匹配的ETag'），
        ceph没做校验，所以不会返回412错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        e = assert_raises(ClientError, client.copy_object, Bucket=bucket_name, CopySource=bucket_name + '/foo',
                          CopySourceIfMatch='ABCORZ', Key='bar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

    @pytest.mark.ess
    @pytest.mark.fails_on_ess  # TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40808 is resolved
    @pytest.mark.xfail(reason="预期：不匹配的ETag会返回错误，ceph有bug", run=True, strict=True)
    def test_copy_object_if_none_match_good(self, s3cfg_global_unique):
        """
        测试-验证copy-object接口的CopySourceIfNoneMatch参数（x-amz-copy-source-if-none-match: the latest ETag'），
        ceph没做校验，所以不会返回412错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        e = assert_raises(ClientError, client.copy_object, Bucket=bucket_name, CopySource=bucket_name + '/foo',
                          CopySourceIfNoneMatch=resp['ETag'], Key='bar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

    @pytest.mark.ess
    def test_copy_object_if_none_match_failed(self, s3cfg_global_unique):
        """
        测试-验证copy-object接口的CopySourceIfMatch参数（x-amz-copy-source-if-none-match: 不匹配的 ETag'），
        ceph没做校验，所以是否添加此参数均会返回成功响应。
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        resp = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        client.copy_object(Bucket=bucket_name, CopySource=bucket_name + '/foo', CopySourceIfNoneMatch='ABCORZ',
                           Key='bar')
        response = client.get_object(Bucket=bucket_name, Key='bar')
        body = self.get_body(response)
        self.eq(body, 'bar')


class TestPresignedURLs(TestObjectBase):
    """
    All objects and buckets are private by default.
    However, you can use a presigned URL to optionally share objects
        or allow your customers/users to upload objects to buckets without AWS security credentials or permissions.

    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/using-presigned-url.html

    The following are credentials that you can use to create a presigned URL:
        1. IAM instance profile: Valid up to 6 hours.
        2. AWS Security Token Service: Valid up to 36 hours when signed with permanent credentials,
            such as the credentials of the AWS account root user or an IAM user.
        3. IAM user: Valid up to 7 days when using AWS Signature Version 4.
        4. To create a presigned URL that's valid for up to 7 days,
            first designate IAM user credentials (the access key and secret key) to the SDK that you're using.
            Then, generate a presigned URL using AWS Signature Version 4.
    """

    @pytest.mark.ess
    def test_object_raw_get_x_amz_expires_not_expired(self, s3cfg_global_unique):
        """
        测试-验证对象预签名在未过期时可以通过HTTP的GET请求直接下载
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object(s3cfg_global_unique)  # only to create bucket and put one object.
        params = {'Bucket': bucket_name, 'Key': 'foo'}

        url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=100000,
                                            HttpMethod='GET')

        res = requests.get(url, verify=s3cfg_global_unique.default_ssl_verify).__dict__
        self.eq(res['status_code'], 200)

    @pytest.mark.ess
    def test_object_raw_get_x_amz_expires_out_range_zero(self, s3cfg_global_unique):
        """
        测试-验证对象预签名在时间过期后不可以通过HTTP的GET请求直接下载，
        403错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object(s3cfg_global_unique)  # only to create bucket and put one object.
        params = {'Bucket': bucket_name, 'Key': 'foo'}

        url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=0, HttpMethod='GET')

        res = requests.get(url, verify=s3cfg_global_unique.default_ssl_verify).__dict__
        self.eq(res['status_code'], 403)

    @pytest.mark.ess
    def test_object_raw_get_x_amz_expires_out_max_range(self, s3cfg_global_unique):
        """
        测试-验证对象预签名的过期时间超过最大值（7天），
        403错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object(s3cfg_global_unique)  # only to create bucket and put one object.
        params = {'Bucket': bucket_name, 'Key': 'foo'}

        url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=609901,
                                            HttpMethod='GET')

        res = requests.get(url, verify=s3cfg_global_unique.default_ssl_verify).__dict__
        self.eq(res['status_code'], 403)

    @pytest.mark.ess
    def test_object_raw_get_x_amz_expires_out_positive_range(self, s3cfg_global_unique):
        """
        测试-验证对象预签名的过期时间为负值的时候是否符合预期，
        403错误
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object(s3cfg_global_unique)  # only to create bucket and put one object.
        params = {'Bucket': bucket_name, 'Key': 'foo'}

        url = client.generate_presigned_url(ClientMethod='get_object', Params=params, ExpiresIn=-7, HttpMethod='GET')

        res = requests.get(url, verify=s3cfg_global_unique.default_ssl_verify).__dict__
        self.eq(res['status_code'], 403)


class TestGetObjectParameters(TestObjectBase):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_GetObject.html
    """

    # IfMatch: Return the object only if its entity tag (ETag) is the same as the one specified;
    #   otherwise, return a 412 (precondition failed) error.

    @pytest.mark.ess
    def test_get_object_if_match_good(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfMatch参数（etag跟获取对象的etag相等）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        etag = response['ETag']

        response = client.get_object(Bucket=bucket_name, Key='foo', IfMatch=etag)
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_get_object_if_match_failed(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfMatch参数（设置错误的参数，返回412）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfMatch='"ABCORZ"')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

    # If-None-Match: Return the object only if its entity tag (ETag) is different from the one specified;
    #   otherwise, return a 304 (not modified) error.

    @pytest.mark.ess
    def test_get_object_if_none_match_good(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfNoneMatch参数（设置为获取对象的etag，返回304）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        response = client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        etag = response['ETag']

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfNoneMatch=etag)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 304)
        self.eq(e.response['Error']['Message'], 'Not Modified')

    @pytest.mark.ess
    def test_get_object_if_none_match_failed(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfNoneMatch参数（设置为不匹配的值，正确获取对象）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo', IfNoneMatch='ABCORZ')
        body = self.get_body(response)
        self.eq(body, 'bar')

    # If-Modified-Since: Return the object only if it has been modified since the specified time;
    #   otherwise, return a 304 (not modified) error.

    @pytest.mark.ess
    def test_get_object_if_modified_since_good(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfModifiedSince参数（设置一个过去时间，验证通过）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo', IfModifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_get_object_if_modified_since_failed(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfModifiedSince参数（设置一个将来时间，验证失败，返回304）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object(Bucket=bucket_name, Key='foo')
        last_modified = str(response['LastModified'])

        last_modified = last_modified.split('+')[0]
        mtime = datetime.datetime.strptime(last_modified, '%Y-%m-%d %H:%M:%S')

        after = mtime + datetime.timedelta(seconds=1)
        after_str = time.strftime("%a, %d %b %Y %H:%M:%S GMT", after.timetuple())

        time.sleep(1)

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo', IfModifiedSince=after_str)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 304)
        self.eq(e.response['Error']['Message'], 'Not Modified')

    # If-Unmodified-Since: Return the object only if it has not been modified since the specified time;
    #   otherwise, return a 412 (precondition failed) error.

    @pytest.mark.ess
    def test_get_object_if_unmodified_since_good(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfUnmodifiedSince参数（设置一个过去时间，验证失败，返回412 PreconditionFailed）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo',
                          IfUnmodifiedSince='Sat, 29 Oct 1994 19:43:31 GMT')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

    @pytest.mark.ess
    def test_get_object_if_unmodified_since_failed(self, s3cfg_global_unique):
        """
        测试-验证get_object方法中的IfUnmodifiedSince参数（设置一个将来时间，验证成功获取对象）
        (operation='get w/ If-Unmodified-Since: after')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo', IfUnmodifiedSince='Sat, 29 Oct 2100 19:43:31 GMT')
        body = self.get_body(response)
        self.eq(body, 'bar')


@pytest.mark.ess
# @pytest.mark.ess_maybe
class TestRegisterHeadersBeforePutObject(TestObjectBase):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutObject.html

    TODO: Do not have those parameters in the docs, I think it's unnecessary to test it for now.
    """

    def test_put_object_if_match_good(self, s3cfg_global_unique):
        """
        (operation='data re-write w/ If-Match: the latest ETag')
        (assertion='replaces previous data and metadata')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        etag = response['ETag'].replace('"', '')

        # pass in custom header 'If-Match' before PutObject call
        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': etag}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key='foo', Body='zar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'zar')

    def test_put_object_if_match_failed(self, s3cfg_global_unique):
        """
        (operation='get w/ If-Match: bogus ETag')
        (assertion='fails 412')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        # pass in custom header 'If-Match' before PutObject call
        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '"ABCORZ"'}))
        client.meta.events.register('before-call.s3.PutObject', lf)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_put_object_if_match_overwrite_existed_good(self, s3cfg_global_unique):
        """
        (operation='overwrite existing object w/ If-Match: *')
        (assertion='replaces previous data and metadata')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key='foo', Body='zar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'zar')

    def test_put_object_if_match_non_existed_failed(self, s3cfg_global_unique):
        """
        (operation='overwrite non-existing object w/ If-Match: *')
        (assertion='fails 412')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-Match': '*'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='bar')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

    def test_put_object_if_non_match_good(self, s3cfg_global_unique):
        """
        (operation='overwrite existing object w/ If-None-Match: outdated ETag')
        (assertion='replaces previous data and metadata')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': 'ABCORZ'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        response = client.put_object(Bucket=bucket_name, Key='foo', Body='zar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'zar')

    def test_put_object_if_non_match_failed(self, s3cfg_global_unique):
        """
        (operation='overwrite existing object w/ If-None-Match: the latest ETag')
        (assertion='fails 412')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        etag = response['ETag'].replace('"', '')

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': etag}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_put_object_if_non_match_non_existed_good(self, s3cfg_global_unique):
        """
        (operation='overwrite non-existing object w/ If-None-Match: *')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_put_object_if_non_match_overwrite_existed_failed(self, s3cfg_global_unique):
        """
        (operation='overwrite existing object w/ If-None-Match: *')
        (assertion='fails 412')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        lf = (lambda **kwargs: kwargs['params']['headers'].update({'If-None-Match': '*'}))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='foo', Body='zar')

        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 412)
        self.eq(error_code, 'PreconditionFailed')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')


class TestBrowserBasedUploadsUsingPost(TestObjectBase):
    # Authenticating Requests in Browser-Based Uploads Using POST (AWS Signature Version 4)
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/sigv4-UsingHTTPPOST.html

    AWSAccessKeyId:
        The AWS access key ID of the owner of the bucket who grants an Anonymous
            user access for a request that satisfies the set of constraints in the policy.
        Type: String
        Default: None
        Constraints: Required if a policy document is included with the request.

        Required: Conditional

    acl:
        The specified Amazon S3 access control list (ACL).
        If the specified ACL is not valid, an error is generated.
        For more information about ACLs,
            see Access Control List (ACL) Overview in the Amazon Simple Storage Service User Guide.
        Type: String
        Default: private
        Valid Values: private | public-read | public-read-write | aws-exec-read | authenticated-read
                    | bucket-owner-read | bucket-owner-full-control

        Required: No

    Cache-Control, Content-Type, Content-Disposition, Content-Encoding, Expires:
        The REST-specific headers. For more information, see PutObject.
        Type: String
        Default: None

        Required: No

    file:
        The file or text content.
        The file or text content must be the last field in the form.
        You cannot upload more than one file at a time.
        Type: File or text content
        Default: None

        Required: Yes

    key:
        The name of the uploaded key.
        To use the file name provided by the user, use the ${filename} variable.
            For example, if a user named Mary uploads the file example.jpg and you specify /user/mary/${filename},
                the key name is /user/mary/example.jpg.
        For more information, see Object Key and Metadata in the Amazon Simple Storage Service User Guide.
            https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/UsingMetadata.html
        Type: String
        Default: None

        Required: Yes

    policy:
        The security policy that describes what is permitted in the request.
            Requests without a security policy are considered anonymous and work only on publicly writable buckets.
            For more information, see HTML Forms and Upload Examples.
                https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/HTTPPOSTForms.html
                    Expiration:
                        The expiration element specifies the expiration date of the policy in ISO 8601 UTC date format.
                        For example, "2007-12-01T12:00:00.000Z" specifies
                            that the policy is not valid after midnight UTC on 2007-12-01.
                        Expiration is required in a policy.
                    Conditions:
                        The conditions in the policy document validate the contents of the uploaded object.
                        Each form field that you specify in the form (except AWSAccessKeyId, signature, file, policy,
                            and field names that have an x-ignore- prefix) must be included in the list of conditions.

                        The following table describes policy document conditions.
                        acl:
                            Specifies conditions that the ACL must meet.
                            Supports exact matching and starts-with.

                        content-length-range:
                            Specifies the minimum and maximum allowable size for the uploaded content.
                            Supports range matching.

                        Cache-Control, Content-Type, Content-Disposition, Content-Encoding, Expires:
                            REST-specific headers.
                            Supports exact matching and starts-with.

                        key:
                            The name of the uploaded key.
                            Supports exact matching and starts-with.

                        success_action_redirect, redirect:
                            The URL to which the client is redirected upon successful upload.
                            Supports exact matching and starts-with.

                        success_action_status:
                            The status code returned to the client upon successful upload
                                if success_action_redirect is not specified.
                            Supports exact matching.

                        x-amz-security-token:
                            Amazon DevPay security token.
                            Each request that uses Amazon DevPay requires two x-amz-security-token form fields:
                                one for the product token and one for the user token.
                            As a result, the values must be separated by commas.
                            For example,
                                if the user token is eW91dHViZQ== and the product token is b0hnNVNKWVJIQTA=, you set
                                    the policy entry to: { "x-amz-security-token": "eW91dHViZQ==,b0hnNVNKWVJIQTA=" }.

                        Other field names prefixed with x-amz-meta-
                            User-specified metadata.
                            Supports exact matching and starts-with.

                https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/HTTPPOSTExamples.html
        Type: String
        Default: None
        Constraints: A security policy is required if the bucket is not publicly writable.

        Required: Conditional

    success_action_redirect, redirect:
        The URL to which the client is redirected upon successful upload.
        If success_action_redirect is not specified,
            Amazon S3 returns the empty document type specified in the success_action_status field.
        If Amazon S3 cannot interpret the URL, it acts as if the field is not present.
        If the upload fails, Amazon S3 displays an error and does not redirect the user to a URL.
        Type: String
        Default: None
        Note:
            The redirect field name is deprecated,
                and support for the redirect field name will be removed in the future.

        Required: No

    success_action_status:
        If you don't specify success_action_redirect,
            the status code is returned to the client when the upload succeeds.
        This field accepts the values 200, 201, or 204 (the default).
        If the value is set to 200 or 204, Amazon S3 returns an empty document with a 200 or 204 status code.
        If the value is set to 201, Amazon S3 returns an XML document with a 201 status code.
        If the value is not set or if it is set to a value that is not valid,
            Amazon S3 returns an empty document with a 204 status code.
        Type: String
        Default: None

        Required: No

    tagging:
        The specified set of tags to add to the object. To add tags, use the following encoding scheme.
            <Tagging>
              <TagSet>
                <Tag>
                  <Key>TagName</Key>
                  <Value>TagValue</Value>
                </Tag>
                ...
              </TagSet>
            </Tagging>
        For more information, see Object Tagging in the Amazon Simple Storage Service User Guide.
        Type: String
        Default: None

        Required: No

    x-amz-storage-class:
        The storage class to use for storing the object.
            If you don't specify a class, Amazon S3 uses the default storage class, STANDARD.
            Amazon S3 supports other storage classes.
            For more information, see Storage Classes in the Amazon Simple Storage Service User Guide.
        Type: String
        Default: STANDARD
        Valid values: STANDARD | REDUCED_REDUNDANCY | GLACIER | GLACIER_IR |
                      STANDARD_IA | ONEZONE_IA | INTELLIGENT_TIERING | DEEP_ARCHIVE

        Required: No

    x-amz-meta-*:
        Headers starting with this prefix are user-defined metadata.
            Each one is stored and returned as a set of key-value pairs.
            Amazon S3 doesn't validate or interpret user-defined metadata. For more information, see PutObject.
        Type: String
        Default: None

        Required: No

    x-amz-security-token:
        The Amazon DevPay security token.
        Each request that uses Amazon DevPay requires two x-amz-security-token form fields:
            one for the product token and one for the user token.
        Type: String
        Default: None

        Required: No

    x-amz-signature:
        (AWS Signature Version 4) The HMAC-SHA256 hash of the security policy.
        Type: String
        Default: None

        Required: Conditional

    x-amz-website-redirect-location:
        If the bucket is configured as a website,
            this field redirects requests for this object to another object in the same bucket or to an external URL.
            Amazon S3 stores the value of this header in the object metadata.
            For information about object metadata, see Object Key and Metadata.
        In the following example,
            the request header sets the redirect to an object (anotherPage.html) in the same bucket:
        x-amz-website-redirect-location: /anotherPage.html
        In the following example, the request header sets the object redirect to another website:
        x-amz-website-redirect-location: http://www.example.com/
        For more information about website hosting in Amazon S3,
            see Hosting Websites on Amazon S3
            and How to Configure Website Page Redirects in the Amazon Simple Storage Service User Guide.
        Type: String
        Default: None
        Constraints: The value must be prefixed by /, http://, or https://. The length of the value is limited to 2 KB.

        Required: No
    """

    @pytest.mark.ess
    def test_post_object_anonymous_request(self, s3cfg_global_unique):
        """
        测试-验证匿名用户使用Post请求上传对象,
        桶ACL: public-read-write; 对象ACL：public-read
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        url = self.get_post_url(s3cfg_global_unique, bucket_name)

        payload = OrderedDict([("key", "foo.txt"),
                               ("acl", "public-read"),
                               ("Content-Type", "text/plain"),
                               ('file', ('foo.txt', 'bar'))])

        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_post_object_authenticated_request(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象,
        桶Policy: private; 对象ACL：private
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
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_post_object_authenticated_no_content_type(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象(no content-type header),
        桶ACL:public-read-write; 桶Policy: private; 对象ACL：private
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key="foo.txt")
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_post_object_authenticated_request_bad_access_key(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象(bad access key),
        桶ACL:public-read-write; 桶Policy: private; 对象ACL：private
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", 'foo'),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_set_success_code(self, s3cfg_global_unique):
        """
        测试-验证匿名用户使用Post请求上传对象, 设置成功的success code
        桶ACL: public-read-write; 对象ACL：public-read; success_action_status: 201
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        payload = OrderedDict([("key", "foo.txt"), ("acl", "public-read"),
                               ("success_action_status", "201"),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 201)
        message = self.ele_tree.fromstring(r.content).find('Key')
        self.eq(message.text, 'foo.txt')

    @pytest.mark.ess
    def test_post_object_set_invalid_success_code(self, s3cfg_global_unique):
        """
        测试-验证匿名用户使用Post请求上传对象, 设置无效的success code
        桶ACL: public-read-write; 对象ACL：public-read; success_action_status: 404
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        payload = OrderedDict([("key", "foo.txt"), ("acl", "public-read"),
                               ("success_action_status", "404"),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        content = r.content.decode()
        self.eq(content, '')

    @pytest.mark.ess
    def test_post_object_upload_larger_than_chunk(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，0B≤content-length-range≤5MiB；
        ACL：private
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
                               ["content-length-range", 0, 5 * 1024 * 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        foo_string = 'foo' * 1024 * 1024

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', foo_string)])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, foo_string)

    @pytest.mark.ess
    def test_post_object_set_key_from_filename(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，通过filename参数设置key
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
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "${filename}"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', ('foo.txt', 'bar'))])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_post_object_ignored_header(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，设置被忽略的请求头
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
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ("x-ignore-foo", "bar"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)

    @pytest.mark.ess
    def test_post_object_case_insensitive_condition_fields(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，各字段是否大小写敏感
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bUcKeT": bucket_name},
                               ["StArTs-WiTh", "$KeY", "foo"],
                               {"AcL": "private"},
                               ["StArTs-WiTh", "$CoNtEnT-TyPe", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("kEy", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("aCl", "private"), ("signature", signature), ("pOLICy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)

    @pytest.mark.ess
    def test_post_object_escaped_field_values(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，测试转义字符（将 $ 进行转义）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "\$foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='\$foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_post_object_success_redirect_action(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，测试 success_action_redirect
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        redirect_url = self.get_post_url(s3cfg_global_unique, bucket_name)

        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["eq", "$success_action_redirect", redirect_url],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ("success_action_redirect", redirect_url),
                               ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 200)
        url = r.url
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')

        etag = response['ETag'].strip('"')
        self.eq(url, f'{redirect_url}?bucket={bucket_name}&key={"foo.txt"}&etag=%22{etag}%22')

    @pytest.mark.ess
    def test_post_object_invalid_signature(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，使用无效的signature
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())[
                    ::-1]

        payload = OrderedDict([("key", "\$foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_invalid_access_key(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，使用无效的accessKey
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "\$foo.txt"), ("AWSAccessKeyId", aws_access_key_id[::-1]),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_invalid_date_format(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，使用无效的日期格式
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": str(expires),  # invalid date format
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "\$foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_no_key_specified(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，不设置key参数
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_missing_signature(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，不设置signature参数
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_access_key_id = s3cfg_global_unique.main_access_key

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_missing_policy_condition(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy的condition缺少bucket参数
        """

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               ["starts-with", "$key", "\$foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_user_specified_header(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy的starts-with参数结合header使用
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
                               ["starts-with", "$x-amz-meta-foo", "bar"]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('x-amz-meta-foo', 'barclamp'), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        self.eq(response['Metadata']['foo'], 'barclamp')

    @pytest.mark.ess
    def test_post_object_request_missing_policy_specified_field(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy的starts-with参数结合header使用（header未设置相关请求头）
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
                               ["starts-with", "$x-amz-meta-foo", "bar"]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_condition_is_case_sensitive(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy中的conditions是大小写敏感的
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "CONDITIONS": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_expires_is_case_sensitive(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy中的expiration是大小写敏感的
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"EXPIRATION": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_expired_policy(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，使用过期的policy
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=-6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {"bucket": bucket_name},
                               ["starts-with", "$key", "foo"],
                               {"acl": "private"},
                               ["starts-with", "$Content-Type", "text/plain"],
                               ["content-length-range", 0, 1024],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_invalid_request_field_value(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，conditions里使用精确匹配（验证不匹配的情况）
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
                               ["eq", "$x-amz-meta-foo", ""]
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())
        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('x-amz-meta-foo', 'barclamp'), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 403)

    @pytest.mark.ess
    def test_post_object_missing_expires_condition(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy中缺少expiration必选字段
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)

        policy_document = {
            "conditions": [
                {"bucket": bucket_name},
                ["starts-with", "$key", "foo"],
                {"acl": "private"},
                ["starts-with", "$Content-Type", "text/plain"],
                ["content-length-range", 0, 1024],
            ]
        }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_missing_conditions_list(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，policy中缺少conditions列表
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ")}

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_upload_size_limit_exceeded(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，content-length-range设置为0，验证是否无法上传
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
                               ["content-length-range", 0, 0],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_missing_content_length_argument(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，content-length-range设置错误（不设置最大值或最小值）
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
                               ["content-length-range", 0],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_invalid_content_length_argument(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，content-length-range设置错误（最小值设置为-1，最大值设置为0）
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
                               ["content-length-range", -1, 0],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_upload_size_below_minimum(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，对象大小小于content-length-range的最小值
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
                               ["content-length-range", 512, 1000],
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)

    @pytest.mark.ess
    def test_post_object_empty_conditions(self, s3cfg_global_unique):
        """
        测试-验证已认证用户使用Post请求上传对象，conditions设置为空列表
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        utc = pytz.utc
        expires = datetime.datetime.now(utc) + datetime.timedelta(seconds=+6000)

        policy_document = {"expiration": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
                           "conditions": [
                               {}
                           ]
                           }

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"), ('file', 'bar')])

        r = requests.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 400)
