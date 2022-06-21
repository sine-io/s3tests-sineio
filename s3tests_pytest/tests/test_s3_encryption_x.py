import base64
import datetime
import hashlib
import hmac
import json
from collections import OrderedDict
from unittest import SkipTest

import pytest
import pytz
import httpx

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestEncryptionBase(TestBaseClass):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/UsingServerSideEncryption.html

    1. 服务端加密（以下三个选项互斥）
        服务器端加密是指由接收数据的应用程序或服务在目标位置对数据进行加密。
        Amazon S3 在将您的数据写入数据中心内的磁盘时会在对象级别加密这些数据，并在您访问这些数据时解密这些数据。
        只要您验证了您的请求并且拥有访问权限，您访问加密和未加密对象的方式就没有区别。
        1.1 具有 Amazon S3 托管密钥的服务器端加密 (SSE-S3)
        1.2 在 AWS Key Management Service 中存储KMS密钥的服务器端加密 (SSE-KMS)。
        1.3 具有客户提供密钥的服务器端加密 (SSE-C)
    2. 客户端加密
    """

    def sse_kms_customer_write(self, config, file_size, key_id='testkey-1'):
        """
        Tests Create a file of A's, use it to set_contents_from_file.
        Create a file of B's, use it to re-set_contents_from_file.
        Re-read the contents, and confirm we get B's
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        """
        1. x-amz-server-side-encryption:
            The server-side encryption algorithm used when storing this object in Amazon S3
            (for example, AES256, aws:kms).
        2. x-amz-server-side-encryption-aws-kms-key-id: 
            If x-amz-server-side-encryption has a valid value of aws:kms,
            this header specifies the ID of the AWS Key Management Service (AWS KMS)
            symmetrical encryption customer managed key that was used for the object.
            If you specify x-amz-server-side-encryption:aws:kms,
            but do not provide x-amz-server-side-encryption-aws-kms-key-id,
            Amazon S3 uses the AWS managed key to protect the data.
            If the KMS key does not exist in the same account issuing the command,
            you must use the full ARN and not just the ID.
        """
        sse_kms_client_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': key_id
        }
        data = 'A' * file_size

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key='testobj', Body=data)

        response = client.get_object(Bucket=bucket_name, Key='testobj')
        body = self.get_body(response)
        self.eq(body, data)

    def encryption_sse_customer_write(self, config, file_size):
        """
        Tests Create a file of A's, use it to set_contents_from_file.
        Create a file of B's, use it to re-set_contents_from_file.
        Re-read the contents, and confirm we get B's
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)
        key = 'testobj'
        data = 'A' * file_size
        """
        1. x-amz-server-side-encryption-customer-algorithm:
            Specifies the algorithm to use to when encrypting the object (for example, AES256).
        2. x-amz-server-side-encryption-customer-key:
            Specifies the customer-provided encryption key for Amazon S3 to use in encrypting data. 
            This value is used to store the object and then it is discarded; 
            Amazon S3 does not store the encryption key. 
            The key must be appropriate for use with the algorithm specified 
            in the x-amz-server-side-encryption-customer-algorithm header.
        3. x-amz-server-side-encryption-customer-key-MD5:
            Specifies the 128-bit MD5 digest of the encryption key according to RFC 1321. 
            Amazon S3 uses this header for a message integrity check to ensure that 
            the encryption key was transmitted without error.
        """

        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
            # 'x-amz-server-side-encryption-customer-key-MD5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, data)

    def multipart_upload_enc(self, client, bucket_name, key, size, part_size, init_headers, part_headers, metadata,
                             resend_parts):
        """
        generate a multi-part upload for a random file of specifed size,
        if requested, generate a list of the parts
        return the upload descriptor
        """

        lf = (lambda **kwargs: kwargs['params']['headers'].update(init_headers))
        client.meta.events.register('before-call.s3.CreateMultipartUpload', lf)

        if metadata is None:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        else:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata)

        upload_id = response['UploadId']
        s = ''
        parts = []
        for i, part in enumerate(self.generate_random(size, part_size)):
            # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
            part_num = i + 1
            s += part
            lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
            client.meta.events.register('before-call.s3.UploadPart', lf)
            response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num,
                                          Body=part)
            parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
            if i in resend_parts:
                lf = (lambda **kwargs: kwargs['params']['headers'].update(part_headers))
                client.meta.events.register('before-call.s3.UploadPart', lf)
                client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

        return upload_id, s, parts

    def check_content_using_range_enc(self, client, bucket_name, key, data, step, enc_headers=None):
        response = client.get_object(Bucket=bucket_name, Key=key)
        size = response['ContentLength']
        for ofs in range(0, size, step):
            toread = size - ofs
            if toread > step:
                toread = step
            end = ofs + toread - 1
            lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
            client.meta.events.register('before-call.s3.GetObject', lf)
            r = 'bytes={s}-{e}'.format(s=ofs, e=end)
            response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
            read_range = response['ContentLength']
            body = self.get_body(response)
            self.eq(read_range, toread)
            self.eq(body, data[ofs:end + 1])


class TestObjectEncryption(TestEncryptionBase):

    def test_encrypted_transfer_1b(self, s3cfg_global_unique):
        """
        (operation='Test SSE-C encrypted transfer 1 byte')
        (assertion='success')
        """
        self.encryption_sse_customer_write(s3cfg_global_unique, 1)

    def test_encrypted_transfer_1kb(self, s3cfg_global_unique):
        """
        (operation='Test SSE-C encrypted transfer 1KB')
        (assertion='success')
        """
        self.encryption_sse_customer_write(s3cfg_global_unique, 1024)

    def test_encrypted_transfer_1mb(self, s3cfg_global_unique):
        """
        (operation='Test SSE-C encrypted transfer 1MB')
        (assertion='success')
        """
        self.encryption_sse_customer_write(s3cfg_global_unique, 1024 * 1024)

    def test_encrypted_transfer_13b(self, s3cfg_global_unique):
        """
        (operation='Test SSE-C encrypted transfer 13 bytes')
        (assertion='success')
        """
        self.encryption_sse_customer_write(s3cfg_global_unique, 13)

    def test_encryption_sse_c_method_head(self, s3cfg_global_unique):
        """
        (assertion='success')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 1000
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.HeadObject', lf)
        response = client.head_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_encryption_sse_c_present(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-C and read without SSE-C')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 1000
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_encryption_sse_c_other_key(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-C but read with other key')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 100
        key = 'testobj'
        sse_client_headers_a = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }
        sse_client_headers_b = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
            'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_a))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers_b))
        client.meta.events.register('before-call.s3.GetObject', lf)
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_encryption_sse_c_invalid_md5(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-C, but md5 is bad')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 100
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_encryption_sse_c_no_md5(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-C, but dont provide MD5')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 100
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)

    def test_encryption_sse_c_no_key(self, s3cfg_global_unique):
        """
        (operation='declare SSE-C but do not provide key')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 100
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)

    def test_encryption_key_no_sse_c(self, s3cfg_global_unique):
        """
        (operation='Do not declare SSE-C but provide key and MD5')
        (assertion='operation successfull, no encryption')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        data = 'A' * 100
        key = 'testobj'
        sse_client_headers = {
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_encryption_sse_c_post_object_authenticated_request(self, s3cfg_global_unique):
        """
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
                               ["starts-with", "$x-amz-server-side-encryption-customer-algorithm", ""],
                               ["starts-with", "$x-amz-server-side-encryption-customer-key", ""],
                               ["starts-with", "$x-amz-server-side-encryption-customer-key-md5", ""],
                               ["content-length-range", 0, 1024]
                           ]}

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"),
                               ('x-amz-server-side-encryption-customer-algorithm', 'AES256'),
                               ('x-amz-server-side-encryption-customer-key',
                                'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs='),
                               ('x-amz-server-side-encryption-customer-key-md5', 'DWygnHRtgiJ77HCm+1rvHw=='),
                               ('file', 'bar')])

        r = httpx.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)

        get_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw=='
        }
        lf = (lambda **kwargs: kwargs['params']['headers'].update(get_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_sse_kms_method_head(self, s3cfg_global_unique):
        """
        (operation='Test SSE-KMS encrypted does perform head properly')
        (assertion='success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        sse_kms_client_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
        }
        data = 'A' * 1000
        key = 'testobj'

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        response = client.head_object(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption'], 'aws:kms')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['x-amz-server-side-encryption-aws-kms-key-id'], kms_keyid)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.HeadObject', lf)
        e = assert_raises(ClientError, client.head_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_sse_kms_present(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-KMS and read without SSE-KMS')
        (assertion='operation success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        sse_kms_client_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid
        }
        data = 'A' * 100
        key = 'testobj'

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        client.put_object(Bucket=bucket_name, Key=key, Body=data)

        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, data)

    def test_sse_kms_no_key(self, s3cfg_global_unique):
        """
        (operation='declare SSE-KMS but do not provide key_id')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        sse_kms_client_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
        }
        data = 'A' * 100
        key = 'testobj'

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_sse_kms_not_declared(self, s3cfg_global_unique):
        """
        (operation='Do not declare SSE-KMS but provide key_id')
        (assertion='operation successfull, no encryption')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        sse_kms_client_headers = {
            'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-2'
        }
        data = 'A' * 100
        key = 'testobj'

        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.PutObject', lf)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key, Body=data)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_sse_kms_post_object_authenticated_request(self, s3cfg_global_unique):
        """
        (operation='authenticated KMS browser based upload via POST request')
        (assertion='succeeds and returns written data')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
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
                               ["starts-with", "$x-amz-server-side-encryption", ""],
                               ["starts-with", "$x-amz-server-side-encryption-aws-kms-key-id", ""],
                               ["content-length-range", 0, 1024]
                           ]}

        json_policy_document = json.JSONEncoder().encode(policy_document)
        bytes_json_policy_document = bytes(json_policy_document, 'utf-8')
        policy = base64.b64encode(bytes_json_policy_document)
        aws_secret_access_key = s3cfg_global_unique.main_secret_key
        aws_access_key_id = s3cfg_global_unique.main_access_key

        signature = base64.b64encode(hmac.new(bytes(aws_secret_access_key, 'utf-8'), policy, hashlib.sha1).digest())

        payload = OrderedDict([("key", "foo.txt"), ("AWSAccessKeyId", aws_access_key_id),
                               ("acl", "private"), ("signature", signature), ("policy", policy),
                               ("Content-Type", "text/plain"),
                               ('x-amz-server-side-encryption', 'aws:kms'),
                               ('x-amz-server-side-encryption-aws-kms-key-id', kms_keyid),
                               ('file', 'bar')])

        r = httpx.post(url, files=payload, verify=s3cfg_global_unique.default_ssl_verify)
        self.eq(r.status_code, 204)

        response = client.get_object(Bucket=bucket_name, Key='foo.txt')
        body = self.get_body(response)
        self.eq(body, 'bar')

    def test_sse_kms_transfer_1b(self, s3cfg_global_unique):
        """
        (operation='Test SSE-KMS encrypted transfer 1 byte')
        (assertion='success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        if kms_keyid is None:
            raise SkipTest
        self.sse_kms_customer_write(s3cfg_global_unique, 1, key_id=kms_keyid)

    def test_sse_kms_transfer_1kb(self, s3cfg_global_unique):
        """
        (operation='Test SSE-KMS encrypted transfer 1KB')
        (assertion='success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        if kms_keyid is None:
            raise SkipTest
        self.sse_kms_customer_write(s3cfg_global_unique, 1024, key_id=kms_keyid)

    def test_sse_kms_transfer_1mb(self, s3cfg_global_unique):
        """
        (operation='Test SSE-KMS encrypted transfer 1MB')
        (assertion='success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        if kms_keyid is None:
            raise SkipTest
        self.sse_kms_customer_write(s3cfg_global_unique, 1024 * 1024, key_id=kms_keyid)

    def test_sse_kms_transfer_13b(self, s3cfg_global_unique):
        """
        (operation='Test SSE-KMS encrypted transfer 13 bytes')
        (assertion='success')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        if kms_keyid is None:
            raise SkipTest
        self.sse_kms_customer_write(s3cfg_global_unique, 13, key_id=kms_keyid)

    def test_sse_kms_read_declare(self, s3cfg_global_unique):
        """
        (operation='write encrypted with SSE-KMS and read with SSE-KMS')
        (assertion='operation fails')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        sse_kms_client_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-1'
        }
        data = 'A' * 100
        key = 'testobj'

        client.put_object(Bucket=bucket_name, Key=key, Body=data)
        lf = (lambda **kwargs: kwargs['params']['headers'].update(sse_kms_client_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    # @pytest.mark.fails_on_aws  # allow-unordered is a non-standard extension
    def test_encryption_sse_c_multipart_upload(self, s3cfg_global_unique):
        """
        (operation='complete multi-part upload')
        (assertion='successful')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        enc_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
            'Content-Type': content_type
        }
        resend_parts = []

        (upload_id, data, parts) = self.multipart_upload_enc(client, bucket_name, key, obj_len,
                                                             part_size=5 * 1024 * 1024, init_headers=enc_headers,
                                                             part_headers=enc_headers, metadata=metadata,
                                                             resend_parts=resend_parts)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
        client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.head_bucket(Bucket=bucket_name)
        rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
        self.eq(rgw_object_count, 1)
        rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', obj_len))
        self.eq(rgw_bytes_used, obj_len)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        response = client.get_object(Bucket=bucket_name, Key=key)

        self.eq(response['Metadata'], metadata)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)

        body = self.get_body(response)
        self.eq(body, data)
        size = response['ContentLength']
        self.eq(len(body), size)

        self.check_content_using_range_enc(client, bucket_name, key, data, 1000000, enc_headers=enc_headers)
        self.check_content_using_range_enc(client, bucket_name, key, data, 10000000, enc_headers=enc_headers)

    # @pytest.mark.fails_on_rgw  # TODO: remove this fails_on_rgw when I fix it
    def test_encryption_sse_c_multipart_invalid_chunks_1(self, s3cfg_global_unique):
        """
        (operation='multipart upload with bad key for uploading chunks')
        (assertion='successful')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        init_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
            'Content-Type': content_type
        }
        part_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
            'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
        }
        resend_parts = []

        e = assert_raises(ClientError, self.multipart_upload_enc, client=client, bucket_name=bucket_name,
                          key=key, size=obj_len, part_size=5 * 1024 * 1024, init_headers=init_headers,
                          part_headers=part_headers, metadata=metadata, resend_parts=resend_parts)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    # @pytest.mark.fails_on_rgw  # TODO: remove this fails_on_rgw when I fix it
    def test_encryption_sse_c_multipart_invalid_chunks_2(self, s3cfg_global_unique):
        """
        (operation='multipart upload with bad md5 for chunks')
        (assertion='successful')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        init_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
            'Content-Type': content_type
        }
        part_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'AAAAAAAAAAAAAAAAAAAAAA=='
        }
        resend_parts = []

        e = assert_raises(ClientError, self.multipart_upload_enc, client=client, bucket_name=bucket_name,
                          key=key, size=obj_len, part_size=5 * 1024 * 1024, init_headers=init_headers,
                          part_headers=part_headers, metadata=metadata, resend_parts=resend_parts)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_encryption_sse_c_multipart_bad_download(self, s3cfg_global_unique):
        """
        (operation='complete multi-part upload and download with bad key')
        (assertion='successful')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        put_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': 'pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=',
            'x-amz-server-side-encryption-customer-key-md5': 'DWygnHRtgiJ77HCm+1rvHw==',
            'Content-Type': content_type
        }
        get_headers = {
            'x-amz-server-side-encryption-customer-algorithm': 'AES256',
            'x-amz-server-side-encryption-customer-key': '6b+WOZ1T3cqZMxgThRcXAQBrS5mXKdDUphvpxptl9/4=',
            'x-amz-server-side-encryption-customer-key-md5': 'arxBvwY2V4SiOne6yppVPQ=='
        }
        resend_parts = []

        (upload_id, data, parts) = self.multipart_upload_enc(client, bucket_name, key, obj_len,
                                                             part_size=5 * 1024 * 1024, init_headers=put_headers,
                                                             part_headers=put_headers, metadata=metadata,
                                                             resend_parts=resend_parts)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
        client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.head_bucket(Bucket=bucket_name)
        rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
        self.eq(rgw_object_count, 1)
        rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', obj_len))
        self.eq(rgw_bytes_used, obj_len)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(put_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        response = client.get_object(Bucket=bucket_name, Key=key)

        self.eq(response['Metadata'], metadata)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(get_headers))
        client.meta.events.register('before-call.s3.GetObject', lf)
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    def test_sse_kms_multipart_upload(self, s3cfg_global_unique):
        """
        (operation='complete KMS multi-part upload')
        (assertion='successful')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        enc_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
            'Content-Type': content_type
        }
        resend_parts = []

        (upload_id, data, parts) = self.multipart_upload_enc(client, bucket_name, key, obj_len,
                                                             part_size=5 * 1024 * 1024, init_headers=enc_headers,
                                                             part_headers=enc_headers, metadata=metadata,
                                                             resend_parts=resend_parts)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))
        client.meta.events.register('before-call.s3.CompleteMultipartUpload', lf)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.head_bucket(Bucket=bucket_name)
        rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
        self.eq(rgw_object_count, 1)
        rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', obj_len))
        self.eq(rgw_bytes_used, obj_len)

        lf = (lambda **kwargs: kwargs['params']['headers'].update(enc_headers))  # change part_headers to enc_headers
        client.meta.events.register('before-call.s3.UploadPart', lf)

        response = client.get_object(Bucket=bucket_name, Key=key)

        self.eq(response['Metadata'], metadata)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-type'], content_type)

        body = self.get_body(response)
        self.eq(body, data)
        size = response['ContentLength']
        self.eq(len(body), size)

        self.check_content_using_range(client, key, bucket_name, data, 1000000)
        self.check_content_using_range(client, key, bucket_name, data, 10000000)

    def test_sse_kms_multipart_invalid_chunks_1(self, s3cfg_global_unique):
        """
        (operation='multipart KMS upload with bad key_id for uploading chunks')
        (assertion='successful')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        kms_keyid2 = s3cfg_global_unique.main_kms_keyid2
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/bla'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        init_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
            'Content-Type': content_type
        }
        part_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid2
        }
        resend_parts = []

        self.multipart_upload_enc(client, bucket_name, key, obj_len, part_size=5 * 1024 * 1024,
                                  init_headers=init_headers, part_headers=part_headers, metadata=metadata,
                                  resend_parts=resend_parts)

    def test_sse_kms_multipart_invalid_chunks_2(self, s3cfg_global_unique):
        """
        (operation='multipart KMS upload with unexistent key_id for chunks')
        (assertion='successful')
        """
        kms_keyid = s3cfg_global_unique.main_kms_keyid
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "multipart_enc"
        content_type = 'text/plain'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}
        init_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': kms_keyid,
            'Content-Type': content_type
        }
        part_headers = {
            'x-amz-server-side-encryption': 'aws:kms',
            'x-amz-server-side-encryption-aws-kms-key-id': 'testkey-not-present'
        }
        resend_parts = []

        self.multipart_upload_enc(client, bucket_name, key, obj_len, part_size=5 * 1024 * 1024,
                                  init_headers=init_headers, part_headers=part_headers, metadata=metadata,
                                  resend_parts=resend_parts)


class TestBucketEncryption(TestBaseClass):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/bucket-encryption.html
    设置默认存储桶加密
    """

    def test_put_bucket_encryption(self, s3cfg_global_unique):
        """
        (operation='put bucket encryption on bucket')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        server_side_encryption_conf = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                },
            ]
        }

        response = client.put_bucket_encryption(
            Bucket=bucket_name, ServerSideEncryptionConfiguration=server_side_encryption_conf)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    def test_get_bucket_encryption(self, s3cfg_global_unique):
        """
        (operation='get bucket encryption on bucket')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        response_code = ""
        try:
            client.get_bucket_encryption(Bucket=bucket_name)
        except ClientError as e:
            response_code = e.response['Error']['Code']

        self.eq(response_code, 'ServerSideEncryptionConfigurationNotFoundError')

        server_side_encryption_conf = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                },
            ]
        }

        client.put_bucket_encryption(
            Bucket=bucket_name, ServerSideEncryptionConfiguration=server_side_encryption_conf)

        response = client.get_bucket_encryption(Bucket=bucket_name)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(response['ServerSideEncryptionConfiguration']['Rules'][0]['ApplyServerSideEncryptionByDefault'][
                    'SSEAlgorithm'],
                server_side_encryption_conf['Rules'][0]['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'])

    def test_delete_bucket_encryption(self, s3cfg_global_unique):
        """
        (operation='delete bucket encryption on bucket')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        response = client.delete_bucket_encryption(Bucket=bucket_name)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)

        server_side_encryption_conf = {
            'Rules': [
                {
                    'ApplyServerSideEncryptionByDefault': {
                        'SSEAlgorithm': 'AES256'
                    }
                },
            ]
        }

        client.put_bucket_encryption(Bucket=bucket_name, ServerSideEncryptionConfiguration=server_side_encryption_conf)

        response = client.delete_bucket_encryption(Bucket=bucket_name)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)
