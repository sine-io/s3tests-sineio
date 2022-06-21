import os
import pytz
import json
import hmac
import base64
import hashlib
import random
import string
import time
import datetime
import threading
import logging
import unittest
from collections import defaultdict, OrderedDict
from xml.etree import ElementTree
from concurrent.futures import ThreadPoolExecutor, wait

from fabric import Connection

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# different clients.
def get_client(config):
    client = boto3.client(service_name='s3',
                          aws_access_key_id=config.main_access_key,
                          aws_secret_access_key=config.main_secret_key,
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version='s3v4'))  # default is s3v4
    return client


def get_v2_client(config):
    client = boto3.client(service_name='s3',
                          aws_access_key_id=config.main_access_key,
                          aws_secret_access_key=config.main_secret_key,
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version='s3'))
    return client


def get_alt_client(config):
    client = boto3.client(service_name='s3',
                          aws_access_key_id=config.alt_access_key,
                          aws_secret_access_key=config.alt_secret_key,
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version='s3v4'))
    return client


def get_unauthenticated_client(config):
    client = boto3.client(service_name='s3',
                          aws_access_key_id='',
                          aws_secret_access_key='',
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version=UNSIGNED))
    return client


def get_bad_auth_client(config, aws_access_key_id='badauth'):
    client = boto3.client(service_name='s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key='roflmao',
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version='s3v4'))
    return client


def get_svc_client(config, svc='s3'):
    client = boto3.client(service_name=svc,
                          aws_access_key_id=config.main_access_key,
                          aws_secret_access_key=config.main_secret_key,
                          endpoint_url=config.default_endpoint,
                          use_ssl=config.default_is_secure,
                          verify=config.default_ssl_verify,
                          config=Config(signature_version='s3v4'))
    return client


def get_s3_resource_client(config):
    client = boto3.resource('s3',
                            aws_access_key_id=config.main_access_key,
                            aws_secret_access_key=config.main_secret_key,
                            endpoint_url=config.default_endpoint,
                            use_ssl=config.default_is_secure,
                            verify=config.default_ssl_verify)
    return client


def assert_raises(exc_class, callable_obj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callable_obj(*args, **kwargs)
    except exc_class as e:
        return e
    else:
        if hasattr(exc_class, '__name__'):
            exc_name = exc_class.__name__
        else:
            exc_name = str(exc_class)
        raise AssertionError("%s not raised" % exc_name)


def parse_xml_to_json(xml):
    response = {}

    for child in list(xml):
        if len(list(child)) > 0:
            response[child.tag] = parse_xml_to_json(child)
        else:
            response[child.tag] = child.text or ''
        # one-liner equivalent
        # response[child.tag] = parseXmlToJson(child) if len(list(child)) > 0 else child.text or ''
    return response


def get_buckets_list(client, prefix):
    response = client.list_buckets()
    bucket_dicts = response['Buckets']
    buckets_list = []
    for bucket in bucket_dicts:
        if prefix in bucket['Name']:
            buckets_list.append(bucket['Name'])

    return buckets_list


def list_versions(client, bucket, batch_size):
    """
    generator function that returns object listings in batches, where each
    batch is a list of dicts compatible with delete_objects()
    """
    key_marker = ''
    version_marker = ''
    truncated = True
    while truncated:
        listing = client.list_object_versions(
            Bucket=bucket,
            KeyMarker=key_marker,
            VersionIdMarker=version_marker,
            MaxKeys=batch_size)

        key_marker = listing.get('NextKeyMarker')
        version_marker = listing.get('NextVersionIdMarker')
        truncated = listing['IsTruncated']

        objs = listing.get('Versions', []) + listing.get('DeleteMarkers', [])

        if len(objs):
            yield [{'Key': o['Key'], 'VersionId': o['VersionId']} for o in objs]


def nuke_bucket(client, bucket):
    batch_size = 128
    max_retain_date = None

    # list and delete objects in batches
    for objects in list_versions(client, bucket, batch_size):
        delete = client.delete_objects(Bucket=bucket,
                                       Delete={'Objects': objects, 'Quiet': True},
                                       BypassGovernanceRetention=True)

        # check for object locks on 403 AccessDenied errors
        for err in delete.get('Errors', []):
            if err.get('Code') != 'AccessDenied':
                continue
            try:
                res = client.get_object_retention(Bucket=bucket,
                                                  Key=err['Key'], VersionId=err['VersionId'])
                retain_date = res['Retention']['RetainUntilDate']
                if not max_retain_date or max_retain_date < retain_date:
                    max_retain_date = retain_date
            except ClientError:
                pass

    if max_retain_date:
        # wait out the retention period (up to 60 seconds)
        now = datetime.datetime.now(max_retain_date.tzinfo)
        if max_retain_date > now:
            delta = max_retain_date - now
            if delta.total_seconds() > 60:
                raise RuntimeError(
                    f'bucket {bucket} still has objects locked for {delta.total_seconds()} more seconds, '
                    f'not waiting for bucket cleanup')
            print('nuke_bucket', bucket, 'waiting', delta.total_seconds(), 'seconds for object locks to expire')
            time.sleep(delta.total_seconds())

        for objects in list_versions(client, bucket, batch_size):
            client.delete_objects(Bucket=bucket,
                                  Delete={'Objects': objects, 'Quiet': True},
                                  BypassGovernanceRetention=True)
    client.delete_bucket(Bucket=bucket)


def nuke_prefixed_buckets(client, prefix, msg=""):
    buckets = get_buckets_list(client, prefix)

    err = None
    for bucket_name in buckets:
        try:
            nuke_bucket(client, bucket_name)
        except Exception as e:
            # The exception shouldn't be raised when doing cleanup. Pass and continue
            # the bucket cleanup process. Otherwise left buckets wouldn't be cleared
            # resulting in some kind of resource leak. err is used to hint user some
            # exception once occurred.
            err = e
            pass
    if err:
        raise err
    print(f"\nDone with cleanup of buckets in tests: {buckets}, {msg}")


class Counter(object):
    def __init__(self, default_val):
        self.val = default_val

    def inc(self):
        self.val = self.val + 1


class ActionOnCount(object):
    def __init__(self, trigger_count, action):
        self.count = 0
        self.trigger_count = trigger_count
        self.action = action
        self.result = 0

    def trigger(self):
        self.count = self.count + 1

        if self.count == self.trigger_count:
            self.result = self.action()


class FakeFile(object):
    """
    file that simulates seek, tell, and current character
    """

    def __init__(self, char='A', interrupt=None):
        self.offset = 0
        self.char = bytes(char, 'utf-8')
        self.interrupt = interrupt

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset
        elif whence == os.SEEK_CUR:
            self.offset += offset

    def tell(self):
        return self.offset


class FakeWriteFile(FakeFile):
    """
    file that simulates interruptable reads of constant data
    """

    def __init__(self, size, char='A', interrupt=None):
        super().__init__(char, interrupt)
        self.size = size

    def read(self, size=-1):
        if size < 0:
            size = self.size - self.offset
        count = min(size, self.size - self.offset)
        self.offset += count

        # Sneaky! do stuff before we return (the last time)
        if self.interrupt is not None and self.offset == self.size and count > 0:
            self.interrupt()

        return self.char * count


class FakeReadFile(FakeFile):
    """
    file that simulates writes, interrupting after the second
    """

    def __init__(self, size, char='A', interrupt=None):
        super().__init__(char, interrupt)
        self.interrupted = False
        self.size = 0
        self.expected_size = size

    def write(self, chars):
        assert chars == self.char * len(chars)
        self.offset += len(chars)
        self.size += len(chars)

        # Sneaky! do stuff on the second seek
        if not self.interrupted and self.interrupt is not None and self.offset > 0:
            self.interrupt()
            self.interrupted = True

    def close(self):
        assert self.size == self.expected_size


class FakeFileVerifier(object):
    """
    file that verifies expected data has been written
    """

    def __init__(self, char=None):
        self.char = char
        self.size = 0

    def write(self, data):
        size = len(data)
        if self.char is None:
            self.char = data[0]
        self.size += size
        assert data.decode() == self.char * size


class TestBaseClass(object):

    @classmethod
    def setup_class(cls) -> None:
        cls.logger = logger
        cls.assertion = unittest.TestCase()
        cls.eq = cls.assertion.assertEqual
        cls.ele_tree = ElementTree

    @classmethod
    def teardown_class(cls) -> None:
        pass

    @staticmethod
    def exec_cmd(host, user, passwd, port, command, **kwargs):
        # TODO: maybe need to modify, it's useful for now.
        conn = Connection(
            host=host,
            user=user,
            port=port,
            connect_kwargs={
                "password": passwd
            },
            **kwargs
        )
        return conn.run(command, hide=True)

    @staticmethod
    def get_status(response):
        status = response['ResponseMetadata']['HTTPStatusCode']
        return status

    @staticmethod
    def get_status_and_error_code(response):
        status = response['ResponseMetadata']['HTTPStatusCode']
        error_code = response['Error']['Code']
        return status, error_code

    @staticmethod
    def get_objects_list(client, bucket, prefix=None):
        if prefix is None:
            response = client.list_objects(Bucket=bucket)
        else:
            response = client.list_objects(Bucket=bucket, Prefix=prefix)
        objects_list = []

        if 'Contents' in response:
            contents = response['Contents']
            for obj in contents:
                objects_list.append(obj['Key'])

        return objects_list

    @staticmethod
    def get_new_bucket_name(config):
        """
        Get a bucket name that probably does not exist.

        We make every attempt to use a unique random prefix, so if a
        bucket by this name happens to exist, it's ok if tests give
        false negatives.
        """
        name = '{prefix}{num}'.format(
            prefix=config.bucket_prefix,
            num=next(config.bucket_counter),
        )
        return name

    @staticmethod
    def get_keys(response):
        """
        return lists of strings that are the keys from a client.list_objects() response
        """
        keys = []
        if 'Contents' in response:
            objects_list = response['Contents']
            keys = [obj['Key'] for obj in objects_list]
        return keys

    @staticmethod
    def make_objs_dict(keys_in):
        objs_list = []
        for key in keys_in:
            obj_dict = {'Key': key}
            objs_list.append(obj_dict)
        objs_dict = {'Objects': objs_list}
        return objs_dict

    @staticmethod
    def get_body(response):
        body = response['Body']
        got = body.read()
        if type(got) is bytes:
            got = got.decode()
        return got

    @staticmethod
    def do_create_object(client, bucket_name, key, i):
        body = 'data {i}'.format(i=i)
        client.put_object(Bucket=bucket_name, Key=key, Body=body)

    @staticmethod
    def do_create_versioned_obj_concurrent(client, target_func, bucket_name, key, num):
        t = []
        for i in range(num):
            thr = threading.Thread(target=target_func, args=(client, bucket_name, key, i))
            thr.start()
            t.append(thr)
        return t

    @staticmethod
    def do_remove_ver(client, bucket_name, key, version_id):
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)

    @staticmethod
    def do_clear_versioned_bucket_concurrent(client, target_func, bucket_name):
        t = []
        response = client.list_object_versions(Bucket=bucket_name)
        for version in response.get('Versions', []):
            thr = threading.Thread(target=target_func,
                                   args=(client, bucket_name, version['Key'], version['VersionId']))
            thr.start()
            t.append(thr)
        return t

    @staticmethod
    def do_wait_completion(t):
        for thr in t:
            thr.join()

    @staticmethod
    def get_post_url(config, bucket_name):
        endpoint = config.default_endpoint
        return '{endpoint}/{bucket_name}'.format(endpoint=endpoint, bucket_name=bucket_name)

    @staticmethod
    def generate_random(size, part_size=5 * 1024 * 1024):
        """
        Generate the specified number random data.
        (actually each MB is a repetition of the first KB)
        """
        chunk = 1024
        allowed = string.ascii_letters
        for x in range(0, size, part_size):
            str_part = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
            s = ''
            left = size - x
            this_part_size = min(left, part_size)
            for y in range(this_part_size // chunk):
                s = s + str_part
            if this_part_size > len(s):
                s = s + str_part[0:this_part_size - len(s)]
            yield s
            if x == size:
                return

    @staticmethod
    def gen_rand_string(size, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    @staticmethod
    def create_simple_tag_set(count):
        tag_set = []
        for i in range(count):
            tag_set.append({'Key': str(i), 'Value': str(i)})

        return {'TagSet': tag_set}

    def do_test_multipart_upload_contents(self, client, bucket_name, key, num_parts):
        payload = self.gen_rand_string(5) * 1024 * 1024

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response['UploadId']

        parts = []

        for part_num in range(0, num_parts):
            part = bytes(payload, 'utf-8')
            response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num + 1,
                                          Body=part)
            parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num + 1})

        last_payload = '123' * 1024 * 1024
        last_part = bytes(last_payload, 'utf-8')
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=num_parts + 1,
                                      Body=last_part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': num_parts + 1})

        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=bucket_name, Key=key)
        test_string = self.get_body(response)

        all_payload = payload * num_parts + last_payload

        assert test_string == all_payload

        return all_payload

    def multipart_upload(self, config, bucket_name, key, size,
                         part_size=5 * 1024 * 1024, client=None, content_type=None, metadata=None, resend_parts=[]):
        """
        generate a multi-part upload for a random file of specified  size,
        if requested, generate a list of the parts
        return the upload descriptor
        """
        if client is None:
            client = get_client(config)

        if content_type is None and metadata is None:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        else:
            response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata,
                                                      ContentType=content_type)

        upload_id = response['UploadId']
        s = ''
        parts = []
        for i, part in enumerate(self.generate_random(size, part_size)):
            # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
            part_num = i + 1
            s += part
            response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num,
                                          Body=part)
            parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
            if i in resend_parts:
                client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

        return upload_id, s, parts

    def create_key_with_random_content(self, config, key_name, size=7 * 1024 * 1024, bucket_name=None, client=None):
        if client is None:
            client = get_client(config)

        if bucket_name is None:
            bucket_name = self.get_new_bucket(client, config)

        data_str = str(next(self.generate_random(size, size)))
        data = bytes(data_str, 'utf-8')
        client.put_object(Bucket=bucket_name, Key=key_name, Body=data)
        # print(client.list_objects(Bucket=bucket_name))

        return bucket_name

    @staticmethod
    def multipart_copy(config, src_bucket_name, src_key, dest_bucket_name, dest_key, size, client=None,
                       part_size=5 * 1024 * 1024, version_id=None):
        if client is None:
            client = get_client(config)

        response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
        upload_id = response['UploadId']

        if version_id is None:
            copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
        else:
            copy_source = {'Bucket': src_bucket_name, 'Key': src_key, 'VersionId': version_id}

        parts = []

        i = 0
        for start_offset in range(0, size, part_size):
            end_offset = min(start_offset + part_size - 1, size - 1)
            part_num = i + 1
            copy_source_range = 'bytes={start}-{end}'.format(start=start_offset, end=end_offset)
            response = client.upload_part_copy(Bucket=dest_bucket_name, Key=dest_key, CopySource=copy_source,
                                               PartNumber=part_num, UploadId=upload_id,
                                               CopySourceRange=copy_source_range)
            parts.append({'ETag': response['CopyPartResult']['ETag'], 'PartNumber': part_num})
            i = i + 1

        return upload_id, parts

    def check_key_content(self, client, src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=None):

        if version_id is None:
            response = client.get_object(Bucket=src_bucket_name, Key=src_key)
        else:
            response = client.get_object(Bucket=src_bucket_name, Key=src_key, VersionId=version_id)
        src_size = response['ContentLength']

        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        dest_size = response['ContentLength']
        dest_data = self.get_body(response)
        assert (src_size >= dest_size)

        r = 'bytes={s}-{e}'.format(s=0, e=dest_size - 1)
        if version_id is None:
            response = client.get_object(Bucket=src_bucket_name, Key=src_key, Range=r)
        else:
            response = client.get_object(Bucket=src_bucket_name, Key=src_key, Range=r, VersionId=version_id)
        src_data = self.get_body(response)
        self.eq(src_data, dest_data)

    def check_versioning(self, client, bucket_name, status):
        try:
            response = client.get_bucket_versioning(Bucket=bucket_name)
            self.eq(response['Status'], status)
        except KeyError:
            self.eq(status, None)

    def check_configure_versioning_retry(self, client, bucket_name, status, expected_string):
        # amazon is eventual consistent, retry a bit if failed
        client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': status})

        read_status = None

        for i in range(5):
            try:
                response = client.get_bucket_versioning(Bucket=bucket_name)
                read_status = response['Status']
            except KeyError:
                read_status = None

            if expected_string == read_status:
                break

            time.sleep(1)

        self.eq(expected_string, read_status)

    def get_bucket_key_names(self, config, bucket_name):
        client = get_client(config)
        objs_list = self.get_objects_list(client=client, bucket=bucket_name)
        return frozenset(obj for obj in objs_list)

    @staticmethod
    def list_bucket_storage_class(client, bucket_name):
        result = defaultdict(list)
        response = client.list_object_versions(Bucket=bucket_name)
        for k in response['Versions']:
            result[k['StorageClass']].append(k)

        return result

    @staticmethod
    def list_bucket_versions(client, bucket_name):
        result = defaultdict(list)
        response = client.list_object_versions(Bucket=bucket_name)
        for k in response['Versions']:
            result[response['Name']].append(k)

        return result

    def check_access_denied(self, fn, *args, **kwargs):
        e = assert_raises(ClientError, fn, *args, **kwargs)
        status = self.get_status(e.response)
        self.eq(status, 403)

    def setup_bucket_object(self, config):
        """
        put an object to a (new or existing) bucket.
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket_name(config)
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key='foo')

        return bucket_name

    def setup_bucket_acl(self, config, bucket_acl=None):
        """
        set up a new bucket with specified acl
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket_name(config)
        client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)

        return bucket_name

    def compare_dates(self, datetime1, datetime2):
        """
        changes ms from datetime1 to 0, compares it to datetime2
        """
        # both times are in datetime format but datetime1 has
        # microseconds and datetime2 does not
        datetime1 = datetime1.replace(microsecond=0)
        self.eq(datetime1, datetime2)

    def get_new_bucket_resource(self, config, name=None):
        """
        Get a bucket that exists and is empty.

        Always recreates a bucket from scratch. This is useful to also
        reset ACLs and such.
        """
        if name is None:
            name = self.get_new_bucket_name(config)

        client = get_s3_resource_client(config)
        bucket = client.Bucket(name)
        bucket.create()
        return bucket

    def get_new_bucket(self, client, config, name=None, **kwargs):
        """
        Get a bucket that exists and is empty.

        Always recreates a bucket from scratch. This is useful to also
        reset ACLs and such.
        """
        if name is None:
            name = self.get_new_bucket_name(config)

        client.create_bucket(Bucket=name, **kwargs)
        return name

    def create_objects(self, config, keys, bucket_name=None, threads=1):
        """
        Populate a (specified or new) bucket with objects with
        specified names (and contents identical to their names).
        """
        if bucket_name is None:
            bucket_name = self.get_new_bucket_name(config)
        bucket = self.get_new_bucket_resource(config, name=bucket_name)

        # Add.
        with ThreadPoolExecutor(max_workers=threads) as _exec:
            _futures_tasks = [_exec.submit(bucket.put_object, Body=key, Key=key) for key in keys]
            wait(_futures_tasks)

            # for task in _futures_tasks:
            #     print(task.result())

        # for key in keys:
        #     bucket.put_object(Body=key, Key=key)

        return bucket_name

    @staticmethod
    def make_arn_resource(path="*"):
        return "arn:aws:s3:::{}".format(path)

    def verify_atomic_key_data(self, client, bucket_name, key, size=-1, char=None):
        """
        Make sure file is of the expected size and (simulated) content
        """
        fp_verify = FakeFileVerifier(char)
        client.download_fileobj(bucket_name, key, fp_verify)
        if size >= 0:
            self.eq(fp_verify.size, size)

    def check_obj_content(self, client, bucket_name, key, version_id, content):
        response = client.get_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        if content is not None:
            body = self.get_body(response)
            self.eq(body, content)
        else:
            self.eq(response['DeleteMarker'], True)

    def check_obj_versions(self, client, bucket_name, key, version_ids, contents):
        # check to see if objects is pointing at correct version

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        # obj versions in versions come out created last to first not first to last like version_ids & contents
        versions.reverse()

        i = 0
        for version in versions:
            self.eq(version['VersionId'], version_ids[i])
            self.eq(version['Key'], key)
            self.check_obj_content(client, bucket_name, key, version['VersionId'], contents[i])
            i += 1

    def create_multiple_versions(self, client, bucket_name, key, num_versions, version_ids=None, contents=None,
                                 check_versions=True):
        contents = contents or []
        version_ids = version_ids or []

        for i in range(num_versions):
            body = 'content-{i}'.format(i=i)
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
            version_id = response['VersionId']

            contents.append(body)
            version_ids.append(version_id)

        if check_versions:
            self.check_obj_versions(client, bucket_name, key, version_ids, contents)

        return version_ids, contents
