import pytest

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestVersioningBase(TestBaseClass):

    @staticmethod
    def clean_up_bucket(client, bucket_name, key, version_ids):
        for version_id in version_ids:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)

        client.delete_bucket(Bucket=bucket_name)

    def create_remove_versions(self, client, bucket_name, key, num_versions, remove_start_idx, idx_inc):
        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)

        idx = remove_start_idx

        for j in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)
            idx += idx_inc

        response = client.list_object_versions(Bucket=bucket_name)
        if 'Versions' in response:
            print(response['Versions'])

    def delete_suspended_versioning_obj(self, client, bucket_name, key, version_ids, contents):
        client.delete_object(Bucket=bucket_name, Key=key)

        # clear out old null objects in lists since they will get overwritten
        self.eq(len(version_ids), len(contents))
        i = 0
        for version_id in version_ids:
            if version_id == 'null':
                version_ids.pop(i)
                contents.pop(i)
            i += 1

        return version_ids, contents

    def overwrite_suspended_versioning_obj(self, client, bucket_name, key, version_ids, contents, content):
        client.put_object(Bucket=bucket_name, Key=key, Body=content)

        # clear out old null objects in lists since they will get overwritten
        self.eq(len(version_ids), len(contents))
        i = 0
        for version_id in version_ids:
            if version_id == 'null':
                version_ids.pop(i)
                contents.pop(i)
            i += 1

        # add new content with 'null' version id to the end
        contents.append(content)
        version_ids.append('null')

        return version_ids, contents

    def remove_obj_version(self, client, bucket_name, key, version_ids, contents, index):
        self.eq(len(version_ids), len(contents))
        index = index % len(version_ids)
        rm_version_id = version_ids.pop(index)
        rm_content = contents.pop(index)

        self.check_obj_content(client, bucket_name, key, rm_version_id, rm_content)

        client.delete_object(Bucket=bucket_name, Key=key, VersionId=rm_version_id)

        if len(version_ids) != 0:
            self.check_obj_versions(client, bucket_name, key, version_ids, contents)


class TestVersioning(TestVersioningBase):

    @pytest.mark.ess
    def test_versioning_bucket_create_suspend(self, s3cfg_global_unique):
        """
        测试-验证can create and suspend bucket versioning
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_versioning(client, bucket_name, None)

        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")

    @pytest.mark.ess
    def test_versioning_obj_list_marker(self, s3cfg_global_unique):
        """
        测试-验证list_object_versions是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'testobj'
        key2 = 'testobj-1'
        num_versions = 5

        contents = []
        version_ids = []
        contents2 = []
        version_ids2 = []

        # for key #1
        for i in range(num_versions):
            body = 'content-{i}'.format(i=i)
            response = client.put_object(Bucket=bucket_name, Key=key, Body=body)
            version_id = response['VersionId']

            contents.append(body)
            version_ids.append(version_id)

        # for key #2
        for i in range(num_versions):
            body = 'content-{i}'.format(i=i)
            response = client.put_object(Bucket=bucket_name, Key=key2, Body=body)
            version_id = response['VersionId']

            contents2.append(body)
            version_ids2.append(version_id)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        # obj versions in versions come out created last to first not first to last like version_ids & contents
        versions.reverse()

        i = 0
        # test the last 5 created objects first
        for i in range(5):
            version = versions[i]
            self.eq(version['VersionId'], version_ids2[i])
            self.eq(version['Key'], key2)
            self.check_obj_content(client, bucket_name, key2, version['VersionId'], contents2[i])
            i += 1

        # then the first 5
        for j in range(5):
            version = versions[i]
            self.eq(version['VersionId'], version_ids[j])
            self.eq(version['Key'], key)
            self.check_obj_content(client, bucket_name, key, version['VersionId'], contents[j])
            i += 1

    @pytest.mark.ess
    def test_versioning_copy_obj_version(self, s3cfg_global_unique):
        """
        测试-验证多版本和copy_object结合
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'testobj'
        num_versions = 3

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)

        for i in range(num_versions):
            new_key_name = 'key_{i}'.format(i=i)
            copy_source = {'Bucket': bucket_name, 'Key': key, 'VersionId': version_ids[i]}
            client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=new_key_name)
            response = client.get_object(Bucket=bucket_name, Key=new_key_name)
            body = self.get_body(response)
            self.eq(body, contents[i])

        another_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        for i in range(num_versions):
            new_key_name = 'key_{i}'.format(i=i)
            copy_source = {'Bucket': bucket_name, 'Key': key, 'VersionId': version_ids[i]}
            client.copy_object(Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name)
            response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
            body = self.get_body(response)
            self.eq(body, contents[i])

        new_key_name = 'new_key'
        copy_source = {'Bucket': bucket_name, 'Key': key}
        client.copy_object(Bucket=another_bucket_name, CopySource=copy_source, Key=new_key_name)

        response = client.get_object(Bucket=another_bucket_name, Key=new_key_name)
        body = self.get_body(response)
        self.eq(body, contents[-1])

    @pytest.mark.ess
    def test_bucket_list_return_data_versioning(self, s3cfg_global_unique):
        """
        测试-验证list_object_versions的响应中各个字段符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key_names = ['bar', 'baz', 'foo']
        bucket_name = self.create_objects(s3cfg_global_unique, bucket_name=bucket_name, keys=key_names)

        data = {}

        for key_name in key_names:
            obj_response = client.head_object(Bucket=bucket_name, Key=key_name)
            acl_response = client.get_object_acl(Bucket=bucket_name, Key=key_name)
            data.update({
                key_name: {
                    'ID': acl_response['Owner']['ID'],
                    'DisplayName': acl_response['Owner']['DisplayName'],
                    'ETag': obj_response['ETag'],
                    'LastModified': obj_response['LastModified'],
                    'ContentLength': obj_response['ContentLength'],
                    'VersionId': obj_response['VersionId']
                }
            })

        response = client.list_object_versions(Bucket=bucket_name)
        objs_list = response['Versions']

        for obj in objs_list:
            key_name = obj['Key']
            key_data = data[key_name]
            self.eq(obj['Owner']['DisplayName'], key_data['DisplayName'])
            self.eq(obj['ETag'], key_data['ETag'])
            self.eq(obj['Size'], key_data['ContentLength'])
            self.eq(obj['Owner']['ID'], key_data['ID'])
            self.eq(obj['VersionId'], key_data['VersionId'])
            self.compare_dates(obj['LastModified'], key_data['LastModified'])

    @pytest.mark.ess
    def test_object_copy_versioned_bucket(self, s3cfg_global_unique):
        """
        测试-验证copy object to/from versioned bucket
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        size = 1 * 5
        data = bytearray(size)
        data_str = data.decode()
        key1 = 'foo123bar'
        client.put_object(Bucket=bucket_name, Key=key1, Body=data)

        response = client.get_object(Bucket=bucket_name, Key=key1)
        version_id = response['VersionId']

        # copy object in the same bucket
        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key2 = 'bar321foo'
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
        response = client.get_object(Bucket=bucket_name, Key=key2)
        body = self.get_body(response)
        self.eq(data_str, body)
        self.eq(size, response['ContentLength'])

        # second copy
        version_id2 = response['VersionId']
        copy_source = {'Bucket': bucket_name, 'Key': key2, 'VersionId': version_id2}
        key3 = 'bar321foo2'
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
        response = client.get_object(Bucket=bucket_name, Key=key3)
        body = self.get_body(response)
        self.eq(data_str, body)
        self.eq(size, response['ContentLength'])

        # copy to another versioned bucket
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name2, "Enabled", "Enabled")
        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key4 = 'bar321foo3'
        client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
        response = client.get_object(Bucket=bucket_name2, Key=key4)
        body = self.get_body(response)
        self.eq(data_str, body)
        self.eq(size, response['ContentLength'])

        # copy to another non versioned bucket
        bucket_name3 = self.get_new_bucket(client, s3cfg_global_unique)
        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key5 = 'bar321foo4'
        client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
        response = client.get_object(Bucket=bucket_name3, Key=key5)
        body = self.get_body(response)
        self.eq(data_str, body)
        self.eq(size, response['ContentLength'])

        # copy from a non versioned bucket
        copy_source = {'Bucket': bucket_name3, 'Key': key5}
        key6 = 'foo123bar2'
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key6)
        response = client.get_object(Bucket=bucket_name, Key=key6)
        body = self.get_body(response)
        self.eq(data_str, body)
        self.eq(size, response['ContentLength'])

    @pytest.mark.ess
    def test_object_copy_versioned_url_encoding(self, s3cfg_global_unique):
        """
        测试-验证copy object to/from versioned bucket with url-encoded name
        """
        client = get_client(s3cfg_global_unique)

        bucket = self.get_new_bucket_resource(s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket.name, "Enabled", "Enabled")
        src_key = 'foo?bar'
        src = bucket.put_object(Key=src_key)
        src.load()  # HEAD request tests that the key exists

        # copy object in the same bucket
        dst_key = 'bar&foo'
        dst = bucket.Object(dst_key)
        dst.copy_from(CopySource={'Bucket': src.bucket_name, 'Key': src.key, 'VersionId': src.version_id})
        dst.load()  # HEAD request tests that the key exists

    @pytest.mark.ess
    def test_versioning_obj_create_read_remove(self, s3cfg_global_unique):
        """
        测试-验证can create access and remove appropriate versions
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_bucket_versioning(Bucket=bucket_name,
                                     VersioningConfiguration={'MFADelete': 'Disabled', 'Status': 'Enabled'})
        key = 'testobj'
        num_versions = 5

        self.create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
        self.create_remove_versions(client, bucket_name, key, num_versions, -1, 0)
        self.create_remove_versions(client, bucket_name, key, num_versions, 0, 0)
        self.create_remove_versions(client, bucket_name, key, num_versions, 1, 0)
        self.create_remove_versions(client, bucket_name, key, num_versions, 4, -1)
        self.create_remove_versions(client, bucket_name, key, num_versions, 3, 3)

    @pytest.mark.ess
    def test_versioning_obj_create_read_remove_head(self, s3cfg_global_unique):
        """
        测试-验证create and remove versioned object and head
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_bucket_versioning(Bucket=bucket_name,
                                     VersioningConfiguration={'MFADelete': 'Disabled', 'Status': 'Enabled'})
        key = 'testobj'
        num_versions = 5

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)

        # removes old head object, checks new one
        removed_version_id = version_ids.pop()
        contents.pop()
        num_versions = num_versions - 1

        client.delete_object(Bucket=bucket_name, Key=key, VersionId=removed_version_id)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, contents[-1])

        # add a delete marker
        response = client.delete_object(Bucket=bucket_name, Key=key)
        self.eq(response['DeleteMarker'], True)

        delete_marker_version_id = response['VersionId']
        version_ids.append(delete_marker_version_id)

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(len(response['Versions']), num_versions)
        self.eq(len(response['DeleteMarkers']), 1)
        self.eq(response['DeleteMarkers'][0]['VersionId'], delete_marker_version_id)

        self.clean_up_bucket(client, bucket_name, key, version_ids)

    @pytest.mark.ess
    def test_versioning_obj_plain_null_version_removal(self, s3cfg_global_unique):
        """
        测试-验证create object, then switch to versioning
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_versioning(client, bucket_name, None)

        key = 'testobjfoo'
        content = 'fooz'
        client.put_object(Bucket=bucket_name, Key=key, Body=content)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)

    @pytest.mark.ess
    def test_versioning_obj_plain_null_version_overwrite(self, s3cfg_global_unique):
        """
        测试-验证开启多版本的存储桶中，删除VersionId为null的多版本对象是否符合预期
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_versioning(client, bucket_name, None)

        key = 'testobjfoo'
        content = 'fooz'
        client.put_object(Bucket=bucket_name, Key=key, Body=content)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        content2 = 'zzz'
        client.put_object(Bucket=bucket_name, Key=key, Body=content2)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, content2)

        version_id = response['VersionId']
        client.delete_object(Bucket=bucket_name, Key=key, VersionId=version_id)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, content)

        client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)

    @pytest.mark.ess
    def test_versioning_obj_plain_null_version_overwrite_suspended(self, s3cfg_global_unique):
        """
        测试-验证暂停多版本的存储桶中，删除VersionId为null的多版本对象是否符合预期
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_versioning(client, bucket_name, None)

        key = 'testobjbar'
        content = 'foooz'
        client.put_object(Bucket=bucket_name, Key=key, Body=content)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")

        content2 = 'zzz'
        client.put_object(Bucket=bucket_name, Key=key, Body=content2)
        response = client.get_object(Bucket=bucket_name, Key=key)
        body = self.get_body(response)
        self.eq(body, content2)

        response = client.list_object_versions(Bucket=bucket_name)
        # original object with 'null' version id still counts as a version
        self.eq(len(response['Versions']), 1)

        client.delete_object(Bucket=bucket_name, Key=key, VersionId='null')
        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key=key)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)

    @pytest.mark.ess
    def test_versioning_obj_suspend_versions(self, s3cfg_global_unique):
        """
        测试-验证暂停多版本后，进行多版本对象操作
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        # versioning enabled
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'testobj'
        num_versions = 5

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)
        # versioning suspended
        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")
        """
        {'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:35 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023dd3-00628ee776-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023dd3-00628ee776-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        # 1. delete-object, so DeleteMarkers will be created, and versionId is null.
        # 2. pop this version, when versionId is null --- I think it is useless.
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 35, 298000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:35 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023dd5-00628ee777-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023dd5-00628ee777-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 35, 604000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:35 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023dd7-00628ee777-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023dd7-00628ee777-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        self.overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 1')
        """
        {'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:36 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023dd9-00628ee777-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023dd9-00628ee777-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"1a1f650ff56ed3e64b7cd8d54f71356d"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 35, 876000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 14,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'null'},
                      {'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        self.overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 2')
        """
        {'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:36 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023ddb-00628ee778-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023ddb-00628ee778-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"b721f894506e869bdd4e40703fc9ff7d"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 36, 222000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 14,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'null'},
                      {'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 36, 500000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:36 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023ddd-00628ee778-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023ddd-00628ee778-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        # 1. overwrite this object which versionId is null, so DeleteMarkers is disappeared.
        # 2. VersionId's total number is six.
        self.overwrite_suspended_versioning_obj(client, bucket_name, key, version_ids, contents, 'null content 3')
        """
        {'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:37 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023ddf-00628ee779-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023ddf-00628ee779-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"ed68e721bd04ac967d30315758fb854f"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 36, 958000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 14,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'null'},
                      {'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        self.delete_suspended_versioning_obj(client, bucket_name, key, version_ids, contents)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 45, 154000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-0p2spgzh9y90srzfzidnprefm-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 03:21:45 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023e9e-00628ef249-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023e9e-00628ef249-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 43, 652000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'XNxji2.wyXoLhEddffTLe3-orbZRf2L'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 43, 476000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': '62so9CM9UcZf2sLTfVNQ15l8F6xwAZ-'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 43, 299000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'G3vMF5GBdFWEaDN4K1EEesRkGuvmn93'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 43, 167000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'XhEx6we4Jy4j5IM3Y2m2v9I05tjTr4O'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 21, 42, 982000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'h-Opi70qx-VrRplaHevcElFLO1tSgkQ'}]}
        """
        # versioning enabled.
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        # create other three versions.
        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, 3, version_ids, contents)
        num_versions += 3  # so num_versions is 8.
        """
        {'DeleteMarkers': [{'IsLatest': False,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 37, 206000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:38 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023def-00628ee77a-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023def-00628ee77a-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 37, 861000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'eaoFKtZ9nLXOIvxfEbkB5HVkW4lmUQJ'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 37, 743000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'v5PckXSgnzL8ujIaYpgijBg-wkyj0Vc'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 37, 575000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'iCjtn-7N3QtXDYBgIXZqnUfjZU8DRNF'},
                      {'ETag': '"27ddd45b69e721be1dec90860cb39c5e"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 130000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'xv8AaYPLXxaHzuo2gOCK-nppqujRtnD'},
                      {'ETag': '"6deb7d5ca68840f07d8e56764c8dc673"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 34, 39000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'ZTrIKvShJ3.z40lozRYs4WCTYQPF3cB'},
                      {'ETag': '"610bd1fcc43e89895ec87162b65226c3"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 907000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'Yopxs88Ik5lyS4zXyHBJFW8WMFNswLd'},
                      {'ETag': '"928cae62314b95527918ee4a2447da01"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 779000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'gA8gHvl5h0rlT0S95hGSOzCOXLVS12n'},
                      {'ETag': '"8e689ba88b92e4020c6508e8a73e4ec1"',
                       'IsLatest': False,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 33, 592000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 9,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'f2Pr-lv5VjDrcFwV.tjd6vBqkixby10'}]}
        """
        for idx in range(num_versions):
            # delete all versions, except DeleteMarkers
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 2, 35, 37, 206000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-ko0a01tu9h1p1rdnyb3fvmhn0-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 02:35:44 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023e23-00628ee780-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023e23-00628ee780-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': ''}
        """
        self.eq(len(version_ids), 0)
        self.eq(len(version_ids), len(contents))

        client.delete_object(Bucket=bucket_name, Key=key)
        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)
        """
        {'DeleteMarkers': [{'IsLatest': True,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 3, 32, 10, 12000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'SP19U6flH86mcT6X7cFXh8m2sGPRMxL'},
                           {'IsLatest': False,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 3, 32, 3, 875000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-qporxi1n9tbr51fqrxhff7fjr-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 03:32:10 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023f58-00628ef4ba-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023f58-00628ef4ba-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': ''}
        """
        # client.put_object(Bucket=bucket_name, Key=key, Body="123")
        """
        {'DeleteMarkers': [{'IsLatest': False,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 3, 32, 10, 12000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'SP19U6flH86mcT6X7cFXh8m2sGPRMxL'},
                           {'IsLatest': False,
                            'Key': 'testobj',
                            'LastModified': datetime.datetime(2022, 5, 26, 3, 32, 3, 875000, tzinfo=tzutc()),
                            'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                            'VersionId': 'null'}],
         'EncodingType': 'url',
         'IsTruncated': False,
         'KeyMarker': '',
         'MaxKeys': 1000,
         'Name': 'ess-qporxi1n9tbr51fqrxhff7fjr-1',
         'Prefix': '',
         'ResponseMetadata': {'HTTPHeaders': {'connection': 'Keep-Alive',
                                              'content-type': 'application/xml',
                                              'date': 'Thu, 26 May 2022 03:32:10 GMT',
                                              'transfer-encoding': 'chunked',
                                              'x-amz-request-id': 'tx000000000000000023f5a-00628ef4ba-33a7983-zone-1647582137'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000023f5a-00628ef4ba-33a7983-zone-1647582137',
                              'RetryAttempts': 0},
         'VersionIdMarker': '',
         'Versions': [{'ETag': '"202cb962ac59075b964b07152d234b70"',
                       'IsLatest': True,
                       'Key': 'testobj',
                       'LastModified': datetime.datetime(2022, 5, 26, 3, 32, 10, 309000, tzinfo=tzutc()),
                       'Owner': {'DisplayName': 'wanghx', 'ID': 'wanghx'},
                       'Size': 3,
                       'StorageClass': 'STANDARD',
                       'VersionId': 'VVY-uC27gaYGmJ5sNhsFY1LY0bX42SP'}]}
        """

    @pytest.mark.ess
    def test_versioning_obj_create_versions_remove_all(self, s3cfg_global_unique):
        """
        测试-验证删除多版本接口符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'testobj'
        num_versions = 10

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)
        for idx in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

        self.eq(len(version_ids), 0)
        self.eq(len(version_ids), len(contents))

    @pytest.mark.ess
    def test_versioning_obj_create_versions_remove_special_names(self, s3cfg_global_unique):
        """
        测试-验证多版本和特殊对象名称（_, :, ' '）结合
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        keys = ['_testobj', '_', ':', ' ']
        num_versions = 10

        for key in keys:
            (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)
            for idx in range(num_versions):
                self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

            self.eq(len(version_ids), 0)
            self.eq(len(version_ids), len(contents))

    @pytest.mark.ess
    def test_versioning_multi_object_delete(self, s3cfg_global_unique):
        """
        测试-验证多次删除多版本对象，依旧成功（幂等特性）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'key'
        num_versions = 2

        self.create_multiple_versions(client, bucket_name, key, num_versions)

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        versions.reverse()

        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version['VersionId'])

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)

        # now remove again, should all succeed due to idempotency
        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version['VersionId'])

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)

    @pytest.mark.ess
    def test_versioning_multi_object_delete_with_marker(self, s3cfg_global_unique):
        """
        测试-验证删除对象（使用deleteMarker）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'key'
        num_versions = 2

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)

        client.delete_object(Bucket=bucket_name, Key=key)
        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        delete_markers = response['DeleteMarkers']

        version_ids.append(delete_markers[0]['VersionId'])
        self.eq(len(version_ids), 3)
        self.eq(len(delete_markers), 1)

        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version['VersionId'])

        for delete_marker in delete_markers:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=delete_marker['VersionId'])

        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)
        self.eq(('DeleteMarkers' in response), False)

        for version in versions:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=version['VersionId'])

        for delete_marker in delete_markers:
            client.delete_object(Bucket=bucket_name, Key=key, VersionId=delete_marker['VersionId'])

        # now remove again, should all succeed due to idempotency
        response = client.list_object_versions(Bucket=bucket_name)
        self.eq(('Versions' in response), False)
        self.eq(('DeleteMarkers' in response), False)

    @pytest.mark.ess
    def test_versioning_multi_object_delete_with_marker_create(self, s3cfg_global_unique):
        """
        测试-验证删除对象时不带VersionId，会给对象添加DeleteMarkers标签
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'key'

        response = client.delete_object(Bucket=bucket_name, Key=key)
        delete_marker_version_id = response['VersionId']

        response = client.list_object_versions(Bucket=bucket_name)
        delete_markers = response['DeleteMarkers']

        self.eq(len(delete_markers), 1)
        self.eq(delete_marker_version_id, delete_markers[0]['VersionId'])
        self.eq(key, delete_markers[0]['Key'])

    @pytest.mark.ess
    def test_versioned_concurrent_object_create_concurrent_remove(self, s3cfg_global_unique):
        """
        测试-验证并发上传多版本对象和并发删除多版本对象
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'myobj'
        num_versions = 5

        for i in range(5):
            t = self.do_create_versioned_obj_concurrent(client, self.do_create_object, bucket_name, key, num_versions)
            self.do_wait_completion(t)

            response = client.list_object_versions(Bucket=bucket_name)
            versions = response['Versions']

            self.eq(len(versions), num_versions)

            t = self.do_clear_versioned_bucket_concurrent(client, self.do_remove_ver, bucket_name)
            self.do_wait_completion(t)

            response = client.list_object_versions(Bucket=bucket_name)
            self.eq(('Versions' in response), False)

    @pytest.mark.ess
    def test_versioned_concurrent_object_create_and_remove(self, s3cfg_global_unique):
        """
        测试-验证并发上传多版本对象后进行删除
        (operation='concurrent creation and removal of objects')
        (assertion='works')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'myobj'
        num_versions = 3

        all_threads = []

        for i in range(3):
            t = self.do_create_versioned_obj_concurrent(client, self.do_create_object, bucket_name, key, num_versions)
            all_threads.append(t)

            t = self.do_clear_versioned_bucket_concurrent(client, self.do_remove_ver, bucket_name)
            all_threads.append(t)

        for t in all_threads:
            self.do_wait_completion(t)

        t = self.do_clear_versioned_bucket_concurrent(client, self.do_remove_ver, bucket_name)
        self.do_wait_completion(t)

    @pytest.mark.ess
    def test_versioning_bucket_atomic_upload_return_version_id(self, s3cfg_global_unique):
        """
        测试-验证多版本开启和暂停时，上传的对象VersionId是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'bar'

        # for versioning-enabled-bucket, an non-empty version-id should return
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")
        response = client.put_object(Bucket=bucket_name, Key=key)
        version_id = response['VersionId']

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        for version in versions:
            self.eq(version['VersionId'], version_id)

        # for versioning-default-bucket, no version-id should return.
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'baz'
        response = client.put_object(Bucket=bucket_name, Key=key)
        self.eq(('VersionId' in response), False)

        # for versioning-suspended-bucket, no version-id should return.
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'baz'
        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")
        response = client.put_object(Bucket=bucket_name, Key=key)
        self.eq(('VersionId' in response), False)

    @pytest.mark.ess
    def test_object_copy_versioning_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证分段上传，多版本，拷贝对象
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key1 = "srcmultipart"
        key1_metadata = {'foo': 'bar'}
        content_type = 'text/bla'
        obj_len = 30 * 1024 * 1024
        upload_id, data, parts = self.multipart_upload(
            config=s3cfg_global_unique,
            bucket_name=bucket_name,
            key=key1,
            size=obj_len,
            content_type=content_type,
            metadata=key1_metadata
        )

        client.complete_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=bucket_name, Key=key1)
        key1_size = response['ContentLength']
        version_id = response['VersionId']

        # copy object in the same bucket
        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key2 = 'dstmultipart'
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key2)
        response = client.get_object(Bucket=bucket_name, Key=key2)
        version_id2 = response['VersionId']
        body = self.get_body(response)
        self.eq(data, body)
        self.eq(key1_size, response['ContentLength'])
        self.eq(key1_metadata, response['Metadata'])
        self.eq(content_type, response['ContentType'])

        # second copy
        copy_source = {'Bucket': bucket_name, 'Key': key2, 'VersionId': version_id2}
        key3 = 'dstmultipart2'
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=key3)
        response = client.get_object(Bucket=bucket_name, Key=key3)
        body = self.get_body(response)
        self.eq(data, body)
        self.eq(key1_size, response['ContentLength'])
        self.eq(key1_metadata, response['Metadata'])
        self.eq(content_type, response['ContentType'])

        # copy to another versioned bucket
        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)
        self.check_configure_versioning_retry(client, bucket_name2, "Enabled", "Enabled")

        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key4 = 'dstmultipart3'
        client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key=key4)
        response = client.get_object(Bucket=bucket_name2, Key=key4)
        body = self.get_body(response)
        self.eq(data, body)
        self.eq(key1_size, response['ContentLength'])
        self.eq(key1_metadata, response['Metadata'])
        self.eq(content_type, response['ContentType'])

        # copy to another non versioned bucket
        bucket_name3 = self.get_new_bucket(client, s3cfg_global_unique)
        copy_source = {'Bucket': bucket_name, 'Key': key1, 'VersionId': version_id}
        key5 = 'dstmultipart4'
        client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key5)
        response = client.get_object(Bucket=bucket_name3, Key=key5)
        body = self.get_body(response)
        self.eq(data, body)
        self.eq(key1_size, response['ContentLength'])
        self.eq(key1_metadata, response['Metadata'])
        self.eq(content_type, response['ContentType'])

        # copy from a non versioned bucket
        copy_source = {'Bucket': bucket_name3, 'Key': key5}
        key6 = 'dstmultipart5'
        client.copy_object(Bucket=bucket_name3, CopySource=copy_source, Key=key6)
        response = client.get_object(Bucket=bucket_name3, Key=key6)
        body = self.get_body(response)
        self.eq(data, body)
        self.eq(key1_size, response['ContentLength'])
        self.eq(key1_metadata, response['Metadata'])
        self.eq(content_type, response['ContentType'])

    @pytest.mark.ess
    def test_multipart_copy_versioned(self, s3cfg_global_unique):
        """
        测试-验证upload_part_copy和多版本
        """
        client = get_client(s3cfg_global_unique)

        src_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        dest_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        dest_key = "mymultipart"
        self.check_versioning(client, src_bucket_name, None)

        src_key = 'foo'
        self.check_configure_versioning_retry(client, src_bucket_name, "Enabled", "Enabled")

        size = 15 * 1024 * 1024
        self.create_key_with_random_content(s3cfg_global_unique, src_key, size=size, bucket_name=src_bucket_name)
        self.create_key_with_random_content(s3cfg_global_unique, src_key, size=size, bucket_name=src_bucket_name)
        self.create_key_with_random_content(s3cfg_global_unique, src_key, size=size, bucket_name=src_bucket_name)

        version_id = []
        response = client.list_object_versions(Bucket=src_bucket_name)
        for ver in response['Versions']:
            version_id.append(ver['VersionId'])

        for vid in version_id:
            (upload_id, parts) = self.multipart_copy(
                s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size, version_id=vid)
            client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                             MultipartUpload={'Parts': parts})
            response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
            self.eq(size, response['ContentLength'])
            self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name, version_id=vid)

    @pytest.mark.ess
    def test_versioning_obj_create_overwrite_multipart(self, s3cfg_global_unique):
        """
        测试-验证分段上传多版本的对象
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'testobj'
        num_versions = 3
        contents = []
        version_ids = []

        for i in range(num_versions):
            ret = self.do_test_multipart_upload_contents(client, bucket_name, key, 3)
            contents.append(ret)

        response = client.list_object_versions(Bucket=bucket_name)
        for version in response['Versions']:
            version_ids.append(version['VersionId'])

        version_ids.reverse()
        self.check_obj_versions(client, bucket_name, key, version_ids, contents)

        for idx in range(num_versions):
            self.remove_obj_version(client, bucket_name, key, version_ids, contents, idx)

        self.eq(len(version_ids), 0)
        self.eq(len(version_ids), len(contents))

    @pytest.mark.ess
    def test_versioning_bucket_multipart_upload_return_version_id(self, s3cfg_global_unique):
        """
        测试-验证多版本的对象在complete_multipart_upload操作时会返回VersionId
        """
        content_type = 'text/bla'
        obj_len = 30 * 1024 * 1024

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'bar'
        metadata = {'foo': 'baz'}

        # for versioning-enabled-bucket, an non-empty version-id should return
        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        (upload_id, data, parts) = self.multipart_upload(
            config=s3cfg_global_unique, bucket_name=bucket_name,
            key=key, size=obj_len, client=client, content_type=content_type, metadata=metadata)

        response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                                    MultipartUpload={'Parts': parts})
        version_id = response['VersionId']

        response = client.list_object_versions(Bucket=bucket_name)
        versions = response['Versions']
        for version in versions:
            self.eq(version['VersionId'], version_id)

        # for versioning-default-bucket, no version-id should return.
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'baz'

        (upload_id, data, parts) = self.multipart_upload(
            config=s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len, client=client,
            content_type=content_type, metadata=metadata)

        response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                                    MultipartUpload={'Parts': parts})
        self.eq(('VersionId' in response), False)

        # for versioning-suspended-bucket, no version-id should return
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'foo'
        self.check_configure_versioning_retry(client, bucket_name, "Suspended", "Suspended")

        (upload_id, data, parts) = self.multipart_upload(config=s3cfg_global_unique, bucket_name=bucket_name, key=key,
                                                         size=obj_len, client=client,
                                                         content_type=content_type, metadata=metadata)

        response = client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                                    MultipartUpload={'Parts': parts})
        self.eq(('VersionId' in response), False)
