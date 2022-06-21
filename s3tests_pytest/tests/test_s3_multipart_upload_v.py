import pytest

from s3tests_pytest.tests import (
    TestBaseClass,
    assert_raises,
    ClientError,
    Counter,
    FakeWriteFile,
    ActionOnCount,
    get_client,
    get_alt_client
)


class TestMultipartBase(TestBaseClass):

    def check_content_using_range(self, client, key, bucket_name, data, step):
        response = client.get_object(Bucket=bucket_name, Key=key)
        size = response['ContentLength']

        for ofs in range(0, size, step):
            toread = size - ofs
            if toread > step:
                toread = step
            end = ofs + toread - 1
            r = 'bytes={s}-{e}'.format(s=ofs, e=end)
            response = client.get_object(Bucket=bucket_name, Key=key, Range=r)
            self.eq(response['ContentLength'], toread)
            body = self.get_body(response)
            self.eq(body, data[ofs:end + 1])

    def check_upload_multipart_resend(self, config, bucket_name, key, obj_len, resend_parts):
        client = get_client(config)
        content_type = 'text/bla'
        metadata = {'foo': 'bar'}
        (upload_id, data, parts) = self.multipart_upload(
            config=config,
            bucket_name=bucket_name, key=key, size=obj_len,
            content_type=content_type, metadata=metadata,
            resend_parts=resend_parts)

        client.complete_multipart_upload(
            Bucket=bucket_name, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=bucket_name, Key=key)
        self.eq(response['ContentType'], content_type)
        self.eq(response['Metadata'], metadata)
        body = self.get_body(response)
        self.eq(len(body), response['ContentLength'])
        self.eq(body, data)

        self.check_content_using_range(client, key, bucket_name, data, 1000000)
        self.check_content_using_range(client, key, bucket_name, data, 10000000)


class TestObjectMultipartUpload(TestMultipartBase):

    @pytest.mark.ess
    def test_multipart_upload_empty(self, s3cfg_global_unique):
        """
        测试-验证合并分段上传任务的时候，不提供Parts，
        400，MalformedXML
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        key1 = "mymultipart"
        obj_len = 0
        upload_id, data, parts = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key1, size=obj_len)
        e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key1,
                          UploadId=upload_id)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'MalformedXML')

    @pytest.mark.ess
    def test_multipart_upload_small(self, s3cfg_global_unique):
        """
        测试-验证上传一片，且分段对象大小是1
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        key1 = "mymultipart"
        obj_len = 1
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key1, size=obj_len)
        client.complete_multipart_upload(
            Bucket=bucket_name, Key=key1, UploadId=upload_id, MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=bucket_name, Key=key1)
        self.eq(response['ContentLength'], obj_len)

    @pytest.mark.ess
    def test_multipart_copy_small(self, s3cfg_global_unique):
        """
        测试-验证upload_part_copy接口拷贝小的分片对象
        """
        client = get_client(s3cfg_global_unique)

        src_key = 'foo'
        src_bucket_name = self.create_key_with_random_content(s3cfg_global_unique, src_key)
        dest_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        dest_key = "mymultipart"
        size = 1

        upload_id, parts = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        self.eq(size, response['ContentLength'])
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

    @pytest.mark.ess
    def test_multipart_copy_invalid_range(self, s3cfg_global_unique):
        """
        测试-验证分段上传拷贝接口，使用无效的range，查看是否符合预期
        """
        client = get_client(s3cfg_global_unique)
        src_key = 'source'
        src_bucket_name = self.create_key_with_random_content(s3cfg_global_unique, src_key, size=5)

        response = client.create_multipart_upload(Bucket=src_bucket_name, Key='dest')
        upload_id = response['UploadId']

        copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
        copy_source_range = 'bytes={start}-{end}'.format(start=0, end=21)

        e = assert_raises(
            ClientError, client.upload_part_copy,
            Bucket=src_bucket_name, Key='dest',
            UploadId=upload_id, CopySource=copy_source,
            CopySourceRange=copy_source_range, PartNumber=1)
        status, error_code = self.get_status_and_error_code(e.response)

        valid_status = [400, 416]
        if status not in valid_status:
            raise AssertionError("Invalid response " + str(status))
        self.eq(error_code, 'InvalidRange')

    @pytest.mark.ess
    @pytest.mark.fails_on_ess
    @pytest.mark.xfail(reason="预期：无效的CopySourceRange取值应该返回错误响应", run=True, strict=True)
    def test_multipart_copy_improper_range(self, s3cfg_global_unique):
        """
        测试-验证CopySourceRange参数的不同取值(异常取值)下的响应，
        """
        # TODO: remove fails_on_rgw when https://tracker.ceph.com/issues/40795 is resolved
        client = get_client(s3cfg_global_unique)

        src_key = 'source'
        src_bucket_name = self.create_key_with_random_content(s3cfg_global_unique, src_key, size=5)

        response = client.create_multipart_upload(
            Bucket=src_bucket_name, Key='dest')
        upload_id = response['UploadId']

        copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
        test_ranges = [
            '{start}-{end}'.format(start=0, end=2),
            'bytes={start}'.format(start=0),
            'bytes=hello-world',  # succeed, so strange.
            'bytes=0-bar',  # succeed, so strange.
            'bytes=hello-',  # succeed, so strange.
            'bytes=0-2,3-5'  # succeed, so strange.
        ]

        """
        CopySourceRange:
            The range of bytes to copy from the source object. 
            The range value must use the form bytes=first-last, 
                where the first and last are the zero-based byte offsets to copy. 
            For example, bytes=0-9 indicates that you want to copy the first 10 bytes of the source. 
            You can copy a range only if the source object is greater than 5 MB.
        """
        for test_range in test_ranges:
            e = assert_raises(ClientError, client.upload_part_copy,
                              Bucket=src_bucket_name, Key='dest',
                              UploadId=upload_id,
                              CopySource=copy_source,
                              CopySourceRange=test_range,
                              PartNumber=1)
            status, error_code = self.get_status_and_error_code(e.response)
            self.eq(status, 400)
            self.eq(error_code, 'InvalidArgument')

    @pytest.mark.ess
    def test_multipart_copy_without_range(self, s3cfg_global_unique):
        """
        测试-验证check multipart copies without x-amz-copy-source-range
        """
        client = get_client(s3cfg_global_unique)

        src_key = 'source'
        src_bucket_name = self.create_key_with_random_content(s3cfg_global_unique, src_key, size=10)
        dest_bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        self.get_new_bucket(client, s3cfg_global_unique, name=dest_bucket_name)
        dest_key = "mymultipartcopy"

        response = client.create_multipart_upload(Bucket=dest_bucket_name, Key=dest_key)
        upload_id = response['UploadId']
        parts = []

        copy_source = {'Bucket': src_bucket_name, 'Key': src_key}
        part_num = 1

        response = client.upload_part_copy(Bucket=dest_bucket_name, Key=dest_key, CopySource=copy_source,
                                           PartNumber=part_num, UploadId=upload_id)

        parts.append({'ETag': response['CopyPartResult']['ETag'], 'PartNumber': part_num})
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
        self.eq(response['ContentLength'], 10)
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

    @pytest.mark.ess
    def test_multipart_copy_special_names(self, s3cfg_global_unique):
        """
        测试-验证复制分段上传接口，单个小片(size=10 bytes)
        """
        client = get_client(s3cfg_global_unique)

        src_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        dest_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        dest_key = "mymultipart"
        size = 1

        for src_key in (' ', '_', '__', '?versionId'):
            self.create_key_with_random_content(
                s3cfg_global_unique, src_key, bucket_name=src_bucket_name, size=10)  # add size=10 to save time.
            (upload_id, parts) = self.multipart_copy(
                s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
            client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key,
                                             UploadId=upload_id, MultipartUpload={'Parts': parts})
            print(client.list_objects(Bucket=dest_bucket_name))
            response = client.get_object(Bucket=dest_bucket_name, Key=dest_key)
            self.eq(size, response['ContentLength'])
            self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

    @pytest.mark.ess
    def test_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证结束分段上传任务，并验证结果是否正确；
        含headers里的bytes-used、object-count；body是否正确。
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"
        content_type = 'text/bla'
        obj_len = 30 * 1024 * 1024
        metadata = {'foo': 'bar'}

        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name,
            key=key, size=obj_len, content_type=content_type, metadata=metadata)

        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.head_bucket(Bucket=bucket_name)
        rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', obj_len))
        self.eq(rgw_bytes_used, obj_len)

        rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 1))
        self.eq(rgw_object_count, 1)

        response = client.get_object(Bucket=bucket_name, Key=key)
        self.eq(response['ContentType'], content_type)
        self.eq(response['Metadata'], metadata)
        body = self.get_body(response)
        self.eq(len(body), response['ContentLength'])
        self.eq(body, data)

        self.check_content_using_range(client, key, bucket_name, data, 1000000)
        self.check_content_using_range(client, key, bucket_name, data, 10000000)

    @pytest.mark.ess
    def test_multipart_upload_resend_part(self, s3cfg_global_unique):
        """
        测试-验证不同文件大小下，结束分段上传是否成功
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"
        obj_len = 30 * 1024 * 1024

        self.check_upload_multipart_resend(s3cfg_global_unique, bucket_name, key, obj_len, [0])
        self.check_upload_multipart_resend(s3cfg_global_unique, bucket_name, key, obj_len, [1])
        self.check_upload_multipart_resend(s3cfg_global_unique, bucket_name, key, obj_len, [2])
        self.check_upload_multipart_resend(s3cfg_global_unique, bucket_name, key, obj_len, [1, 2])
        self.check_upload_multipart_resend(s3cfg_global_unique, bucket_name, key, obj_len, [0, 1, 2, 3, 4, 5])

    @pytest.mark.ess
    def test_multipart_upload_multiple_sizes(self, s3cfg_global_unique):
        """
        测试-验证不同文件大小下结束分段上传是否成功
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"

        obj_len = 5 * 1024 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        obj_len = 5 * 1024 * 1024 + 100 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        obj_len = 5 * 1024 * 1024 + 600 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        obj_len = 10 * 1024 * 1024 + 100 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        obj_len = 10 * 1024 * 1024 + 600 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        obj_len = 10 * 1024 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

    @pytest.mark.ess
    def test_multipart_copy_multiple_sizes(self, s3cfg_global_unique):
        """
        测试-验证不同文件大小下upload_part_copy是否成功
        """
        client = get_client(s3cfg_global_unique)

        src_key = 'foo'
        src_bucket_name = self.create_key_with_random_content(s3cfg_global_unique, src_key, 12 * 1024 * 1024)

        dest_bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        dest_key = "mymultipart"

        size = 5 * 1024 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

        size = 5 * 1024 * 1024 + 100 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

        size = 5 * 1024 * 1024 + 600 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

        size = 10 * 1024 * 1024 + 100 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

        size = 10 * 1024 * 1024 + 600 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

        size = 10 * 1024 * 1024
        (upload_id, parts) = self.multipart_copy(
            s3cfg_global_unique, src_bucket_name, src_key, dest_bucket_name, dest_key, size)
        client.complete_multipart_upload(Bucket=dest_bucket_name, Key=dest_key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})
        self.check_key_content(client, src_key, src_bucket_name, dest_key, dest_bucket_name)

    @pytest.mark.ess
    def test_multipart_upload_size_too_small(self, s3cfg_global_unique):
        """
        测试-验证分段小于5MiB时（除最后一段），进行合并会报错，
        400，EntityTooSmall
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"

        size = 100 * 1024
        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=size, part_size=10 * 1024)
        e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key,
                          UploadId=upload_id, MultipartUpload={'Parts': parts})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'EntityTooSmall')

    @pytest.mark.ess
    def test_multipart_upload_contents(self, s3cfg_global_unique):
        """
        测试-验证分段上传对象body与上传的源对象是一致的
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        self.do_test_multipart_upload_contents(client, bucket_name, 'mymultipart', 3)

    @pytest.mark.ess
    def test_multipart_upload_overwrite_existing_object(self, s3cfg_global_unique):
        """
        测试-对已存在的对象进行覆盖写（使用分段上传）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = 'mymultipart'
        payload = '12345' * 1024 * 1024
        num_parts = 2
        client.put_object(Bucket=bucket_name, Key=key, Body=payload)

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response['UploadId']

        parts = []

        for part_num in range(0, num_parts):
            response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num + 1,
                                          Body=payload)
            parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num + 1})

        client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        response = client.get_object(Bucket=bucket_name, Key=key)
        test_string = self.get_body(response)

        assert test_string == payload * num_parts

    @pytest.mark.ess
    def test_abort_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证中断分段上传任务
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"
        obj_len = 10 * 1024 * 1024

        (upload_id, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=obj_len)
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id)

        response = client.head_bucket(Bucket=bucket_name)
        rgw_bytes_used = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-bytes-used', 0))
        self.eq(rgw_bytes_used, 0)

        rgw_object_count = int(response['ResponseMetadata']['HTTPHeaders'].get('x-rgw-object-count', 0))
        self.eq(rgw_object_count, 0)

    @pytest.mark.ess
    def test_abort_multipart_upload_not_found(self, s3cfg_global_unique):
        """
        测试-验证中断不存在的分段上传任务，
        404，NoSuchUpload
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"
        client.put_object(Bucket=bucket_name, Key=key)

        e = assert_raises(ClientError, client.abort_multipart_upload, Bucket=bucket_name, Key=key, UploadId='56788')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchUpload')

    @pytest.mark.ess
    def test_list_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证list_multipart_uploads结果是否正确，
        含对同一个对象多次分段上传的情况。
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"
        mb = 1024 * 1024

        upload_ids = []
        (upload_id1, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=5 * mb)
        upload_ids.append(upload_id1)
        (upload_id2, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key, size=6 * mb)
        upload_ids.append(upload_id2)

        key2 = "mymultipart2"
        (upload_id3, data, parts) = self.multipart_upload(
            s3cfg_global_unique, bucket_name=bucket_name, key=key2, size=5 * mb)
        upload_ids.append(upload_id3)

        response = client.list_multipart_uploads(Bucket=bucket_name)
        uploads = response['Uploads']
        resp_uploadids = []

        for i in range(0, len(uploads)):
            resp_uploadids.append(uploads[i]['UploadId'])

        for i in range(0, len(upload_ids)):
            self.eq(True, (upload_ids[i] in resp_uploadids))

        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id1)
        client.abort_multipart_upload(Bucket=bucket_name, Key=key, UploadId=upload_id2)
        client.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload_id3)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess  # TODO: ObjectOwnership parameter is not suitable.
    @pytest.mark.xfail(reason="预期：list_multipart_uploads中owner按照ObjectOwnership显示", run=True, strict=True)
    def test_list_multipart_upload_owner(self, s3cfg_global_unique):
        """
        测试-验证使用不同对象用户对public-read-write的桶进行list_multipart_uploads操作
        """
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acls.html
        # https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/about-object-ownership.html

        client1 = get_client(s3cfg_global_unique)
        user1 = s3cfg_global_unique.main_user_id
        name1 = s3cfg_global_unique.main_display_name

        client2 = get_alt_client(s3cfg_global_unique)
        user2 = s3cfg_global_unique.alt_user_id
        name2 = s3cfg_global_unique.alt_display_name
        bucket_name = self.get_new_bucket(client1, s3cfg_global_unique)
        # ObjectOwnership: 'BucketOwnerPreferred'|'ObjectWriter'|'BucketOwnerEnforced'
        # bucket_name = self.get_new_bucket(client1, s3cfg_global_unique, ObjectOwnership='ObjectWriter')

        # add bucket acl for public read/write access
        client1.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

        key1 = 'multipart1'
        key2 = 'multipart2'
        upload1 = client1.create_multipart_upload(Bucket=bucket_name, Key=key1)['UploadId']
        try:
            upload2 = client2.create_multipart_upload(Bucket=bucket_name, Key=key2)['UploadId']
            try:
                # match fields of an Upload from ListMultipartUploadsResult
                def match(upload, key, uploadid, userid, username):
                    self.eq(upload['Key'], key)
                    self.eq(upload['UploadId'], uploadid)
                    self.eq(upload['Initiator']['ID'], userid)
                    self.eq(upload['Initiator']['DisplayName'], username)
                    self.eq(upload['Owner']['ID'], userid)
                    self.eq(upload['Owner']['DisplayName'], username)

                # list uploads with client1
                uploads1 = client1.list_multipart_uploads(Bucket=bucket_name)['Uploads']
                self.eq(len(uploads1), 2)
                match(uploads1[0], key1, upload1, user1, name1)
                match(uploads1[1], key2, upload2, user2, name2)

                # list uploads with client2
                uploads2 = client2.list_multipart_uploads(Bucket=bucket_name)['Uploads']
                self.eq(len(uploads2), 2)
                match(uploads2[0], key1, upload1, user1, name1)
                match(uploads2[1], key2, upload2, user2, name2)
            finally:
                client2.abort_multipart_upload(Bucket=bucket_name, Key=key2, UploadId=upload2)
        finally:
            client1.abort_multipart_upload(Bucket=bucket_name, Key=key1, UploadId=upload1)

    @pytest.mark.ess
    def test_multipart_upload_missing_part(self, s3cfg_global_unique):
        """
        测试-验证使用错误的PartNumber进行合并分段任务，
        400，InvalidPart
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response['UploadId']

        parts = []
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1,
                                      Body=bytes('\x00', 'utf-8'))
        # 'PartNumber should be 1'
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 9999})

        e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key,
                          UploadId=upload_id, MultipartUpload={'Parts': parts})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidPart')

    @pytest.mark.ess
    def test_multipart_upload_incorrect_etag(self, s3cfg_global_unique):
        """
         测试-验证使用错误的ETag进行合并分段任务，
         400，InvalidPart
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key = "mymultipart"

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
        upload_id = response['UploadId']

        parts = []
        client.upload_part(
            UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=1, Body=bytes('\x00', 'utf-8'))
        # 'ETag' should be "93b885adfe0da089cdf634904fd59f71"
        parts.append({'ETag': "ffffffffffffffffffffffffffffffff", 'PartNumber': 1})

        e = assert_raises(ClientError, client.complete_multipart_upload, Bucket=bucket_name, Key=key,
                          UploadId=upload_id, MultipartUpload={'Parts': parts})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidPart')

    @pytest.mark.ess
    def test_atomic_multipart_upload_write(self, s3cfg_global_unique):
        """
        测试-验证对已存在的对象进行创建分段上传任务后中断此任务，查看是否影响此对象。
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.create_multipart_upload(Bucket=bucket_name, Key='foo')
        upload_id = response['UploadId']

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

        client.abort_multipart_upload(Bucket=bucket_name, Key='foo', UploadId=upload_id)

        response = client.get_object(Bucket=bucket_name, Key='foo')
        body = self.get_body(response)
        self.eq(body, 'bar')

    @pytest.mark.ess
    def test_multipart_resend_first_finishes_last(self, s3cfg_global_unique):
        """
        测试-验证对同一个分段进行覆盖写，查看结果是否符合预期，
        合并分段时使用覆盖后的etag，则对象内容是覆盖写的内容。
        """
        # TODO: 是否可以增加一个步骤：合并分段后，查看被覆盖的分段是否还存在且被回收
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key_name = "mymultipart"

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
        upload_id = response['UploadId']

        # file_size = 8*1024*1024
        file_size = 8

        counter = Counter(0)
        # upload_part might read multiple times from the object
        # first time when it calculates md5, second time when it writes data
        # out. We want to interject only on the last time, but we can't be
        # sure how many times it's going to read, so let's have a test run
        # and count the number of reads

        fp_dry_run = FakeWriteFile(file_size, 'C', lambda: counter.inc())

        parts = []

        response = client.upload_part(
            UploadId=upload_id, Bucket=bucket_name, Key=key_name, PartNumber=1, Body=fp_dry_run)

        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 1})
        client.complete_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        client.delete_object(Bucket=bucket_name, Key=key_name)

        # clear parts
        parts[:] = []

        # ok, now for the actual test
        fp_b = FakeWriteFile(file_size, 'B')

        def upload_fp_b():
            res = client.upload_part(
                UploadId=upload_id, Bucket=bucket_name, Key=key_name, Body=fp_b, PartNumber=1)
            parts.append({'ETag': res['ETag'].strip('"'), 'PartNumber': 1})

        action = ActionOnCount(counter.val, lambda: upload_fp_b())

        response = client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
        upload_id = response['UploadId']

        fp_a = FakeWriteFile(file_size, 'A', lambda: action.trigger())

        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key_name, PartNumber=1, Body=fp_a)

        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': 1})
        client.complete_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id,
                                         MultipartUpload={'Parts': parts})

        self.verify_atomic_key_data(client, bucket_name, key_name, file_size, 'A')
