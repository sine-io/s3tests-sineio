import pytest

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestMetadataBase(TestBaseClass):

    def set_get_metadata(self, config, metadata, bucket_name=None):
        """
        create a new bucket new or use an existing
        name to create an object that bucket,
        set the meta1 property to a specified, value,
        and then re-read and return that property
        """
        client = get_client(config)
        if bucket_name is None:
            bucket_name = self.get_new_bucket(client, config)

        metadata_dict = {'meta1': metadata}
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar', Metadata=metadata_dict)

        response = client.get_object(Bucket=bucket_name, Key='foo')
        return response['Metadata']['meta1']

    def set_get_metadata_unreadable(self, config, metadata, bucket_name=None):
        """
        set and then read back a meta-data value (which presumably
        includes some interesting characters), and return a list
        containing the stored value AND the encoding with which it
        was returned.

        This should return a 400 bad request because the webserver
        rejects the request.
        """
        client = get_client(config)
        if bucket_name is None:
            bucket_name = self.get_new_bucket(client, config)

        metadata_dict = {'meta1': metadata}
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key='bar', Metadata=metadata_dict)
        return e


class TestObjectMetadata(TestMetadataBase):

    @pytest.mark.ess
    def test_object_set_get_metadata_none_to_good(self, s3cfg_global_unique):
        """
        测试-设置并获取自定义对象元数据
        """
        got = self.set_get_metadata(s3cfg_global_unique, 'mymeta')
        self.eq(got, 'mymeta')

    @pytest.mark.ess
    def test_object_set_get_metadata_none_to_empty(self, s3cfg_global_unique):
        """
        测试-设置并获取自定义对象元数据（设置空值）
        """
        got = self.set_get_metadata(s3cfg_global_unique, '')
        self.eq(got, '')

    @pytest.mark.ess
    def test_object_set_get_metadata_overwrite_to_empty(self, s3cfg_global_unique):
        """
        测试-设置并获取自定义对象元数据，之后设置为空
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        got = self.set_get_metadata(s3cfg_global_unique, 'oldmeta', bucket_name)
        self.eq(got, 'oldmeta')
        got = self.set_get_metadata(s3cfg_global_unique, '', bucket_name)
        self.eq(got, '')

    @pytest.mark.fails_on_ess
    def test_object_set_get_unicode_metadata(self, s3cfg_global_unique):
        """
        @attr(operation='metadata write/re-write')
        @attr(assertion='UTF-8 values passed through')
        """
        # TODO: the decoding of this unicode metadata is not happening properly for unknown reasons
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        def set_unicode_metadata(**kwargs):
            kwargs['params']['headers']['x-amz-meta-meta1'] = u"Hello World\xe9"

        client.meta.events.register('before-call.s3.PutObject', set_unicode_metadata)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object(Bucket=bucket_name, Key='foo')
        got = response['Metadata']['meta1']
        # got = response['Metadata']['meta1'].decode('utf-8')  # AttributeError: 'str' object has no attribute 'decode'
        # self.eq(got, u"Hello World\xe9")
        # 可使用encode('raw_unicode_escape')将此str转化为bytes, 再decode为str
        # 可使用decode('raw_unicode_escape')输出内容为bytes形式的字符串
        self.eq(got.encode('raw_unicode_escape').decode(), u"Hello World\xe9")

    def test_object_set_get_non_utf8_metadata(self, s3cfg_global_unique):
        """
        @attr(operation='metadata write/re-write')
        @attr(assertion='non-UTF-8 values detected, but rejected by webserver')
        @attr(assertion='fails 400')
        """
        metadata = '\x04mymeta'
        e = self.set_get_metadata_unreadable(s3cfg_global_unique, metadata)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400 or 403)

    def test_object_set_get_metadata_empty_to_unreadable_prefix(self, s3cfg_global_unique):
        """
        @attr(operation='metadata write')
        @attr(assertion='non-printing prefixes rejected by webserver')
        @attr(assertion='fails 400')
        """
        metadata = '\x04w'
        e = self.set_get_metadata_unreadable(s3cfg_global_unique, metadata)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400 or 403)

    def test_object_set_get_metadata_empty_to_unreadable_suffix(self, s3cfg_global_unique):
        """
        @attr(operation='metadata write')
        @attr(assertion='non-printing suffixes rejected by webserver')
        @attr(assertion='fails 400')
        """
        metadata = 'h\x04'
        e = self.set_get_metadata_unreadable(s3cfg_global_unique, metadata)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400 or 403)

    def test_object_set_get_metadata_empty_to_unreadable_infix(self, s3cfg_global_unique):
        """
        @attr(operation='metadata write')
        @attr(assertion='non-priting in-fixes rejected by webserver')
        @attr(assertion='fails 400')
        """
        metadata = 'h\x04w'
        e = self.set_get_metadata_unreadable(s3cfg_global_unique, metadata)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400 or 403)
