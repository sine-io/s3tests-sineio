from email.utils import formatdate

import pytest

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client, get_v2_client


def tag(*tags):
    def wrap(func):
        for _tag in tags:
            setattr(func, _tag, True)
        return func

    return wrap


class TestHeadersBase(TestBaseClass):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/RESTCommonRequestHeaders.html
    Common Request Headers:
        1. Authorization
            The information required for request authentication. For more information,
            go to The Authentication Header in the Amazon Simple Storage Service Developer Guide.
            For anonymous requests this header is not required.
        2. Content-Length
            Length of the message (without the headers) according to RFC 2616.
            This header is required for PUTs and operations that load XML, such as logging and ACLs.
        3. Content-Type
            The content type of the resource in case the request content in the body. Example: text/plain
        4. Content-MD5
            The base64 encoded 128-bit MD5 digest of the message (without the headers) according to RFC 1864.
            This header can be used as a message integrity check to verify that the data is the same data
                that was originally sent.
            Although it is optional, we recommend using the Content-MD5 mechanism as an end-to-end integrity check.
            For more information about REST request authentication,
                go to REST Authentication in the Amazon Simple Storage Service Developer Guide.
        5. Date
            The date that can be used to create the signature contained in the Authorization header.
            If the Date header is to be used for signing it must be specified in the ISO 8601 basic format.
            In this case, the x-amz-date header is not needed.
            Note that when x-amz-date is present, it always overrides the value of the Date header.

            If the Date header is not used for signing,
            it can be one of the full date formats specified by RFC 2616,  section 3.3.
            For example, the date/time Wed, 01 Mar 2006 12:00:00 GMT is a valid date/time header for use with Amazon S3.

            If you are using the Date header for signing,
                then it must be in the ISO 8601 basic YYYYMMDD'T'HHMMSS'Z' format.

            If Date is specified but is not in ISO 8601 basic format, then you must also include the x-amz-date header.
            If Date is specified in ISO 8601 basic format,
                then this is sufficient for signing requests and you do not need the x-amz-date header.
            For more information, see Handling Dates in Signature Version 4 in the Amazon Web Services Glossary.
                https://docs.aws.amazon.com/zh_cn/general/latest/gr/sigv4-date-handling.html
                处理签名版本 4 中的日期
                您在凭证范围中使用的日期必须与您的请求的日期匹配。您可以用多种方法将日期包括在请求中。
                    您可以使用 date 标头或 x-amz-date 标头，或者将 x-amz-date 作为查询参数包含在内。
                    有关示例请求，请参阅签名版本 4 完整签名过程的示例（Python）。
                    https://docs.aws.amazon.com/zh_cn/general/latest/gr/sigv4-signed-request-examples.html

                时间戳必须采用 UTC 表示，并具有以下 ISO 8601 格式：YYYYMMDD'T'HHMMSS'Z'。
                    例如，20150830T123600Z 是有效时间戳。请勿在时间戳中包含毫秒。

                AWS 先检查时间戳的 x-amz-date 标头或参数。如果 AWS 无法找到 x-amz-date 的值，则将寻找 date 标头。
                    AWS 检查八位数字字符串形式的凭证范围，表示请求的年 (YYYY)、月 (MM) 和日 (DD)。
                    例如，如果 x-amz-date 标头值为 20111015T080000Z，并且凭证范围的日期部分为 20111015，则 AWS 允许身份验证过程继续执行。

                如果日期不匹配，则 AWS 拒绝请求，即使时间戳距离凭证范围中的日期仅有数秒之差也是如此。
                    例如，AWS 将拒绝其 x-amz-date 标头值为 20151014T235959Z 且凭证范围包括日期 20151015 的请求。
        6. Expect
            When your application uses 100-continue, it does not send the request body until it receives an acknowledgment.
            If the message is rejected based on the headers, the body of the message is not sent.
            This header can be used only if you are sending a body.

            Valid Values: 100-continue
        7. Host
            For path-style requests, the value is s3.amazonaws.com.
            For virtual-style requests, the value is BucketName.s3.amazonaws.com.
            For more information, go to Virtual Hosting in the Amazon Simple Storage Service User Guide.

            This header is required for HTTP 1.1 (most toolkits add this header automatically);
                optional for HTTP/1.0 requests.
        8. x-amz-content-sha256
            When using signature version 4 to authenticate request, this header provides a hash of the request payload.
            For more information see Signature Calculations for the Authorization Header:
                Transferring Payload in a Single Chunk (AWS Signature Version 4).
            When uploading object in chunks, you set the value to STREAMING-AWS4-HMAC-SHA256-PAYLOAD to indicate
                that the signature covers only headers and that there is no payload.
            For more information, see Signature Calculations for the Authorization Header:
                Transferring Payload in Multiple Chunks (Chunked Upload) (AWS Signature Version 4).
        9. x-amz-date
            The date used to create the signature in the Authorization header.
            The format must be ISO 8601 basic in the YYYYMMDD'T'HHMMSS'Z' format.
            For example, the date/time 20170210T120000Z is a valid x-amz-date for use with Amazon S3.

            x-amz-date is optional for all requests; it can be used to override the date used for signing requests.
            If the Date header is specified in the ISO 8601 basic format, then x-amz-date is not needed.
            When x-amz-date is present, it always overrides the value of the Date header.
            For more information, see Handling Dates in Signature Version 4 in the Amazon Web Services Glossary.
        10. x-amz-security-token
            This header can be used in the following scenarios:
                Provide security tokens for Amazon DevPay operations - Each request that uses Amazon DevPay requires
                    two x-amz-security-token headers: one for the product token and one for the user token.
                    When Amazon S3 receives an authenticated request,
                    it compares the computed signature with the provided signature.
                    Improperly formatted multi-value headers used to calculate a signature can cause authentication issues.
                Provide security token when using temporary security credentials -
                    When making requests using temporary security credentials you obtained from IAM
                    you must provide a security token using this header.
                    To learn more about temporary security credentials, go to Making Requests.

            This header is required for requests that use Amazon DevPay and requests
                that are signed using temporary security credentials.
    """

    def add_header_create_object(self, config, headers, client=None):
        """
        Create a new bucket, add an object w/header customizations
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket(client, config)

        key_name = 'foo'

        # pass in custom headers before PutObject call
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.PutObject', add_headers)
        client.put_object(Bucket=bucket_name, Key=key_name)

        return bucket_name, key_name

    def add_header_create_bad_object(self, config, headers, client=None):
        """
        Create a new bucket, add an object with a header. This should cause a failure
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket(client, config)

        key_name = 'foo'

        # pass in custom headers before PutObject call
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.PutObject', add_headers)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

        return e

    def remove_header_create_object(self, config, remove, client=None):
        """
        Create a new bucket, add an object without a header
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket(client, config)

        key_name = 'foo'

        # remove custom headers before PutObject call
        def remove_header(**kwargs):
            if remove in kwargs['params']['headers']:
                del kwargs['params']['headers'][remove]

        client.meta.events.register('before-call.s3.PutObject', remove_header)
        client.put_object(Bucket=bucket_name, Key=key_name)

        return bucket_name, key_name

    def remove_header_create_bad_object(self, config, remove, client=None):
        """
        Create a new bucket, add an object without a header. This should cause a failure
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket(client, config)

        key_name = 'foo'

        # remove custom headers before PutObject call
        def remove_header(**kwargs):
            if remove in kwargs['params']['headers']:
                del kwargs['params']['headers'][remove]

        client.meta.events.register('before-call.s3.PutObject', remove_header)
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body='bar')

        return e

    def add_header_create_bucket(self, config, headers, client=None):
        """
        Create a new bucket, w/header customizations
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket_name(config)

        # pass in custom headers before PutObject call
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.CreateBucket', add_headers)
        client.create_bucket(Bucket=bucket_name)

        return bucket_name

    def add_header_create_bad_bucket(self, config, headers=None, client=None):
        """
        Create a new bucket, w/header customizations that should cause a failure
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket_name(config)

        # pass in custom headers before PutObject call
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.CreateBucket', add_headers)
        e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

        return e

    def remove_header_create_bucket(self, config, remove, client=None):
        """
        Create a new bucket, without a header
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket_name(config)

        # remove custom headers before PutObject call
        def remove_header(**kwargs):
            if remove in kwargs['params']['headers']:
                del kwargs['params']['headers'][remove]

        client.meta.events.register('before-call.s3.CreateBucket', remove_header)
        client.create_bucket(Bucket=bucket_name)

        return bucket_name

    def remove_header_create_bad_bucket(self, config, remove, client=None):
        """
        Create a new bucket, without a header. This should cause a failure
        """
        if client is None:
            client = get_client(config)

        bucket_name = self.get_new_bucket_name(config)

        # remove custom headers before PutObject call
        def remove_header(**kwargs):
            if remove in kwargs['params']['headers']:
                del kwargs['params']['headers'][remove]

        client.meta.events.register('before-call.s3.CreateBucket', remove_header)
        e = assert_raises(ClientError, client.create_bucket, Bucket=bucket_name)

        return e


class TestObjectHeaders(TestHeadersBase):
    """
    response = client.put_object(
        ACL='private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control',
        Body=b'bytes'|file,
        Bucket='string',
        CacheControl='string',
        ContentDisposition='string',
        ContentEncoding='string',
        ContentLanguage='string',
        ContentLength=123,
        ContentMD5='string',
        ContentType='string',
        ChecksumAlgorithm='CRC32'|'CRC32C'|'SHA1'|'SHA256',
        ChecksumCRC32='string',
        ChecksumCRC32C='string',
        ChecksumSHA1='string',
        ChecksumSHA256='string',
        Expires=datetime(2015, 1, 1),
        GrantFullControl='string',
        GrantRead='string',
        GrantReadACP='string',
        GrantWriteACP='string',
        Key='string',
        Metadata={
            'string': 'string'
        },
        ServerSideEncryption='AES256'|'aws:kms',
        StorageClass='STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'|'DEEP_ARCHIVE'|'OUTPOSTS'|'GLACIER_IR',
        WebsiteRedirectLocation='string',
        SSECustomerAlgorithm='string',
        SSECustomerKey='string',
        SSEKMSKeyId='string',
        SSEKMSEncryptionContext='string',
        BucketKeyEnabled=True|False,
        RequestPayer='requester',
        Tagging='string',
        ObjectLockMode='GOVERNANCE'|'COMPLIANCE',
        ObjectLockRetainUntilDate=datetime(2015, 1, 1),
        ObjectLockLegalHoldStatus='ON'|'OFF',
        ExpectedBucketOwner='string'
    )
    """

    @tag('auth_common')
    def test_object_create_bad_md5_invalid_short(self, s3cfg_global_unique):
        """
        (operation='create w/invalid MD5')
        (assertion='fails 400')
        """
        e = self.add_header_create_bad_object(s3cfg_global_unique, {'Content-MD5': 'YWJyYWNhZGFicmE='})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidDigest')

    @tag('auth_common')
    def test_object_create_bad_md5_bad(self, s3cfg_global_unique):
        """
        (operation='create w/mismatched MD5')
        (assertion='fails 400')
        """
        e = self.add_header_create_bad_object(s3cfg_global_unique, {'Content-MD5': 'rL0Y20xC+Fzt72VPzMSk2A=='})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'BadDigest')

    @tag('auth_common')
    def test_object_create_bad_md5_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty MD5')
        (assertion='fails 400')
        """
        e = self.add_header_create_bad_object(s3cfg_global_unique, {'Content-MD5': ''})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidDigest')

    @tag('auth_common')
    def test_object_create_bad_md5_none(self, s3cfg_global_unique):
        """
        (operation='create w/no MD5 header')
        (assertion='succeeds')
        """
        bucket_name, key_name = self.remove_header_create_object(s3cfg_global_unique, 'Content-MD5')
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    def test_object_create_bad_expect_mismatch(self, s3cfg_global_unique):
        """
        (operation='create w/Expect 200')
        (assertion='garbage, but S3 succeeds!')
        """
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, {'Expect': 200})
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    def test_object_create_bad_expect_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty expect')
        (assertion='succeeds ... should it?')
        """
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, {'Expect': ''})
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    def test_object_create_bad_expect_none(self, s3cfg_global_unique):
        """
        (operation='create w/no expect')
        (assertion='succeeds')
        """
        bucket_name, key_name = self.remove_header_create_object(s3cfg_global_unique, 'Expect')
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    @pytest.mark.fails_on_ess
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    def test_object_create_bad_content_length_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty content length')
        (assertion='fails 400')
        """
        e = self.add_header_create_bad_object(s3cfg_global_unique, {'Content-Length': ''})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    def test_object_create_bad_content_length_negative(self, s3cfg_global_unique):
        """
        (operation='create w/negative content length')
        (assertion='fails 400')
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key_name = 'foo'
        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, ContentLength=-1)
        status = self.get_status(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_content_length_none(self, s3cfg_global_unique):
        """
        (operation='create w/no content length')
        (assertion='fails 411')
        """
        e = self.remove_header_create_bad_object(s3cfg_global_unique, 'Content-Length')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 411)
        self.eq(error_code, 'MissingContentLength')

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_content_length_mismatch_above(self, s3cfg_global_unique):
        """
        (operation='create w/content length too long')
        (assertion='fails 400')
        """
        content = 'bar'
        length = len(content) + 1

        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key_name = 'foo'
        headers = {'Content-Length': str(length)}
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-sign.s3.PutObject', add_headers)

        e = assert_raises(ClientError, client.put_object, Bucket=bucket_name, Key=key_name, Body=content)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    def test_object_create_bad_content_type_invalid(self, s3cfg_global_unique):
        """
        (operation='create w/content type text/plain')
        (assertion='succeeds')
        """
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, {'Content-Type': 'text/plain'})
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    def test_object_create_bad_content_type_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty content type')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)
        key_name = 'foo'
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar', ContentType='')

    @tag('auth_common')
    def test_object_create_bad_content_type_none(self, s3cfg_global_unique):
        """
        (operation='create w/no content type')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        key_name = 'foo'
        # as long as ContentType isn't specified in put_object it isn't going into the request
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the authorization header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_authorization_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty authorization')
        (assertion='fails 403')
        """
        e = self.add_header_create_bad_object(s3cfg_global_unique, {'Authorization': ''})
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header
    #  during signing and not 'X-Amz-Date' before
    @pytest.mark.fails_on_ess
    def test_object_create_date_and_amz_date(self, s3cfg_global_unique):
        """
        (operation='create w/date and x-amz-date')
        (assertion='succeeds')
        """
        date = formatdate(usegmt=True)
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, {'Date': date, 'X-Amz-Date': date})

        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to pass both the 'Date' and 'X-Amz-Date' header during signing and not 'X-Amz-Date' before
    @pytest.mark.fails_on_ess
    def test_object_create_amz_date_and_no_date(self, s3cfg_global_unique):
        """
        (operation='create w/x-amz-date and no date')
        (assertion='succeeds')
        """
        date = formatdate(usegmt=True)
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, {'Date': '', 'X-Amz-Date': date})
        client = get_client(s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    # the teardown is really messed up here. check it out
    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the authorization header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_authorization_none(self, s3cfg_global_unique):
        """
        (operation='create w/no authorization')
        (assertion='fails 403')
        """
        e = self.remove_header_create_bad_object(s3cfg_global_unique, 'Authorization')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)


class TestBucketHeaders(TestHeadersBase):

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    @pytest.mark.fails_on_ess
    def test_bucket_create_content_length_none(self, s3cfg_global_unique):
        """
        (operation='create w/no content length')
        (assertion='succeeds')
        """
        remove = 'Content-Length'
        self.remove_header_create_bucket(s3cfg_global_unique, remove)

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    @pytest.mark.fails_on_ess
    def test_object_acl_create_content_length_none(self, s3cfg_global_unique):
        """
        (operation='set w/no content length')
        (assertion='succeeds')
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        remove = 'Content-Length'

        def remove_header(**kwargs):
            if remove in kwargs['params']['headers']:
                del kwargs['params']['headers'][remove]

        client.meta.events.register('before-call.s3.PutObjectAcl', remove_header)
        client.put_object_acl(Bucket=bucket_name, Key='foo', ACL='public-read')

    @tag('auth_common')
    def test_bucket_put_bad_canned_acl(self, s3cfg_global_unique):
        """
        (operation='set w/invalid permission')
        (assertion='fails 400')
        """
        client = get_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        headers = {'x-amz-acl': 'public-ready'}
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.PutBucketAcl', add_headers)

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, ACL='public-read')
        status = self.get_status(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    def test_bucket_create_bad_expect_mismatch(self, s3cfg_global_unique):
        """
        (operation='create w/expect 200')
        (assertion='garbage, but S3 succeeds!')
        """
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client = get_client(s3cfg_global_unique)

        headers = {'Expect': 200}
        add_headers = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.CreateBucket', add_headers)
        client.create_bucket(Bucket=bucket_name)

    @tag('auth_common')
    def test_bucket_create_bad_expect_empty(self, s3cfg_global_unique):
        """
        (operation='create w/expect empty')
        (assertion='garbage, but S3 succeeds!')
        """
        headers = {'Expect': ''}
        self.add_header_create_bucket(s3cfg_global_unique, headers)

    @tag('auth_common')
    # TODO: The request isn't even making it to the RGW past the frontend
    # This test had 'fails_on_ess' before the move to boto3
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_content_length_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty content length')
        (assertion='fails 400')
        """
        headers = {'Content-Length': ''}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    def test_bucket_create_bad_content_length_negative(self, s3cfg_global_unique):
        """
        (operation='create w/negative content length')
        (assertion='fails 400')
        """
        headers = {'Content-Length': '-1'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers)
        status = self.get_status(e.response)
        self.eq(status, 400)

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the content-length header
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_content_length_none(self, s3cfg_global_unique):
        """
        (operation='create w/no content length')
        (assertion='succeeds')
        """
        remove = 'Content-Length'
        self.remove_header_create_bucket(s3cfg_global_unique, remove)

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to manipulate the authorization header
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_authorization_empty(self, s3cfg_global_unique):
        """
        (operation='create w/empty authorization')
        (assertion='fails 403')
        """
        headers = {'Authorization': ''}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_common')
    # TODO: remove 'fails_on_ess' and once we have learned how to manipulate the authorization header
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_authorization_none(self, s3cfg_global_unique):
        """
        (operation='create w/no authorization')
        (assertion='fails 403')
        """
        e = self.remove_header_create_bad_bucket(s3cfg_global_unique, 'Authorization')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_object_create_bad_md5_invalid_garbage_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/invalid MD5')
        (assertion='fails 400')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'Content-MD5': 'AWS HAHAHA'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidDigest')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to manipulate the Content-Length header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_content_length_mismatch_below_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/content length too short')
        (assertion='fails 400')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        content = 'bar'
        length = len(content) - 1
        headers = {'Content-Length': str(length)}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'BadDigest')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to manipulate the authorization header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_authorization_incorrect_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/incorrect authorization')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'Authorization': 'AWS AKIAIGR7ZNNBHC5BKSUB:FWeDfwojDSdS2Ztmpfeubhd9isU='}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'InvalidDigest')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to manipulate the authorization header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_authorization_invalid_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/invalid authorization')
        (assertion='fails 400')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'Authorization': 'AWS HAHAHA'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidArgument')

    @tag('auth_aws2')
    def test_object_create_bad_ua_empty_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/empty user agent')
        (assertion='succeeds')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'User-Agent': ''}
        bucket_name, key_name = self.add_header_create_object(s3cfg_global_unique, headers, v2_client)
        v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_aws2')
    def test_object_create_bad_ua_none_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/no user agent')
        (assertion='succeeds')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        remove = 'User-Agent'
        bucket_name, key_name = self.remove_header_create_object(s3cfg_global_unique, remove, v2_client)
        v2_client.put_object(Bucket=bucket_name, Key=key_name, Body='bar')

    @tag('auth_aws2')
    def test_object_create_bad_date_invalid_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/invalid date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Bad Date'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_object_create_bad_date_empty_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/empty date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': ''}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the date header
    @pytest.mark.fails_on_ess
    def test_object_create_bad_date_none_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/no date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        remove = 'x-amz-date'
        e = self.remove_header_create_bad_object(s3cfg_global_unique, remove, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_object_create_bad_date_before_today_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date in past')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'RequestTimeTooSkewed')

    @tag('auth_aws2')
    def test_object_create_bad_date_before_epoch_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date before epoch')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_object_create_bad_date_after_end_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date after 9999')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
        e = self.add_header_create_bad_object(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'RequestTimeTooSkewed')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the date header
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_authorization_invalid_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/invalid authorization')
        (assertion='fails 400')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'Authorization': 'AWS HAHAHA'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidArgument')

    @tag('auth_aws2')
    def test_bucket_create_bad_ua_empty_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/empty user agent')
        (assertion='succeeds')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'User-Agent': ''}
        self.add_header_create_bucket(s3cfg_global_unique, headers, v2_client)

    @tag('auth_aws2')
    def test_bucket_create_bad_ua_none_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/no user agent')
        (assertion='succeeds')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        remove = 'User-Agent'
        self.remove_header_create_bucket(s3cfg_global_unique, remove, v2_client)

    @tag('auth_aws2')
    def test_bucket_create_bad_date_invalid_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/invalid date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Bad Date'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_bucket_create_bad_date_empty_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/empty date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': ''}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    # TODO: remove 'fails_on_ess' and once we have learned how to remove the date header
    @pytest.mark.fails_on_ess
    def test_bucket_create_bad_date_none_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/no date')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        remove = 'x-amz-date'
        e = self.remove_header_create_bad_bucket(s3cfg_global_unique, remove, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @tag('auth_aws2')
    def test_bucket_create_bad_date_before_today_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date in past')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 2010 21:53:04 GMT'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'RequestTimeTooSkewed')

    @tag('auth_aws2')
    def test_bucket_create_bad_date_after_today_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date in future')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 2030 21:53:04 GMT'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'RequestTimeTooSkewed')

    @tag('auth_aws2')
    def test_bucket_create_bad_date_before_epoch_aws2(self, s3cfg_global_unique):
        """
        (operation='create w/date before epoch')
        (assertion='fails 403')
        """
        v2_client = get_v2_client(s3cfg_global_unique)
        headers = {'x-amz-date': 'Tue, 07 Jul 1950 21:53:04 GMT'}
        e = self.add_header_create_bad_bucket(s3cfg_global_unique, headers, v2_client)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')
