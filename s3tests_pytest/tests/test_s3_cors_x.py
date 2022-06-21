
import time

import pytest
import httpx

from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client


class TestCorsBase(TestBaseClass):
    """
    https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/userguide/cors.html
    跨源资源共享

    AllowedMethod 元素
        在 CORS 配置中，您可以为 AllowedMethod 元素指定以下值。
        GET
        PUT
        POST
        DELETE
        HEAD
    AllowedOrigin 元素
        在 AllowedOrigin 元素中，可指定您希望允许从中发送跨源请求的源，例如 http://www.example.com。
        源字符串只能包含至少一个 * 通配符，例如 http://*.example.com。
        您可以选择将 * 指定为源，以允许所有源发送跨源请求。您还可以指定 https 只允许安全的源。
    AllowedHeader 元素
        AllowedHeader 元素通过 Access-Control-Request-Headers 标头指定预检请求中允许哪些标头。
        Access-Control-Request-Headers 标头中的每个标头名称必须匹配规则中的相应条目。
        Amazon S3 将仅发送请求的响应中允许的标头。
        有关适用于发送至 Amazon S3 的请求中的标头示例列表，请参阅 Amazon Simple Storage Service API 参考指南中的常见请求标头。
        规则中的每个 AllowedHeader 字符串可以包含至少一个 * 通配符字符。
        例如，<AllowedHeader>x-amz-*</AllowedHeader> 将允许所有特定于 Amazon 的标头。
    ExposeHeader 元素
        每个 ExposeHeader 元素标识您希望客户能够从其应用程序 (例如，从 JavaScript XMLHttpRequest 对象) 进行访问的响应标头。
        有关常见的 Amazon S3 响应标头的列表，请参阅 Amazon Simple Storage Service API 参考指南中的常见响应标头。
    MaxAgeSeconds 元素
        MaxAgeSeconds 元素指定在预检请求被资源、HTTP 方法和源识别之后，浏览器将为预检请求缓存响应的时间 (以秒为单位)。
    """

    def cors_request_and_check(
            self, config, func, url, headers, expect_status, expect_allow_origin, expect_allow_methods):
        r = func(url, headers=headers, verify=config.default_ssl_verify)
        self.eq(r.status_code, expect_status)
        self.eq(r.headers.get('access-control-allow-origin', None), expect_allow_origin)
        self.eq(r.headers.get('access-control-allow-methods', None), expect_allow_methods)


class TestCors(TestCorsBase):

    def test_set_cors(self, s3cfg_global_unique):
        """
        测试-给存储桶设置CORS
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        allowed_methods = ['GET', 'PUT']
        allowed_origins = ['*.get', '*.put']

        cors_config = {
            'CORSRules': [
                {'AllowedMethods': allowed_methods,
                 'AllowedOrigins': allowed_origins,
                 },
            ]
        }

        e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
        status = self.get_status(e.response)
        self.eq(status, 404)

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)
        response = client.get_bucket_cors(Bucket=bucket_name)
        self.eq(response['CORSRules'][0]['AllowedMethods'], allowed_methods)
        self.eq(response['CORSRules'][0]['AllowedOrigins'], allowed_origins)

        client.delete_bucket_cors(Bucket=bucket_name)
        e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
        status = self.get_status(e.response)
        self.eq(status, 404)

    def test_cors_origin_response(self, s3cfg_global_unique):
        """
        测试-设定AllowedOrigin，并进行验证
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_acl(s3cfg_global_unique, bucket_acl='public-read')

        cors_config = {
            'CORSRules': [
                {'AllowedMethods': ['GET'],
                 'AllowedOrigins': ['*suffix'],
                 },
                {'AllowedMethods': ['GET'],
                 'AllowedOrigins': ['start*end'],
                 },
                {'AllowedMethods': ['GET'],
                 'AllowedOrigins': ['prefix*'],
                 },
                {'AllowedMethods': ['PUT'],
                 'AllowedOrigins': ['*.put'],
                 }
            ]
        }

        e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
        status = self.get_status(e.response)
        self.eq(status, 404)

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        time.sleep(3)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)

        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, None, 200, None, None)
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'foo.suffix'}, 200, 'foo.suffix', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'foo.bar'}, 200, None, None)
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'foo.suffix.get'}, 200, None, None)
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'startend'}, 200, 'startend', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'start1end'}, 200, 'start1end', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'start12end'}, 200, 'start12end', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': '0start12end'}, 200, None, None)
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'prefix'}, 200, 'prefix', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'prefix.suffix'}, 200, 'prefix.suffix', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, url, {'Origin': 'bla.prefix'}, 200, None, None)

        obj_url = '{u}/{o}'.format(u=url, o='bar')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.put, obj_url,
                                    {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                     'content-length': '0'}, 403, 'foo.suffix', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.put, obj_url,
                                    {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'PUT',
                                     'content-length': '0'}, 403, None, None)

        self.cors_request_and_check(s3cfg_global_unique, httpx.put, obj_url,
                                    {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'DELETE',
                                     'content-length': '0'}, 403, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.put, obj_url,
                                    {'Origin': 'foo.suffix', 'content-length': '0'}, 403, None, None)

        self.cors_request_and_check(s3cfg_global_unique, httpx.put, obj_url,
                                    {'Origin': 'foo.put', 'content-length': '0'}, 403, 'foo.put', 'PUT')

        self.cors_request_and_check(
            s3cfg_global_unique, httpx.get, obj_url, {'Origin': 'foo.suffix'}, 404, 'foo.suffix', 'GET')
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.options, url, None, 400, None, None)
        self.cors_request_and_check(
            s3cfg_global_unique, httpx.options, url, {'Origin': 'foo.suffix'}, 400, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url, {'Origin': 'bla'}, 400, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, obj_url,
                                    {'Origin': 'foo.suffix', 'Access-Control-Request-Method': 'GET',
                                     'content-length': '0'}, 200, 'foo.suffix', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'foo.bar', 'Access-Control-Request-Method': 'GET'},
                                    403, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'foo.suffix.get', 'Access-Control-Request-Method': 'GET'}, 403, None,
                                    None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'startend', 'Access-Control-Request-Method': 'GET'},
                                    200, 'startend', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'start1end', 'Access-Control-Request-Method': 'GET'},
                                    200, 'start1end', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'start12end', 'Access-Control-Request-Method': 'GET'},
                                    200, 'start12end', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': '0start12end', 'Access-Control-Request-Method': 'GET'}, 403, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'prefix', 'Access-Control-Request-Method': 'GET'},
                                    200, 'prefix', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'prefix.suffix', 'Access-Control-Request-Method': 'GET'}, 200,
                                    'prefix.suffix', 'GET')
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'bla.prefix', 'Access-Control-Request-Method': 'GET'},
                                    403, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'foo.put', 'Access-Control-Request-Method': 'GET'},
                                    403, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.options, url,
                                    {'Origin': 'foo.put', 'Access-Control-Request-Method': 'PUT'},
                                    200, 'foo.put', 'PUT')

    def test_cors_origin_wildcard(self, s3cfg_global_unique):
        """
        测试-验证AllowedOrigins设置为通配符时，结果是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_acl(s3cfg_global_unique, bucket_acl='public-read')

        cors_config = {
            'CORSRules': [
                {'AllowedMethods': ['GET'],
                 'AllowedOrigins': ['*'],
                 },
            ]
        }

        e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
        status = self.get_status(e.response)
        self.eq(status, 404)

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        time.sleep(3)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)

        self.cors_request_and_check(s3cfg_global_unique, httpx.get, url, None, 200, None, None)
        self.cors_request_and_check(s3cfg_global_unique, httpx.get, url, {'Origin': 'example.origin'}, 200, '*',
                                    'GET')

    def test_cors_header_option(self, s3cfg_global_unique):
        """
        测试-验证设置ExposeHeaders参数，响应是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_acl(s3cfg_global_unique, bucket_acl='public-read')

        cors_config = {
            'CORSRules': [
                {'AllowedMethods': ['GET'],
                 'AllowedOrigins': ['*'],
                 'ExposeHeaders': ['x-amz-meta-header1'],
                 },
            ]
        }

        e = assert_raises(ClientError, client.get_bucket_cors, Bucket=bucket_name)
        status = self.get_status(e.response)
        self.eq(status, 404)

        client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)

        time.sleep(3)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        obj_url = '{u}/{o}'.format(u=url, o='bar')

        self.cors_request_and_check(
            s3cfg_global_unique, httpx.options, obj_url,
            {'Origin': 'example.origin', 'Access-Control-Request-Headers': 'x-amz-meta-header2',
             'Access-Control-Request-Method': 'GET'}, 403, None, None)
