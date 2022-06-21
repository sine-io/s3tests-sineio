import pytest

from s3tests_pytest.functional.policy import Statement, Policy, make_json_policy
from s3tests_pytest.tests import TestBaseClass, assert_raises, ClientError, get_client, get_alt_client


class TestTaggingPolicyMixin(TestBaseClass):

    @pytest.mark.ess
    def test_get_tags_acl_public(self, s3cfg_global_unique):
        """
        测试-验证policy为GetObjectTagging时，获取tagging
        """
        key = 'testputtagsacl'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        resource = self.make_arn_resource("{}/{}".format(bucket_name, key))
        policy_document = make_json_policy("s3:GetObjectTagging", resource)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.create_simple_tag_set(10)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

    @pytest.mark.ess
    def test_put_tags_acl_public(self, s3cfg_global_unique):
        """
        测试-验证policy为PutObjectTagging时，进行设置对象tagging
        """
        key = 'testputtagsacl'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        resource = self.make_arn_resource("{}/{}".format(bucket_name, key))
        policy_document = make_json_policy("s3:PutObjectTagging", resource)
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.create_simple_tag_set(10)
        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['TagSet'], input_tag_set['TagSet'])

    @pytest.mark.ess
    def test_delete_tags_obj_public(self, s3cfg_global_unique):
        """
        测试-验证policy为DeleteObjectTagging时，进行删除对象tagging操作
        """
        key = 'testputtagsacl'
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_key_with_random_content(s3cfg_global_unique, key)

        resource = self.make_arn_resource("{}/{}".format(bucket_name, key))
        policy_document = make_json_policy("s3:DeleteObjectTagging",
                                           resource)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        input_tag_set = self.create_simple_tag_set(10)
        response = client.put_object_tagging(Bucket=bucket_name, Key=key, Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.delete_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 204)

        response = client.get_object_tagging(Bucket=bucket_name, Key=key)
        self.eq(len(response['TagSet']), 0)

    @pytest.mark.ess
    def test_bucket_policy_get_obj_existing_tag(self, s3cfg_global_unique):
        """
        测试-验证ExistingObjectTag conditional on get object
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['publictag', 'privatetag', 'invalidtag'])

        tag_conditional = {"StringEquals": {
            "s3:ExistingObjectTag/security": "public"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObject",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        tag_set = [{'Key': 'security', 'Value': 'public'}, {'Key': 'foo', 'Value': 'bar'}]
        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set2 = [{'Key': 'security', 'Value': 'private'}]
        input_tag_set = {'TagSet': tag_set2}

        response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set3 = [{'Key': 'security1', 'Value': 'public'}]
        input_tag_set = {'TagSet': tag_set3}

        response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object(Bucket=bucket_name, Key='publictag')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='privatetag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='invalidtag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    def test_bucket_policy_get_obj_tagging_existing_tag(self, s3cfg_global_unique):
        """
        测试-验证ExistingObjectTag conditional on get object tagging
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['publictag', 'privatetag', 'invalidtag'])

        tag_conditional = {"StringEquals": {
            "s3:ExistingObjectTag/security": "public"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObjectTagging",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        tag_set = [{'Key': 'security', 'Value': 'public'}, {'Key': 'foo', 'Value': 'bar'}]

        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set2 = [{'Key': 'security', 'Value': 'private'}]

        input_tag_set = {'TagSet': tag_set2}

        response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set3 = [{'Key': 'security1', 'Value': 'public'}]

        input_tag_set = {'TagSet': tag_set3}

        response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object_tagging(Bucket=bucket_name, Key='publictag')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        # A get object itself should fail since we allowed only GetObjectTagging
        e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='publictag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='privatetag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='invalidtag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    def test_bucket_policy_put_obj_tagging_existing_tag(self, s3cfg_global_unique):
        """
        测试-验证ExistingObjectTag conditional on put object tagging
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['publictag', 'privatetag', 'invalidtag'])

        tag_conditional = {"StringEquals": {
            "s3:ExistingObjectTag/security": "public"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:PutObjectTagging",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        tag_set = [{'Key': 'security', 'Value': 'public'}, {'Key': 'foo', 'Value': 'bar'}]

        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set2 = [{'Key': 'security', 'Value': 'private'}]

        input_tag_set = {'TagSet': tag_set2}

        response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        # PUT requests with object tagging are a bit wierd, if you forget to put
        # the tag which is supposed to be existing anymore well, well subsequent
        # put requests will fail

        test_tag_set1 = [{'Key': 'security', 'Value': 'public'}, {'Key': 'foo', 'Value': 'bar'}]

        input_tag_set = {'TagSet': test_tag_set1}

        response = alt_client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        e = assert_raises(ClientError, alt_client.put_object_tagging, Bucket=bucket_name, Key='privatetag',
                          Tagging=input_tag_set)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        test_tag_set2 = [{'Key': 'security', 'Value': 'private'}]

        input_tag_set = {'TagSet': test_tag_set2}

        response = alt_client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        # Now try putting the original tags again, this should fail
        input_tag_set = {'TagSet': test_tag_set1}

        e = assert_raises(ClientError, alt_client.put_object_tagging, Bucket=bucket_name, Key='publictag',
                          Tagging=input_tag_set)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    def test_bucket_policy_put_obj_copy_source(self, s3cfg_global_unique):
        """
        测试-验证copy-source conditional on put obj
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['public/foo', 'public/bar', 'private/foo'])

        src_resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObject",
                                           src_resource)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        bucket_name2 = self.get_new_bucket(client, s3cfg_global_unique)

        tag_conditional = {"StringLike": {
            "s3:x-amz-copy-source": bucket_name + "/public/*"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name2, "*"))
        policy_document = make_json_policy("s3:PutObject",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name2, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        copy_source = {'Bucket': bucket_name, 'Key': 'public/foo'}
        alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key='new_foo')

        # This is possible because we are still the owner, see the grants with
        # policy on how to do this right
        response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo')
        body = self.get_body(response)
        self.eq(body, 'public/foo')

        copy_source = {'Bucket': bucket_name, 'Key': 'public/bar'}
        alt_client.copy_object(Bucket=bucket_name2, CopySource=copy_source, Key='new_foo2')

        response = alt_client.get_object(Bucket=bucket_name2, Key='new_foo2')
        body = self.get_body(response)
        self.eq(body, 'public/bar')

        copy_source = {'Bucket': bucket_name, 'Key': 'private/foo'}
        self.check_access_denied(alt_client.copy_object, Bucket=bucket_name2, CopySource=copy_source, Key='new_foo2')

    @pytest.mark.ess
    def test_bucket_policy_put_obj_copy_source_meta(self, s3cfg_global_unique):
        """
        测试-验证copy-source conditional on put obj
        """
        client = get_client(s3cfg_global_unique)
        src_bucket_name = self.create_objects(s3cfg_global_unique, keys=['public/foo', 'public/bar'])

        src_resource = self.make_arn_resource("{}/{}".format(src_bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObject",
                                           src_resource)

        client.put_bucket_policy(Bucket=src_bucket_name, Policy=policy_document)

        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        tag_conditional = {"StringEquals": {
            "s3:x-amz-metadata-directive": "COPY"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:PutObject",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-metadata-directive": "COPY"}))
        alt_client.meta.events.register('before-call.s3.CopyObject', lf)

        copy_source = {'Bucket': src_bucket_name, 'Key': 'public/foo'}
        alt_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key='new_foo')

        # This is possible because we are still the owner, see the grants with
        # policy on how to do this right
        response = alt_client.get_object(Bucket=bucket_name, Key='new_foo')
        body = self.get_body(response)
        self.eq(body, 'public/foo')

        # remove the x-amz-metadata-directive header
        def remove_header(**kwargs):
            if "x-amz-metadata-directive" in kwargs['params']['headers']:
                del kwargs['params']['headers']["x-amz-metadata-directive"]

        alt_client.meta.events.register('before-call.s3.CopyObject', remove_header)

        copy_source = {'Bucket': src_bucket_name, 'Key': 'public/bar'}
        self.check_access_denied(alt_client.copy_object, Bucket=bucket_name, CopySource=copy_source, Key='new_foo2',
                                 Metadata={"foo": "bar"})

    @pytest.mark.ess
    def test_bucket_policy_put_obj_acl(self, s3cfg_global_unique):
        """
        测试-验证put obj with canned-acl not to be public
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        # An allow conditional will require atleast the presence of an x-amz-acl
        # attribute a Deny conditional would negate any requests that try to set a
        # public-read/write acl
        conditional = {"StringLike": {
            "s3:x-amz-acl": "public*"
        }}

        p = Policy()
        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        s1 = Statement("s3:PutObject", resource)
        s2 = Statement("s3:PutObject", resource, effect="Deny", condition=conditional)

        policy_document = p.add_statement(s1).add_statement(s2).to_json()
        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        key1 = 'private-key'

        # if we want to be really pedantic, we should check that this doesn't raise
        # and mark a failure, however if this does raise nosetests would mark this
        # as an ERROR anyway
        response = alt_client.put_object(Bucket=bucket_name, Key=key1, Body=key1)
        # response = alt_client.put_object_acl(Bucket=bucket_name, Key=key1, ACL='private')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        key2 = 'public-key'

        lf = (lambda **kwargs: kwargs['params']['headers'].update({"x-amz-acl": "public-read"}))
        alt_client.meta.events.register('before-call.s3.PutObject', lf)

        e = assert_raises(ClientError, alt_client.put_object, Bucket=bucket_name, Key=key2, Body=key2)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

    @pytest.mark.ess
    @pytest.mark.fails_on_ess  # TODO: remove this fails_on_rgw when I fix it
    @pytest.mark.xfail(reason="预期：上传对象的时候，可以在headers里设置x-amz-tagging参数", run=True, strict=True)
    def test_bucket_policy_put_obj_request_obj_tag(self, s3cfg_global_unique):
        """
        测试-验证put obj with RequestObjectTag
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        tag_conditional = {"StringEquals": {
            "s3:RequestObjectTag/security": "public"
        }}

        p = Policy()
        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))

        s1 = Statement("s3:PutObject", resource, effect="Allow", condition=tag_conditional)
        policy_document = p.add_statement(s1).to_json()

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)

        alt_client = get_alt_client(s3cfg_global_unique)
        key1_str = 'testobj'
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1_str, Body=key1_str)

        headers = {"x-amz-tagging": "security=public"}
        lf = (lambda **kwargs: kwargs['params']['headers'].update(headers))
        client.meta.events.register('before-call.s3.PutObject', lf)
        # TODO: why is this a 400 and not passing
        alt_client.put_object(Bucket=bucket_name, Key=key1_str, Body=key1_str)

    @pytest.mark.ess
    def test_bucket_policy_get_obj_acl_existing_tag(self, s3cfg_global_unique):
        """
        测试-验证ExistingObjectTag conditional on get object acl
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.create_objects(s3cfg_global_unique, keys=['publictag', 'privatetag', 'invalidtag'])

        tag_conditional = {"StringEquals": {
            "s3:ExistingObjectTag/security": "public"
        }}

        resource = self.make_arn_resource("{}/{}".format(bucket_name, "*"))
        policy_document = make_json_policy("s3:GetObjectAcl",
                                           resource,
                                           conditions=tag_conditional)

        client.put_bucket_policy(Bucket=bucket_name, Policy=policy_document)
        tag_set = [{'Key': 'security', 'Value': 'public'}, {'Key': 'foo', 'Value': 'bar'}]

        input_tag_set = {'TagSet': tag_set}

        response = client.put_object_tagging(Bucket=bucket_name, Key='publictag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set2 = [{'Key': 'security', 'Value': 'private'}]

        input_tag_set = {'TagSet': tag_set2}

        response = client.put_object_tagging(Bucket=bucket_name, Key='privatetag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        tag_set3 = [{'Key': 'security1', 'Value': 'public'}]

        input_tag_set = {'TagSet': tag_set3}

        response = client.put_object_tagging(Bucket=bucket_name, Key='invalidtag', Tagging=input_tag_set)
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object_acl(Bucket=bucket_name, Key='publictag')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

        # A get object itself should fail since we allowed only GetObjectTagging
        e = assert_raises(ClientError, alt_client.get_object, Bucket=bucket_name, Key='publictag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='privatetag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)

        e = assert_raises(ClientError, alt_client.get_object_tagging, Bucket=bucket_name, Key='invalidtag')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
