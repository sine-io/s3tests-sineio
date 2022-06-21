
import threading

import pytest

from s3tests_pytest.tests import (
    TestBaseClass, assert_raises, ClientError,
    get_client, get_alt_client, get_unauthenticated_client
)


class TestAclBase(TestBaseClass):
    """
    标准 ACL:
        存储桶和对象:
            private: 所有者将获得 FULL_CONTROL。其他人没有访问权限 (默认)。
            public-read: 所有者将获得 FULL_CONTROL。AllUsers 组 (参阅 谁是被授权者？) 将获得 READ 访问权限。
            public-read-write: 所有者将获得 FULL_CONTROL。AllUsers 组将获得 READ 和 WRITE 访问权限。
                               通常不建议在存储桶上授予该权限。
            aws-exec-read: 所有者将获得 FULL_CONTROL。
                           Amazon EC2 从 Amazon S3 获取对 READ Amazon Machine Image (AMI) 服务包的 GET 访问权限。
            authenticated-read: 所有者将获得 FULL_CONTROL。AuthenticatedUsers 组将获得 READ 访问权限。
        对象:
            bucket-owner-read: 对象所有者将获得 FULL_CONTROL。存储桶拥有者将获得 READ 访问权限。
                               如果您在创建存储段时指定此标准的 ACL，Amazon S3 将忽略它。
            bucket-owner-full-control: 对象所有者和存储桶拥有者均可获得对对象的 FULL_CONTROL。
                                       如果您在创建存储段时指定此标准的 ACL，Amazon S3 将忽略它。
        存储桶:
            log-delivery-write: LogDelivery 组将获得针对存储桶的 WRITE 和 READ_ACP 许可。
                                有关日志的更多信息，请参阅（使用服务器访问日志记录来记录请求）。
        注意:
            您可以在请求中仅指定这些标准 ACL 中的一个。

    """

    @staticmethod
    def do_set_bucket_canned_acl_concurrent(client, target_func, bucket_name, canned_acl, num, results):
        t = []
        for i in range(num):
            thr = threading.Thread(target=target_func, args=(client, bucket_name, canned_acl, i, results))
            thr.start()
            t.append(thr)
        return t

    @staticmethod
    def do_set_bucket_canned_acl(client, bucket_name, canned_acl, i, results):
        try:
            client.put_bucket_acl(ACL=canned_acl, Bucket=bucket_name)
            results[i] = True
        except ClientError:  # noqa, need to verify whether ClientError or not.
            results[i] = False

    @staticmethod
    def add_obj_user_grant(config, bucket_name, key, grant):
        """
        Adds a grant to the existing grants meant to be passed into
        the AccessControlPolicy argument of put_object_acls for an object
        owned by the main user, not the alt user
        A grant is a dictionary in the form of:
        {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
        """
        client = get_client(config)
        main_user_id = config.main_user_id
        main_display_name = config.main_display_name

        response = client.get_object_acl(Bucket=bucket_name, Key=key)

        grants = response['Grants']
        grants.append(grant)

        grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

        return grant

    @staticmethod
    def add_bucket_user_grant(config, bucket_name, grant):
        """
        Adds a grant to the existing grants meant to be passed into
        the AccessControlPolicy argument of put_object_acls for an object
        owned by the main user, not the alt user
        A grant is a dictionary in the form of:
        {u'Grantee': {u'Type': 'type', u'DisplayName': 'name', u'ID': 'id'}, u'Permission': 'PERM'}
        """
        client = get_client(config)
        main_user_id = config.main_user_id
        main_display_name = config.main_display_name

        response = client.get_bucket_acl(Bucket=bucket_name)

        grants = response['Grants']
        grants.append(grant)

        grant = {'Grants': grants, 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

        return grant

    def bucket_acl_grant_userid(self, config, permission):
        """
        create a new bucket, grant a specific user the specified
        permission, read back the acl and verify correct setting
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)

        main_user_id = config.main_user_id
        main_display_name = config.main_display_name

        alt_user_id = config.alt_user_id
        alt_display_name = config.alt_display_name

        grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser'}, 'Permission': permission}
        grant = self.add_bucket_user_grant(config, bucket_name, grant)

        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

        response = client.get_bucket_acl(Bucket=bucket_name)

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission=permission,
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=main_user_id,
                    DisplayName=main_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

        return bucket_name

    @staticmethod
    def check_bucket_acl_grant_can_read(config, bucket_name):
        """
        verify ability to read the specified bucket
        """
        alt_client = get_alt_client(config)
        alt_client.head_bucket(Bucket=bucket_name)

    @staticmethod
    def check_bucket_acl_grant_can_read_acp(config, bucket_name):
        """
        verify ability to read acls on specified bucket
        """
        alt_client = get_alt_client(config)
        alt_client.get_bucket_acl(Bucket=bucket_name)

    @staticmethod
    def check_bucket_acl_grant_can_write(config, bucket_name):
        """
        verify ability to write the specified bucket
        """
        alt_client = get_alt_client(config)
        alt_client.put_object(Bucket=bucket_name, Key='foo-write', Body='bar')

    @staticmethod
    def check_bucket_acl_grant_can_write_acp(config, bucket_name):
        """
        verify ability to set acls on the specified bucket
        """
        alt_client = get_alt_client(config)
        alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

    def check_bucket_acl_grant_cant_read(self, config, bucket_name):
        """
        verify inability to read the specified bucket
        """
        alt_client = get_alt_client(config)
        self.check_access_denied(alt_client.head_bucket, Bucket=bucket_name)

    def check_bucket_acl_grant_cant_read_acp(self, config, bucket_name):
        """
        verify inability to read acls on specified bucket
        """
        alt_client = get_alt_client(config)
        self.check_access_denied(alt_client.get_bucket_acl, Bucket=bucket_name)

    def check_bucket_acl_grant_cant_write(self, config, bucket_name):
        """
        verify inability to write the specified bucket
        """
        alt_client = get_alt_client(config)
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key='foo-write', Body='bar')

    def check_bucket_acl_grant_cant_write_acp(self, config, bucket_name):
        """
        verify inability to set acls on the specified bucket
        """
        alt_client = get_alt_client(config)
        self.check_access_denied(alt_client.put_bucket_acl, Bucket=bucket_name, ACL='public-read')

    @staticmethod
    def get_acl_header(config, user_id=None, perms=None):
        all_headers = ["read", "write", "read-acp", "write-acp", "full-control"]
        headers = []

        if user_id is None:
            user_id = config.alt_user_id

        if perms is not None:
            for perm in perms:
                header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
                headers.append(header)
        else:
            for perm in all_headers:
                header = ("x-amz-grant-{perm}".format(perm=perm), "id={uid}".format(uid=user_id))
                headers.append(header)

        return headers

    def check_object_acl(self, config, permission):
        """
        Sets the permission on an object then checks to see if it was set
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)

        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        policy = {'Owner': response['Owner'], 'Grants': response['Grants']}
        policy['Grants'][0]['Permission'] = permission

        client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=policy)

        response = client.get_object_acl(Bucket=bucket_name, Key='foo')
        grants = response['Grants']

        main_user_id = config.main_user_id
        main_display_name = config.main_display_name

        self.check_grants(
            grants,
            [
                dict(
                    Permission=permission,
                    ID=main_user_id,
                    DisplayName=main_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    def setup_access(self, config, bucket_acl, object_acl):
        """
        Simple test fixture: create a bucket with given ACL, with objects:
        - a: owning user, given ACL
        - a2: same object accessed by some other user
        - b: owning user, default ACL in bucket w/given ACL
        - b2: same object accessed by a some other user
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket(client, config)

        key1 = 'foo'
        key2 = 'bar'
        new_key = 'new'

        client.put_bucket_acl(Bucket=bucket_name, ACL=bucket_acl)
        client.put_object(Bucket=bucket_name, Key=key1, Body='foocontent')
        client.put_object_acl(Bucket=bucket_name, Key=key1, ACL=object_acl)
        client.put_object(Bucket=bucket_name, Key=key2, Body='barcontent')

        return bucket_name, key1, key2, new_key

    def setup_bucket_object_acl(self, config, bucket_acl='private', object_acl='private'):
        """
        add a foo key, and specified key and bucket acls to
        a (new or existing) bucket.
        """
        client = get_client(config)
        bucket_name = self.get_new_bucket_name(config)
        client.create_bucket(ACL=bucket_acl, Bucket=bucket_name)
        client.put_object(ACL=object_acl, Bucket=bucket_name, Key='foo')

        return bucket_name

    def check_grants(self, got, want):
        """
        Check that grants list in got matches the dictionaries in want,
        in any order.
        """
        self.eq(len(got), len(want))

        # There are instances when got does not match due the order of item.
        if got[0]["Grantee"].get("DisplayName"):
            got.sort(key=lambda x: x["Grantee"].get("DisplayName"))
            want.sort(key=lambda x: x["DisplayName"])

        for g, w in zip(got, want):
            w = dict(w)
            g = dict(g)
            self.eq(g.pop('Permission', None), w['Permission'])
            self.eq(g['Grantee'].pop('DisplayName', None), w['DisplayName'])
            self.eq(g['Grantee'].pop('ID', None), w['ID'])
            self.eq(g['Grantee'].pop('Type', None), w['Type'])
            self.eq(g['Grantee'].pop('URI', None), w['URI'])
            self.eq(g['Grantee'].pop('EmailAddress', None), w['EmailAddress'])
            self.eq(g, {'Grantee': {}})


class TestBucketAcl(TestAclBase):

    @pytest.mark.ess
    def test_bucket_list_objects_anonymous(self, s3cfg_global_unique):
        """
        测试-验证ACL public-read规则生效
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        unauthenticated_client.list_objects(Bucket=bucket_name)

    @pytest.mark.ess
    def test_bucket_list_v2_objects_anonymous(self, s3cfg_global_unique):
        """
        测试-验证ACL public-read规则生效，使用list-objects-v2方法
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        client.put_bucket_acl(Bucket=bucket_name, ACL='public-read')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        unauthenticated_client.list_objects_v2(Bucket=bucket_name)

    @pytest.mark.ess
    def test_bucket_concurrent_set_canned_acl(self, s3cfg_global_unique):
        """
        测试-验证对同一个桶并发设置ACL
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        num_threads = 50  # boto2 retry defaults to 5 so we need a thread to fail at least 5 times
        # this seems like a large enough number to get through retry (if bug exists)
        results = [None] * num_threads

        t = self.do_set_bucket_canned_acl_concurrent(
            client, self.do_set_bucket_canned_acl, bucket_name, 'public-read', num_threads, results)
        self.do_wait_completion(t)

        for r in results:
            self.eq(r, True)

    @pytest.mark.ess
    def test_bucket_recreate_overwrite_acl(self, s3cfg_global_unique):
        """
        测试-验证多次创建同一个存储桶的表现（不同用户：409错误码，BucketAlreadyExists）；
        按Ceph的实现，相同用户多次创建同一个存储桶不会报异常，ACL以第一次创建为准；
        """
        client = get_client(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name, ACL='public-read')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        unauthenticated_client.list_objects(Bucket=bucket_name)  # also can list-objects

        client.delete_bucket(Bucket=bucket_name)  # For ESS, must delete first or use put-bucket-acl to change ACL.
        client.create_bucket(Bucket=bucket_name)
        e = assert_raises(ClientError, unauthenticated_client.list_objects, Bucket=bucket_name)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, "AccessDenied")

        e = assert_raises(ClientError, alt_client.create_bucket, Bucket=bucket_name)  # different user, raise Error.
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 409)
        self.eq(error_code, 'BucketAlreadyExists')

    @pytest.mark.ess
    def test_bucket_recreate_new_acl(self, s3cfg_global_unique):
        """
        测试-验证多次创建同一个存储桶的表现（不同用户：409错误码，BucketAlreadyExists）；
        按Ceph的实现，相同用户多次创建同一个存储桶不会报异常，ACL以第一次创建为准；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        e = assert_raises(ClientError, unauthenticated_client.list_objects, Bucket=bucket_name)  # raise Error.
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

        client.delete_bucket(Bucket=bucket_name)  # For ESS, must delete first or use put-bucket-acl to change ACL.
        client.create_bucket(Bucket=bucket_name, ACL='public-read')
        unauthenticated_client.list_objects(Bucket=bucket_name)  # can list-objects

        # different user, will raise Error.
        alt_client = get_alt_client(s3cfg_global_unique)
        e = assert_raises(ClientError, alt_client.create_bucket, Bucket=bucket_name, ACL='public-read')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 409)
        self.eq(error_code, 'BucketAlreadyExists')

    @pytest.mark.ess
    def test_bucket_acl_no_grants(self, s3cfg_global_unique):
        """
        测试-验证owner with no grants；
        can: read obj, get/set bucket acl, cannot write objs
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_bucket_acl(Bucket=bucket_name)
        old_grants = response['Grants']
        """
        'Grants': [
            {'Grantee': 
                {'DisplayName': 'xxx', 'ID': 'xxx', 'Type': 'CanonicalUser'}, 
                'Permission': 'FULL_CONTROL'
            }
        ]
        """
        policy = {'Owner': response['Owner'], 'Grants': []}  # clear grants

        # remove read/write permission
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)
        # can read objs
        client.get_object(Bucket=bucket_name, Key='foo')
        # can't write objs
        self.check_access_denied(client.put_object, Bucket=bucket_name, Key='baz', Body='a')

        # TODO fix this test once a fix is in for same issues in test_access_bucket_private_object_private
        client2 = get_client(s3cfg_global_unique)
        # owner can read acl
        client2.get_bucket_acl(Bucket=bucket_name)
        # owner can write acl
        client2.put_bucket_acl(Bucket=bucket_name, ACL='private')

        # set policy back to original so that bucket can be cleaned up
        policy['Grants'] = old_grants
        client2.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    @pytest.mark.ess
    def test_bucket_acl_canned_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证ACL public-read-write设置成功后，获取的ACL是否正确；
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read-write', Bucket=bucket_name)
        response = client.get_bucket_acl(Bucket=bucket_name)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id
        grants = response['Grants']
        """
        [{
            'Grantee': {'Type': 'Group', 
                        'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
            'Permission': 'READ'
        },
        {
            'Grantee': {'Type': 'Group',
                        'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
            'Permission': 'WRITE'},
        {
            'Grantee': {'DisplayName': 'xxx',
                        'ID': 'xxx',
                        'Type': 'CanonicalUser'},
            'Permission': 'FULL_CONTROL'}],
        """
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='WRITE',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_bucket_acl_default(self, s3cfg_global_unique):
        """
        测试-验证存储桶默认的ACL响应体
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        response = client.get_bucket_acl(Bucket=bucket_name)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        # 'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'}
        self.eq(response['Owner']['DisplayName'], display_name)
        self.eq(response['Owner']['ID'], user_id)

        grants = response['Grants']
        """
        'Grants': [
            {'Grantee': 
                {'DisplayName': 'xxx', 
                'ID': 'xxx', 
                'Type': 'CanonicalUser'}, 
            'Permission': 'FULL_CONTROL'}]
        }
        """
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_bucket_acl_canned_during_create(self, s3cfg_global_unique):
        """
        测试-验证ACL public-read的响应是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read', Bucket=bucket_name)
        response = client.get_bucket_acl(Bucket=bucket_name)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        """
        [{
            'Grantee': {'Type': 'Group', 
                        'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
            'Permission': 'READ'
        },
        {
            'Grantee': {'DisplayName': 'xxx',
                        'ID': 'xxx',
                        'Type': 'CanonicalUser'},
            'Permission': 'FULL_CONTROL'}],
        """
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_bucket_acl_canned(self, s3cfg_global_unique):
        """
        测试-验证ACL public-read， private的响应是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='public-read', Bucket=bucket_name)
        response = client.get_bucket_acl(Bucket=bucket_name)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

        client.put_bucket_acl(ACL='private', Bucket=bucket_name)
        response = client.get_bucket_acl(Bucket=bucket_name)

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_bucket_acl_canned_authenticated_read(self, s3cfg_global_unique):
        """
        测试-验证ACL authenticated-read的响应是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(ACL='authenticated-read', Bucket=bucket_name)
        response = client.get_bucket_acl(Bucket=bucket_name)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        """
        'Grants': [
            {'Grantee': {
                'Type': 'Group', 
                'URI': 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers'}, 
            'Permission': 'READ'}, 
            {'Grantee': {'DisplayName': 'xxx', 'ID': 'xxx', 'Type': 'CanonicalUser'}, 
            'Permission': 'FULL_CONTROL'}]}
        """
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_bucket_acl_canned_private_to_private(self, s3cfg_global_unique):
        """
        测试-验证设置private的ACL是否成功；
        a private object can be set to private
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        response = client.put_bucket_acl(Bucket=bucket_name, ACL='private')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_bucket_acl_grant_userid_full_control(self, s3cfg_global_unique):
        """
        测试-验证ACL赋予其他对象用户FULL_CONTROL权限；
        允许被授权者在存储桶上的 READ、WRITE、READ_ACP 和 WRITE_ACP 许可
        """
        bucket_name = self.bucket_acl_grant_userid(s3cfg_global_unique, 'FULL_CONTROL')

        # alt user can read
        self.check_bucket_acl_grant_can_read(s3cfg_global_unique, bucket_name)
        # can read acl
        self.check_bucket_acl_grant_can_read_acp(s3cfg_global_unique, bucket_name)
        # can write
        self.check_bucket_acl_grant_can_write(s3cfg_global_unique, bucket_name)
        # can write acl
        self.check_bucket_acl_grant_can_write_acp(s3cfg_global_unique, bucket_name)
        client = get_client(s3cfg_global_unique)

        bucket_acl_response = client.get_bucket_acl(Bucket=bucket_name)
        owner_id = bucket_acl_response['Owner']['ID']
        owner_display_name = bucket_acl_response['Owner']['DisplayName']

        main_display_name = s3cfg_global_unique.main_display_name
        main_user_id = s3cfg_global_unique.main_user_id

        self.eq(owner_id, main_user_id)
        self.eq(owner_display_name, main_display_name)

    @pytest.mark.ess
    def test_bucket_acl_grant_userid_read(self, s3cfg_global_unique):
        """
        测试-验证ACL赋予其他对象用户READ权限；
        允许被授权者列出存储桶中的对象
        """
        bucket_name = self.bucket_acl_grant_userid(s3cfg_global_unique, 'READ')

        # alt user can read
        self.check_bucket_acl_grant_can_read(s3cfg_global_unique, bucket_name)
        # can't read acl
        self.check_bucket_acl_grant_cant_read_acp(s3cfg_global_unique, bucket_name)
        # can't write
        self.check_bucket_acl_grant_cant_write(s3cfg_global_unique, bucket_name)
        # can't write acl
        self.check_bucket_acl_grant_cant_write_acp(s3cfg_global_unique, bucket_name)

    @pytest.mark.ess
    def test_bucket_acl_grant_userid_read_acp(self, s3cfg_global_unique):
        """
        测试-验证ACL赋予其他对象用户READ_ACP权限；
        允许被授权者读取存储桶 ACL
        """
        bucket_name = self.bucket_acl_grant_userid(s3cfg_global_unique, 'READ_ACP')

        # alt user can't read
        self.check_bucket_acl_grant_cant_read(s3cfg_global_unique, bucket_name)
        # can read acl
        self.check_bucket_acl_grant_can_read_acp(s3cfg_global_unique, bucket_name)
        # can't write
        self.check_bucket_acl_grant_cant_write(s3cfg_global_unique, bucket_name)
        # can't write acp
        self.check_bucket_acl_grant_cant_write_acp(s3cfg_global_unique, bucket_name)

    @pytest.mark.ess
    def test_bucket_acl_grant_userid_write(self, s3cfg_global_unique):
        """
        测试-验证ACL赋予其他对象用户WRITE权限；
        允许被授权者在存储桶中创建新对象。对于现有对象的存储桶和对象所有者，还允许删除和覆写这些对象。
        """
        bucket_name = self.bucket_acl_grant_userid(s3cfg_global_unique, 'WRITE')

        # alt user can't read
        self.check_bucket_acl_grant_cant_read(s3cfg_global_unique, bucket_name)
        # can't read acl
        self.check_bucket_acl_grant_cant_read_acp(s3cfg_global_unique, bucket_name)
        # can write
        self.check_bucket_acl_grant_can_write(s3cfg_global_unique, bucket_name)
        # can't write acl
        self.check_bucket_acl_grant_cant_write_acp(s3cfg_global_unique, bucket_name)

    @pytest.mark.ess
    def test_bucket_acl_grant_userid_write_acp(self, s3cfg_global_unique):
        """
        测试-验证ACL赋予其他对象用户WRITE_ACP权限；
        允许被授权者为适用的存储桶编写 ACL
        """
        bucket_name = self.bucket_acl_grant_userid(s3cfg_global_unique, 'WRITE_ACP')

        # alt user can't read
        self.check_bucket_acl_grant_cant_read(s3cfg_global_unique, bucket_name)
        # can't read acl
        self.check_bucket_acl_grant_cant_read_acp(s3cfg_global_unique, bucket_name)
        # can't write
        self.check_bucket_acl_grant_cant_write(s3cfg_global_unique, bucket_name)
        # can write acl
        self.check_bucket_acl_grant_can_write_acp(s3cfg_global_unique, bucket_name)

    @pytest.mark.ess
    def test_bucket_acl_grant_non_exist_user(self, s3cfg_global_unique):
        """
        测试-验证为不存在的user设置ACL；
        400， InvalidArgument
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        bad_user_id = '_foo'
        grant = {'Grantee': {'ID': bad_user_id, 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}
        grant = self.add_bucket_user_grant(s3cfg_global_unique, bucket_name, grant)

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy=grant)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'InvalidArgument')

    @pytest.mark.ess
    def test_bucket_header_acl_grants(self, s3cfg_global_unique):
        """
        测试-通过headers给alt user设置全部权限("read", "write", "read-acp", "write-acp", "full-control")
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)

        headers = self.get_acl_header(s3cfg_global_unique)

        def add_headers_before_sign(**kwargs):
            updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
            kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

        client.meta.events.register('before-sign.s3.CreateBucket', add_headers_before_sign)
        client.create_bucket(Bucket=bucket_name)

        response = client.get_bucket_acl(Bucket=bucket_name)
        grants = response['Grants']
        alt_user_id = s3cfg_global_unique.alt_user_id
        alt_display_name = s3cfg_global_unique.alt_display_name
        alt_client = get_alt_client(s3cfg_global_unique)

        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='WRITE',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='READ_ACP',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='WRITE_ACP',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

        alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        # set bucket acl to public-read-write so that teardown can work
        alt_client.put_bucket_acl(Bucket=bucket_name, ACL='public-read-write')

    @pytest.mark.ess
    def test_bucket_acl_grant_email(self, s3cfg_global_unique):
        """
        测试-通过设置alt user的email，赋予FULL_CONTROL权限，并验证响应正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        alt_user_id = s3cfg_global_unique.alt_user_id
        alt_display_name = s3cfg_global_unique.alt_display_name
        alt_email_address = s3cfg_global_unique.alt_email

        main_user_id = s3cfg_global_unique.main_user_id
        main_display_name = s3cfg_global_unique.main_display_name

        grant = {'Grantee': {'EmailAddress': alt_email_address, 'Type': 'AmazonCustomerByEmail'},
                 'Permission': 'FULL_CONTROL'}

        grant = self.add_bucket_user_grant(s3cfg_global_unique, bucket_name, grant)

        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

        response = client.get_bucket_acl(Bucket=bucket_name)

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=main_user_id,
                    DisplayName=main_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ]
        )

    @pytest.mark.ess
    def test_bucket_acl_grant_email_not_exist(self, s3cfg_global_unique):
        """
        测试-验证给不存在的email添加ACL；
        400， UnresolvableGrantByEmailAddress
        """
        # behavior not documented by amazon
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        non_existent_email = 'doesnotexist@dreamhost.com.invalid'
        grant = {'Grantee': {'EmailAddress': non_existent_email, 'Type': 'AmazonCustomerByEmail'},
                 'Permission': 'FULL_CONTROL'}

        grant = self.add_bucket_user_grant(s3cfg_global_unique, bucket_name, grant)

        e = assert_raises(ClientError, client.put_bucket_acl, Bucket=bucket_name, AccessControlPolicy=grant)
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 400)
        self.eq(error_code, 'UnresolvableGrantByEmailAddress')

    @pytest.mark.ess
    def test_bucket_acl_revoke_all(self, s3cfg_global_unique):
        """
        测试-验证去掉所有ACLs，获取ACL的响应中grants为空
        """
        # revoke all access, including the owner's access
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_bucket_acl(Bucket=bucket_name)
        old_grants = response['Grants']
        policy = {'Owner': response['Owner'], 'Grants': []}  # clear grants

        # remove read/write permission for everyone
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

        response = client.get_bucket_acl(Bucket=bucket_name)
        self.eq(len(response['Grants']), 0)

        # set policy back to original so that bucket can be cleaned up
        policy['Grants'] = old_grants
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=policy)

    @pytest.mark.ess
    def test_object_anon_put_write_access(self, s3cfg_global_unique):
        """
        测试-验证未认证用户对已存在的对象进行覆盖写（桶ACL：public-read-write）
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_acl(s3cfg_global_unique, 'public-read-write')
        client.put_object(Bucket=bucket_name, Key='foo')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)

        response = unauthenticated_client.put_object(Bucket=bucket_name, Key='foo', Body='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)


class TestObjectAcl(TestAclBase):

    @pytest.mark.ess
    def test_object_acl_default(self, s3cfg_global_unique):
        """
        测试-验证新上传的object的默认ACL，响应符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned_during_create(self, s3cfg_global_unique):
        """
        测试-验证上传对象时设置public-read ACL，并验证响应符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned(self, s3cfg_global_unique):
        """
        测试-验证上传对象时分别设置public-read和private ACL，并验证ACL获取后符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        # Since it defaults to private, set it public-read first
        client.put_object(ACL='public-read', Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

        # Then back to private.
        client.put_object_acl(ACL='private', Bucket=bucket_name, Key='foo')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')
        grants = response['Grants']

        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证上传对象时设置public-read-write ACL，并验证ACL获取后符合预期
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(ACL='public-read-write', Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='WRITE',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned_authenticated_read(self, s3cfg_global_unique):
        """
        测试-验证上传对象时设置authenticated-read ACL，并验证ACL获取后符合预期；
        所有者将获得 FULL_CONTROL。AuthenticatedUsers 组将获得 READ 访问权限。
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        client.put_object(ACL='authenticated-read', Bucket=bucket_name, Key='foo', Body='bar')
        response = client.get_object_acl(Bucket=bucket_name, Key='foo')

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AuthenticatedUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned_bucket_owner_read(self, s3cfg_global_unique):
        """
        测试-验证上传对象时设置bucket-owner-read ACL，并验证ACL获取后符合预期；
        对象所有者将获得 FULL_CONTROL。存储桶拥有者将获得 READ 访问权限。
        如果您在创建存储段时指定此标准的 ACL，Amazon S3 将忽略它。
        此权限只能给object设置。
        """
        main_client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)

        main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
        bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
        bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

        alt_client.put_object(ACL='bucket-owner-read', Bucket=bucket_name, Key='foo')
        response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

        alt_display_name = s3cfg_global_unique.alt_display_name
        alt_user_id = s3cfg_global_unique.alt_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='READ',
                    ID=bucket_owner_id,
                    DisplayName=bucket_owner_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_canned_bucket_owner_full_control(self, s3cfg_global_unique):
        """
        测试-验证上传对象时设置bucket-owner-full-control ACL，并验证ACL获取后符合预期；
        对象所有者和存储桶拥有者均可获得对对象的 FULL_CONTROL。
        如果您在创建存储段时指定此标准的 ACL，Amazon S3 将忽略它。
        此权限只能给object设置。
        """
        main_client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)

        main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

        alt_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')

        bucket_acl_response = main_client.get_bucket_acl(Bucket=bucket_name)
        bucket_owner_id = bucket_acl_response['Grants'][2]['Grantee']['ID']
        bucket_owner_display_name = bucket_acl_response['Grants'][2]['Grantee']['DisplayName']

        alt_client.put_object(ACL='bucket-owner-full-control', Bucket=bucket_name, Key='foo')
        response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')

        alt_display_name = s3cfg_global_unique.alt_display_name
        alt_user_id = s3cfg_global_unique.alt_user_id

        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='FULL_CONTROL',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=bucket_owner_id,
                    DisplayName=bucket_owner_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_object_acl_full_control_verify_owner(self, s3cfg_global_unique):
        """
        测试-通过put_object_acl设置修改Grantee的ID为alt ID、Permission修改为READ_ACP，
        验证owner不修改，只改了Grantee的DisplayName
        """
        main_client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)

        main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')
        main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        """
        {'Grants': [{'Grantee': {'DisplayName': 'xxx',
                         'ID': 'xxx',
                         'Type': 'CanonicalUser'},
                    'Permission': 'FULL_CONTROL'}],
        'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """
        main_display_name = s3cfg_global_unique.main_display_name
        main_user_id = s3cfg_global_unique.main_user_id

        alt_client = get_alt_client(s3cfg_global_unique)
        alt_user_id = s3cfg_global_unique.alt_user_id
        grant = {'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}],
                 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

        main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)
        """
        {'Grants': [{'Grantee': {'DisplayName': 'xxx2',
                         'ID': 'xxx2',
                         'Type': 'CanonicalUser'},
                    'Permission': 'FULL_CONTROL'}],
        'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """
        grant = {'Grants': [{'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser'}, 'Permission': 'READ_ACP'}],
                 'Owner': {'DisplayName': main_display_name, 'ID': main_user_id}}

        alt_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grant)
        """
        {'Grants': [{'Grantee': {'DisplayName': 'xxx2',
                         'ID': 'xxx2',
                         'Type': 'CanonicalUser'},
                    'Permission': 'READ_ACP'}],
        'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """
        response = alt_client.get_object_acl(Bucket=bucket_name, Key='foo')
        """
        {'Grants': [{'Grantee': {'DisplayName': 'xxx2',
                         'ID': 'xxx2',
                         'Type': 'CanonicalUser'},
                    'Permission': 'READ_ACP'}],
        'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """
        self.eq(response['Owner']['ID'], main_user_id)

    @pytest.mark.ess
    def test_object_acl_full_control_verify_attributes(self, s3cfg_global_unique):
        """
        测试-通过put_object_acl设置修改Grantee的ID为alt ID、Permission修改为FULL_CONTROL，
        验证get-object中ContentType和ETag未改变
        """
        main_client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)

        main_client.create_bucket(Bucket=bucket_name, ACL='public-read-write')

        header = {'x-amz-foo': 'bar'}
        # lambda to add any header
        add_header = (lambda **kwargs: kwargs['params']['headers'].update(header))
        main_client.meta.events.register('before-call.s3.PutObject', add_header)  # no headers added.
        main_client.put_object(Bucket=bucket_name, Key='foo', Body='bar')
        """
        {'AcceptRanges': 'bytes',
         'ContentLength': 3,
         'ContentType': 'binary/octet-stream',
         'ETag': '"37b51d194a7513e45b56f6524f2d51f2"',
         'LastModified': datetime.datetime(2022, 4, 27, 2, 59, 12, tzinfo=tzutc()),
         'Metadata': {},
         'ResponseMetadata': {'HTTPHeaders': 
                                {'accept-ranges': 'bytes',
                                  'connection': 'Keep-Alive',
                                  'content-length': '3',
                                  'content-type': 'binary/octet-stream',
                                  'date': 'Wed, 27 Apr 2022 02:59:13 GMT',
                                  'etag': '"37b51d194a7513e45b56f6524f2d51f2"',
                                  'last-modified': 'Wed, 27 Apr 2022 '
                                                   '02:59:12 GMT',
                                  'x-amz-request-id': 'tx000000000000000042e49-006268b181-39e502-zone-1647582137',
                                  'x-amz-storage-class': 'STANDARD',
                                  'x-rgw-object-type': 'Normal',
                                  'x_amz_archive_flags': '0'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000042e49-006268b181-39e502-zone-1647582137',
                              'RetryAttempts': 0},
         'StorageClass': 'STANDARD'}
        """
        response = main_client.get_object(Bucket=bucket_name, Key='foo')
        content_type = response['ContentType']
        etag = response['ETag']

        alt_user_id = s3cfg_global_unique.alt_user_id

        grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}
        grants = self.add_obj_user_grant(s3cfg_global_unique, bucket_name, 'foo', grant)
        # grant FULL_CONTROL permission to alt user.
        main_client.put_object_acl(Bucket=bucket_name, Key='foo', AccessControlPolicy=grants)
        """
        {'Grants': [{'Grantee': {'DisplayName': 'xxx',
                                 'ID': 'xxx',
                                 'Type': 'CanonicalUser'},
                     'Permission': 'FULL_CONTROL'},
                    {'Grantee': {'DisplayName': 'xxx2',
                                 'ID': 'xxx2',
                                 'Type': 'CanonicalUser'},
                     'Permission': 'FULL_CONTROL'}],
         'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """
        response = main_client.get_object(Bucket=bucket_name, Key='foo')
        """
        {'AcceptRanges': 'bytes',
         'ContentLength': 3,
         'ContentType': 'binary/octet-stream',
         'ETag': '"37b51d194a7513e45b56f6524f2d51f2"',
         'LastModified': datetime.datetime(2022, 4, 27, 2, 59, 12, tzinfo=tzutc()),
         'Metadata': {},
         'ResponseMetadata': {'HTTPHeaders': 
                                {'accept-ranges': 'bytes',
                                  'connection': 'Keep-Alive',
                                  'content-length': '3',
                                  'content-type': 'binary/octet-stream',
                                  'date': 'Wed, 27 Apr 2022 02:59:13 GMT',
                                  'etag': '"37b51d194a7513e45b56f6524f2d51f2"',
                                  'last-modified': 'Wed, 27 Apr 2022 '
                                                   '02:59:12 GMT',
                                  'x-amz-request-id': 'tx000000000000000042e49-006268b181-39e502-zone-1647582137',
                                  'x-amz-storage-class': 'STANDARD',
                                  'x-rgw-object-type': 'Normal',
                                  'x_amz_archive_flags': '0'},
                              'HTTPStatusCode': 200,
                              'HostId': '',
                              'RequestId': 'tx000000000000000042e49-006268b181-39e502-zone-1647582137',
                              'RetryAttempts': 0},
         'StorageClass': 'STANDARD'}
        """
        self.eq(content_type, response['ContentType'])
        self.eq(etag, response['ETag'])

    @pytest.mark.ess
    def test_object_acl(self, s3cfg_global_unique):
        """
        测试-验证put_object_acl设置FULL_CONTROL权限，并验证ACL符合要求
        """
        self.check_object_acl(s3cfg_global_unique, 'FULL_CONTROL')

    @pytest.mark.ess
    def test_object_acl_write(self, s3cfg_global_unique):
        """
        测试-验证put_object_acl设置WRITE权限，并验证ACL符合要求
        """
        self.check_object_acl(s3cfg_global_unique, 'WRITE')

    @pytest.mark.ess
    def test_object_acl_write_acp(self, s3cfg_global_unique):
        """
        测试-验证put_object_acl设置WRITE_ACP权限，并验证ACL符合要求
        """
        self.check_object_acl(s3cfg_global_unique, 'WRITE_ACP')

    @pytest.mark.ess
    def test_object_acl_read(self, s3cfg_global_unique):
        """
        测试-验证put_object_acl设置READ权限，并验证ACL符合要求
        """
        self.check_object_acl(s3cfg_global_unique, 'READ')

    @pytest.mark.ess
    def test_object_acl_read_acp(self, s3cfg_global_unique):
        """
        测试-验证put_object_acl设置READ_ACP权限，并验证ACL符合要求
        """
        self.check_object_acl(s3cfg_global_unique, 'READ_ACP')

    @pytest.mark.ess
    def test_object_header_acl_grants(self, s3cfg_global_unique):
        """
        测试-验证通过headers赋予alt用户所有权限
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        alt_user_id = s3cfg_global_unique.alt_user_id
        alt_display_name = s3cfg_global_unique.alt_display_name

        headers = self.get_acl_header(s3cfg_global_unique)

        def add_headers_before_sign(**kwargs):
            updated_headers = (kwargs['request'].__dict__['headers'].__dict__['_headers'] + headers)
            kwargs['request'].__dict__['headers'].__dict__['_headers'] = updated_headers

        client.meta.events.register('before-sign.s3.PutObject', add_headers_before_sign)
        client.put_object(Bucket=bucket_name, Key='foo_key', Body='bar')

        response = client.get_object_acl(Bucket=bucket_name, Key='foo_key')
        grants = response['Grants']

        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='WRITE',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='READ_ACP',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='WRITE_ACP',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=alt_user_id,
                    DisplayName=alt_display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

    @pytest.mark.ess
    def test_versioned_object_acl(self, s3cfg_global_unique):
        """
        测试-验证多版本对象和ACL是否正确
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'xyz'
        num_versions = 3

        (version_ids, contents) = self.create_multiple_versions(client, bucket_name, key, num_versions)

        version_id = version_ids[1]

        response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        self.eq(response['Owner']['DisplayName'], display_name)
        self.eq(response['Owner']['ID'], user_id)

        grants = response['Grants']
        default_policy = [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
            ),
        ]

        self.check_grants(grants, default_policy)

        client.put_object_acl(ACL='public-read', Bucket=bucket_name, Key=key, VersionId=version_id)

        response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )

        client.put_object(Bucket=bucket_name, Key=key)

        response = client.get_object_acl(Bucket=bucket_name, Key=key)
        grants = response['Grants']
        self.check_grants(grants, default_policy)

    @pytest.mark.ess
    def test_versioned_object_acl_no_version_specified(self, s3cfg_global_unique):
        """
        测试-验证多版本对象和ACL的情况
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)

        self.check_configure_versioning_retry(client, bucket_name, "Enabled", "Enabled")

        key = 'xyz'
        num_versions = 3

        self.create_multiple_versions(client, bucket_name, key, num_versions)

        response = client.get_object(Bucket=bucket_name, Key=key)
        version_id = response['VersionId']

        response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)

        display_name = s3cfg_global_unique.main_display_name
        user_id = s3cfg_global_unique.main_user_id

        self.eq(response['Owner']['DisplayName'], display_name)
        self.eq(response['Owner']['ID'], user_id)

        grants = response['Grants']
        default_policy = [
            dict(
                Permission='FULL_CONTROL',
                ID=user_id,
                DisplayName=display_name,
                URI=None,
                EmailAddress=None,
                Type='CanonicalUser',
            ),
        ]

        self.check_grants(grants, default_policy)

        client.put_object_acl(ACL='public-read', Bucket=bucket_name, Key=key)

        response = client.get_object_acl(Bucket=bucket_name, Key=key, VersionId=version_id)
        grants = response['Grants']
        self.check_grants(
            grants,
            [
                dict(
                    Permission='READ',
                    ID=None,
                    DisplayName=None,
                    URI='http://acs.amazonaws.com/groups/global/AllUsers',
                    EmailAddress=None,
                    Type='Group',
                ),
                dict(
                    Permission='FULL_CONTROL',
                    ID=user_id,
                    DisplayName=display_name,
                    URI=None,
                    EmailAddress=None,
                    Type='CanonicalUser',
                ),
            ],
        )


class TestBucketObjectAclMixin(TestAclBase):

    @pytest.mark.ess
    def test_object_raw_get_bucket_acl(self, s3cfg_global_unique):
        """
        测试-验证桶ACL为private，对象ACL为public-read
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'private', 'public-read')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        response = unauthenticated_client.get_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_get_object_acl(self, s3cfg_global_unique):
        """
        测试-验证桶设置为public-read、对象设置private ACL，未认证的情况下返回403，AccessDenied
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'private')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 403)
        self.eq(error_code, 'AccessDenied')

    @pytest.mark.ess
    def test_object_raw_get(self, s3cfg_global_unique):
        """
        测试-验证未认证的用户进行get_object操作（Bucket和Object的ACL均为public-read）
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)
        response = unauthenticated_client.get_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_get_bucket_gone(self, s3cfg_global_unique):
        """
        测试-验证未认证的用户对已删除的对象和桶进行get_object操作（Bucket和Object的ACL均为public-read）；
        404，NoSuchBucket
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        client.delete_object(Bucket=bucket_name, Key='foo')
        client.delete_bucket(Bucket=bucket_name)

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)

        e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchBucket')

    @pytest.mark.ess
    def test_object_delete_key_bucket_gone(self, s3cfg_global_unique):
        """
        测试-验证未认证的用户对已删除的对象和桶进行delete_object操作（Bucket和Object的ACL均为public-read）；
        404，NoSuchBucket
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        client.delete_object(Bucket=bucket_name, Key='foo')
        client.delete_bucket(Bucket=bucket_name)

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)

        e = assert_raises(ClientError, unauthenticated_client.delete_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchBucket')

    @pytest.mark.ess
    def test_object_raw_get_object_gone(self, s3cfg_global_unique):
        """
        测试-验证未认证的用户对已删除的对象进行get_object操作（Bucket和Object的ACL均为public-read）；
        404，NoSuchKey
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        client.delete_object(Bucket=bucket_name, Key='foo')

        unauthenticated_client = get_unauthenticated_client(s3cfg_global_unique)

        e = assert_raises(ClientError, unauthenticated_client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

    @pytest.mark.ess
    def test_object_raw_authenticated(self, s3cfg_global_unique):
        """
        测试-验证已认证用户进行get_object操作（Bucket和Object的ACL均为public-read）
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')
        client = get_client(s3cfg_global_unique)

        response = client.get_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_response_headers(self, s3cfg_global_unique):
        """
        测试-验证已认证用户进行get_object操作（Bucket和Object的ACL均为private）
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'private', 'private')
        client = get_client(s3cfg_global_unique)

        response = client.get_object(Bucket=bucket_name, Key='foo', ResponseCacheControl='no-cache',
                                     ResponseContentDisposition='bla', ResponseContentEncoding='aaa',
                                     ResponseContentLanguage='esperanto', ResponseContentType='foo/bar',
                                     ResponseExpires='123')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-type'], 'foo/bar')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-disposition'], 'bla')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-language'], 'esperanto')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['content-encoding'], 'aaa')
        self.eq(response['ResponseMetadata']['HTTPHeaders']['cache-control'], 'no-cache')

    @pytest.mark.ess
    def test_object_raw_authenticated_bucket_acl(self, s3cfg_global_unique):
        """
        测试-验证已认证用户进行get_object操作（Bucket/Object的ACL分别为private/public-read）
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'private', 'public-read')
        client = get_client(s3cfg_global_unique)

        response = client.get_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_authenticated_object_acl(self, s3cfg_global_unique):
        """
        测试-验证已认证用户进行get_object操作（Bucket/Object的ACL分别为public-read/private）
        """
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'private')
        client = get_client(s3cfg_global_unique)

        response = client.get_object(Bucket=bucket_name, Key='foo')
        self.eq(response['ResponseMetadata']['HTTPStatusCode'], 200)

    @pytest.mark.ess
    def test_object_raw_authenticated_bucket_gone(self, s3cfg_global_unique):
        """
        测试-验证已认证用户对已删除的桶和对象进行get_object操作（Bucket/Object的ACL均为public-read），
        404，NoSuchBucket
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        client.delete_object(Bucket=bucket_name, Key='foo')
        client.delete_bucket(Bucket=bucket_name)

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchBucket')

    @pytest.mark.ess
    def test_object_raw_authenticated_object_gone(self, s3cfg_global_unique):
        """
        测试-验证已认证用户对已删除的对象进行get_object操作（Bucket/Object的ACL均为public-read），
        404，NoSuchKey
        """
        client = get_client(s3cfg_global_unique)
        bucket_name = self.setup_bucket_object_acl(s3cfg_global_unique, 'public-read', 'public-read')

        client.delete_object(Bucket=bucket_name, Key='foo')

        e = assert_raises(ClientError, client.get_object, Bucket=bucket_name, Key='foo')
        status, error_code = self.get_status_and_error_code(e.response)
        self.eq(status, 404)
        self.eq(error_code, 'NoSuchKey')

    @pytest.mark.ess
    def test_object_copy_not_owned_object_bucket(self, s3cfg_global_unique):
        """
        测试-验证不同用户间拷贝对象（non-owned object in a non-owned bucket, 赋予ACL权限：FULL_CONTROL）
        """
        client = get_client(s3cfg_global_unique)
        alt_client = get_alt_client(s3cfg_global_unique)

        bucket_name = self.get_new_bucket_name(s3cfg_global_unique)
        client.create_bucket(Bucket=bucket_name)
        client.put_object(Bucket=bucket_name, Key='foo123bar', Body='foo')

        alt_user_id = s3cfg_global_unique.alt_user_id

        grant = {'Grantee': {'ID': alt_user_id, 'Type': 'CanonicalUser'}, 'Permission': 'FULL_CONTROL'}
        grants = self.add_obj_user_grant(s3cfg_global_unique, bucket_name, 'foo123bar', grant)
        client.put_object_acl(Bucket=bucket_name, Key='foo123bar', AccessControlPolicy=grants)

        grant = self.add_bucket_user_grant(s3cfg_global_unique, bucket_name, grant)
        client.put_bucket_acl(Bucket=bucket_name, AccessControlPolicy=grant)

        alt_client.get_object(Bucket=bucket_name, Key='foo123bar')  # OK

        copy_source = {'Bucket': bucket_name, 'Key': 'foo123bar'}
        alt_client.copy(copy_source, bucket_name, 'bar321foo')

        response = alt_client.get_object(Bucket=bucket_name, Key='bar321foo')
        body = self.get_body(response)
        self.eq('foo', body)

    @pytest.mark.ess
    def test_access_bucket_private_object_private(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/private时，使用list-objects，
        public has no access to bucket or objects
        """
        # all the test_access_* tests follow this template
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='private')

        alt_client = get_alt_client(s3cfg_global_unique)

        # acled object read fail
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
        # default object read fail
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
        # bucket read fail
        self.check_access_denied(alt_client.list_objects, Bucket=bucket_name)
        # acled object write fail
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')

        # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
        # anymore. This can be solved either by:
        # 1) putting an empty string ('') in the 'Body' field of those put_object calls
        # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
        # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

        alt_client2 = get_alt_client(s3cfg_global_unique)
        # default object write fail
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')
        # bucket write fail
        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_private_object_v2_private(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/private时，使用list-objects-v2，
        public has no access to bucket or objects
        """
        # all the test_access_* tests follow this template
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='private')

        alt_client = get_alt_client(s3cfg_global_unique)
        # acled object read fail
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
        # default object read fail
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
        # bucket read fail
        self.check_access_denied(alt_client.list_objects_v2, Bucket=bucket_name)

        # acled object write fail
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')
        # NOTE: The above put's causes the connection to go bad, therefore the client can't be used
        # anymore. This can be solved either by:
        # 1) putting an empty string ('') in the 'Body' field of those put_object calls
        # 2) getting a new client hence the creation of alt_client{2,3} for the tests below
        # TODO: Test it from another host and on AWS, Report this to Amazon, if findings are identical

        alt_client2 = get_alt_client(s3cfg_global_unique)
        # default object write fail
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')
        # bucket write fail
        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_private_object_public_read(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/public-read时，使用list-objects，
        public can only read readable object
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='public-read')

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        # a should be public-read, b gets default (private)
        self.eq(body, 'foocontent')

        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_private_object_v2_public_read(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/public-read时，使用list-objects-v2，
        public can only read readable object
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='public-read')
        alt_client = get_alt_client(s3cfg_global_unique)

        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        # a should be public-read, b gets default (private)
        self.eq(body, 'foocontent')

        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_private_object_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/public-read/write时，使用list-objects，
        public can only read the readable object
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='public-read-write')

        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        # a should be public-read-only ... because it is in a private bucket
        # b gets default (private)
        self.eq(body, 'foocontent')

        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.list_objects, Bucket=bucket_name)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_private_object_v2_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: private/public-read/write时，使用list-objects-v2，
        public can only read the readable object
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='private', object_acl='public-read-write')
        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        # a should be public-read-only ... because it is in a private bucket
        # b gets default (private)
        self.eq(body, 'foocontent')

        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')
        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client3.list_objects_v2, Bucket=bucket_name)
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_object_private(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read/private时，
        public can only list the bucket
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read', object_acl='private')
        alt_client = get_alt_client(s3cfg_global_unique)

        # a should be private, b gets default (private)
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='barcontent')

        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        objs = self.get_objects_list(client=alt_client3, bucket=bucket_name)
        self.eq(objs, ['bar', 'foo'])
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_object_public_read(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read/public-read时，
        public can read readable objects and list bucket
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read', object_acl='public-read')
        alt_client = get_alt_client(s3cfg_global_unique)

        response = alt_client.get_object(Bucket=bucket_name, Key=key1)
        # a should be public-read, b gets default (private)
        body = self.get_body(response)
        self.eq(body, 'foocontent')

        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')

        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)

        objs = self.get_objects_list(client=alt_client3, bucket=bucket_name)

        self.eq(objs, ['bar', 'foo'])
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_object_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read/public-read-write时，
        public can read readable objects and list bucket
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read', object_acl='public-read-write')
        alt_client = get_alt_client(s3cfg_global_unique)
        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        # a should be public-read-only ... because it is in a r/o bucket
        # b gets default (private)
        self.eq(body, 'foocontent')
        self.check_access_denied(alt_client.put_object, Bucket=bucket_name, Key=key1, Body='foooverwrite')

        alt_client2 = get_alt_client(s3cfg_global_unique)
        self.check_access_denied(alt_client2.get_object, Bucket=bucket_name, Key=key2)
        self.check_access_denied(alt_client2.put_object, Bucket=bucket_name, Key=key2, Body='baroverwrite')

        alt_client3 = get_alt_client(s3cfg_global_unique)
        objs = self.get_objects_list(client=alt_client3, bucket=bucket_name)
        self.eq(objs, ['bar', 'foo'])
        self.check_access_denied(alt_client3.put_object, Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_write_object_private(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read-write/private时，
        private objects cannot be read, but can be overwritten
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read-write', object_acl='private')
        alt_client = get_alt_client(s3cfg_global_unique)

        # a should be private, b gets default (private)
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key1)
        alt_client.put_object(Bucket=bucket_name, Key=key1, Body='barcontent')

        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
        alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')

        objs = self.get_objects_list(client=alt_client, bucket=bucket_name)
        self.eq(objs, ['bar', 'foo'])
        alt_client.put_object(Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_write_object_public_read(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read-write/public-read时，
        private objects cannot be read, but can be overwritten
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read-write', object_acl='public-read')
        alt_client = get_alt_client(s3cfg_global_unique)

        """
        1. should be public-read：
        'Grants': [{'Grantee': {'Type': 'Group',
                                 'URI': 'http://acs.amazonaws.com/groups/global/AllUsers'},
                     'Permission': 'READ'},
                    {'Grantee': {'DisplayName': 'xxx',
                                 'ID': 'xxx',
                                 'Type': 'CanonicalUser'},
                     'Permission': 'FULL_CONTROL'}],
         'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},

        2. gets default (private): 
        'Grants': [{'Grantee': {'DisplayName': 'xxx',
                                 'ID': 'xxx',
                                 'Type': 'CanonicalUser'},
                     'Permission': 'FULL_CONTROL'}],
         'Owner': {'DisplayName': 'xxx', 'ID': 'xxx'},
        """

        response = alt_client.get_object(Bucket=bucket_name, Key=key1)

        body = self.get_body(response)
        self.eq(body, 'foocontent')
        alt_client.put_object(Bucket=bucket_name, Key=key1, Body='barcontent')

        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
        alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')

        objs = self.get_objects_list(client=alt_client, bucket=bucket_name)
        self.eq(objs, ['bar', 'foo'])
        alt_client.put_object(Bucket=bucket_name, Key=new_key, Body='newcontent')

    @pytest.mark.ess
    def test_access_bucket_public_read_write_object_public_read_write(self, s3cfg_global_unique):
        """
        测试-验证set bucket/object acls: public-read-write/public-read-write时，
        private objects cannot be read, but can be overwritten
        """
        bucket_name, key1, key2, new_key = self.setup_access(
            s3cfg_global_unique, bucket_acl='public-read-write', object_acl='public-read-write')
        alt_client = get_alt_client(s3cfg_global_unique)

        response = alt_client.get_object(Bucket=bucket_name, Key=key1)
        body = self.get_body(response)

        # a should be public-read-write, b gets default (private)
        self.eq(body, 'foocontent')
        alt_client.put_object(Bucket=bucket_name, Key=key1, Body='foooverwrite')
        self.check_access_denied(alt_client.get_object, Bucket=bucket_name, Key=key2)
        alt_client.put_object(Bucket=bucket_name, Key=key2, Body='baroverwrite')
        objs = self.get_objects_list(client=alt_client, bucket=bucket_name)
        self.eq(objs, ['bar', 'foo'])
        alt_client.put_object(Bucket=bucket_name, Key=new_key, Body='newcontent')
