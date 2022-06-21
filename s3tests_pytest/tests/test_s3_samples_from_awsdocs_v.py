
import datetime

import pytest
import requests

from s3tests_pytest.tests import TestBaseClass, get_client
from s3tests_pytest.functional.s3_sigv4 import AWS4SignerForAuthorizationHeader


@pytest.mark.ess
class TestSamples(TestBaseClass):

    def test_abort_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证abort multipart upload, 204
        """
        # https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html

        client = get_client(s3cfg_global_unique)

        # get a new bucket
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        # create multipart upload
        upload_id, _, _ = self.multipart_upload(
            config=s3cfg_global_unique,
            bucket_name=bucket_name,
            key="multi001",
            size=5,
            client=client
        )
        # # abort it.
        # res = client.abort_multipart_upload(
        #     Bucket=bucket_name,
        #     Key="multi001",
        #     UploadId=upload_id,
        # )

        """
        {'ResponseMetadata': {
            'RequestId': 'tx000000000000000019ea5-00628b268c-33a7983-zone-1647582137', 
            'HostId': '', 
            'HTTPStatusCode': 204, 
            'HTTPHeaders': {
                'x-amz-request-id': 'tx000000000000000019ea5-00628b268c-33a7983-zone-1647582137', 
                'date': 'Mon, 23 May 2022 06:15:40 GMT', 
                'connection': 'Keep-Alive'}, 
            'RetryAttempts': 0}}
        """
        # self.eq(res['ResponseMetadata']['HTTPStatusCode'], 204)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        ak = s3cfg_global_unique.main_access_key
        sk = s3cfg_global_unique.main_secret_key

        r_url = url + f'/multi001?uploadId={upload_id}'
        signer = AWS4SignerForAuthorizationHeader(r_url, 'DELETE', 's3', 'us-east-1')
        authorization_headers = signer.compute_signature_headers({}, '', ak, sk)
        res = requests.delete(r_url, headers=authorization_headers, verify=s3cfg_global_unique.default_ssl_verify)

        self.eq(res.status_code, 204)

    def test_complete_multipart_upload(self, s3cfg_global_unique):
        """
        测试-验证complete multipart upload, 200OK
        """

        client = get_client(s3cfg_global_unique)

        # get a new bucket
        bucket_name = self.get_new_bucket(client, s3cfg_global_unique)
        # create multipart upload
        upload_id, _, parts = self.multipart_upload(
            config=s3cfg_global_unique,
            bucket_name=bucket_name,
            key="multi001",
            size=5,
            client=client
        )
        # complete it.
        # res = client.complete_multipart_upload(
        #     Bucket=bucket_name,
        #     Key='multi001',
        #     UploadId=upload_id,
        #     MultipartUpload={"Parts": parts}
        # )
        """
        {'ResponseMetadata': {
            'RequestId': 'tx00000000000000001a6b1-00628b5613-33a7983-zone-1647582137', 
            'HostId': '', 
            'HTTPStatusCode': 200, 
            'HTTPHeaders': {
                'x-amz-request-id': 'tx00000000000000001a6b1-00628b5613-33a7983-zone-1647582137', 
                'content-type': 'application/xml', 
                'content-length': '349', 
                'date': 'Mon, 23 May 2022 09:38:27 GMT', 
                'connection': 'Keep-Alive'}, 
            'RetryAttempts': 0}, 
        'Location': 'http://172.38.60.40:7480/ess-3b4kdtr167faaya5f6jesvdei-1/multi001', 
        'Bucket': 'ess-3b4kdtr167faaya5f6jesvdei-1', 
        'Key': 'multi001', 
        'ETag': 'c1ee8d149b5f1d0e2456e73e636cbed0-1'}
        """
        # self.eq(res['ResponseMetadata']['HTTPStatusCode'], 200)
        # self.eq(res['Key'], 'multi001')
        # self.eq(res['Bucket'], bucket_name)

        url = self.get_post_url(s3cfg_global_unique, bucket_name)
        ak = s3cfg_global_unique.main_access_key
        sk = s3cfg_global_unique.main_secret_key

        r_url = url + f'/multi001?uploadId={upload_id}'
        payload = f"""
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                    <ETag>"{parts[0].get('ETag')}"</ETag>
                </Part>
            </CompleteMultipartUpload>
            """
        signer = AWS4SignerForAuthorizationHeader(r_url, 'POST', 's3', 'us-east-1')
        authorization_headers = signer.compute_signature_headers({}, payload, ak, sk)
        res = requests.post(r_url,
                            headers=authorization_headers,
                            data=payload, verify=s3cfg_global_unique.default_ssl_verify)

        self.eq(res.status_code, 200)
        print(res.text)

    def test_complete_multipart_upload_entity_too_small(self, s3cfg_global_unique):
        """
        测试-验证complete multipart upload, entity too small.
        """



