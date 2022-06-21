
from typing import Dict
import datetime
import hmac
import hashlib

from urllib.parse import urlparse


class AWS4SignerBase(object):
    # Common methods and properties for all AWS4 signer variants

    # SHA256 hash of an empty request body
    EMPTY_BODY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"

    SCHEME = "AWS4"
    ALGORITHM = "HMAC-SHA256"
    TERMINATOR = "aws4_request"

    # format strings for the date/time and date stamps required during signing
    ISO8601BasicFormat = '%Y%m%dT%H%M%SZ'
    DateStringFormat = '%Y%m%d'

    def __init__(self, endpoint_url, http_method, service_name, region_name):
        self.endpoint_url = endpoint_url
        self.http_method = http_method
        self.service_name = service_name
        self.region_name = region_name

        self.parsed_endpoint = urlparse(self.endpoint_url)
        self.host = self.parsed_endpoint.scheme + '://' + self.parsed_endpoint.netloc

        _t = datetime.datetime.utcnow()
        self.datetime_format = _t.strftime(self.ISO8601BasicFormat)
        self.datestamp_format = _t.strftime(self.DateStringFormat)

    def get_canonical_uri(self):
        """
        Returns the canonicalized resource path for the service endpoint.
        """
        path = self.parsed_endpoint.path

        return path if path else "/"

    def get_canonical_querystring(self):
        """
        Examines the specified query string parameters and returns a canonicalized form.
        The canonicalized query string is formed by first sorting all the query
        string parameters, then URI encoding both the key and value and then
        joining them, in order, separating key value pairs with an '&'.
        """
        return self.parsed_endpoint.query

    @staticmethod
    def get_signed_header(headers: Dict):
        """
        Returns the canonical collection of header names that will be included in
        the signature. For AWS4, all header names must be included in the process
        in sorted canonicalized order.
        """
        sorted_keys = sorted(headers.keys())

        return ';'.join(sorted_keys).lower()

    @staticmethod
    def get_canonical_headers(headers: Dict):
        """
        Computes the canonical headers with values for the request. For AWS4, all
        headers must be included in the signing process.
        """

        canonical_headers = ''

        sorted_headers = sorted(headers.items(), key=lambda x: x[0])
        for k, v in dict(sorted_headers).items():
            canonical_headers += f"{k.lower()}:{v}\n"  # must be trimmed and lowercase

        return canonical_headers

    def get_body_hash(self, payload):
        # Converts byte data to a Hex-encoded string.
        if not payload:
            return self.EMPTY_BODY_SHA256

        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    def get_canonical_request(self, headers: Dict, payload):
        """
        Returns the canonical request string to go into the signer process; this
        consists of several canonical sub-parts.
        """
        canonical_request = \
            self.http_method + "\n" + \
            self.get_canonical_uri() + "\n" + \
            self.get_canonical_querystring() + "\n" + \
            self.get_canonical_headers(headers) + "\n" + \
            self.get_signed_header(headers) + "\n" + \
            self.get_body_hash(payload=payload)

        return canonical_request

    def get_canonical_scope(self):
        """
        Get canonical scope.
        """
        return self.datestamp_format + '/' + self.region_name + '/' + self.service_name + '/' + self.TERMINATOR

    def get_string_to_sign(self, canonical_request):
        """
        Get string to sign.
        """
        hex_canonical_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

        string_to_sign = \
            self.SCHEME + '-' + self.ALGORITHM + '\n' + \
            self.datetime_format + '\n' + self.get_canonical_scope() + '\n' + hex_canonical_request

        return string_to_sign


class AWS4SignerForAuthorizationHeader(AWS4SignerBase):
    """
    Sample AWS4 signer demonstrating how to sign requests to Amazon S3 using an 'Authorization' header.
    """

    def __init__(self, endpoint_url, http_method, service_name, region_name):
        super().__init__(
            endpoint_url=endpoint_url,
            http_method=http_method,
            service_name=service_name,
            region_name=region_name
        )

    def compute_signature_headers(self, headers: Dict, payload, aws_access_key, aws_secret_key):
        """
        Computes an AWS4 signature for a request, ready for inclusion as an
        'Authorization' header.

        @param headers
            The request headers; 'Host' and 'X-Amz-Date' will be added to this set.
        @param payload
            The request body content;
            this value should also be set as the header 'X-Amz-Content-SHA256' for non-streaming uploads.
        @param aws_access_key
            The user's AWS Access Key.
        @param aws_secret_key
            The user's AWS Secret Key.
        @return The computed authorization string for the request.
            This value needs to be set as the header 'Authorization' on the subsequent HTTP request.
        """

        # Python note: The 'host' header is added automatically by the Python 'requests' library.
        headers['Host'] = self.host
        headers['X-Amz-Date'] = self.datetime_format
        headers['X-Amz-Content-Sha256'] = self.get_body_hash(payload)

        # Key derivation functions. See:
        # http://docs.aws.amazon.com/general/latest/gr/signature-v4-examples.html#signature-v4-examples-python
        def sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        def get_signature_key(key, datestamp, region_name, service_name):
            k_date = sign(('AWS4' + key).encode('utf-8'), datestamp)
            k_region = sign(k_date, region_name)
            k_service = sign(k_region, service_name)
            k_signing = sign(k_service, 'aws4_request')
            return k_signing

        # Create the signing key using the function defined above.
        signature_key = get_signature_key(aws_secret_key, self.datestamp_format, self.region_name, self.service_name)

        # Sign the string_to_sign using the signing_key
        canonical_request = self.get_canonical_request(headers, payload)
        string_to_sign = self.get_string_to_sign(canonical_request)
        credential_scope = self.get_canonical_scope()
        signed_headers = self.get_signed_header(headers)
        signature = hmac.new(signature_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

        # Put the signature information in a header named Authorization.
        authorization_header = \
            self.SCHEME + '-' + self.ALGORITHM + ' ' + 'Credential=' + aws_access_key + '/' + \
            credential_scope + ',' + ' ' + 'SignedHeaders=' + signed_headers + ',' + ' ' + 'Signature=' + signature

        headers['Authorization'] = authorization_header

        return headers


class AWS4SignerForChunkedUpload(AWS4SignerBase):
    """
    Sample AWS4 signer demonstrating how to sign 'chunked' uploads
    """

    # SHA256 substitute marker used in place of x-amz-content-sha256 when employing chunked uploads
    STREAMING_BODY_SHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
    CLRF = "\r\n"
    CHUNK_STRING_TO_SIGN_PREFIX = "AWS4-HMAC-SHA256-PAYLOAD"
    CHUNK_SIGNATURE_HEADER = ";chunk-signature="
    SIGNATURE_LENGTH = 64
    FINAL_CHUNK = None

    def __init__(self, endpoint_url, http_method, service_name, region_name):
        super().__init__(
            endpoint_url=endpoint_url,
            http_method=http_method,
            service_name=service_name,
            region_name=region_name
        )
