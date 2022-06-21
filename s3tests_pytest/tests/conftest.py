
from munch import Munch

import pytest

from s3tests_pytest.tests import (
    nuke_prefixed_buckets, logger, get_client, get_alt_client
)


@pytest.fixture(scope="session", autouse=True)
def setup_and_teardown_package_level(s3cfg_global_unique: Munch) -> None:
    """
    This function will be ran only once.
    """
    logger.info(" Setup package --- started ")
    client = get_client(s3cfg_global_unique)
    alt_client = get_alt_client(s3cfg_global_unique)

    prefix = s3cfg_global_unique.bucket_prefix
    nuke_prefixed_buckets(client=client, prefix=prefix, msg="main client")
    nuke_prefixed_buckets(client=alt_client, prefix=prefix, msg="alt client")

    logger.info(" Setup package --- ended ")

    yield

    logger.info(" Teardown package --- started ")

    nuke_prefixed_buckets(client=client, prefix=prefix, msg="main client")
    nuke_prefixed_buckets(client=alt_client, prefix=prefix, msg="alt client")

    logger.info(" Teardown package --- ended ")
