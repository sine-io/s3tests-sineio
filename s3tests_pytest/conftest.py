
from typing import Any
from configparser import RawConfigParser

import urllib3
import random
import string
import itertools
from datetime import datetime
import os
from pathlib2 import Path

import pytest
from py.xml import html

from munch import Munch


# -------------------------------------------- DO NOT MODIFY ------------------------------------------------ #
CONFTEST_PATH = Path(os.path.abspath(__file__)).parent  # will return abs path of conftest.py
CFG_PATH = Path(CONFTEST_PATH, 's3tests.conf')
# -------------------------------------------- DO NOT MODIFY ------------------------------------------------ #

# -------------------------------------------- Gen s3cfg from s3tests.conf start ---------------------------- #
S3CFG = Munch()  # global dict for users.


def _add_default_section(cfg: RawConfigParser) -> None:
    """Add default section to S3CFG"""
    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')

    _defaults = cfg.defaults()
    S3CFG.default_host = _defaults.get("host")
    S3CFG.default_port = int(_defaults.get("port"))
    S3CFG.default_is_secure = cfg.getboolean('DEFAULT', "is_secure")
    S3CFG.ssh_user = _defaults.get("ssh_user")
    S3CFG.ssh_passwd = _defaults.get("ssh_passwd")
    S3CFG.ssh_port = int(_defaults.get("ssh_port"))

    proto = 'https' if S3CFG.default_is_secure else 'http'
    S3CFG.default_endpoint = f"{proto}://{S3CFG.default_host}:{S3CFG.default_port}"
    S3CFG.default_ssl_verify = cfg.getboolean('DEFAULT', "ssl_verify")

    # Disable InsecureRequestWarning reported by urllib3 when ssl_verify is False
    if not S3CFG.default_ssl_verify:
        urllib3.disable_warnings()


def _add_fixture_section(cfg: RawConfigParser) -> None:
    """Add fixture section to S3CFG"""
    if not cfg.has_section("fixtures"):
        raise RuntimeError('Your config file is missing the fixtures section!')

    def choose_bucket_prefix(template, max_len=30):
        """
        Choose a prefix for our test buckets, so they're easy to identify.

        Use template and feed it more and more random filler, until it's
        as long as possible but still below max_len.
        """
        rand = ''.join(
            random.choice(string.ascii_lowercase + string.digits)
            for c in range(255)
        )

        while rand:
            s = template.format(random=rand)
            if len(s) <= max_len:
                return s
            rand = rand[:-1]

        raise RuntimeError(
            'Bucket prefix template is impossible to fulfill: {template!r}'.format(
                template=template,
            ),
        )

    S3CFG.bucket_prefix = choose_bucket_prefix(cfg.get('fixtures', "bucket prefix"))


def _add_s3main_section(cfg: RawConfigParser) -> None:
    """Add s3 main section to S3CFG"""
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')

    S3CFG.main_access_key = cfg.get('s3 main', "access_key")
    S3CFG.main_secret_key = cfg.get('s3 main', "secret_key")
    S3CFG.main_display_name = cfg.get('s3 main', "display_name")
    S3CFG.main_user_id = cfg.get('s3 main', "user_id")
    S3CFG.main_email = cfg.get('s3 main', "email")
    S3CFG.storage_classes = cfg.get('s3 main', "storage_classes")
    S3CFG.lc_debug_interval = int(cfg.get('s3 main', "lc_debug_interval"))

    S3CFG.main_api_name = cfg.get('s3 main', "api_name") if cfg.has_option('s3 main', "api_name") else None
    S3CFG.main_kms_keyid = cfg.get('s3 main', "kms_keyid") if cfg.has_option('s3 main', "kms_keyid") else None
    S3CFG.main_kms_keyid2 = cfg.get('s3 main', "kms_keyid2") if cfg.has_option('s3 main', "kms_keyid2") else None


def _add_s3alt_section(cfg: RawConfigParser) -> None:
    """Add s3 alt section to S3CFG"""
    if not cfg.has_section("s3 alt"):
        raise RuntimeError('Your config file is missing the "s3 alt" section!')

    S3CFG.alt_access_key = cfg.get('s3 alt', "access_key")
    S3CFG.alt_secret_key = cfg.get('s3 alt', "secret_key")
    S3CFG.alt_display_name = cfg.get('s3 alt', "display_name")
    S3CFG.alt_user_id = cfg.get('s3 alt', "user_id")
    S3CFG.alt_email = cfg.get('s3 alt', "email")


@pytest.fixture(scope="session", autouse=True)
def s3cfg_global_unique(pytestconfig: Any) -> Munch:
    """
        Read s3tests.conf, provide different values.
        fixture s3cfg_global_unique is global unique in the package, do not redefine.
    """
    s3cfg = RawConfigParser()

    cfg_path = pytestconfig.getoption('--s3cfg')
    try:
        fp = open(cfg_path)  # check the file exist or not.
        fp.close()
    except FileNotFoundError:
        raise RuntimeError(
            """
            To run tests, please confirm that s3tests.conf was provided.\n
            You can use --s3cfg to configure it, or you can put it into the s3tests_pytest directory.
            """
        )

    s3cfg.read(cfg_path)

    _add_default_section(s3cfg)
    _add_s3main_section(s3cfg)
    _add_s3alt_section(s3cfg)
    _add_fixture_section(s3cfg)

    # global parameter
    S3CFG.bucket_counter = itertools.count(1)

    return S3CFG


def pytest_addoption(parser: Any) -> None:
    group = parser.getgroup("s3tests", "S3Tests")
    group.addoption(
        "--s3cfg",
        default=CFG_PATH,
        help="s3tests.conf path, defaults to s3tests_ess/s3tests.conf.",
    )

# -------------------------------------------- Gen s3cfg from s3tests.conf end ---------------------------- #


# -------------------------------------------- Enhancing report start -------------------------- #
# modify header section
REPORT_TITLE = "S3 Compatibility Automation Test Report"


def pytest_html_report_title(report):
    """ Called before adding the title to the report """
    report.title = REPORT_TITLE


# modify Environment section
# To modify the Environment section before tests are run, use pytest_configure
def pytest_configure(config):
    # config._metadata["Tester"] = "PTC Automation Test"
    # config._metadata.pop("JAVA_HOME")
    pass


# To modify the Environment section after tests are run, use pytest_sessionfinish:
@pytest.hookimpl(tryfirst=True)
def pytest_sessionfinish(session, exitstatus):
    # session.config._metadata["foo2"] = "bar2"
    pass


# modify summary section
# Additional summary information
def pytest_html_results_summary(prefix, summary, postfix):
    """ Called before adding the summary section to the report """
    prefix.extend([html.p("Department : Products Testing Center")])
    prefix.extend(([html.p("Test Group: PTC Automation-Test Group")]))


# modify result section
def pytest_html_results_table_header(cells):
    """ Called after building results table header. """
    cells.insert(2, html.th('TestCase Description'))
    cells.insert(1, html.th('Start Time', class_='sortable time', col='time'))
    cells.pop()


def pytest_html_results_table_row(report, cells):
    """ Called after building results table row. """
    cells.insert(2, html.td(report.description))
    cells.insert(1, html.td(datetime.utcnow(), class_='col-time'))
    cells.pop()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    setattr(report, "duration_formatter", "%H:%M:%S.%f")  # Formatting the Duration Column
    report.description = str(item.function.__doc__)  # case docs

    # report.nodeid = report.nodeid.encode("utf-8").decode("unicode_escape")  # resolve Chinese


def pytest_html_results_table_html(report, data):
    """ Called after building results table additional HTML. """
    # if passed, del log
    if report.passed:
        del data[:]
        data.append(html.div("No log output captured.", class_="empty log"))

# -------------------------------------------- Enhancing report end ---------------------------- #
