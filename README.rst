========================
 S3 compatibility tests with pytest
========================

This is a set of unofficial Amazon AWS S3 compatibility
tests, that can be useful to people implementing software
that exposes an S3-like API. The tests use the Boto3 libraries.

The tests use the pytest test framework. To get started, ensure you have
the software installed::

	pip install -r requirements.txt -i https://pypi.douban.com/simple

You will need to create a configuration file with the location of the
service and two different credentials. A sample configuration file named
``s3tests.conf.SAMPLE`` has been provided in this repo.

Once you have that file copied and renamed(s3tests.conf)
and edited(``host, port, ssh_user, ssh_passwd, ssh_port``),
you can create users::

    cd s3tests-pytest
    python bootstrap.py

Then you can run the tests with::

	cd s3tests-pytest/s3tests_pytest
	pytest or pytest --s3cfg s3tests.conf-path

You can specify which test to run::

	pytest -k EXPRESSION
	e.g. :
	    pytest -k test_bucket_list_empty
	    pytest -k 'not test_bucket_list_empty'

Some tests have attributes set based on their current reliability and
things like AWS not enforcing their spec strictly. You can filter tests
based on their attributes::

	pytest -m "ess and not lifecycle_need_speedup" -n 50 --reruns 3

Reports in the report directory::

    s3-tests-ehualu
        |___ s3tests_pytest
                |___ report  # <--- here.
                |___ tests

Open it via chrome or firefox.
