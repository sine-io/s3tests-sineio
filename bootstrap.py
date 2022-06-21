
import os
import sys
from pathlib2 import Path
from pprint import pprint

from configparser import RawConfigParser
from fabric import Connection


BASE_PATH = Path(os.path.abspath(__file__)).parent  # will return abs path of bootstrap.py
CFG_PATH = Path(BASE_PATH, 's3tests_pytest/s3tests.conf')

s3cfg = RawConfigParser()
s3cfg.read(CFG_PATH)

# default section.
_defaults = s3cfg.defaults()
default_host = _defaults.get("host")
ssh_user = _defaults.get("ssh_user")
ssh_passwd = _defaults.get("ssh_passwd")
ssh_port = int(_defaults.get("ssh_port"))

# main user section.
main_access_key = s3cfg.get('s3 main', "access_key")
main_secret_key = s3cfg.get('s3 main', "secret_key")
main_display_name = s3cfg.get('s3 main', "display_name")
main_user_id = s3cfg.get('s3 main', "user_id")
main_email = s3cfg.get('s3 main', "email")

# alt user section.
alt_access_key = s3cfg.get('s3 alt', "access_key")
alt_secret_key = s3cfg.get('s3 alt', "secret_key")
alt_display_name = s3cfg.get('s3 alt', "display_name")
alt_user_id = s3cfg.get('s3 alt', "user_id")
alt_email = s3cfg.get('s3 alt', "email")


def exec_cmd(command, **kwargs):
    conn = Connection(
        host=default_host,
        user=ssh_user,
        port=ssh_port,
        connect_kwargs={
            "password": ssh_passwd
        },
        **kwargs
    )
    return conn.run(command, hide=True)


if __name__ == "__main__":
    print("******************************************************************************")
    print("0. Connect to the host via ssh and get the default zone group in the ceph cluster.")
    res = exec_cmd("radosgw-admin zonegroup list")
    api_name = eval(res.stdout).get('zonegroups')
    print("zonggroup is: ", api_name[0])

    print("******************************************************************************")
    print("1. Set the api_name in the s3tests.conf --- start.")
    s3cfg.set('s3 main', "api_name", api_name[0])
    with open(CFG_PATH, "w") as fp:
        s3cfg.write(fp)
    print("1. Set the api_name in the s3tests.conf --- done.")

    print("******************************************************************************")
    print("1. Create the main s3 user --- start.")
    res = exec_cmd(f"radosgw-admin user create --uid {main_user_id} --display-name {main_display_name} "
                   f"--access-key {main_access_key} --secret-key {main_secret_key} --email {main_email}")
    if not res.stderr:
        pprint(res.stdout)
    else:
        print("Error: ", res.stderr)
        sys.exit(0)
    print("1. Create the main s3 user --- end.")

    print("******************************************************************************")
    print("2. Create the alt s3 user --- start.")
    res = exec_cmd(f"radosgw-admin user create --uid {alt_user_id} --display-name {alt_display_name} "
                   f"--access-key {alt_access_key} --secret-key {alt_secret_key} --email {alt_email}")
    if not res.stderr:
        pprint(res.stdout)
    else:
        print("Error: ", res.stderr)
        sys.exit(0)
    print("2. Create the alt s3 user --- end.")
    print("******************************************************************************")
