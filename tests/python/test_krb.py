import signal
import socket
import subprocess
import utils

REALM = 'EXAMPLE.COM'
SUPPORTED_ENCRYPTION_TYPES = 'aes256-cts-hmac-sha1-96:normal'
KADMIN_PRINCIPAL = 'root'
KADMIN_PASSWORD = 'root'
KDC_KADMIN_SERVER = socket.gethostname()

LOGDIR = 'log'
PG_LOG = f'{LOGDIR}/krb.log'
# Assumes packages are installed; krb5-kdc and krb5-admin-server on debian
KADMIN_PRINCIPAL_FULL = f'{KADMIN_PRINCIPAL}@{REALM}'
MASTER_PASSWORD = 'master_password'


def setup_krb():
    krb5_conf = f"""
[libdefaults]
        default_realm = {REALM}
        rdns = false

[realms]
        {REALM} = {{
                kdc_ports = 88,750
                kadmind_port = 749
                kdc = {KDC_KADMIN_SERVER}
                admin_server = {KDC_KADMIN_SERVER}
        }}
    """
    with open("/etc/krb5.conf", "w") as text_file:
        text_file.write(krb5_conf)

    kdc_conf = f"""
[realms]
        {REALM} = {{
                acl_file = /etc/krb5kdc/kadm5.acl
                max_renewable_life = 7d 0h 0m 0s
                supported_enctypes = {SUPPORTED_ENCRYPTION_TYPES}
                default_principal_flags = +preauth
        }}
    """
    with open("/etc/krb5kdc/kdc.conf", "w") as text_file:
        text_file.write(kdc_conf)

    kadm5_acl = f"""
    {KADMIN_PRINCIPAL_FULL} *
    """
    with open("/etc/krb5kdc/kadm5.acl", "w") as text_file:
        text_file.write(kadm5_acl)

    kerberos_command = f"""
    krb5_newrealm <<EOF
    {MASTER_PASSWORD}
    {MASTER_PASSWORD}
    EOF
    """
    subprocess.run(kerberos_command, check=False, shell=True)

    delete_principal = f'kadmin.local -q "delete_principal -force {KADMIN_PRINCIPAL_FULL}"'
    subprocess.run(delete_principal, check=True, shell=True)

    create_principal = f'kadmin.local -q "addprinc -pw {KADMIN_PASSWORD} {KADMIN_PRINCIPAL_FULL}"'
    subprocess.run(create_principal, check=True, shell=True)

    kinit_command = f'echo {KADMIN_PASSWORD} | kinit'
    subprocess.run(kinit_command, check=True, shell=True)

    utils.pgcat_start()


def teardown_krb():
    subprocess.run('kdestroy', check=True, shell=True)

    delete_principal = f'kadmin.local -q "delete_principal -force {KADMIN_PRINCIPAL_FULL}"'
    subprocess.run(delete_principal, check=True, shell=True)

    utils.pg_cat_send_signal(signal.SIGINT)


def test_krb():
    setup_krb()
    # TODO test connect to database

    utils.pgcat_start()
    conn, cur = utils.connect_db(autocommit=False)
    cur.execute("SELECT 1")
    res = cur.fetchall()
    print(res)
    utils.cleanup_conn(conn, cur)

    teardown_krb()
