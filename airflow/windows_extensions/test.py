from airflow.configuration import AirflowConfigParser
from airflow.windows_extensions import signal
from time import sleep
import os
from cryptography.fernet import Fernet
from base64 import b64encode


def expand_env_var(env_var):
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated

def get_airflow_home():
    """Get path to Airflow Home"""
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))

def parameterized_config(template):
    """
    Generates a configuration from the provided template + variables defined in
    current scope

    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)  # noqa

def _default_config_file_path(file_name: str):
    templates_dir = os.path.join(os.path.dirname(__file__), '..', 'config_templates')
    return os.path.join(templates_dir, file_name)

def _parameterized_config_from_template(filename) -> str:
    TEMPLATE_START = '# ----------------------- TEMPLATE BEGINS HERE -----------------------\n'

    path = _default_config_file_path(filename)
    with open(path) as fh:
        for line in fh:
            if line != TEMPLATE_START:
                continue
            return parameterized_config(fh.read().strip())
    raise RuntimeError(f"Template marker not found in {path!r}")


FERNET_KEY = Fernet.generate_key().decode()
SECRET_KEY = b64encode(os.urandom(16)).decode('utf-8')
AIRFLOW_HOME = get_airflow_home()
default_config = _parameterized_config_from_template('default_airflow.cfg')
conf = AirflowConfigParser(default_config=default_config)
conf.read(r"C:\Users\dwerner\airflow\airflow.cfg")


print(signal.SIGALRM)
print(signal.ITIMER_REAL)

def handle_timeout(signum, frame):  # pylint: disable=unused-argument
    """Logs information and raises AirflowTaskTimeout."""
    raise TimeoutError()


signal.signal(signal.SIGALRM, handle_timeout)
signal.setitimer(signal.ITIMER_REAL, 1)

sleep(4)
signal.setitimer(signal.ITIMER_REAL, 0)
#signal.setitimer(3, 5)