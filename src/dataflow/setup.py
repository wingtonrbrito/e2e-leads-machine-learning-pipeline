from distutils.command.build import build as _build
import subprocess

import setuptools


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
    """A build command class that will be invoked during package install.
    The package built using the current setup.py will be staged and later
    installed in the worker using `pip install package'. This class will be
    instantiated during install for this specific scenario and will trigger
    running the custom commands specified.
    """
    sub_commands = _build.sub_commands + [('CustomCommands', None)]


# Some custom command to run during setup. The command is not essential for
# this workflow. It is used here as an example. Each command will spawn a
# child process. Typically, these commands will include steps to install
# non-Python packages.
#
# First, note that there is no need to use the sudo command because the setup
# script runs with appropriate access.
# Second, if apt-get tool is used then the first command needs to be 'apt-get
# update' so the tool refreshes itself and initializes links to download
# repositories.  Without this initial step the other apt-get install commands
# will fail with package not found errors. Note also --assume-yes option which
# shortcuts the interactive confirmation.
#
# The output of custom commands (including failures) will be logged in the
# worker-startup log.
"""
CUSTOM_COMMANDS = [
    ['apt-get', 'update'],
    ['apt-get', '--assume-yes', 'install', 'net-tools'],
    ['apt-get', '--assume-yes', 'install', 'python-dev'],
    ['apt-get', '--assume-yes', 'install', 'libssl1.0.0'],
    ['apt-get', '--assume-yes', 'install', 'libffi-dev'],
    ['apt-get', '--assume-yes', 'install', 'libssl-dev'],
    ['apt-get', '--assume-yes', 'install', 'libxml2-dev'],
    ['apt-get', '--assume-yes', 'install', 'libxslt1-dev'],
    ['pip', 'install', 'pyga==2.5.1'],
    ['pip', 'install', 'MySQL-python==1.2.5'],
    ['pip', 'install', 'fluent-logger==0.4.4'],
    ['pip', 'install', 'phonenumbers==7.7.2'],
    ['pip', 'install', 'python-dateutil==2.5.3'],
    ['pip', 'install', 'google-api-python-client==1.5.4'],
    ['pip', 'install', 'suds==0.4'],
    ['pip', 'install', 'websocket-client==0.37.0'],
    ['pip', 'install', 'tornado==4.4.2'],
    ['pip', 'install', 'progressbar2==3.10.1'],
    ['pip', 'install', 'pyOpenSSL==16.2.0'],
    ['pip', 'install', 'futures==3.0.5'],
    ['pip', 'install', 'requests==2.4.3'],
    ['pip', 'install', 'SQLAlchemy==1.1.2']
]
"""

CUSTOM_COMMANDS = []


class CustomCommands(setuptools.Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print
        'Running command: %s' % command_list
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, _ = p.communicate()
        print
        'Command output: %s' % stdout_data
        if p.returncode != 0:
            print(f'Command {command_list} failed: exit code: {p.returncode}')
            exit(p.returncode)

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


# Configure the required packages and scripts to install.
# Note that the Python Dataflow containers come with numpy already installed
# so this dependency will not trigger anything to be installed unless a version
# restriction is specified.

REQUIRED_PACKAGES = [
    'pymysql==0.9.3',
    'pycloudsqlproxy==0.0.15',
    'pyconfighelper==0.0.8',
    'google-api-python-client==1.11.0',
    'google-cloud-secret-manager==2.0.0',
    'google-cloud-storage==1.31.2',
    'cryptography==2.9.1',
    'neo4j==4.0.0',
    'simplejson==3.17.0'
]

if __name__ == '__main__':
    setuptools.setup(
        name='dataflow-load-sql-to-bq',
        version='0.0.1',
        description='''Dataflow Pipeline to load data from existing CloudSql MySQL
                    database into Big Query''',
        install_requires=REQUIRED_PACKAGES,
        packages=setuptools.find_packages(),
        cmdclass={
            # Command class instantiated and run during pip install scenarios.
            'build': build,
            'CustomCommands': CustomCommands,
        }
    )
