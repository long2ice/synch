import re

from setuptools import setup, find_packages


def version():
    ver_str_line = open('mysql2ch/__init__.py', 'rt').read()
    mob = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", ver_str_line, re.M)
    if not mob:
        raise RuntimeError('Unable to find version string')
    return mob.group(1)


def requirements():
    return open('requirements.txt', 'rt').read().splitlines()


setup(
    name='mysql2ch',
    author='long2ice',
    url='https://github.com/long2ice/mysql2ch',
    author_email='long2ice@gamil.com',
    zip_safe=True,
    include_package_data=True,
    version=version(),
    packages=find_packages(include=['mysql2ch*']),
    description='A tool replication data from MySQL to ClickHouse.',
    long_description_content_type='text/x-rst',
    long_description=open('README.rst', 'r').read(),
    entry_points={
        'console_scripts': ['mysql2ch = mysql2ch.main:cli'],
    },
    keywords=('mysql clickhouse replication',),
    install_requires=requirements(),
)
