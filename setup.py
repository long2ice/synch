import os
import re

from setuptools import find_packages, setup


def version():
    ver_str_line = open('mysql2ch/__init__.py', 'rt').read()
    mob = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", ver_str_line, re.M)
    if not mob:
        raise RuntimeError("Unable to find version string")
    return mob.group(1)


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    long_description = f.read()


def requirements():
    return open('requirements.txt', 'rt').read().splitlines()


setup(
    name='mysql2ch',
    version=version(),
    description='Sync data from MySQL to ClickHouse, support full and increment ETL.',
    author='long2ice',
    long_description_content_type='text/x-rst',
    long_description=long_description,
    author_email='long2ice@gmail.com',
    url='https://github.com/long2ice/mysql2ch',
    license='MIT License',
    packages=find_packages(include=['mysql2ch*']),
    include_package_data=True,
    zip_safe=True,
    entry_points={
        'console_scripts': ['mysql2ch = mysql2ch.cli:cli'],
    },
    platforms='any',
    keywords=(
        'MySQL ClickHouse Sync'
    ),
    install_requires=requirements(),
)
