from setuptools import setup, find_packages

description = 'A tool replication data from MySQL to ClickHouse.'
setup(
    name='mysql2ch',
    author='long2ice',
    author_email='long2ice@gamil.com',
    version='0.0.1',
    packages=find_packages(),
    description=description,
    long_description=description,
    entry_points={
        'console_scripts': ['mysql2ch = mysql2ch.main:cli'],
    },
)
