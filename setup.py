# This should be only one line. If it must be multi-line, indent the second
# line onwards to keep the PKG-INFO file format intact.
"""Backup and restore for block devices.
"""

from setuptools import setup, find_packages
import glob
import os.path


def project_path(*names):
    return os.path.join(os.path.dirname(__file__), *names)


def long_desc():
    parts = []
    for name in ('README.txt', 'CHANGES.txt'):
        with open(project_path(name)) as f:
            parts.append(f.read())
    return '\n\n'.join(parts)

setup(
    name='backy',
    version='2.0b2',
    install_requires=[
        'consulate',
        'fallocate',
        'nagiosplugin',
        'prettytable',
        'pytest-asyncio',
        'pytz',
        'PyYaml',
        'setuptools',
        'shortuuid',
        'telnetlib3',
    ],
    extras_require={
        'test': [
        ],
    },
    entry_points="""
        [console_scripts]
            backy = backy.main:main

        [backy.sources]
            ceph-rbd = backy.sources.ceph.source:CephRBD
            file = backy.sources.file:File
            flyingcircus = \
                backy.sources.flyingcircus.source:FlyingCircusRootDisk

    """,
    author=('Christian Theune <ct@flyingcircus.io>, '
            'Daniel Kraft <daniel.kraft@d9t.de>'),
    author_email='ct@flyingcircus.io',
    license='GPL-3',
    url='https://bitbucket.org/flyingcircus/backy/',
    keywords='backup',
    classifiers="""\
Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.2
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Topic :: System :: Archiving :: Backup
"""[:-1].split('\n'),
    description=__doc__.strip(),
    long_description=long_desc(),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    data_files=[('', glob.glob(project_path('*.txt')))],
    zip_safe=False,
)
