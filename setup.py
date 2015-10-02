"""Backup and restore for block devices."""

from setuptools import setup, find_packages
import codecs
import glob
import os.path as p


def open_project_path(filename):
    fullname = p.join(p.dirname(__file__), filename)
    return codecs.open(fullname, encoding='ascii')


def long_desc():
    parts = []
    for name in ('README.txt', 'CHANGES.txt'):
        with open_project_path(name) as f:
            parts.append(f.read())
    return '\n'.join(parts)


def version():
    with open_project_path('version.txt') as f:
        return f.read().strip()


setup(
    name='backy',
    version=version(),
    install_requires=[
        'consulate',
        'fallocate',
        'nagiosplugin',
        'prettytable',
        'pytz',
        'PyYaml',
        'setuptools',
        'shortuuid',
        'telnetlib3',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-asyncio',
            'pytest-cache',
            'pytest-capturelog',
            'pytest-codecheckers',
            'pytest-cov',
            'pytest-timeout',
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
            'Christian Kauhaus <kc@flyingcircus.io>, '
            'Daniel Kraft <daniel.kraft@d9t.de>'),
    author_email='ct@flyingcircus.io',
    license='GPL-3',
    url='https://bitbucket.org/flyingcircus/backy',
    keywords='backup',
    classifiers="""\
Development Status :: 4 - Beta
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3.4
Topic :: System :: Archiving :: Backup
"""[:-1].split('\n'),
    description=__doc__.strip(),
    long_description=long_desc(),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    data_files=[('', glob.glob('*.txt'))],
    zip_safe=False,
)
