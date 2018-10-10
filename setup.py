"""Block-based backup and restore utility for virtual machine images"""

from setuptools import setup, find_packages, Command
import codecs
import glob
import os.path as p
import subprocess
import sys


class PyTest(Command):
    """Invoke py.test from `bin/python setup.py test`."""
    user_options = []
    initialize_options = lambda self: None
    finalize_options = lambda self: None

    def run(self):
        errno = subprocess.call([
            sys.executable,
            p.join(p.dirname(__file__), 'bin', 'py.test'), '-m1'])
        raise SystemExit(errno)


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
        'packaging',
        'prettytable',
        'pytz',
        'PyYaml',
        'setuptools',
        'shortuuid',
        'python-lzo',
        'telnetlib3',
        'murmurhash3',
        'structlog',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-asyncio',
            'pytest-cache',
            'pytest-cov',
            'pytest-flake8',
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
    author_email='mail@flyingcircus.io',
    license='GPL-3',
    url='https://bitbucket.org/flyingcircus/backy',
    keywords='backup',
    classifiers="""\
Development Status :: 5 - Production/Stable
Environment :: Console
Intended Audience :: System Administrators
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: POSIX
Programming Language :: Python
Programming Language :: Python :: 3
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
    cmdclass={'test': PyTest},
)
