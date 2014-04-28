# This should be only one line. If it must be multi-line, indent the second
# line onwards to keep the PKG-INFO file format intact.
"""Backup and restore for block devices.
"""

from setuptools import setup, find_packages
import glob
import os.path


def project_path(*names):
    return os.path.join(os.path.dirname(__file__), *names)


setup(
    name='backy',
    version='2.0a1dev',
    install_requires=[
        'setuptools',
        'fallocate',
    ],
    extras_require={
        'test': [
        ],
    },
    entry_points="""
        [console_scripts]
            backy = backy.main:main

        [backy.sources]
            ceph-rbd = backy.sources.ceph:CephRBD
            file = backy.sources.file:File

    """,
    author='Daniel Kraft <daniel.kraft@d9t.de>, Christian Theune <ct@gocept.com>',
    author_email='daniel.kraft@d9t.de',
    license='GPL 3',
    url='https://bitbucket.org/ctheune/backy/',
    keywords='backup',
    classifiers="""\
License :: OSI Approved :: GPL
Programming Language :: Python
Programming Language :: Python :: 3
Programming Language :: Python :: 3.3
Programming Language :: Python :: 3 :: Only
"""[:-1].split('\n'),
    description=__doc__.strip(),
    long_description='\n\n'.join(open(project_path(name)).read() for name in (
        'README.md',
        'CHANGES.txt',
        )),
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    data_files=[('', glob.glob(project_path('*.txt')))],
    zip_safe=False,
)
