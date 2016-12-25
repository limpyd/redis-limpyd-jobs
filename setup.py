#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
from pip.req import parse_requirements
import os
from setuptools import setup, find_packages

# The `session` argument for the `parse_requirements` function is available (but
# optional) in pip 1.5, and mandatory in next versions
try:
    from pip.download import PipSession
except ImportError:
    parse_args = {}
else:
    parse_args = {'session': PipSession()}

basedir = os.path.dirname(__file__)


def get_requirements(source):
    install_reqs = parse_requirements(source, **parse_args)
    return set([str(ir.req) for ir in install_reqs])


requirements = get_requirements('requirements.txt')


def get_infos():
    with codecs.open(os.path.join(basedir, 'limpyd_jobs/version.py'), "r", "utf-8") as f:
        locals = {}
        exec(f.read(), locals)
        return {
            '__doc__': locals['__doc__'],
            '__version__': locals['__version__'],
            '__author__': locals['__author__'],
            '__contact__': locals['__contact__'],
            '__homepage__': locals['__homepage__'],
        }
    raise RuntimeError('No infos found.')


long_description = codecs.open(os.path.join(basedir, 'README.rst'), "r", "utf-8").read()
infos = get_infos()

setup(
    name = "redis-limpyd-jobs",
    version = infos['__version__'],
    author = infos['__author__'],
    author_email = infos['__contact__'],
    description = infos['__doc__'],
    keywords = "redis, jobs",
    url = infos['__homepage__'],
    download_url = "https://github.com/limpyd/redis-limpyd-jobs/tags",
    packages = find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=requirements,
    license = "DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE",
    platforms=["any"],
    zip_safe=True,

    entry_points = {
        'console_scripts': [
            'limpyd-jobs-worker = limpyd_jobs.scripts.worker:main',
        ],
    },

    long_description = long_description,

    classifiers = [
        "Development Status :: 3 - Alpha",
        #"Environment :: Web Environment",
        "Intended Audience :: Developers",
        #"License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
    ],
)
