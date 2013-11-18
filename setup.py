#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
import os

from setuptools import setup

basedir = os.path.dirname(__file__)


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
    download_url = "https://github.com/twidi/redis-limpyd-jobs/tags",
    packages = ['limpyd_jobs', 'limpyd_jobs.scripts'],
    include_package_data=True,
    install_requires=["redis-limpyd", "redis-limpyd-extensions", "python-dateutil", "setproctitle"],
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
    ],
)

