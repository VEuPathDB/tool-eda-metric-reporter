#!/usr/bin/env python

from distutils.core import setup

# Enables us to pip install our package and import modules using usagemetrics.*. See README for more details.
setup(name='usagemetrics',
      version='1.0',
      package_dir={"": "src"},
      packages=["usagemetrics"],
      description='Usage Metrics functionality',
      install_requires=['pandas', 'cx_Oracle', 'dataclasses', 'python-ldap'],
      author='VEuPathDB')
