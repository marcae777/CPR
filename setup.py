#!/usr/bin/env python
import os
from numpy.distutils.core import setup, Extension

setup(
    name='cpr_v1',
    version='1.0.0',
    author='Hidrologia SIATA',
    author_email='hidrosiata@gmail.com',
    packages=['cpr_v1'],
    package_data={'cpr_v1':['Level','SqlDb.py','settings.py','static.py']},
    url='https://github.com/SIATAhidro/CPR.git',
    license='LICENSE.txt',
    description='Consultas-Plots y Reportes',
    long_description=open('README.md').read(),
    install_requires=[ ],
    )
