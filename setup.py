#!/usr/bin/env python
import os
from numpy.distutils.core import setup, Extension

setup(
    name='cpr',
    version='0.0.1',
    author='Hidrologia SIATA',
    author_email='hidrosiata@gmail.com',
    packages=['cpr'],
    package_data={'cpr':['Nivel.py','SqlDb.py','static.py','information.py']},
    url='https://github.com/SIATAhidro/CPR.git',
    license='LICENSE.txt',
    description='Consultas-Plots y Reportes',
    long_description=open('README.md').read(),
    install_requires=[ ],
    )
