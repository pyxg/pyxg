#!/usr/bin/env python

from distutils.core import setup
import bdist_mpkg

setup(name='PyXG',
    version='0.2.0',
    description='A Python interface to Xgrid',
    author='Brian Granger/Barry Wark',
    author_email='ellisonbg@gmail.com',
    url='http://pyxg.scipy.org',
    py_modules=['xg']
    )
