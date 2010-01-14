#!/usr/bin/env python

from distutils.core import setup
import bdist_mpkg

setup(name='PyXG',
    version='0.3.0',
    description='A Python interface to Xgrid',
    author='Brian Granger/Barry Wark/Beat Rupp',
    author_email='ellisonbg@gmail.com',
    url='http://pyxg.scipy.org',
    py_modules=['xg']
    )
