#!/usr/bin/env python

from distutils.core import setup
import bdist_mpkg

setup(name='PyXG',
    version='0.3.0',
    description='A Python interface to Xgrid',
    license='BSD',
    author='Brian Granger/Barry Wark/Beat Rupp',
    author_email='ellisonbg@gmail.com',
    url='http://launchpad.net/pyxg',
    py_modules=['xg'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: MacOS X',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing'
        ]
    )
