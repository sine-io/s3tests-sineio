#!/usr/bin/python
from setuptools import setup, find_packages


setup(
    name='s3tests-sineio',
    version='0.0.0.1',
    packages=find_packages(exclude=['contrib', 'docs', 'tests', 'build', 'dist']),

    author='sine',
    author_email='sinecelia.wang@gmail.com',
    maintainer='sine',
    maintainer_email='sinecelia.wang@gmail.com',
    url='https://github.com/sine-io/s3tests-sineio.git',
    description='Unofficial Amazon AWS S3 compatibility tests',
    license='MIT',
    keywords='s3 pytest testing',

    python_requires='>=3.6',
    install_requires=[
        'boto3 >=1.0.0',
        'munch >=2.0.0',
        'isodate >=0.4.4',
        'pytest >= 7.1.1',
        'requests >= 2.22.0',
    ],

    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        'Framework :: Pytest',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Testing',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Operating System :: OS Independent',
        'License :: OSI Approved :: MIT License',
    ],
)
