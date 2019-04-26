import setuptools  # type: ignore


setuptools.setup(
    name='async-tasks',
    version='0.0.1',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.7',
    ],
    packages=setuptools.find_packages(exclude=['tests']),
    python_requires='>=3.7',
    install_requires=[
        'google-cloud-pubsub',
        'ipython',
        'marshmallow',
        'mypy',
        'psycopg2',
        'pyyaml',
    ],
    extras_require={
        'dev': ['ipython'],
        'test': ['pytest'],
    },
)
