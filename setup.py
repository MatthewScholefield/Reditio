from setuptools import setup

setup(
    name='reditio',
    version='0.2.0',
    description='Simple typed Python interface to Redis',
    url='https://github.com/MatthewScholefield/reditio',
    author='Matthew D. Scholefield',
    author_email='matthew331199@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    keywords='reditio',
    py_modules=['reditio'],
    install_requires=[
        'redis',
        'pydantic',
    ],
    extras_require={'dev': ['pytest']},
)
