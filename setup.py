
from setuptools import setup

setup(
    name='importia',
    version='0.3.0',
    description='Integration tools for import.io',
    url='http://github.com/ajrowr/portia/',
    author='Alan Rowarth',
    author_email='alan@codex.cx',
    license='MIT',
    packages=['portia'],
    install_requires=['requests'],
    zip_safe=False
)

