from setuptools import setup, find_packages
VERSION = '0.0.1'
SHORT_DESCRIPTION = 'Shared code used between various figgy projects'
LONG_DESCRIPTION = 'This codebase is not needed for general figgy use. ' \
                   'This is maintained and used by the figgy creators.'

with open('./requirements.txt', 'r') as file:
    requirements = file.readlines()

setup(
    name="figgy-lib",
    packages=find_packages() + ['.'],
    entry_points={
        "console_scripts": ['figgy = figgy:main']
    },
    version=VERSION,
    description=SHORT_DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    author="Jordan Mance",
    author_email="jordan@figgy.dev",
    url='https://github.com/mancej/figgy.lib',
    python_requires='>=3.7',
    install_requires=requirements
)