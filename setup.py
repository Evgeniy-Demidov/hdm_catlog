from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='hdm_catalog',  # Replace with your package name
    version='0.1.0',  # Replace with your package version
    packages=find_packages(),  # Automatically find all packages and subpackages
    install_requires=requirements,
    #long_description=open('README.md').read(),  # If you have a README file, you can use it as the long description
)