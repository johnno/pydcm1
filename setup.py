from setuptools import setup

version = '0.2'

with open("README.md", "r", encoding="utf-8") as f:
    long_descr = f.read()

setup(
    name='pydcm1',
    packages=['pydcm1'],
    version=version,
    license='Apache 2.0',
    description='Control Cloud DCM1 Zone Mixer',
    long_description=long_descr,
    long_description_content_type='text/markdown',    
    author='johnno',
    author_email='johnno@example.com',
    url='https://github.com/johnno/pydcm1',
    download_url=f'https://github.com/johnno/pydcm1/archive/{version}.tar.gz',
    keywords=['Cloud', 'DCM1', 'Zone Mixer'],
    install_requires=[
        "aiohttp>=3.8.3",
        "xmltodict>=0.11.0"
    ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.10'
    ],
)
