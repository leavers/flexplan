import os
import re

from setuptools import setup


def get_version(package: str = 'pipegram'):
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    with open(os.path.join(package, "__init__.py")) as f:
        return re.search(r'__version__\s*=\s*[\'"]([^\'"]+)[\'"]', f.read()).group(1)


def get_readme(file: str = 'README.md'):
    """
    Return the README.
    """
    with open(file, encoding='utf-8') as f:
        return f.read()


def get_packages(package: str = 'pipegram'):
    """
    Return root package and all sub-packages.
    """
    return [
        dir_path
        for dir_path, dir_names, filenames in os.walk(package)
        if os.path.exists(os.path.join(dir_path, '__init__.py'))
    ]


setup(
    name='pipegram',
    python_requires='>=3.8',
    version=get_version(),
    url='https://github.com/encode/starlette',
    license='MIT',
    description='Create and manage workflow pipelines.',
    long_description=get_readme(),
    long_description_content_type='text/markdown',
    author='Leavers',
    author_email='leavers930@gmail.com',
    packages=get_packages(),
    # keywords=[],
    # classifiers=[
    #     "Development Status :: 3 - Alpha",
    #     "Environment :: Web Environment",
    #     "Intended Audience :: Developers",
    #     "License :: OSI Approved :: BSD License",
    #     "Operating System :: OS Independent",
    #     "Topic :: Internet :: WWW/HTTP",
    #     "Programming Language :: Python :: 3",
    #     "Programming Language :: Python :: 3.6",
    #     "Programming Language :: Python :: 3.7",
    #     "Programming Language :: Python :: 3.8",
    #     "Programming Language :: Python :: 3.9",
    # ],
    zip_safe=False,
    # package_data={'pipegram': ['py.typed']},
    # include_package_data=True,
    # extras_require={
    #     "full": [
    #         "aiofiles",
    #         "graphene",
    #         "itsdangerous",
    #         "jinja2",
    #         "python-multipart",
    #         "pyyaml",
    #         "requests",
    #     ]
    # },
)
