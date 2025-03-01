from setuptools import setup, find_packages

setup(
    name="openmatch",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "sqlalchemy>=1.4.0",
        "psycopg2-binary>=2.9.0",
        "pydantic>=1.8.0",
        "numpy>=1.21.0",
        "pandas>=1.3.0",
        "python-Levenshtein>=0.12.0",
        "python-dateutil>=2.8.0",
    ],
    python_requires=">=3.8",
    author="OpenMatch Team",
    author_email="info@openmatch.dev",
    description="Master Data Management System",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/openmatch/openmatch",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)
