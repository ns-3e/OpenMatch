from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="openmatch",
    version="0.1.0",
    author="Nick Smith",
    author_email="nsmith@skailr.io",
    description="Enterprise-Grade Master Data Management Library",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ns-3e/OpenMatch",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "numpy>=1.19.0",
        "pandas>=1.2.0",
        "scikit-learn>=0.24.0",
        "sentence-transformers>=2.0.0",
        "faiss-cpu>=1.7.0",
        "pyyaml>=5.4.1",
        "networkx>=2.5",
        "phonenumbers>=8.12.0",
        "python-Levenshtein>=0.12.0",
        "jellyfish>=0.8.0",
        "recordlinkage>=0.14",
    ],
    extras_require={
        "all": [
            "databricks-connect>=7.3",
            "snowflake-snowpark-python>=0.7.0",
            "azure-synapse-spark>=0.7.0",
            "fastapi>=0.68.0",
            "uvicorn>=0.15.0",
            "torch>=1.9.0",
            "transformers>=4.5.0",
            "dask>=2021.6.0",
            "great-expectations>=0.13.0",
        ],
        "cloud": [
            "databricks-connect>=7.3",
            "snowflake-snowpark-python>=0.7.0",
            "azure-synapse-spark>=0.7.0",
        ],
        "ml": [
            "torch>=1.9.0",
            "transformers>=4.5.0",
        ],
        "api": [
            "fastapi>=0.68.0",
            "uvicorn>=0.15.0",
        ],
        "dev": [
            "pytest>=6.0.0",
            "pytest-cov>=2.12.0",
            "black>=21.5b2",
            "isort>=5.9.0",
            "flake8>=3.9.0",
            "mypy>=0.910",
            "sphinx>=4.0.0",
            "sphinx-rtd-theme>=0.5.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "openmatch=openmatch.cli:main",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/ns-3e/OpenMatch/issues",
        "Source Code": "https://github.com/ns-3e/OpenMatch",
    },
)
