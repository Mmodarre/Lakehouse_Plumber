from setuptools import setup, find_packages

setup(
    name="lakehouse-plumber",
    version="0.1.0",
    description="Action-based DLT pipeline generator",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pydantic>=2.0",
        "jinja2>=3.0",
        "click>=8.0",
        "black>=23.0",
        "jsonschema>=4.0",
        "pyyaml>=6.0",
    ],
    extras_require={
        "notebook": [
            "ipython>=7.0",
            "jupyter>=1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "lhp=lhp.cli.main:cli",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
) 