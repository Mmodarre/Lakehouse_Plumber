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
    entry_points={
        "console_scripts": [
            "lhp=lhp.cli.main:cli",
        ],
    },
) 