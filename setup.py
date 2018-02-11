from setuptools import setup, find_packages

with open("./pycoki/version.py") as f:
    exec(f.read())

setup(
    name="pycoki",
    version=__version__,
    url="https://github.com/uezo/pycoki",
    author="uezo",
    author_email="uezo@uezo.net",
    maintainer="uezo",
    maintainer_email="uezo@uezo.net",
    description="Pycoki - Python Compatible Key-value-store Interface for databases",
    packages=find_packages(exclude=["examples*", "tests*"]),
    install_requires=["pytz"],
    license="Apache v2",
    classifiers=[
        "Programming Language :: Python :: 3"
    ]
)
