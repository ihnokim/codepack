import setuptools


with open("README.rst", "r") as f:
    long_description = f.read()


with open("requirements.txt", "r") as f:
    requirements = [x.strip() for x in f.readlines()]


setuptools.setup(
    name="codepack",
    version="0.0.5",
    author="ihnokim",
    author_email="ihnokim58@gmail.com",
    description="Codepack is the package for making workflows with Python",
    long_description=long_description,
    url="https://github.com/ihnokim/codepack",
    packages=setuptools.find_packages(),
    keywords=["codepack", "workflow", "pipeline"],
    install_requires=requirements,
    package_data={'codepack': ['config/default/*.ini', 'employee/scripts/*.py']},
    data_files=[("codepack/config", ["config/conn.ini", "config/logging.json", "config/codepack.ini"])],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires=">=3.6",
)
