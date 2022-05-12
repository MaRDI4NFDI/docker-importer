import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mardi_importer",
    version="0.0.1",
    author="MaRDI TA5",
    author_email="accounts_ta5@mardi4nfdi.de",
    description="Data importer for the MaRDI knowledge graph",
    long_description="file: README.md",
    long_description_content_type="text/markdown",
    url="https://github.com/mardi4nfdi/docker-importer",
    project_urls={
        "Bug Tracker": "https://github.com/mardi4nfdi/docker-importer/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Development Status :: 2 - Pre-Alpha",
    ],
    package_dir={"": "src"},
    packages=setuptools.find_packages(
        where="src",  # exclude=["scripts", "scripts.*"]
    ),
    python_requires=">=3.6",
    install_requires=[
        "sickle",
        "habanero",
        "pandas",
        "lxml",
        "mysql-connector-python",
    ],
    # entry_points={"console_scripts": ["import = scripts.main:main"]},
    #scripts=["scripts/import.py"],
)
