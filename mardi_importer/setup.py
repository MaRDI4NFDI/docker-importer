import setuptools

setuptools.setup(
    name="mardi_importer",
    version="0.0.1",
    author="MaRDI TA5",
    author_email="accounts_ta5@mardi4nfdi.de",
    description="Data importer for the MaRDI knowledge graph",
    url="https://github.com/mardi4nfdi/docker-importer",
    project_urls={
        "Bug Tracker": "https://github.com/mardi4nfdi/docker-importer/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Development Status :: 2 - Pre-Alpha",
    ],
    packages=['mardi_importer'],
    python_requires=">=3.6",
    install_requires=[
        "bs4",
        "configparser",
        "feedparser",
        "habanero",
        "lxml",
        "mysql-connector-python",
        "nameparser",
        "openml",
        "pandas",
        "sickle",
        "sparqlwrapper",
        "sqlalchemy",
        "validators",
        "wikibaseintegrator"
    ],
    # entry_points={"console_scripts": ["import = scripts.main:main"]},
    # scripts=["scripts/import.py"],
)
