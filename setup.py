import setuptools

with open("README.md", 'r') as fh:
    long_description = fh.read()

setuptools.setup(
        name="dalio",
        version="0.0.1",
        description="graphical workflow for financial modeling",
        author="Renato Mateus Zimmermann",
        author_email="renatomatz@gmail.com",
        url="https://dalio.readthedocs.io/en/latest/",
        packages=setuptools.find_packages("dalio"),
        package_dir={"": "dalio"},
        classifiers=[
            "Development Status :: 2 - Pre-Alpha",
            "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
            "Programming Language :: Python :: 3 :: Only",
            "Programming Language :: Python :: 3.7",
            "Operating System :: OS Independent"
        ],
        long_description=long_description,
        long_description_content_type="text/markdown",
)
