from pybuilder.core import use_plugin, init, Project, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
use_plugin("python.distutils")

name = "sparkle-session"
summary = "Spark Session and DataFrame extensions"
description = "Common patterns and often used code from dozens of pyspark projects available at your fingertips"
default_task = ["clean", "analyze", "publish"]
version = "1.2.0"

url = "https://github.com/machielg/sparkle-session/"
licence = "GPLv3+"

authors = [Author("Machiel Keizer Groeneveld", "machielg@cyrstalline.io")]


@init
def set_properties(project: Project):
    project.set_property("distutils_classifiers", [
        "License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)",
        "Environment :: Console",
        'Programming Language :: Python :: 3.7',
        'Topic :: Software Development :: Testing'
    ])
