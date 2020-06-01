from setuptools import setup, find_packages


setup(
    name="uengine",
    version="3.5.21",
    description="a micro webframework based on flask and pymongo",
    url="https://github.com/viert/uengine",
    author="Pavel Vorobyov",
    author_email="aquavitale@yandex.ru",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "uengine": ["*.txt", "*.py"]
    },
    install_requires=[
        "jinja2",
        "flask",
        "pymongo",
        "cachelib",
        "lazy_object_proxy",
        "ipython",
        "pylint",
        "mongomock",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "uengine=uengine.__main__:main",
        ]
    }
)
