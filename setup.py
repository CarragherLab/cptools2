from setuptools import setup

setup(name="cptools2",
      version=0.1,
      url="https://github.com/swarchal/cptools2",
      description="Tools for running CellProfiler on HPC setups",
      author="Scott Warchal",
      license="MIT",
      packages=["cptools2"],
      test_require=["pytest"],
      dependency_links=["https://github.com/swarchal/parserix/tarball/master#egg=parserix-0.1"],
      install_requires=["pandas>=0.16", "parserix>=0.1"])
