from setuptools import setup

setup(name="cptools2",
      version=0.2,
      url="https://github.com/swarchal/cptools2",
      description="Tools for running CellProfiler on HPC setups",
      author="Scott Warchal",
      license="MIT",
      packages=["cptools2"],
      tests_require=["pytest"],
      python_requires=">=3.5",
      entry_points={
          "console_scripts": ["cptools2 = cptools2.__main__:main"]
          },
      dependency_links=[
            "https://github.com/carragherlab/parserix/tarball/new_ix#egg=parserix-0.1",
            "https://github.com/carragherlab/scissorhands/tarball/master#egg=scissorhands-0.1",
      ],
      install_requires=["pyyaml", "pandas>=0.16", "parserix>=0.1", "scissorhands>=0.1"])
