{ }:

let
  pkgs = import <nixpkgs> { };

  python = pythonPackages.python;
  pythonPackages = pkgs.python34Packages;

  telnetlib3 = pythonPackages.buildPythonPackage rec {
    name = "telnetlib3-0.2.3";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/t/telnetlib3/${name}.tar.gz";
      md5 = "964a2f7f9b1b0b7f9024942fa413fc94";
    };
# telnetlib3 has problems with newer setuptools
# https://github.com/jquast/telnetlib3/issues/8
# asyncio is already included in Python >= 3.4
    patchPhase = ''
      sed -i -e "/^requirements = /d" \
        -e "/^install_requires = /d" \
        -e "/\s\+install_requires=/d" \
        setup.py
    '';
    doCheck = false;
  };

  shortuuid = pythonPackages.buildPythonPackage rec {
    name = "shortuuid-0.4.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/s/shortuuid/${name}.tar.gz";
      md5 = "142e3ae4e7cd32d41a71deb359db4cfe";
    };
    doCheck = false;
  };

  nagiosplugin = pythonPackages.buildPythonPackage rec {
    name = "nagiosplugin-1.2.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/n/nagiosplugin/${name}.tar.gz";
      md5 = "c85e1641492d606d929b02aa262bf55d";
    };
    doCheck = false;
  };

  fallocate = pythonPackages.buildPythonPackage rec {
    name = "fallocate-1.4.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/f/fallocate/${name}.tar.gz";
      md5 = "4c03e83699b7f4bb88c3d205032d9371";
    };
    doCheck = false;
  };

  consulate = pythonPackages.buildPythonPackage rec {
    name = "consulate-0.6.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/c/consulate/${name}.tar.gz";
      md5 = "15bd25472f1a8c346f36903fdf54ece5";
    };
    doCheck = false;
  };

in pythonPackages.buildPythonPackage {
  name = "backy-2.0b3.dev0";
  src = ./.;
  buildInputs = [
  ];
  propagatedBuildInputs = [
    consulate
    fallocate
    nagiosplugin
    shortuuid
    telnetlib3
    pythonPackages.prettytable
    pythonPackages.pytz
    pythonPackages.pyyaml
    pythonPackages.requests2
  ];
}
