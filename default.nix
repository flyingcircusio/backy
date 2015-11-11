{ pytest_args ? "",
  use_ceph ? true }:

let
  pkgs = import <nixpkgs> { };

  ceph_rbd_cmd = if use_ceph then "${pkgs.ceph}/bin/rbd" else "";

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
    buildInputs = [
      pythonPackages.pytest
    ];
  };

  shortuuid = pythonPackages.buildPythonPackage rec {
    name = "shortuuid-0.4.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/s/shortuuid/${name}.tar.gz";
      md5 = "142e3ae4e7cd32d41a71deb359db4cfe";
    };
    # test_pep8 fails for this version
    doCheck = false;
  };

  nagiosplugin = pythonPackages.buildPythonPackage rec {
    name = "nagiosplugin-1.2.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/n/nagiosplugin/${name}.tar.gz";
      md5 = "c85e1641492d606d929b02aa262bf55d";
    };
  };

  fallocate = pythonPackages.buildPythonPackage rec {
    name = "fallocate-1.4.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/f/fallocate/${name}.tar.gz";
      md5 = "4c03e83699b7f4bb88c3d205032d9371";
    };
  };

  consulate = pythonPackages.buildPythonPackage rec {
    name = "consulate-0.6.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/c/consulate/${name}.tar.gz";
      md5 = "15bd25472f1a8c346f36903fdf54ece5";
    };
    buildInputs = [
      pythonPackages.requests2
    ];
  };

  pytestasyncio = pythonPackages.buildPythonPackage rec {
    name = "pytest-asyncio-0.2.0";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/p/pytest-asyncio/${name}.tar.gz";
      md5 = "4cd7009570967eb5592614593cb1a45f";
    };
    buildInputs = [
      pythonPackages.pytest
    ];
  };

  pytestcapturelog = pythonPackages.buildPythonPackage rec {
    name = "pytest-capturelog-0.7";
    src = pkgs.fetchurl {
      url =
      "https://pypi.python.org/packages/source/p/pytest-capturelog/${name}.tar.gz";
      md5 = "cfeac23d8ed254deaeb50a8c0aa141e9";
    };
    buildInputs = [
      pythonPackages.pytest
    ];
  };

  pytestcodecheckers = pythonPackages.buildPythonPackage rec {
    name = "pytest-codecheckers-0.2";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/p/pytest-codecheckers/${name}.tar.gz";
      md5 = "5e7449fc6cd02d35cc11e21709ce1357";
    };
    buildInputs = [
      pythonPackages.pytest
      pythonPackages.pep8
      pythonPackages.pyflakes
    ];
  };

  pytesttimeout = pythonPackages.buildPythonPackage rec {
    name = "pytest-timeout-0.5";
    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/p/pytest-timeout/${name}.tar.gz";
      md5 = "0c44e5e03b15131498a86169000cb050";
    };
    buildInputs = [
      pythonPackages.pytest
    ];
  };

  pytestcov = pythonPackages.buildPythonPackage (rec {
    name = "pytest-cov-2.1.0";

    src = pkgs.fetchurl {
      url = "https://pypi.python.org/packages/source/p/pytest-cov/${name}.tar.gz";
      md5 = "98e94f5be88423f6251e7bad59fa6c06";
    };

   buildInputs = [ pythonPackages.coverage pythonPackages.pytest ];
  });


in pythonPackages.buildPythonPackage rec {
  name = "backy-2.0b3.dev0";
  src = ./.;
  buildInputs = [
    pkgs.makeWrapper
    pytestasyncio
    pytestcapturelog
    pytestcodecheckers
    pytestcov
    pytesttimeout
    pythonPackages.pytest
    pythonPackages.pytestcache
    pythonPackages.coverage
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
  checkPhase = ''
    runHook shellHook
    py.test ${pytest_args}
  '';
  postInstall = ''
    wrapProgram $out/bin/backy \
      --set BACKY_CMD "$tmp_path/bin/backy" \
      --set BACKY_CP "${pkgs.coreutils}/bin/cp" \
      --set BACKY_RBD "${ceph_rbd_cmd}" \
      --set BACKY_BASH "${pkgs.bash}/bin/bash"
  '';
  postShellHook = ''
    export BACKY_CMD="$tmp_path/bin/backy"
    export BACKY_CP="${pkgs.coreutils}/bin/cp"
    export BACKY_RBD="${ceph_rbd_cmd}"
    export BACKY_BASH="${pkgs.bash}/bin/bash"
  '';
}
