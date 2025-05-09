{
  lib,
  stdenv,
  poetry2nix,
  lzo,
  # currently needs to be hardcoded here, as it is hardcoded in the pyproject.toml as well
  python312,
  mkShellNoCC,
  poetry,
  runCommand,
  libiconv,
  darwin,
  rustPlatform,
  ...
}:
let
  poetryOverrides = [
    # https://github.com/nix-community/poetry2nix/pull/899#issuecomment-1620306977
    poetry2nix.defaultPoetryOverrides
    (self: super: {
      python-lzo = super.python-lzo.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ lzo ];
      });
      scriv = super.scriv.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.setuptools ];
      });
      backports-tarfile = super.backports-tarfile.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.setuptools ];
      });
      docutils = super.docutils.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.flit-core ];
      });
      execnet = super.execnet.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.hatchling super.hatch-vcs ];
      });
      pygments = super.pygments.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.hatchling super.hatch-vcs ];
      });
      attrs = super.attrs.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.hatchling super.hatch-vcs super.hatch-fancy-pypi-readme ];
      });
      urllib3 = super.urllib3.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.hatchling super.hatch-vcs ];
      });
      consulate-fc-nix-test = super.consulate-fc-nix-test.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.setuptools super.setuptools-scm ];
      });
      yarl = super.yarl.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.tomli ];
      });
      frozenlist = super.frozenlist.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.tomli ];
      });
      shortuuid = super.shortuuid.overrideAttrs (old: {
        # replace poetry to avoid dependency on vulnerable python-cryptography package
        nativeBuildInputs = [ super.poetry-core ] ++ builtins.filter (p: p.pname or "" != "poetry") old.nativeBuildInputs;
      });
      aiofiles = super.aiofiles.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.hatchling super.hatch-vcs ];
      });
      nh3 =
        let
          getCargoHash = version: {
            "0.2.21" = "sha256-1Ytca/GiHidR8JOcz+DydN6N/iguLchbP8Wnrd/0NTk";
          }.${version} or (
            lib.warn "Unknown nh3 version: '${version}'. Please update getCargoHash." lib.fakeHash
          );
        in
        super.nh3.overridePythonAttrs (old: {
          cargoDeps = rustPlatform.fetchCargoVendor {
            inherit (old) src;
            name = "${old.pname}-${old.version}";
            hash = getCargoHash old.version;
          };
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [
            rustPlatform.cargoSetupHook
            rustPlatform.maturinBuildHook
          ];
          buildInputs = (old.buildInputs or [ ]) ++ lib.optional stdenv.isDarwin [
            libiconv
            darwin.apple_sdk.frameworks.Security
          ];
        });

      cryptography =
        let
          getCargoHash = version: {
            "44.0.2" = "sha256-HbUsV+ABE89UvhCRZYXr+Q/zRDKUy+HgCVdQFHqaP4o=";
          }.${version} or (
            lib.warn "Unknown cryptography version: '${version}'. Please update getCargoHash." lib.fakeHash
          );
          hash = getCargoHash super.cryptography.version;
          isWheel = lib.hasSuffix ".whl" super.cryptography.src;
        in
        super.cryptography.overridePythonAttrs (old:
          lib.optionalAttrs (lib.versionAtLeast old.version "3.5" && !isWheel) {
            cargoDeps =
              rustPlatform.fetchCargoVendor {
                inherit (old) src;
                sourceRoot = "${old.pname}-${old.version}/${old.cargoRoot}";
                name = "${old.pname}-${old.version}";
                inherit hash;
              };
          }
        );
    })
  ];
  poetryEnv = poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    python = python312;
    overrides = poetryOverrides;
    editablePackageSources = {
      backy = ./src;
    };
  };
  poetryApplication = poetry2nix.mkPoetryApplication {
    projectDir = ./.;
    doCheck = true;
    python = python312;
    overrides = poetryOverrides;
  };
in
{
  packages = {
    default = poetryApplication;
    venv = poetryEnv;
  };

  devShells = {
    default = mkShellNoCC {
      BACKY_CLI_CMD = "${poetryEnv}/bin/backy";
      BACKY_RBD_CMD = "${poetryEnv}/bin/backy-rbd";
      packages = [
        poetryEnv
        poetry
      ];
    };
  };

  checks = {
    pytest = runCommand "pytest" {
      nativeBuildInputs = [ poetryEnv ];
      src = ./.;
    } ''
      unpackPhase
      cd $sourceRoot
      export BACKY_CMD=${poetryApplication}/bin/backy
      patchShebangs src
      pytest -vv -p no:cacheprovider --no-cov
      touch $out
    '';
  };
}
