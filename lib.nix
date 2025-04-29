{
  poetry2nix,
  lzo,
  # currently needs to be hardcoded here, as it is hardcoded in the pyproject.toml as well
  python312,
  mkShellNoCC,
  poetry,
  runCommand,
  fetchPypi,
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
      telnetlib3 = super.telnetlib3.overrideAttrs (old: {
        buildInputs = (old.buildInputs or []) ++ [ super.setuptools ];
      });
      execnet = super.execnet.overrideAttrs (old: {
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
      shortuuid = super.shortuuid.overrideAttrs (old: {
        # replace poetry to avoid dependency on vulnerable python-cryptography package
        nativeBuildInputs = [ super.poetry-core ] ++ builtins.filter (p: p.pname or "" != "poetry") old.nativeBuildInputs;
      });
      # This is hacky.
      packaging = super.packaging.overrideAttrs (oldAttrs: {
        src = fetchPypi {
          pname = "packaging";
          version = "23.2";
          hash = "sha256-BI+w6UBQNlGOqvSKVZU8dQwR4aG2jg3RqdYu0MCSz8U=";
        };
      });
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
      BACKY_CMD = "backy";
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
      cd *-source
      export BACKY_CMD=${poetryApplication}/bin/backy
      patchShebangs src
      pytest -vv -p no:cacheprovider --no-cov
      touch $out
    '';
  };
}
