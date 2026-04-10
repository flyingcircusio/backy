{
  uv2nix,
  pyproject-nix,
  pyproject-build-systems,
  lzo,
  lib,
  callPackage,
  callPackages,
  mkShellNoCC,
  runCommand,
  uv,
  # currently needs to be hardcoded here, as it is hardcoded in the pyproject.toml as well
  python312,
  ...
}:

let

  workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

  overlay = workspace.mkPyprojectOverlay {
    sourcePreference = "wheel";
  };

  # This gets exposed slightly different via the flake.nix and default.nix
  py-build-system-wheel-overlay =
    if pyproject-build-systems ? overlays then
      pyproject-build-systems.overlays.wheel
    else
      pyproject-build-systems.wheel;

  pythonSet =
    let
      python = python312;
    in
    (callPackage pyproject-nix.build.packages {
      inherit python;
    }).overrideScope
      (
        lib.composeManyExtensions [
          py-build-system-wheel-overlay
          overlay
          (final: prev: {
            consulate-fc-nix-test = prev.consulate-fc-nix-test.overrideAttrs (oA: {
              nativeBuildInputs = (oA.nativeBuildInputs or [ ]) ++ [ final.setuptools ];
            });
            pytest-cache = prev.pytest-cache.overrideAttrs (oA: {
              nativeBuildInputs = (oA.nativeBuildInputs or [ ]) ++ [ final.setuptools ];
            });
            python-lzo = prev.python-lzo.overrideAttrs (oA: {
              nativeBuildInputs = (oA.nativeBuildInputs or [ ]) ++ [ final.setuptools ];
              buildInputs = (oA.buildInputs or [ ]) ++ [ lzo ];
            });
          })
        ]
      );

in
rec {
  checks =
    let
      virtualenv = pythonSet.mkVirtualEnv "backy-dev-env" workspace.deps.all;
    in
    {
      pytest =
        runCommand "pytest"
          {
            nativeBuildInputs = [ virtualenv ];
            src = ./.;
          }
          ''
            unpackPhase
            cd *-source
            export BACKY_CMD=${packages.default}/bin/backy
            patchShebangs src
            pytest -vv -p no:cacheprovider --no-cov
            touch $out
          '';
    };
  devShells =
    let
      virtualenv = pythonSet.mkVirtualEnv "backy-dev-env" workspace.deps.all;
    in
    {
      default = mkShellNoCC {
        packages = [
          virtualenv
          uv
        ];
        env = {
          UV_NO_SYNC = "1";
          UV_PYTHON = pythonSet.python.interpreter;
          UV_PYTHON_DOWNLOADS = "never";
        };
      };
    };

  packages =
    let
      venv = pythonSet.mkVirtualEnv "backy-env" workspace.deps.default;
      inherit (callPackages pyproject-nix.build.util { }) mkApplication;
    in
    rec {
      default = mkApplication {
        inherit venv;
        package = pythonSet.backy;
      };
      inherit venv;
    };
}
