{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  inputs.flake-compat = {
    url = "github:edolstra/flake-compat";
    flake = false;
  };

  outputs = { self, nixpkgs, ... }:
    let
      supportedSystems = [ "x86_64-linux" "x86_64-darwin" "aarch64-linux" "aarch64-darwin" ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
      pkgs = forAllSystems (system: nixpkgs.legacyPackages.${system});
      poetryOverrides = pkgs: [
        # https://github.com/nix-community/poetry2nix/pull/899#issuecomment-1620306977
        pkgs.poetry2nix.defaultPoetryOverrides
        (self: super: {
          python-lzo = super.python-lzo.overrideAttrs (old: {
            buildInputs = (old.buildInputs or []) ++ [ pkgs.lzo ];
          });
          scriv = super.scriv.overrideAttrs (old: {
            buildInputs = (old.buildInputs or []) ++ [ super.setuptools ];
          });
          consulate-fc-nix-test = super.consulate-fc-nix-test.overrideAttrs (old: {
            buildInputs = (old.buildInputs or []) ++ [ super.setuptools super.setuptools-scm ];
          });
        })
      ];
      poetryEnv = pkgs: pkgs.poetry2nix.mkPoetryEnv {
        projectDir = self;
        python = pkgs.python310;
        overrides = poetryOverrides pkgs;
        editablePackageSources = {
          backy = ./src;
        };
      };
    in
    {
      packages = forAllSystems (system: {
        default = pkgs.${system}.poetry2nix.mkPoetryApplication {
          projectDir = self;
          doCheck = true;
          python = pkgs.${system}.python310;
          overrides = poetryOverrides pkgs.${system};
        };

        venv = poetryEnv pkgs.${system};
      });

      devShells = forAllSystems (system: {
        default = pkgs.${system}.mkShellNoCC {
          BACKY_CMD = "backy";
          packages = with pkgs.${system}; [
            (poetryEnv pkgs.${system})
            poetry
          ];
        };
      });

      checks = forAllSystems (system: {
        pytest = pkgs.${system}.runCommand "pytest" {
          nativeBuildInputs = [ (poetryEnv pkgs.${system}) ];
        } ''
          export BACKY_CMD=backy
          cd ${self}
          pytest -vv -p no:cacheprovider --no-cov
          touch $out
        '';
      });
    };
}
