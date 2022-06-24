{
  pkgs ? import <nixpkgs> {},
  poetry2nix ? pkgs.poetry2nix,
  lzo ? pkgs.lzo,
  ...
}:
poetry2nix.mkPoetryApplication {
    projectDir = ./.;
    overrides = poetry2nix.overrides.withDefaults (self: super: {
      python-lzo = super.python-lzo.overrideAttrs (old: {
        buildInputs = [ lzo ];
      });
    });
}
