{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-compat = {
      url = "github:edolstra/flake-compat";
      flake = false;
    };
    flake-utils.url = "github:numtide/flake-utils";
    poetry2nix = {
      url = "github:nix-community/poetry2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, poetry2nix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config = {
            permittedInsecurePackages = [
              "python3.10-requests-2.28.2"
              "python3.10-requests-2.29.0"
            ];
          };
        };
        lib = pkgs.callPackage "${self}/lib.nix" { poetry2nix = import poetry2nix { inherit pkgs; }; };
      in
      {
        inherit (lib) packages devShells checks;
      });
}
