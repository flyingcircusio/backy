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
      #pkgs = forAllSystems (system: nixpkgs.legacyPackages.${system});
      pkgs = forAllSystems (system: import nixpkgs {
        inherit system;
        config = {
          permittedInsecurePackages = [
            "python3.10-requests-2.28.2"
            "python3.10-requests-2.29.0"
          ];
        };
      });
      lib = forAllSystems (system: pkgs.${system}.callPackage "${self}/lib.nix" {});
    in
    {
      packages = forAllSystems (system:
        lib.${system}.packages
      );

      devShells = forAllSystems (system:
        lib.${system}.devShells
      );

      checks = forAllSystems (system:
        lib.${system}.checks
      );
    };
}
