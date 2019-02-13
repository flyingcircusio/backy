let
  pkgs_18_09_src = (import <nixpkgs> {}).fetchFromGitHub {
    owner = "NixOS";
    repo = "nixpkgs";
    rev = "3b44ccd99";
    sha256 = "17704307xdkxkgharwnlxg46fzchrfz28niprz4z3cjd50shf6hh";
  };
  pkgs_18_09 = import pkgs_18_09_src {};
in
with pkgs_18_09;
stdenv.mkDerivation rec {
  name = "env";
  env = buildEnv { name = name; paths = buildInputs; };
  buildInputs = [
    python36Full
    lzo
  ];

  shellHook = ''
    export C_INCLUDE_PATH=${lzo}/include/lzo
  '';
}
