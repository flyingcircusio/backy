let
  # FCIO 20.09
  pkgs_src = (import <nixpkgs> {}).fetchFromGitHub {
    owner = "flyingcircusio";
    repo = "nixpkgs";
    rev = "b37976e902f753a5bc8be443aeb92e1030946ccb";
    sha256 = "1n5fplxv7rbndhz5ahhhljp6cywdd7s2v5qp783mm4ncwm2dng8k";
  };
  pkgs = import pkgs_src {};
in
with pkgs;
stdenv.mkDerivation rec {
  name = "env";
  env = buildEnv { name = name; paths = buildInputs; };
  buildInputs = [
    python38Full
    lzo
    libyaml
  ];

  shellHook = ''
    export C_INCLUDE_PATH=${lzo}/include/lzo
  '';
}
