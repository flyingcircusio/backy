with import <nixpkgs> {};
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
