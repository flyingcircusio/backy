{
  pkgs ? import <nixpkgs> {},
  ...
}:
pkgs.mkShell {
  buildInputs = [ pkgs.lzo ];
  packages = [ pkgs.poetry ];
  shellHook = ''
    # check if `poetry env info --path`/bin/activate exists
    POETRY_ENV_PATH=$(poetry env info --path)/bin/activate
    if [ -f $POETRY_ENV_PATH ]; then
      source $POETRY_ENV_PATH
    else
      echo "Run \`poetry install\` to install dependencies first"
    fi
  '';
}
