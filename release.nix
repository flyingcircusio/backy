{}:

let
    jobs = rec {
        build = import ./default.nix { pytest_args = "-k asdf"; };
    };

in jobs
