{}:

let
    jobs = rec {
        build = import ./default.nix;
    };

in jobs
