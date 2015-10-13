{ use_btrfs ? true }:

let
    jobs = rec {
        build = import ./default.nix { pytest_args = "-k asdf";
                                       inherit use_btrfs; };
    };

in jobs
