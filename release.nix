{ use_btrfs ? true,
  use_ceph ? true}:

# Use this to run tests with nix-based builds.
# If you want to run this without Linux-only dependencies, you can select
# some dependencies:
#
# nix-build release.nix --arg use_btrfs false --arg use_ceph false

let
    jobs = rec {
        build = import ./default.nix { pytest_args = "";
                                       inherit use_btrfs use_ceph; };
    };

in jobs
