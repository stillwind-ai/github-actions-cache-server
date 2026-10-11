{
  description = "Self-hosted GitHub Actions cache server (Postgres + filesystem storage)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      crane,
    }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      buildHash = self.shortRev or self.dirtyShortRev or null;
      mkPackage =
        pkgs:
        pkgs.callPackage ./nix/package.nix {
          inherit buildHash;
          craneLib = crane.mkLib pkgs;
        };
    in
    {
      packages = forAllSystems (pkgs: rec {
        github-actions-cache-server = mkPackage pkgs;
        default = github-actions-cache-server;
      });

      overlays.default = final: _prev: {
        github-actions-cache-server = mkPackage final;
      };

      # services.github-actions-cache-server; see nix/module.nix for options.
      # The package is built from the system's own nixpkgs, without an overlay,
      # so the module also works where `nixpkgs.pkgs` is read-only.
      nixosModules = rec {
        github-actions-cache-server =
          { lib, pkgs, ... }:
          {
            imports = [ ./nix/module.nix ];
            services.github-actions-cache-server.package = lib.mkDefault (mkPackage pkgs);
          };
        default = github-actions-cache-server;
      };

      checks = forAllSystems (pkgs: {
        package = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
        nixos = pkgs.testers.runNixOSTest (import ./nix/test.nix { module = self.nixosModules.default; });
      });

      devShells = forAllSystems (pkgs: {
        # cargo, rustc, clippy and rustfmt, from the toolchain the package builds with.
        default = (crane.mkLib pkgs).devShell {
          packages = with pkgs; [
            rust-analyzer
            postgresql
            kubernetes-helm
          ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
