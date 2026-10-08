{
  description = "Self-hosted GitHub Actions cache server (Postgres + filesystem storage)";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

  outputs =
    { self, nixpkgs }:
    let
      inherit (nixpkgs) lib;
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forAllSystems = f: lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});
      buildHash = self.shortRev or self.dirtyShortRev or null;
    in
    {
      packages = forAllSystems (pkgs: rec {
        github-actions-cache-server = pkgs.callPackage ./nix/package.nix { inherit buildHash; };
        default = github-actions-cache-server;
      });

      overlays.default = final: _prev: {
        github-actions-cache-server = final.callPackage ./nix/package.nix { inherit buildHash; };
      };

      # services.github-actions-cache-server; see nix/module.nix for options.
      # The package is built from the system's own nixpkgs, without an overlay,
      # so the module also works where `nixpkgs.pkgs` is read-only.
      nixosModules = rec {
        github-actions-cache-server =
          { lib, pkgs, ... }:
          {
            imports = [ ./nix/module.nix ];
            services.github-actions-cache-server.package = lib.mkDefault (
              pkgs.callPackage ./nix/package.nix { inherit buildHash; }
            );
          };
        default = github-actions-cache-server;
      };

      checks = forAllSystems (pkgs: {
        package = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
        nixos = pkgs.testers.runNixOSTest (import ./nix/test.nix { module = self.nixosModules.default; });
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.stdenv.hostPlatform.system}.default ];
          packages = with pkgs; [
            clippy
            rustfmt
            rust-analyzer
            postgresql
            kubernetes-helm
          ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt);
    };
}
