{
  lib,
  craneLib,
  buildHash ? null,
}:

let
  manifest = (lib.importTOML ../Cargo.toml).package;

  commonArgs = {
    src = lib.fileset.toSource {
      root = ../.;
      fileset = lib.fileset.unions [
        ../Cargo.toml
        ../Cargo.lock
        ../src
      ];
    };
    strictDeps = true;
  };

  # The dependencies alone, built from a dummy source: only a change to
  # Cargo.toml or Cargo.lock rebuilds them, so CI keeps them between runs.
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
craneLib.buildPackage (
  commonArgs
  // {
    inherit cargoArtifacts;

    # Shown in the startup banner, like the Docker image's BUILD_HASH.
    env = lib.optionalAttrs (buildHash != null) { BUILD_HASH = buildHash; };

    # The integration tests need a Postgres server; the NixOS test covers them.
    cargoTestExtraArgs = "--lib";

    passthru = { inherit cargoArtifacts; };

    meta = {
      description = manifest.description;
      homepage = "https://gha-cache-server.falcondev.io";
      license = lib.licenses.mit;
      mainProgram = manifest.name;
      platforms = lib.platforms.linux;
    };
  }
)
