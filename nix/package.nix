{
  lib,
  rustPlatform,
  buildHash ? null,
}:

let
  manifest = (lib.importTOML ../Cargo.toml).package;
in
rustPlatform.buildRustPackage {
  pname = manifest.name;
  inherit (manifest) version;

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../src
    ];
  };

  cargoLock.lockFile = ../Cargo.lock;

  # Shown in the startup banner, like the Docker image's BUILD_HASH.
  env = lib.optionalAttrs (buildHash != null) { BUILD_HASH = buildHash; };

  # The integration tests need a Postgres server; the NixOS test covers them.
  cargoTestFlags = [ "--lib" ];

  meta = {
    description = manifest.description;
    homepage = "https://gha-cache-server.falcondev.io";
    license = lib.licenses.mit;
    mainProgram = manifest.name;
    platforms = lib.platforms.linux;
  };
}
