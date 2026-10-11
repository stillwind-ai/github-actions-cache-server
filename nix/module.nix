{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.github-actions-cache-server;
  inherit (lib)
    mkEnableOption
    mkIf
    mkOption
    mkPackageOption
    types
    ;
  user = "github-actions-cache-server";
  stateDirectory = "/var/lib/github-actions-cache-server";
  toEnv = value: if lib.isBool value then lib.boolToString value else toString value;
in
{
  options.services.github-actions-cache-server = {
    enable = mkEnableOption "the GitHub Actions cache server";

    package = mkPackageOption pkgs "github-actions-cache-server" { };

    apiBaseUrl = mkOption {
      type = types.str;
      example = "https://cache.example.com";
      description = "Base URL of the server, as reachable by your runners.";
    };

    host = mkOption {
      type = types.nullOr types.str;
      default = null;
      example = "127.0.0.1";
      description = "Address to listen on. `null` listens on all interfaces.";
    };

    port = mkOption {
      type = types.port;
      default = 3000;
      description = "Port to listen on.";
    };

    openFirewall = mkOption {
      type = types.bool;
      default = false;
      description = "Open {option}`port` in the firewall.";
    };

    storagePath = mkOption {
      type = types.path;
      default = "${stateDirectory}/storage";
      description = ''
        Directory owned by the server for cache data. Paths outside
        `${stateDirectory}` must be writable by the `${user}` user.
      '';
    };

    database = {
      createLocally = mkOption {
        type = types.bool;
        default = true;
        description = ''
          Run PostgreSQL on this host with a `${user}` database, reached over
          its Unix socket with peer authentication.
        '';
      };

      url = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "postgres://cache@db.example.com/cache";
        description = ''
          PostgreSQL connection URL, when not using
          {option}`database.createLocally`. Keep passwords out of the Nix store:
          set `DB_POSTGRES_URL` or `DB_POSTGRES_PASSWORD` in
          {option}`environmentFile` instead.
        '';
      };
    };

    settings = mkOption {
      type = types.attrsOf (
        types.oneOf [
          types.str
          types.int
          types.bool
        ]
      );
      default = { };
      example = {
        CACHE_MAX_SIZE_BYTES = 100 * 1024 * 1024 * 1024;
        CACHE_CLEANUP_OLDER_THAN_DAYS = 30;
        EAGER_MERGE = true;
      };
      description = ''
        Additional environment variables, see the README for all of them.
      '';
    };

    environmentFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      example = "/run/secrets/github-actions-cache-server.env";
      description = ''
        File with secret environment variables such as `MANAGEMENT_API_KEY`
        or `DB_POSTGRES_URL`, loaded by systemd.
      '';
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.database.createLocally || cfg.database.url != null || cfg.environmentFile != null;
        message = "services.github-actions-cache-server needs a database: enable database.createLocally, set database.url, or provide DB_POSTGRES_URL in environmentFile.";
      }
    ];

    services.postgresql = mkIf cfg.database.createLocally {
      enable = true;
      ensureDatabases = [ user ];
      ensureUsers = [
        {
          name = user;
          ensureDBOwnership = true;
        }
      ];
    };

    services.github-actions-cache-server.settings = lib.mkMerge [
      {
        API_BASE_URL = cfg.apiBaseUrl;
        PORT = cfg.port;
        STORAGE_FILESYSTEM_PATH = cfg.storagePath;
      }
      (mkIf (cfg.host != null) { HOST = cfg.host; })
      (mkIf cfg.database.createLocally {
        DB_POSTGRES_URL = "postgres://${user}@localhost/${user}?host=/run/postgresql";
      })
      (mkIf (!cfg.database.createLocally && cfg.database.url != null) {
        DB_POSTGRES_URL = cfg.database.url;
      })
    ];

    users.users.${user} = {
      isSystemUser = true;
      group = user;
      home = stateDirectory;
    };
    users.groups.${user} = { };

    networking.firewall.allowedTCPPorts = mkIf cfg.openFirewall [ cfg.port ];

    systemd.services.github-actions-cache-server = {
      description = "GitHub Actions cache server";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ] ++ lib.optional cfg.database.createLocally "postgresql.target";
      requires = lib.optional cfg.database.createLocally "postgresql.target";

      environment = lib.mapAttrs (_: toEnv) cfg.settings;

      serviceConfig = {
        ExecStart = lib.getExe cfg.package;
        User = user;
        Group = user;
        StateDirectory = "github-actions-cache-server";
        StateDirectoryMode = "0750";
        WorkingDirectory = stateDirectory;
        EnvironmentFile = lib.optional (cfg.environmentFile != null) cfg.environmentFile;
        ReadWritePaths = lib.optional (!lib.hasPrefix "${stateDirectory}/" cfg.storagePath) cfg.storagePath;
        Restart = "on-failure";
        RestartSec = 5;
        # Let in-flight downloads finish and background merges complete.
        TimeoutStopSec = "5min";

        # Hardening.
        CapabilityBoundingSet = "";
        LockPersonality = true;
        MemoryDenyWriteExecute = true;
        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProtectSystem = "strict";
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        SystemCallArchitectures = "native";
        # io_uring is not part of @system-service.
        SystemCallFilter = [
          "@system-service"
          "io_uring_setup"
          "io_uring_enter"
          "io_uring_register"
        ];
        UMask = "0077";
      };
    };
  };
}
