# NixOS VM test: the module with a local Postgres, exercised over HTTP the
# way the cache client uses it.
{ module }:

{
  name = "github-actions-cache-server";

  nodes.machine = {
    imports = [ module ];
    virtualisation.memorySize = 1024;
    services.github-actions-cache-server = {
      enable = true;
      apiBaseUrl = "http://localhost:3000";
      # The test has no GitHub-issued runtime tokens.
      settings.SKIP_TOKEN_VALIDATION = true;
    };
  };

  testScript = ''
    import base64
    import json

    def b64(data):
        return base64.urlsafe_b64encode(data.encode()).decode().rstrip("=")

    token = ".".join([
        b64('{"alg":"HS256","typ":"JWT"}'),
        b64(json.dumps({
            "ac": json.dumps([{"Scope": "refs/heads/main", "Permission": 3}]),
            "repository_id": "123",
        })),
        "signature",
    ])
    service = "http://localhost:3000/twirp/github.actions.results.api.v1.CacheService"

    def twirp(method, body):
        return json.loads(machine.succeed(
            f"curl -sSf -X POST -H 'authorization: Bearer {token}' "
            f"-H 'content-type: application/json' -d '{json.dumps(body)}' {service}/{method}"
        ))

    machine.wait_for_unit("github-actions-cache-server.service")
    machine.wait_for_open_port(3000)
    assert machine.succeed("curl -sSf http://localhost:3000/health") == "healthy"
    # The systemd sandbox must leave io_uring usable.
    machine.succeed("journalctl -u github-actions-cache-server | grep -q 'uses io_uring'")

    with subtest("save a cache entry"):
        created = twirp("CreateCacheEntry", {"key": "nixos-key", "version": "v1"})
        assert created["ok"], created
        machine.succeed("head -c 3000000 /dev/urandom > /tmp/payload")
        machine.succeed(f"curl -sSf -X PUT --data-binary @/tmp/payload '{created['signed_upload_url']}'")
        assert twirp("FinalizeCacheEntryUpload", {"key": "nixos-key", "version": "v1"})["ok"]

    with subtest("restore it, also after a restart"):
        for _ in range(2):
            found = twirp("GetCacheEntryDownloadURL", {"key": "nixos-", "restore_keys": [], "version": "v1"})
            assert found["ok"] and found["matched_key"] == "nixos-key", found
            machine.succeed(f"curl -sSf -o /tmp/restored '{found['signed_download_url']}'")
            machine.succeed("cmp /tmp/payload /tmp/restored")
            machine.systemctl("restart github-actions-cache-server.service")
            machine.wait_for_open_port(3000)
  '';
}
