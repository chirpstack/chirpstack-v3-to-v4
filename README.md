# ChirpStack v3 to v4 data-migration

This utility migrates data from a single ChirpStack Application Server instance
and one or multiple ChirpStack Network Server instances (in case of multiple-regions)
into the new ChirpStack v4 data-structure.

Help:

```
Usage:
  chirpstack-v3-to-v4 [flags]

Flags:
      --as-config-file string         Path to chirpstack-application-server.toml configuration file
      --cs-config-file string         Path to chirpstack.toml configuration file
      --device-session-ttl-days int   Device-session TTL in days (default 31)
      --drop-tenants-and-users        Drop tenants and users before migration
  -h, --help                          help for chirpstack-v3-to-v4
      --migrate-applications          Migrate applications (default true)
      --migrate-device-profiles       Migrate device profiles (default true)
      --migrate-devices               Migrate devices (default true)
      --migrate-gateways              Migrate gateways (default true)
      --migrate-tenants               Migrate tenants (default true)
      --migrate-users                 Migrate users (default true)
      --ns-config-file stringArray    Path to chirpstack-network-server.toml configuration file (can be repeated)
```

Usage example:

```
./chirpstack-v3-to-v4 \
	--cs-config-file /etc/chirpstack/chirpstack.toml \
	--as-config-file /etc/chirpstack-application-server/chirpstack-application-server.toml \
	--ns-config-file /etc/chirpstack-network-server/chirpstack-network-server.toml
```

**Warning:** Always make a backup before starting a migration. If the
`--drop-tenants-and-users` argument is used, then all data in the target
database will be removed!

## Notes

* This utility is compatible with the ChirpStack v4.4.0 or later database schema.
* This utility does not support [environment variables](https://www.chirpstack.io/docs/chirpstack/configuration.html#environment-variables) in configuration files, like ChirpStack does.

## Building from source

For creating a snapshot release:

```
make snapshot
```

For creating a release:

```
make release
```
