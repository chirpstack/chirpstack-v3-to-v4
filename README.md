# ChirpStack v3 to v4 data-migration

This utilty migrates data from a single ChirpStack Application Server instance
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
  -h, --help                          help for chirpstack-v3-to-v4
      --ns-config-file stringArray    Path to chirpstack-network-server.toml configuration file (can be repeated)
```

Usage example:

```
./chirpstack-v3-to-v4 \
	--cs-config-file /etc/chirpstack/chirpstack.toml \
	--as-config-file /etc/chirpstack-application-server/chirpstack-application-server.toml \
	--ns-config-file /etc/chirpstack-network-server/chirpstack-network-server.toml
```
