# Protocol fixture snapshot

These client-facing files are copied from Pulsepond core protocol v1 at commit
`0a6e89bcaf03f575b67375bf1eee60416c912a18`.

The snapshot contains the complete canonical event-batch schema and every valid
and invalid event-batch fixture at that commit. Android host tests consume the
snapshot and validate a mobile-generated batch against the same independent
rules. When protocol v1 changes, update the complete snapshot and the SDK in one
reviewed change.
