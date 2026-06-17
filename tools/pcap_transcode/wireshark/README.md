# plexus Wireshark dissector

`plexus.lua` is a first-cut Lua dissector for the plexus wire frame. It registers on the
DLT_USER0 (link type 147) encapsulation that the pcapng transcode emits, so tshark and the
Wireshark GUI can read a plexus capture as protocol frames rather than opaque bytes.

## Producing a capture

```
pcap_transcode capture.plxr out.pcapng
```

`capture.plxr` is a plexus flat record stream (the recording API's output); `out.pcapng` is a
pcapng with one Enhanced Packet Block per wire frame on a single DLT_USER0 interface.

## Loading with tshark

```
tshark -X lua_script:tools/pcap_transcode/wireshark/plexus.lua -r out.pcapng -V
```

`-X lua_script:` is sufficient: the dissector registers itself on the `wtap_encap` table for
`wtap.USER0`, which is what the DLT_USER0 capture uses, so no `-d` decode-as mapping is needed.

## Loading in the Wireshark GUI

The GUI does not auto-map DLT_USER0 to a payload protocol. After placing `plexus.lua` in a
personal Lua plugins directory (Help ▸ About Wireshark ▸ Folders), map the encapsulation once:
Edit ▸ Preferences ▸ Protocols ▸ DLT_USER ▸ Encapsulations Table → add User 0 (DLT=147) with
payload protocol `plexus`.

## Crypto position (cleartext vs ciphertext)

A capture's crypto tap position is fixed for the whole capture. The transcode carries it two ways:

- section-scoped in the Section Header Block `opt_comment` as
  `plexus.crypto_position=<cleartext|ciphertext>` — the authoritative human-readable record and
  the value a future compiled-plugin upgrade reads (the SHB option is not reachable from a Lua
  dissector at dissect time);
- per-packet in each frame comment as a `crypto_position=<cleartext|ciphertext>` token.

Wireshark 4.7.1 **does** surface the per-packet frame comment to a Lua dissector at dissect time
(via `Field.new("frame.comment")`), so this dissector reads the carried per-packet token directly
and branches on it — this is the primary path. A `Crypto position` preference exists only as an
explicit override; its default, `carried`, derives the position from the per-packet comment.

To force a position regardless of the carried token, pass the preference by its label:

```
tshark -X lua_script:.../plexus.lua -o plexus.crypto_position:ciphertext -r out.pcapng -V
```

(The preference is an enum; supply the value by its name — `carried`, `cleartext`, or
`ciphertext` — not by an ordinal.)

In a **cleartext** capture the dissector parses the frame header and the unidirectional or
bidirectional endpoint header. In a **ciphertext** capture it shows a single opaque "Sealed
bytes" item over the whole frame and parses no field — the sealed payload stays sealed.

## What it shows, and what it does not

- It decodes the frame header (magic, msg type, flags, session id, timestamp, payload length) and
  the unidirectional / bidirectional endpoint header (source, sequence, topic hash, correlation
  id). All fields are filterable under the `plexus.*` namespace.
- It never decodes the application payload: plexus is serializer-agnostic and carries opaque
  bytes, so the payload is shown as raw bytes only.
- A fully ciphertext capture is opaque end to end (the sealed branch).

## Byte order

The plexus wire frame is **big-endian (network byte order)**: every multi-byte field is written
MSB-first. The dissector reads every field with the default big-endian `tvb():uint()` /
`:uint64()` accessors. The little-endian `:le_uint()` variants are never used; they would
byte-reverse every field and emit garbage.

## A richer upgrade

A future compiled-plugin upgrade can read the section-scoped SHB token directly and add richer
column, expert, and reassembly support. This Lua file is the portable first cut; the compiled
plugin is a separate, larger effort.
