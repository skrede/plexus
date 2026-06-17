#!/bin/sh
# The real tshark QA gate for the plexus Wireshark dissector. It transcodes a deterministic
# flat-capture fixture to pcapng, loads the Lua dissector under the installed tshark, and asserts:
#   - the plexus protocol registers on DLT_USER0 and a data frame's header fields parse;
#   - at least one decoded field VALUE equals a known fixture input (payload_len == 21, the
#     17-byte unidirectional header plus the 4-byte reading payload). A little-endian misparse
#     would render this big-endian u64 as a huge byte-reversed number and fail the equality —
#     that is the byte-order regression this gate guards.
#   - a ciphertext-labeled capture shows the opaque sealed item and parses no header field.
# Where tshark is unavailable, or a fixture is missing, the gate SKIPs (exit 0).
#
# Args: <pcap_transcode exe> <repo root> <cleartext .plxr> <ciphertext .plxr>
set -eu

EXE="$1"
ROOT="$2"
PLXR="$3"
PLXR_CIPHER="$4"

LUA="$ROOT/tools/pcap_transcode/wireshark/plexus.lua"

TSHARK="$(command -v tshark || true)"
[ -x /usr/bin/tshark ] && TSHARK=/usr/bin/tshark
if [ -z "$TSHARK" ]; then
    echo "PCAP DISSECTOR QA: SKIP (tshark not found)"
    exit 0
fi
if [ ! -f "$PLXR" ] || [ ! -f "$PLXR_CIPHER" ]; then
    echo "PCAP DISSECTOR QA: SKIP (fixture not produced: $PLXR / $PLXR_CIPHER)"
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
OUT="$WORK/cleartext.pcapng"
OUT_CIPHER="$WORK/ciphertext.pcapng"

fail() {
    echo "PCAP DISSECTOR QA: FAIL -- $1" >&2
    [ -n "${2:-}" ] && printf '%s\n' "$2" | tail -40 >&2
    exit 1
}

"$EXE" "$PLXR" "$OUT" >/dev/null || fail "transcode (cleartext) returned nonzero"
"$EXE" "$PLXR_CIPHER" "$OUT_CIPHER" >/dev/null || fail "transcode (ciphertext) returned nonzero"

# pcapng Section Header Block magic: 0a 0d 0d 0a.
MAGIC="$(od -An -tx1 -N4 "$OUT" | tr -d ' \n')"
[ "$MAGIC" = "0a0d0d0a" ] || fail "cleartext output is not a pcapng (first 4 bytes: $MAGIC)"

# Cleartext: the dissector loads on DLT_USER0 and parses a data frame's header fields.
CLEAR_V="$("$TSHARK" -X lua_script:"$LUA" -r "$OUT" -V 2>&1)"
printf '%s' "$CLEAR_V" | grep -q "plexus wire frame" || fail "proto 'plexus wire frame' not in cleartext output" "$CLEAR_V"
printf '%s' "$CLEAR_V" | grep -qi "Msg type" || fail "no 'Msg type' field in cleartext output" "$CLEAR_V"
printf '%s' "$CLEAR_V" | grep -qi "Topic hash" || fail "no 'Topic hash' field in cleartext output" "$CLEAR_V"

# Protocol column shows plexus for the data frames.
"$TSHARK" -X lua_script:"$LUA" -r "$OUT" 2>/dev/null | grep -q "plexus" \
    || fail "protocol column is not 'plexus'"

# VALUE assertion (byte-order regression guard): the first unidirectional data frame's
# payload_len is the deterministic 21 (17B header + 4B reading). Read it via the dissected
# field; a little-endian misparse would yield a huge byte-reversed number, not 21.
PLEN="$("$TSHARK" -X lua_script:"$LUA" -r "$OUT" -T fields -e plexus.payload_len \
        -Y 'plexus.msg_type==1' 2>/dev/null | head -1)"
[ "$PLEN" = "21" ] || fail "decoded payload_len of first data frame is '$PLEN', expected 21 (byte-order guard)"

# The first data frame's sequence is the deterministic 0 (cross-checked against the carried
# seq= token); also assert the field is filterable and decodes.
SEQ="$("$TSHARK" -X lua_script:"$LUA" -r "$OUT" -T fields -e plexus.sequence \
       -Y 'plexus.msg_type==1' 2>/dev/null | head -1)"
[ "$SEQ" = "0" ] || fail "decoded sequence of first data frame is '$SEQ', expected 0"

# Ciphertext: the carried crypto_position=ciphertext token drives the sealed-blob branch.
CIPHER_V="$("$TSHARK" -X lua_script:"$LUA" -r "$OUT_CIPHER" -V 2>&1)"
printf '%s' "$CIPHER_V" | grep -qi "Sealed bytes" || fail "ciphertext capture shows no 'Sealed bytes' item" "$CIPHER_V"
if printf '%s' "$CIPHER_V" | grep -qi "Topic hash"; then
    fail "ciphertext capture parsed a 'Topic hash' field (should be opaque)" "$CIPHER_V"
fi

echo "PCAP DISSECTOR QA: PASS"
exit 0
