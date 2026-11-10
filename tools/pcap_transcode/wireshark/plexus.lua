-- Dissects the plexus wire frame on the DLT_USER0 (147) encapsulation a plexus pcapng capture uses:
--   tshark -X lua_script:plexus.lua -r capture.pcapng -V
-- The wire codec writes every multi-byte field big-endian, which is what the Wireshark Lua API's tvb():uint() and
-- :uint64() read; :le_uint() would byte-reverse every field. The payload is opaque application bytes, never decoded.

local plexus = Proto("plexus", "plexus wire frame")

local msg_type_names = {
    [0x01] = "unidirectional",
    [0x02] = "bidirectional",
    [0x03] = "handshake_req",
    [0x04] = "handshake_resp",
    [0x05] = "subscribe",
    [0x06] = "unsubscribe",
    [0x07] = "fetch_latched",
    [0x08] = "fetch_metadata",
    [0x09] = "rpc_request",
    [0x0A] = "rpc_response",
    [0x0B] = "subscribe_response",
    [0x0C] = "heartbeat",
}

local source_names = {
    [0x01] = "publisher",
    [0x03] = "signal",
    [0x05] = "attribute",
    [0x06] = "caller",
    [0x07] = "procedure",
    [0x08] = "plexus",
}

local f_magic       = ProtoField.bytes("plexus.magic", "Magic")
local f_msg_type    = ProtoField.uint8("plexus.msg_type", "Msg type", base.HEX, msg_type_names)
local f_flags       = ProtoField.uint8("plexus.flags", "Flags", base.HEX)
local f_flag_source = ProtoField.bool("plexus.flags.source_identity", "Source identity", 8, nil, 0x01)
local f_session_id  = ProtoField.uint64("plexus.session_id", "Session id", base.DEC)
local f_timestamp   = ProtoField.uint64("plexus.timestamp_ns", "Timestamp ns", base.DEC)
local f_payload_len = ProtoField.uint64("plexus.payload_len", "Payload len", base.DEC)
local f_source      = ProtoField.uint8("plexus.source", "Source", base.HEX, source_names)
local f_sequence    = ProtoField.uint64("plexus.sequence", "Sequence", base.DEC)
local f_topic_hash  = ProtoField.uint64("plexus.topic_hash", "Topic hash", base.HEX)
local f_type_hash_1 = ProtoField.uint64("plexus.type_hash_1", "Type hash 1", base.HEX)
local f_type_hash_2 = ProtoField.uint64("plexus.type_hash_2", "Type hash 2", base.HEX)
local f_corr_id     = ProtoField.uint64("plexus.correlation_id", "Correlation id", base.HEX)
local f_payload     = ProtoField.bytes("plexus.payload", "Payload")
local f_sealed      = ProtoField.bytes("plexus.sealed", "Sealed bytes")

plexus.fields = {
    f_magic, f_msg_type, f_flags, f_flag_source, f_session_id, f_timestamp, f_payload_len,
    f_source, f_sequence, f_topic_hash, f_type_hash_1, f_type_hash_2, f_corr_id,
    f_payload, f_sealed,
}

local e_magic = ProtoExpert.new("plexus.magic.bad", "Frame magic is not 'VP'", expert.group.MALFORMED, expert.severity.ERROR)
plexus.experts = { e_magic }

-- The projector records the crypto tap position in the Section Header Block opt_comment, which a Lua dissector cannot
-- reach, and as a crypto_position=<value> token in each frame comment, which Wireshark 4.7.1 surfaces at dissect
-- time. The token drives the branch; the preference only overrides it.
local pref_choices = {
    { 1, "carried", 0 },     -- read the per-packet crypto_position token (default)
    { 2, "cleartext", 1 },   -- force cleartext
    { 3, "ciphertext", 2 },  -- force ciphertext
}
plexus.prefs.crypto_position =
    Pref.enum("Crypto position", 0, "Tap position for this capture (carried = read the per-packet comment)",
              pref_choices, true)

local f_frame_comment = Field.new("frame.comment")

local function carried_position()
    local c = f_frame_comment()
    if not c then return nil end
    local text = tostring(c.value)
    return text:match("crypto_position=(%a+)")
end

local function resolved_position()
    local p = plexus.prefs.crypto_position
    if p == 1 then return "cleartext" end
    if p == 2 then return "ciphertext" end
    return carried_position() or "cleartext"
end

-- LEB128 varint at off, returning value and byte length; best-effort, it stops at the end of the tvb.
local function read_varint(tvb, off)
    local value, shift, len = 0, 0, 0
    while off + len < tvb:len() and len < 10 do
        local b = tvb(off + len, 1):uint()
        value = value + (b % 128) * (2 ^ shift)
        len = len + 1
        if b < 128 then break end
        shift = shift + 7
    end
    return value, len
end

local function dissect_unidirectional(tvb, root, off)
    local sub = root:add(plexus, tvb(off), "Unidirectional header")
    sub:add(f_source, tvb(off, 1))
    sub:add(f_sequence, tvb(off + 1, 8))
    sub:add(f_topic_hash, tvb(off + 9, 8))
    return off + 17
end

local function dissect_bidirectional(tvb, root, off)
    local sub = root:add(plexus, tvb(off), "Bidirectional header")
    sub:add(f_source, tvb(off, 1))
    sub:add(f_sequence, tvb(off + 1, 8))
    sub:add(f_topic_hash, tvb(off + 9, 8))
    sub:add(f_type_hash_1, tvb(off + 17, 8))
    sub:add(f_type_hash_2, tvb(off + 25, 8))
    sub:add(f_corr_id, tvb(off + 33, 8))
    return off + 41
end

local function dissect_frame_header(tvb, root)
    local hdr = root:add(plexus, tvb(0, 28), "Frame header")
    hdr:add(f_magic, tvb(0, 2))
    hdr:add(f_msg_type, tvb(2, 1))
    local flags = hdr:add(f_flags, tvb(3, 1))
    flags:add(f_flag_source, tvb(3, 1))
    hdr:add(f_session_id, tvb(4, 8))
    hdr:add(f_timestamp, tvb(12, 8))
    hdr:add(f_payload_len, tvb(20, 8))
    return tvb(2, 1):uint()
end

local function dissect_message_header(tvb, root, mt)
    if mt == 0x01 then
        local seq_text = " seq=" .. tostring(tvb(29, 8):uint64())
        local off = dissect_unidirectional(tvb, root, 28)
        if (tvb(3, 1):uint() % 2) == 1 then
            local _, vlen = read_varint(tvb, off)
            off = off + vlen
        end
        return off, seq_text
    elseif mt == 0x02 then
        local seq_text = " seq=" .. tostring(tvb(29, 8):uint64())
        return dissect_bidirectional(tvb, root, 28), seq_text
    end
    return 28, ""
end

function plexus.dissector(tvb, pinfo, tree)
    pinfo.cols.protocol = "plexus"
    local root = tree:add(plexus, tvb())
    if resolved_position() == "ciphertext" then
        root:add(f_sealed, tvb())
        pinfo.cols.info = "plexus (sealed)"
        return
    end
    if tvb:len() < 28 or tvb(0, 1):uint() ~= 0x56 or tvb(1, 1):uint() ~= 0x50 then
        root:add(f_magic, tvb(0, math.min(2, tvb:len())))
        root:add_proto_expert_info(e_magic)
        return
    end
    local mt = dissect_frame_header(tvb, root)
    local off, seq_text = dissect_message_header(tvb, root, mt)
    if off < tvb:len() then
        root:add(f_payload, tvb(off))
    end
    pinfo.cols.info = (msg_type_names[mt] or string.format("0x%02x", mt)) .. seq_text
end

local encap = DissectorTable.get("wtap_encap")
if encap then
    encap:add(wtap.USER0, plexus)
end
local user_dlt = DissectorTable.get("user_dlt")
if user_dlt then
    user_dlt:add("USER 0", plexus)
end
