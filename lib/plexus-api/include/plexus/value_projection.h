#ifndef HPP_GUARD_PLEXUS_VALUE_PROJECTION_H
#define HPP_GUARD_PLEXUS_VALUE_PROJECTION_H

#include <ranges>
#include <string>
#include <ostream>
#include <sstream>
#include <concepts>
#include <string_view>

namespace plexus {

// The opt-in projection of a value type into named columns / structured fields, the
// human-facing contract supplied at the logging handle (parallel to the codec at the
// handle). A projection declares the COLUMN NAMES for T and EMITS one record's field
// values; the logger formats those into a reused buffer, so the emit verbs APPEND to a
// caller-owned std::string rather than minting a fresh string per record (the
// no-hot-path-alloc discipline). The projection never sees raw bytes — it works on a
// decoded T, and lives only at the handle, never in the capture store/tap.
//
// A consumer supplies a projection as a small struct exposing:
//   columns()                     -> a range of column names (string_view)
//   emit_fields(const T&, std::string& row, char delim)
//       -> append each field's text value to `row`, separated by `delim`
//   emit_json(const T&, std::string& obj)
//       -> append the body of a JSON object (the "name":value pairs, no braces) to `obj`
template <typename P, typename T>
concept value_projection = requires(const P &p, const T &v, std::string &buf) {
    { p.columns() } -> std::ranges::range;
    requires std::convertible_to<std::ranges::range_value_t<decltype(p.columns())>,
                                 std::string_view>;
    p.emit_fields(v, buf, char{});
    p.emit_json(v, buf);
};

// The text FLOOR for a type that opts into no projection: anything streamable to an
// ostream. It carries no column structure — one value column / one JSON string field /
// the streamed text line.
template <typename T>
concept streamable = requires(std::ostream &os, const T &v) {
    { os << v } -> std::convertible_to<std::ostream &>;
};

// A type is loggable when it either provides a projection or is streamable; the handle
// requires one of the two so a non-formattable value type is a clear compile error.
template <typename P, typename T>
concept loggable_value = value_projection<P, T> || streamable<T>;

// Append the streamed text of a value to a reused buffer through a thread-local string
// stream, so the floor reuses one ostringstream rather than constructing one per record.
// The stream is cleared (not reconstructed) between uses; its grown buffer is retained.
template <streamable T>
inline void stream_to(const T &v, std::string &out)
{
    thread_local std::ostringstream os;
    os.str(std::string{});
    os.clear();
    os << v;
    out += os.str();
}

}

#endif
