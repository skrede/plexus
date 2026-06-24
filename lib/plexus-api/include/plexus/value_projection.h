#ifndef HPP_GUARD_PLEXUS_VALUE_PROJECTION_H
#define HPP_GUARD_PLEXUS_VALUE_PROJECTION_H

#include <ranges>
#include <string>
#include <ostream>
#include <sstream>
#include <concepts>
#include <string_view>

namespace plexus {

// The emit verbs APPEND to a caller-owned std::string rather than minting a fresh string per
// record (the no-hot-path-alloc discipline); emit_json appends the body of a JSON object (the
// "name":value pairs, no braces).
template<typename P, typename T>
concept value_projection = requires(const P &p, const T &v, std::string &buf) {
    { p.columns() } -> std::ranges::range;
    requires std::convertible_to<std::ranges::range_value_t<decltype(p.columns())>, std::string_view>;
    p.emit_fields(v, buf, char{});
    p.emit_json(v, buf);
};

template<typename T>
concept streamable = requires(std::ostream &os, const T &v) {
    { os << v } -> std::convertible_to<std::ostream &>;
};

template<typename P, typename T>
concept loggable_value = value_projection<P, T> || streamable<T>;

// Reuses one thread_local ostringstream across records (cleared, not reconstructed; its grown
// buffer is retained) rather than constructing one per call.
template<streamable T>
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
