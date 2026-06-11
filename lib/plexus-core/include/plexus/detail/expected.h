#ifndef HPP_GUARD_PLEXUS_DETAIL_EXPECTED_H
#define HPP_GUARD_PLEXUS_DETAIL_EXPECTED_H

#include <variant>
#include <utility>
#include <version>
#include <type_traits>

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L
#  include <expected>
#endif

namespace plexus::detail {

#if defined(__cpp_lib_expected) && __cpp_lib_expected >= 202202L

template <typename T, typename E>
using expected = std::expected<T, E>;

template <typename E>
using unexpected = std::unexpected<E>;

using unexpect_t = std::unexpect_t;
inline constexpr unexpect_t unexpect{std::unexpect};

#else

template <typename E>
struct unexpected
{
    E value;

    explicit constexpr unexpected(E e) : value(std::move(e))
    {
    }
};

struct unexpect_t
{
    explicit unexpect_t() = default;
};

inline constexpr unexpect_t unexpect{};

// Signature-equal to the std::expected subset plexus uses (value/error access,
// value_or, unexpect-tag construction) so the C++23 cutover above is a no-op.
template <typename T, typename E>
class expected
{
    std::variant<T, unexpected<E>> m_storage;

public:
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr expected(T val) : m_storage(std::move(val))
    {
    }

    template <typename U,
              std::enable_if_t<std::is_constructible_v<T, U> &&
                               !std::is_same_v<std::remove_cvref_t<U>, expected>, int> = 0>
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr expected(U &&val) : m_storage(T(std::forward<U>(val)))
    {
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr expected(unexpected<E> err) : m_storage(std::move(err))
    {
    }

    template <typename... Args>
    constexpr explicit expected(unexpect_t, Args &&...args)
        : m_storage(unexpected<E>(E(std::forward<Args>(args)...)))
    {
    }

    constexpr explicit operator bool() const noexcept { return m_storage.index() == 0; }
    [[nodiscard]] constexpr bool has_value() const noexcept { return m_storage.index() == 0; }

    constexpr T &operator*() & { return std::get<0>(m_storage); }
    constexpr const T &operator*() const & { return std::get<0>(m_storage); }
    constexpr T &&operator*() && { return std::get<0>(std::move(m_storage)); }
    constexpr const T &&operator*() const && { return std::get<0>(std::move(m_storage)); }

    constexpr T *operator->() { return &std::get<0>(m_storage); }
    constexpr const T *operator->() const { return &std::get<0>(m_storage); }

    [[nodiscard]] constexpr T &value() & { return std::get<0>(m_storage); }
    [[nodiscard]] constexpr const T &value() const & { return std::get<0>(m_storage); }
    [[nodiscard]] constexpr T &&value() && { return std::get<0>(std::move(m_storage)); }
    [[nodiscard]] constexpr const T &&value() const && { return std::get<0>(std::move(m_storage)); }

    [[nodiscard]] constexpr E &error() & { return std::get<1>(m_storage).value; }
    [[nodiscard]] constexpr const E &error() const & { return std::get<1>(m_storage).value; }
    [[nodiscard]] constexpr E &&error() && { return std::get<1>(std::move(m_storage)).value; }
    [[nodiscard]] constexpr const E &&error() const && { return std::get<1>(std::move(m_storage)).value; }

    template <typename U>
    [[nodiscard]] constexpr T value_or(U &&fallback) const &
    {
        return has_value() ? **this : static_cast<T>(std::forward<U>(fallback));
    }

    template <typename U>
    [[nodiscard]] constexpr T value_or(U &&fallback) &&
    {
        return has_value() ? std::move(**this) : static_cast<T>(std::forward<U>(fallback));
    }
};

template <typename E>
class expected<void, E>
{
    std::variant<std::monostate, unexpected<E>> m_storage;

public:
    constexpr expected() noexcept : m_storage(std::monostate{})
    {
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr expected(unexpected<E> err) : m_storage(std::move(err))
    {
    }

    template <typename... Args>
    constexpr explicit expected(unexpect_t, Args &&...args)
        : m_storage(unexpected<E>(E(std::forward<Args>(args)...)))
    {
    }

    constexpr explicit operator bool() const noexcept { return m_storage.index() == 0; }
    [[nodiscard]] constexpr bool has_value() const noexcept { return m_storage.index() == 0; }

    [[nodiscard]] constexpr E &error() & { return std::get<1>(m_storage).value; }
    [[nodiscard]] constexpr const E &error() const & { return std::get<1>(m_storage).value; }
    [[nodiscard]] constexpr E &&error() && { return std::get<1>(std::move(m_storage)).value; }
    [[nodiscard]] constexpr const E &&error() const && { return std::get<1>(std::move(m_storage)).value; }
};

#endif

}

#endif
