#ifndef HPP_GUARD_PLEXUS_DETAIL_COMPAT_H
#define HPP_GUARD_PLEXUS_DETAIL_COMPAT_H

#include <memory>
#include <type_traits>
#include <utility>
#include <version>

namespace plexus::detail {

#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
template<typename Sig>
using move_only_function = std::move_only_function<Sig>;
#else

// Minimal move-only function wrapper for toolchains without
// std::move_only_function (the C++20 floor). Unlike std::function it admits
// move-only callables; it is the handler type the byte_channel / timer / Policy
// concepts are expressed over so the seam never forces a copyable callable.
template<typename Sig>
class move_only_function;

template<typename R, typename... Args>
class move_only_function<R(Args...)>
{
    struct concept_t
    {
        virtual ~concept_t()      = default;
        virtual R invoke(Args...) = 0;
    };

    template<typename F>
    struct model_t final : concept_t
    {
        F fn;

        explicit model_t(F f)
                : fn(std::move(f))
        {
        }

        R invoke(Args... args) override { return fn(std::forward<Args>(args)...); }
    };

    std::unique_ptr<concept_t> m_impl;

public:
    move_only_function() = default;

    move_only_function(std::nullptr_t) noexcept {}

    template<typename F,
             std::enable_if_t<!std::is_same_v<std::remove_cvref_t<F>, move_only_function> &&
                                      std::is_invocable_r_v<R, F &, Args...>,
                              int> = 0>
    // NOLINTNEXTLINE(google-explicit-constructor)
    move_only_function(F f)
            : m_impl(std::make_unique<model_t<std::decay_t<F>>>(std::move(f)))
    {
    }

    move_only_function(move_only_function &&) noexcept            = default;
    move_only_function &operator=(move_only_function &&) noexcept = default;

    move_only_function(const move_only_function &)            = delete;
    move_only_function &operator=(const move_only_function &) = delete;

    explicit operator bool() const noexcept { return m_impl != nullptr; }

    friend bool operator==(const move_only_function &f, std::nullptr_t) noexcept { return !f; }
    friend bool operator==(std::nullptr_t, const move_only_function &f) noexcept { return !f; }
    friend bool operator!=(const move_only_function &f, std::nullptr_t) noexcept { return !!f; }
    friend bool operator!=(std::nullptr_t, const move_only_function &f) noexcept { return !!f; }

    R operator()(Args... args) { return m_impl->invoke(std::forward<Args>(args)...); }
};

#endif

}

#endif
