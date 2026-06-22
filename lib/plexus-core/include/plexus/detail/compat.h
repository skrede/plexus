#ifndef HPP_GUARD_PLEXUS_DETAIL_COMPAT_H
#define HPP_GUARD_PLEXUS_DETAIL_COMPAT_H

#include <new>
#include <cstddef>
#include <utility>
#include <version>
#include <type_traits>

namespace plexus::detail {

#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
// The C++23 path: the real std::move_only_function. The macro IS the guard, so this
// branch is byte-stable — only the C++20-floor fallback below changes (the SBO
// retrofit). The PC build at gnu++20 does NOT satisfy this macro and takes the
// fallback; the pinned on-target GCC at gnu++23 does, and the fallback is inert there.
template<typename Sig>
using move_only_function = std::move_only_function<Sig>;
#else

// Inline storage budget for the fallback's small-buffer optimization. MEASURED, not
// guessed: 136 bytes is the size of the largest callable any seam in the codebase
// wraps (the observability deliver sink's [&engine, fqn=std::string, message_info,
// message_view] closure). Every production seam closure (transport/timer/on_data and
// the posted tasks) is <= 136 bytes, so they all live inline with zero heap; a larger
// callable spills to the heap. max_align_t alignment admits any over-aligned callable
// inline within the budget.
inline constexpr std::size_t k_move_only_fn_sbo = 136;

// Minimal move-only function wrapper for toolchains without
// std::move_only_function (the C++20 floor). Unlike std::function it admits
// move-only callables; it is the handler type the byte_channel / timer / Policy
// concepts are expressed over so the seam never forces a copyable callable. Small
// callables are stored inline (the SBO buffer); larger ones spill to the heap.
template<typename Sig>
class move_only_function;

template<typename R, typename... Args>
class move_only_function<R(Args...)>
{
    // A callable F is stored inline (in the SBO buffer) when it fits the budget, is
    // not over-aligned, and moves without throwing — otherwise it spills to the heap
    // and the buffer holds a single owning F*.
    template<typename F>
    static constexpr bool fits_inline = sizeof(F) <= k_move_only_fn_sbo &&
            alignof(F) <= alignof(std::max_align_t) && std::is_nothrow_move_constructible_v<F>;

    // The type-erased operation table: invoke the target, relocate it from a source
    // buffer into a destination buffer (move-construct + destroy the inline object, or
    // hand over the heap pointer), and destroy+free it. relocate leaves the source
    // empty of the target, so the caller need only clear the source's m_ops. A null
    // m_ops is the empty state. A manual vtable so an inline target needs no heap node
    // and no virtual base.
    struct ops_t
    {
        R (*invoke)(std::byte *buf, Args... args);
        void (*relocate)(std::byte *dst, std::byte *src) noexcept;
        void (*destroy)(std::byte *buf) noexcept;
    };

    template<typename F>
    static F *as(std::byte *buf) noexcept
    {
        if constexpr(fits_inline<F>)
            return reinterpret_cast<F *>(buf);
        else
            return *reinterpret_cast<F **>(buf);
    }

    template<typename F>
    static const ops_t *ops_for() noexcept
    {
        static constexpr ops_t ops{[](std::byte *buf, Args... a) -> R
                                   { return (*as<F>(buf))(std::forward<Args>(a)...); },
                                   [](std::byte *dst, std::byte *src) noexcept
                                   {
                                       if constexpr(fits_inline<F>)
                                       {
                                           ::new(dst) F(std::move(*as<F>(src)));
                                           as<F>(src)->~F();
                                       }
                                       else
                                           *reinterpret_cast<F **>(dst) =
                                                   *reinterpret_cast<F **>(src);
                                   },
                                   [](std::byte *buf) noexcept
                                   {
                                       if constexpr(fits_inline<F>)
                                           as<F>(buf)->~F();
                                       else
                                           delete as<F>(buf);
                                   }};
        return &ops;
    }

    alignas(std::max_align_t) std::byte m_buf[k_move_only_fn_sbo];
    const ops_t *m_ops = nullptr;

    void reset() noexcept
    {
        if(m_ops)
            m_ops->destroy(m_buf);
        m_ops = nullptr;
    }

    template<typename F>
    void emplace(F f)
    {
        if constexpr(fits_inline<F>)
            ::new(static_cast<void *>(m_buf)) F(std::move(f));
        else
            *reinterpret_cast<F **>(m_buf) = new F(std::move(f));
        m_ops = ops_for<F>();
    }

    void steal(move_only_function &o) noexcept
    {
        if(o.m_ops)
            o.m_ops->relocate(m_buf, o.m_buf);
        m_ops   = o.m_ops;
        o.m_ops = nullptr;
    }

public:
    move_only_function() = default;

    move_only_function(std::nullptr_t) noexcept {}

    template<typename F,
             std::enable_if_t<!std::is_same_v<std::remove_cvref_t<F>, move_only_function> &&
                                      std::is_invocable_r_v<R, F &, Args...>,
                              int> = 0>
    // NOLINTNEXTLINE(google-explicit-constructor)
    move_only_function(F f)
    {
        emplace<std::decay_t<F>>(std::move(f));
    }

    move_only_function(move_only_function &&o) noexcept { steal(o); }

    move_only_function &operator=(move_only_function &&o) noexcept
    {
        if(this != &o)
        {
            reset();
            steal(o);
        }
        return *this;
    }

    move_only_function(const move_only_function &)            = delete;
    move_only_function &operator=(const move_only_function &) = delete;

    ~move_only_function() { reset(); }

    explicit operator bool() const noexcept { return m_ops != nullptr; }

    friend bool operator==(const move_only_function &f, std::nullptr_t) noexcept { return !f; }
    friend bool operator==(std::nullptr_t, const move_only_function &f) noexcept { return !f; }
    friend bool operator!=(const move_only_function &f, std::nullptr_t) noexcept { return !!f; }
    friend bool operator!=(std::nullptr_t, const move_only_function &f) noexcept { return !!f; }

    R operator()(Args... args) { return m_ops->invoke(m_buf, std::forward<Args>(args)...); }
};

#endif

}

#endif
