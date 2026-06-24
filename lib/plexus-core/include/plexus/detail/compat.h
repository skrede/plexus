#ifndef HPP_GUARD_PLEXUS_DETAIL_COMPAT_H
#define HPP_GUARD_PLEXUS_DETAIL_COMPAT_H

#include <new>
#include <cstddef>
#include <utility>
#include <version>
#include <type_traits>

namespace plexus::detail {

#if defined(__cpp_lib_move_only_function) && __cpp_lib_move_only_function >= 202110L
template<typename Sig>
using move_only_function = std::move_only_function<Sig>;
#else

// MEASURED: 136 bytes is the largest callable any seam in the codebase wraps, so
// every production seam closure lives inline with zero heap; larger callables spill.
inline constexpr std::size_t k_move_only_fn_sbo = 136;

// A C++20-floor move-only function for toolchains lacking std::move_only_function.
template<typename Sig>
class move_only_function;

template<typename R, typename... Args>
class move_only_function<R(Args...)>
{
    // Inline when it fits the budget, is not over-aligned, and moves without throwing;
    // otherwise the buffer holds a single owning F*.
    template<typename F>
    static constexpr bool fits_inline = sizeof(F) <= k_move_only_fn_sbo && alignof(F) <= alignof(std::max_align_t) && std::is_nothrow_move_constructible_v<F>;

    // A manual vtable so an inline target needs no heap node and no virtual base.
    // relocate leaves the source empty of the target; a null m_ops is the empty state.
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
        static constexpr ops_t ops{[](std::byte *buf, Args... a) -> R { return (*as<F>(buf))(std::forward<Args>(a)...); },
                                   [](std::byte *dst, std::byte *src) noexcept
                                   {
                                       if constexpr(fits_inline<F>)
                                       {
                                           ::new(dst) F(std::move(*as<F>(src)));
                                           as<F>(src)->~F();
                                       }
                                       else
                                           *reinterpret_cast<F **>(dst) = *reinterpret_cast<F **>(src);
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

    move_only_function(std::nullptr_t) noexcept
    {
    }

    template<typename F, std::enable_if_t<!std::is_same_v<std::remove_cvref_t<F>, move_only_function> && std::is_invocable_r_v<R, F &, Args...>, int> = 0>
    // NOLINTNEXTLINE(google-explicit-constructor)
    move_only_function(F f)
    {
        emplace<std::decay_t<F>>(std::move(f));
    }

    move_only_function(move_only_function &&o) noexcept
    {
        steal(o);
    }

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

    ~move_only_function()
    {
        reset();
    }

    explicit operator bool() const noexcept
    {
        return m_ops != nullptr;
    }

    friend bool operator==(const move_only_function &f, std::nullptr_t) noexcept
    {
        return !f;
    }
    friend bool operator==(std::nullptr_t, const move_only_function &f) noexcept
    {
        return !f;
    }
    friend bool operator!=(const move_only_function &f, std::nullptr_t) noexcept
    {
        return !!f;
    }
    friend bool operator!=(std::nullptr_t, const move_only_function &f) noexcept
    {
        return !!f;
    }

    R operator()(Args... args)
    {
        return m_ops->invoke(m_buf, std::forward<Args>(args)...);
    }
};

#endif

}

#endif
