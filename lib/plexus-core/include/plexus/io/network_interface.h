#ifndef HPP_GUARD_PLEXUS_IO_NETWORK_INTERFACE_H
#define HPP_GUARD_PLEXUS_IO_NETWORK_INTERFACE_H

#include <string>
#include <variant>
#include <cstdint>
#include <utility>

namespace plexus::io {

// Named network_interface, not interface, because <windows.h> defines `interface` as a macro. A
// pure-data egress selector: enumeration and resolution live at the OS edge (the backend leaf), never
// here, so core stays free of family and platform symbols. The address alternative is the dotted-quad
// string form because core carries no binary v4 address type; the leaf parses it at resolve time.
class network_interface
{
public:
    struct any_t
    {
    };

    struct name_t
    {
        std::string value;
    };

    struct address_t
    {
        std::string value;
    };

    struct index_t
    {
        std::uint32_t value;
    };

    using selector = std::variant<any_t, name_t, address_t, index_t>;

    static network_interface any()
    {
        return network_interface{any_t{}};
    }

    static network_interface by_name(std::string name)
    {
        return network_interface{name_t{std::move(name)}};
    }

    static network_interface by_address(std::string address)
    {
        return network_interface{address_t{std::move(address)}};
    }

    static network_interface by_index(std::uint32_t index)
    {
        return network_interface{index_t{index}};
    }

    const selector &value() const noexcept
    {
        return m_selector;
    }

    bool is_any() const noexcept
    {
        return std::holds_alternative<any_t>(m_selector);
    }

private:
    explicit network_interface(selector which)
            : m_selector(std::move(which))
    {
    }

    selector m_selector;
};

}

#endif
