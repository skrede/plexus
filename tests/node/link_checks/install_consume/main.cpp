#include "plexus/node_id.h"
#include "plexus/wire_bytes.h"

#ifdef PLEXUS_CONSUME_CRYPTO
    #include "plexus/crypto/aead_cipher.h"
#endif

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <algorithm>

#ifdef PLEXUS_CONSUME_CRYPTO
// Seals then opens a fixed plaintext through the installed crypto OBJECT files. A clean link
// only proves the symbols resolved; running the EVP round-trip and recovering the plaintext
// proves the imported objects also initialize and execute (the A2 / IMPORTED_OBJECTS proof).
static bool crypto_roundtrip()
{
    using namespace plexus::crypto;
    const aead_key key{};
    const std::array<std::byte, k_aead_nonce_len> nonce{};
    const std::array<std::byte, 5> plaintext{std::byte{'p'}, std::byte{'l'}, std::byte{'e'}, std::byte{'x'}, std::byte{'!'}};

    std::vector<std::byte> sealed;
    if(!seal(aead_cipher_id::chacha20_poly1305, key, nonce, {}, plaintext, sealed))
        return false;
    if(sealed.size() != plaintext.size() + k_aead_tag_len)
        return false;

    std::vector<std::byte> opened;
    if(!open(aead_cipher_id::chacha20_poly1305, key, nonce, {}, sealed, opened))
        return false;

    return opened.size() == plaintext.size() && std::equal(plaintext.begin(), plaintext.end(), opened.begin());
}
#endif

// Consumes the INSTALLED package through plexus::plexus plus each requested backend component.
// Constructing both vocabulary types proves target resolution, the include paths, and the
// core->wire transitive link all resolve from the installed tree.
int main()
{
    plexus::node_id id{};
    id[0] = std::byte{0x42};

    std::byte storage[4]{};
    plexus::wire_bytes<> bytes{std::span<const std::byte>{storage}, nullptr};
    if(bytes.size() != sizeof(storage))
        return 1;

#ifdef PLEXUS_CONSUME_CRYPTO
    if(!crypto_roundtrip())
        return 2;
#endif

    return 0;
}
