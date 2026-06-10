#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include <openssl/kdf.h>
#include <openssl/params.h>
#include <openssl/core_names.h>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstddef>
#include <string_view>

namespace plexus::crypto {

namespace {

struct kdf_deleter
{
    void operator()(EVP_KDF *kdf) const noexcept { EVP_KDF_free(kdf); }
};
struct kdf_ctx_deleter
{
    void operator()(EVP_KDF_CTX *ctx) const noexcept { EVP_KDF_CTX_free(ctx); }
};

const unsigned char *as_uc(const std::byte *p) noexcept
{
    return reinterpret_cast<const unsigned char *>(p);
}

unsigned char *as_uc_mut(std::byte *p) noexcept
{
    return reinterpret_cast<unsigned char *>(p);
}

// One EVP_KDF "HKDF" derivation in a single mode (extract-only or expand-only).
bool hkdf(int mode, std::span<const unsigned char> key,
          std::span<const unsigned char> salt_or_info, std::span<unsigned char> out)
{
    std::unique_ptr<EVP_KDF, kdf_deleter> kdf{EVP_KDF_fetch(nullptr, "HKDF", nullptr)};
    if(!kdf)
        return false;
    std::unique_ptr<EVP_KDF_CTX, kdf_ctx_deleter> ctx{EVP_KDF_CTX_new(kdf.get())};
    if(!ctx)
        return false;

    char digest[] = "SHA256";
    int local_mode = mode;
    OSSL_PARAM params[5];
    std::size_t n = 0;
    params[n++] = OSSL_PARAM_construct_utf8_string(OSSL_KDF_PARAM_DIGEST, digest, 0);
    params[n++] = OSSL_PARAM_construct_int(OSSL_KDF_PARAM_MODE, &local_mode);
    params[n++] = OSSL_PARAM_construct_octet_string(
        OSSL_KDF_PARAM_KEY, const_cast<unsigned char *>(key.data()), key.size());
    const char *region = mode == EVP_KDF_HKDF_MODE_EXTRACT_ONLY ? OSSL_KDF_PARAM_SALT
                                                                 : OSSL_KDF_PARAM_INFO;
    params[n++] = OSSL_PARAM_construct_octet_string(
        region, const_cast<unsigned char *>(salt_or_info.data()), salt_or_info.size());
    params[n++] = OSSL_PARAM_construct_end();

    return EVP_KDF_derive(ctx.get(), out.data(), out.size(), params) == 1;
}

std::vector<unsigned char> info_for(std::string_view label, std::span<const std::byte, 32> transcript)
{
    std::vector<unsigned char> info(label.begin(), label.end());
    for(std::byte b : transcript)
        info.push_back(static_cast<unsigned char>(b));
    return info;
}

bool expand_into(std::span<const unsigned char> master, std::string_view label,
                 std::span<const std::byte, 32> transcript, aead_key &out)
{
    const auto info = info_for(label, transcript);
    return hkdf(EVP_KDF_HKDF_MODE_EXPAND_ONLY, master, info,
                std::span<unsigned char>{as_uc_mut(out.data()), out.size()});
}

}

bool derive_keys(std::span<const std::byte> psk,
                 std::span<const std::byte, 16> initiator_nonce,
                 std::span<const std::byte, 16> responder_nonce,
                 std::span<const std::byte, 32> transcript_digest,
                 derived_keys &out)
{
    std::array<unsigned char, 32> salt{};
    std::memcpy(salt.data(), as_uc(initiator_nonce.data()), initiator_nonce.size());
    std::memcpy(salt.data() + 16, as_uc(responder_nonce.data()), responder_nonce.size());

    std::array<unsigned char, 32> master{};
    if(!hkdf(EVP_KDF_HKDF_MODE_EXTRACT_ONLY,
             std::span<const unsigned char>{as_uc(psk.data()), psk.size()},
             std::span<const unsigned char>{salt}, std::span<unsigned char>{master}))
        return false;

    return expand_into(master, "plexus aead snd", transcript_digest, out.k_send)
        && expand_into(master, "plexus aead rcv", transcript_digest, out.k_recv);
}

}
