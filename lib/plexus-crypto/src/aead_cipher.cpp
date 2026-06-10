#include "plexus/crypto/aead_cipher.h"

#include <openssl/evp.h>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <cstddef>

namespace plexus::crypto {

namespace {

struct cipher_ctx_deleter
{
    void operator()(EVP_CIPHER_CTX *ctx) const noexcept { EVP_CIPHER_CTX_free(ctx); }
};
using cipher_ctx = std::unique_ptr<EVP_CIPHER_CTX, cipher_ctx_deleter>;

const EVP_CIPHER *evp_for(aead_cipher_id cipher) noexcept
{
    return cipher == aead_cipher_id::aes_256_gcm ? EVP_aes_256_gcm() : EVP_chacha20_poly1305();
}

const unsigned char *as_uc(const std::byte *p) noexcept
{
    return reinterpret_cast<const unsigned char *>(p);
}

unsigned char *as_uc(std::byte *p) noexcept
{
    return reinterpret_cast<unsigned char *>(p);
}

bool feed_aad(EVP_CIPHER_CTX *ctx, std::span<const std::byte> aad, bool sealing) noexcept
{
    if(aad.empty())
        return true;
    int len = 0;
    const int n = static_cast<int>(aad.size());
    return (sealing ? EVP_EncryptUpdate(ctx, nullptr, &len, as_uc(aad.data()), n)
                    : EVP_DecryptUpdate(ctx, nullptr, &len, as_uc(aad.data()), n)) == 1;
}

}

bool seal(aead_cipher_id cipher, const aead_key &key,
          std::span<const std::byte, k_aead_nonce_len> nonce,
          std::span<const std::byte> aad, std::span<const std::byte> plaintext,
          std::vector<std::byte> &out)
{
    cipher_ctx ctx{EVP_CIPHER_CTX_new()};
    if(!ctx)
        return false;
    if(EVP_EncryptInit_ex(ctx.get(), evp_for(cipher), nullptr, as_uc(key.data()), as_uc(nonce.data())) != 1)
        return false;
    if(!feed_aad(ctx.get(), aad, true))
        return false;

    out.resize(plaintext.size() + k_aead_tag_len);
    int len = 0;
    if(EVP_EncryptUpdate(ctx.get(), as_uc(out.data()), &len, as_uc(plaintext.data()),
                         static_cast<int>(plaintext.size())) != 1)
        return false;
    int final_len = 0;
    if(EVP_EncryptFinal_ex(ctx.get(), as_uc(out.data()) + len, &final_len) != 1)
        return false;
    return EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_GET_TAG, static_cast<int>(k_aead_tag_len),
                               as_uc(out.data()) + plaintext.size()) == 1;
}

bool open(aead_cipher_id cipher, const aead_key &key,
          std::span<const std::byte, k_aead_nonce_len> nonce,
          std::span<const std::byte> aad, std::span<const std::byte> ciphertext_and_tag,
          std::vector<std::byte> &out)
{
    if(ciphertext_and_tag.size() < k_aead_tag_len)
        return false;
    const auto ct_len = ciphertext_and_tag.size() - k_aead_tag_len;
    const auto *ct = ciphertext_and_tag.data();
    const auto *tag = ciphertext_and_tag.data() + ct_len;

    cipher_ctx ctx{EVP_CIPHER_CTX_new()};
    if(!ctx)
        return false;
    if(EVP_DecryptInit_ex(ctx.get(), evp_for(cipher), nullptr, as_uc(key.data()), as_uc(nonce.data())) != 1)
        return false;
    if(!feed_aad(ctx.get(), aad, false))
        return false;

    out.resize(ct_len);
    int len = 0;
    if(EVP_DecryptUpdate(ctx.get(), as_uc(out.data()), &len, as_uc(ct), static_cast<int>(ct_len)) != 1)
        return false;
    if(EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_AEAD_SET_TAG, static_cast<int>(k_aead_tag_len),
                           const_cast<unsigned char *>(as_uc(tag))) != 1)
        return false;
    int final_len = 0;
    return EVP_DecryptFinal_ex(ctx.get(), as_uc(out.data()) + len, &final_len) == 1;
}

}
