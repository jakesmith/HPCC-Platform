/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include "jliball.hpp"
#include <openssl/pem.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include "cryptocommon.hpp"
#include "pke.hpp"

namespace cryptohelper
{

void CLoadedKey::loadKeyBio(const char *keyMem)
{
    keyBio.set(BIO_new_mem_buf(keyMem, -1));
    if (!keyBio)
        throwEVPException(0, "loadKeyBio: Failed to create bio for key");
}

void CLoadedKey::loadKeyFileToMem(MemoryBuffer &keyMb, const char *keyFile)
{
    OwnedIFile iFile = createIFile(keyFile);
    OwnedIFileIO iFileIO = iFile->open(IFOread);
    if (!iFileIO)
        throwEVPException(0, "loadKeyFileToMem: failed to open key: %s", keyFile);
    size32_t sz = read(iFileIO, 0, iFile->size(), keyMb);
    iFileIO.clear();
}

void CLoadedKey::finalize(RSA *rsaKey, const char *_keyName)
{
    if (!rsaKey)
        throwEVPException(0, "Failed to create key: %s", _keyName);
    key.set(EVP_PKEY_new());
    EVP_PKEY_set1_RSA(key, rsaKey);
    keyName.set(_keyName);
}

class CLoadedPublicKeyFromFile : public CLoadedKey
{
public:
    CLoadedPublicKeyFromFile(const char *keyFile, const char *passPhrase)
    {
        MemoryBuffer keyMb;
        loadKeyFileToMem(keyMb, keyFile);
        loadKeyBio((const char *)keyMb.bytes());
        RSA *rsaKey = PEM_read_bio_RSA_PUBKEY(keyBio, nullptr, nullptr, (void*)passPhrase);
        finalize(rsaKey, keyFile);
    }
};

class CLoadedPublicKeyFromMemory : public CLoadedKey
{
public:
    CLoadedPublicKeyFromMemory(const char *key, const char *passPhrase)
    {
        loadKeyBio(key);
        RSA *rsaKey = PEM_read_bio_RSA_PUBKEY(keyBio, nullptr, nullptr, (void*)passPhrase);
        finalize(rsaKey, "<inline>");
    }
};

class CLoadedPrivateKeyFromFile : public CLoadedKey
{
public:
    CLoadedPrivateKeyFromFile(const char *keyFile, const char *passPhrase)
    {
        MemoryBuffer keyMb;
        loadKeyFileToMem(keyMb, keyFile);
        loadKeyBio((const char *)keyMb.bytes());
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, nullptr, (void*)passPhrase);
        finalize(rsaKey, keyFile);
    }
};

class CLoadedPrivateKeyFromMemory : public CLoadedKey
{
public:
    CLoadedPrivateKeyFromMemory(const char *key, const char *passPhrase)
    {
        loadKeyBio(key);
        RSA *rsaKey = PEM_read_bio_RSAPrivateKey(keyBio, nullptr, nullptr, (void*)passPhrase);
        finalize(rsaKey, "<inline>");
    }
};

CLoadedKey *loadPublicKeyFromFile(const char *keyFile, const char *passPhrase)
{
    return new CLoadedPublicKeyFromFile(keyFile, passPhrase);
}

CLoadedKey *loadPublicKeyFromMemory(const char *key, const char *passPhrase)
{
    return new CLoadedPublicKeyFromMemory(key, passPhrase);
}

CLoadedKey *loadPrivateKeyFromFile(const char *keyFile, const char *passPhrase)
{
    return new CLoadedPrivateKeyFromFile(keyFile, passPhrase);
}

CLoadedKey *loadPrivateKeyFromMemory(const char *key, const char *passPhrase)
{
    return new CLoadedPrivateKeyFromMemory(key, passPhrase);
}

// AES
void aesKeyEncrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const char key[aesKeySize], const char iv[aesBlockSize])
{
    OwnedCipherCtx ctx = EVP_CIPHER_CTX_new();
    if (!ctx)
        throw makeEVPException(0, "Failed EVP_CIPHER_CTX_new");

    /* Initialise the encryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     * */
    if (1 != EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, (const unsigned char *)key, (const unsigned char *)iv))
        throw makeEVPException(0, "Failed EVP_EncryptInit_ex");

    /* Provide the message to be encrypted, and obtain the encrypted output.
     * EVP_EncryptUpdate can be called multiple times if necessary
     */

    const size32_t cipherBlockSz = 128;
    size32_t outMaxSz = inSz + cipherBlockSz/8;
    byte *outPtr = (byte *)out.reserveTruncate(outMaxSz);
    int outSz;
    if (1 != EVP_EncryptUpdate(ctx, (unsigned char *)outPtr, &outSz, (unsigned char *)inBytes, inSz))
        throw makeEVPException(0, "Failed EVP_EncryptUpdate");
    int ciphertext_len = outSz;

    /* Finalise the encryption. Further ciphertext bytes may be written at
     * this stage.
     */
    if (1 != EVP_EncryptFinal_ex(ctx, outPtr + outSz, &outSz))
        throw makeEVPException(0, "Failed EVP_EncryptFinal_ex");
    ciphertext_len += outSz;
    out.setLength(ciphertext_len);
}

void aesKeyDecrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const char *key, const char *iv)
{
    OwnedCipherCtx ctx = EVP_CIPHER_CTX_new();
    if (!ctx)
        throw makeEVPException(0, "Failed EVP_CIPHER_CTX_new");

    const size32_t cipherBlockSz = 128;
    // man page says(!) - "should have sufficient room for (inl + cipher_block_size) bytes unless the cipher block size is 1 in which case inl bytes is sufficient
    size32_t outMaxSz = ((inSz==1?8:inSz) + cipherBlockSz/8);
    byte *outPtr = (byte *)out.reserveTruncate(outMaxSz);

    /* Initialise the decryption operation. IMPORTANT - ensure you use a key
     * and IV size appropriate for your cipher
     * In this example we are using 256 bit AES (i.e. a 256 bit key). The
     * IV size for *most* modes is the same as the block size. For AES this
     * is 128 bits
     * */
    if (1 != EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, (const unsigned char *)key, (const unsigned char *)iv))
        throw makeEVPException(0, "Failed EVP_EncryptInit_ex");

    /* Provide the message to be decrypted, and obtain the plaintext output.
     * EVP_DecryptUpdate can be called multiple times if necessary
     */
    int outSz;
    if (1 != EVP_DecryptUpdate(ctx, outPtr, &outSz, (const unsigned char *)inBytes, inSz))
        throw makeEVPException(0, "Failed EVP_DecryptUpdate");
    int plaintext_len = outSz;

    /* Finalise the decryption. Further plaintext bytes may be written at
     * this stage.
     */
    if (1 != EVP_DecryptFinal_ex(ctx, outPtr + outSz, &outSz))
        throw makeEVPException(0, "Failed EVP_DecryptFinal_ex");

    plaintext_len += outSz;
    out.setLength(plaintext_len);
}

// RSA ...
void publicKeyEncrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &publicKey)
{
    OwnedEVPPkeyCtx ctx = EVP_PKEY_CTX_new(publicKey, nullptr);
    if (!ctx || (EVP_PKEY_encrypt_init(ctx) <= 0) || (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_PADDING)) <= 0)
        throwEVPException(0, "publicKeyEncrypt: failed to initialize key: %s", publicKey.queryKeyName());

    /* Determine buffer length */
    size_t outLen;
    if (EVP_PKEY_encrypt(ctx, nullptr, &outLen, (unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "publicKeyEncrypt: [EVP_PKEY_encrypt] failed to encrypt with key: %s", publicKey.queryKeyName());

    OwnedEVPMemory encrypted = OPENSSL_malloc(outLen);
    assertex(encrypted);

    if (EVP_PKEY_encrypt(ctx, (unsigned char *)encrypted.get(), &outLen, (unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "EVP_PKEY_encrypt");

    out.append(outLen, encrypted);
}

void privateKeyDecrypt(MemoryBuffer &out, size32_t inSz, const void *inBytes, const CLoadedKey &privateKey)
{
    OwnedEVPPkeyCtx ctx = EVP_PKEY_CTX_new(privateKey, nullptr);
    if (!ctx || (EVP_PKEY_decrypt_init(ctx) <= 0) || (EVP_PKEY_CTX_set_rsa_padding(ctx, RSA_PKCS1_PADDING)) <= 0)
        throwEVPException(0, "privateKeyDecrypt: failed to initialize key: %s", privateKey.queryKeyName());

    /* Determine buffer length */
    size_t outLen;
    if (EVP_PKEY_decrypt(ctx, nullptr, &outLen, (const unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "privateKeyDecrypt: [EVP_PKEY_decrypt] failed to decrypt with key: %s", privateKey.queryKeyName());

    OwnedEVPMemory decrypted = OPENSSL_malloc(outLen);
    assertex(decrypted);

    if (EVP_PKEY_decrypt(ctx, (unsigned char *)decrypted.get(), &outLen, (const unsigned char *)inBytes, inSz) <= 0)
        throwEVPException(0, "privateKeyDecrypt: [EVP_PKEY_decrypt] failed to decrypt with key: %s", privateKey.queryKeyName());

    out.append(outLen, decrypted);
}


} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

