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

#ifndef CRYPTOCOMMON_HPP
#define CRYPTOCOMMON_HPP

#ifndef CRYPTOHELPER_API

#ifndef CRYPTOHELPER_EXPORTS
    #define CRYPTOHELPER_API DECL_IMPORT
#else
    #define CRYPTOHELPER_API DECL_EXPORT
#endif //CRYPTOHELPER_EXPORTS

#endif

#if defined(_USE_OPENSSL) && !defined(_WIN32)

#include <openssl/evp.h>
#include <openssl/err.h>

namespace cryptohelper
{

CRYPTOHELPER_API IException *makeEVPException(int code, const char *msg);
CRYPTOHELPER_API IException *makeEVPExceptionV(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));
CRYPTOHELPER_API void throwEVPException(int code, const char *format, ...) __attribute__((format(printf, 2, 3)));

template <class TYPE, void (*FREE_FUNC)(TYPE *)> class OwnedEVPObject
{
    TYPE *obj = nullptr;

public:
    OwnedEVPObject<TYPE, FREE_FUNC>() { }
    OwnedEVPObject<TYPE, FREE_FUNC>(TYPE *_obj) : obj(_obj) { }
    ~OwnedEVPObject<TYPE, FREE_FUNC>()
    {
        FREE_FUNC(obj);
    }
    inline void set(TYPE *_obj)
    {
        if (obj)
            FREE_FUNC(obj);
        obj = _obj;
    }
    inline TYPE *get() const           { return obj; }
    inline TYPE * operator -> () const { return obj; }
    inline operator TYPE *() const     { return obj; }
};

inline void myBIOfree(BIO *bio) { BIO_free(bio); }
inline void myOpenSSLFree(void *m) { OPENSSL_free(m); }

typedef OwnedEVPObject<BIO, myBIOfree> OwnedEVPBio;
typedef OwnedEVPObject<EVP_PKEY, EVP_PKEY_free> OwnedEVPPkey;
typedef OwnedEVPObject<EVP_PKEY_CTX, EVP_PKEY_CTX_free> OwnedEVPPkeyCtx;
typedef OwnedEVPObject<void, myOpenSSLFree> OwnedEVPMemory;
typedef OwnedEVPObject<EVP_CIPHER_CTX, EVP_CIPHER_CTX_free> OwnedCipherCtx;

} // end of namespace cryptohelper

#endif // end of #if defined(_USE_OPENSSL) && !defined(_WIN32)

#endif

