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
#ifndef DIGISIGN_HPP
#define DIGISIGN_HPP

#ifndef CRYPTOHELPER_API

#ifndef CRYPTOHELPER_EXPORTS
    #define CRYPTOHELPER_API DECL_IMPORT
#else
    #define CRYPTOHELPER_API DECL_EXPORT
#endif //CRYPTOHELPER_EXPORTS

#endif

#include "jstring.hpp"

#include "pke.hpp"

namespace cryptohelper
{

//Create base 64 encoded digital signature of given text string
bool digiSign(StringBuffer &b64Signature, const char *text, const CLoadedKey &signingKey);

//Verify the given text was used to create the given digital signature
bool digiVerify(StringBuffer &b64Signature, const char *text, const CLoadedKey &verifyingKey);

//General purpose digital signature manager
//Useful to sign a text string, so the consumer can be assured it has not been altered
interface IDigitalSignatureManager : extends IInterface //Public/Private key message signer/verifyer
{
public:
    virtual bool isDigiSignerConfigured() const = 0;
    virtual bool isDigiVerifierConfigured() const = 0;
    virtual bool digiSign(StringBuffer & b64Signature, const char * text) const = 0;//signs, using private key
    virtual bool digiVerify(StringBuffer & b64Signature, const char * text) const = 0;//verifies, using public key
};

//Uses the HPCCPublicKey/HPCCPrivateKey key files specified in environment.conf
CRYPTOHELPER_API IDigitalSignatureManager * queryDigitalSignatureManagerInstanceFromEnv();

//Create using the given key files
CRYPTOHELPER_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromFiles(const char *pubKeyFileName, const char *privKeyFileName, const char *passPhrase);

//Create using the given PEM formatted keys
CRYPTOHELPER_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(const char *pubKeyString, const char *privKeyString, const char *passPhrase);

//Create using preloaded keys.
CRYPTOHELPER_API IDigitalSignatureManager * createDigitalSignatureManagerInstanceFromKeys(CLoadedKey *pubKey, CLoadedKey *privKey);

} // namespace cryptohelper

#endif

