#ifndef _DASTOR_KEY_GENERATOR_H_
#define _DASTOR_KEY_GENERATOR_H_

#include <string>
#include "aes.h"

using namespace std;
using namespace CryptoPP;

class DaStorKeyGenerator
{
public:
    DaStorKeyGenerator(string keyName, string cppName);

    void work();

private:

    void saveKeyFile();

    void saveKeyCpp();

    string keyName;
    string cppName;

	byte   keyData[AES::DEFAULT_KEYLENGTH];
};

#endif

