#include "DaStorKeyGenerator.h"

#include <string>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stdio.h> 
#include <stdlib.h>
#include <time.h>

#include "rsa.h"
#include "osrng.h"
#include "files.h"
#include "base64.h"
#include "modes.h"

DaStorKeyGenerator::DaStorKeyGenerator(string keyName, string cppName)
{
    this->keyName = keyName;
    this->cppName = cppName;
}

void DaStorKeyGenerator::work()
{
    AutoSeededRandomPool rnd;

    cout << "gen key in memery." << endl;
    rnd.GenerateBlock(keyData, AES::DEFAULT_KEYLENGTH);
    saveKeyFile();
    saveKeyCpp();
}

void DaStorKeyGenerator::saveKeyFile()
{
    FILE *fp = fopen(keyName.c_str(), "w+");
    fwrite(keyData, 1, sizeof(keyData), fp);
    fclose(fp);
}

void DaStorKeyGenerator::saveKeyCpp()
{
    FILE *fp = fopen(cppName.c_str(), "w+");

    fprintf(fp, "#include \"DaStorAppKey.h\"\n");
    fprintf(fp, "#include <stdio.h>\n");
    fprintf(fp, "#include <string.h>\n");

    fprintf(fp, "DaStorAppKey * DaStorAppKey::instance = NULL;\n");

    fprintf(fp, "void DaStorAppKey::init()\n");
    fprintf(fp, "{\n");
    for (int i=0; i< sizeof(keyData); i++)
    {
        fprintf(fp, "    dastor_app_key[%d]=%d;\n", i, keyData[i]);
    }
    fprintf(fp, "%s\n", "}\n");

    fclose(fp);
}

int main(int argc, char** argv)
{
    string keyName = argv[1];
    string cppName = argv[2];

    DaStorKeyGenerator gen(keyName, cppName);
    gen.work();
}

