#include "DaStorSignatureGenerator.h"

#include <stdio.h>
#include <string.h>
#include <string>

#include "DaStorMD5.h"

using namespace std;

DaStorSignatureGenerator::DaStorSignatureGenerator(string dastorFile, string tomcatFile, string cppFile)
{
    this->dastorFile = dastorFile;
    this->tomcatFile = tomcatFile;
    this->cppFile = cppFile;
}

void DaStorSignatureGenerator::work()
{
    FILE *fp = fopen(cppFile.c_str(), "w+");

    fprintf(fp, "#include \"DaStorSignature.h\"\n");
    fprintf(fp, "#include <stdio.h>\n");
    fprintf(fp, "#include <string.h>\n");

    fprintf(fp, "DaStorSignature * DaStorSignature::instance = NULL;\n");

    fprintf(fp, "void DaStorSignature::init()\n");
    fprintf(fp, "{\n");
    unsigned char sig[16];
    loadSignature(dastorFile, sig);
    for (int i=0; i<16; i++)
    {
        fprintf(fp, "    sig_dastor[%d] = 0x%02x;\n", i, sig[i]);
    }
    loadSignature(tomcatFile, sig);
    for (int i=0; i<16; i++)
    {
        fprintf(fp, "    sig_tomcat[%d] = 0x%02x;\n", i, sig[i]);
    }
    
    fprintf(fp, "}\n");
    fclose(fp);    
}

void DaStorSignatureGenerator::loadSignature(string file, unsigned char * sig)
{
    memset(sig, 0, 16);

    md5_context context;
    md5_init(&context);

    FILE *fp = fopen(file.c_str(), "r+");

    char * buffer[4096];
    int len = 0;

    while ((len=fread(buffer, 1, 4096, fp)) > 0)
    {
        md5_update(&context, (unsigned char*)buffer, (unsigned int)len);
    }

    md5_final(&context, sig);

    fclose(fp);
    
    char md5String[36];
    sprintf(md5String, 
        "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x", 
        sig[0], sig[1], sig[2], sig[3], sig[4], sig[5], sig[6], sig[7], 
        sig[8], sig[9], sig[10], sig[11], sig[12], sig[13], sig[14], sig[15]);
    md5String[32] = '\0';
    
    printf("MD5 compute : FILE %s, SIG %s.\n", file.c_str(), md5String);
}

int main(int argc, char ** argv)
{
    string dastorFile(argv[1]);
    string tomcatFile(argv[2]);
    string cppFile(argv[3]);

    DaStorSignatureGenerator generator(dastorFile, tomcatFile, cppFile);
    generator.work();

    return 0;
}
