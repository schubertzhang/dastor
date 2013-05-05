#include "DaStorArchive.h"

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

using namespace std;
using namespace CryptoPP;

DaStorArchiveMaker::DaStorArchiveMaker(string dir, string archive, string key)
{
    this->dir = dir;
    this->archive = archive;
    this->key = key;
    
    FILE *fp = fopen(key.c_str(), "r+");
    fread(keyData, 1, sizeof(keyData), fp);
    fclose(fp);
}

void DaStorArchiveMaker::work()
{
    FILE *ar = fopen(archive.c_str(), "w+");
    doArchive(ar, dir, "");
    fclose(ar);
}

void DaStorArchiveMaker::doArchive(FILE *ar, string dirName, string dispName)
{
    string tmpfile="/tmp/tmp";

    DIR* dir=opendir(dirName.c_str());
    
    struct dirent* entry;
    while((entry=readdir(dir)))
    {
        if (strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0)
            continue;
        string fullname = dirName+"/"+entry->d_name;
        struct stat statbuf;
        stat(fullname.c_str(), &statbuf);
        if (statbuf.st_mode & S_IFDIR)
        {
            string dirDispName;
            if (dispName=="")
                dirDispName = entry->d_name;
            else
                dirDispName = dispName+"."+entry->d_name;
            doArchive(ar, fullname, dirDispName);
        }
        else if (statbuf.st_mode & S_IFREG)
        {
            string fileDispName;
            if (dispName=="")
                fileDispName = entry->d_name;
            else
                fileDispName = dispName+"."+entry->d_name;

            ECB_Mode< AES >::Encryption e;
            // CBC Mode does not use an IV
            e.SetKey( keyData, sizeof(keyData) );

            StringSource( fileDispName, true, 
                new StreamTransformationFilter( e,
                    new FileSink(tmpfile.c_str())
                ) // StreamTransformationFilter      
            ); // StringSource

            struct stat statName;
            stat(tmpfile.c_str(), &statName);

            int nameLength = (int)statName.st_size;
            fwrite(&nameLength, 1, sizeof(nameLength), ar);

            cout << "name length :" << nameLength << endl;
            
            char *name = (char*)malloc(nameLength);
            FILE *fpName = fopen(tmpfile.c_str(), "r+");
            fread(name, 1, nameLength, fpName);
            fclose(fpName);
            fwrite(name, 1, nameLength, ar);
            free(name);

            int fileLength = (int)statbuf.st_size;
            fwrite(&fileLength, 1, sizeof(nameLength), ar);

            char *data = (char*)malloc(fileLength);
            FILE *fp = fopen(fullname.c_str(), "r+");
            fread(data, 1, fileLength, fp);
            fclose(fp);
            fwrite(data, 1, fileLength, ar);
            free(data);
        }
    }
    closedir(dir);
}

DaStorArchiveReader::DaStorArchiveReader(string archive, string key)
{
    this->archive = archive;
    this->key = key;
    
    FILE *fp = fopen(key.c_str(), "r+");
    fread(keyData, 1, sizeof(keyData), fp);
    fclose(fp);

    struct stat statArc;
    stat(archive.c_str(), &statArc);
    this->archiveLength = statArc.st_size;

    ar = fopen(archive.c_str(), "r+");
}

DaStorArchiveReader::DaStorArchiveReader(string archive, byte* key)
{
    this->archive = archive;

    memcpy(keyData, key, sizeof(keyData));

    struct stat statArc;
    stat(archive.c_str(), &statArc);
    this->archiveLength = statArc.st_size;

    ar = fopen(archive.c_str(), "r+");
}

bool DaStorArchiveReader::hasNext()
{
    if (ftell(ar) == this->archiveLength)
        return false;
    else
        return true;
}

void DaStorArchiveReader::next(DaStorArchiveEntry & entry)
{
    int nameLength;
   fread(&nameLength, 1, sizeof(nameLength), ar);
    char *cryptName = (char*)malloc(nameLength);

    FILE *tmp = fopen("/tmp/tmp", "w+");
    fread(cryptName, 1, nameLength, ar);
    fwrite(cryptName, 1, nameLength, tmp);
    fclose(tmp);
    free(cryptName);
    
    ECB_Mode< AES >::Decryption d;
    // CBC Mode does not use an IV
    d.SetKey( keyData, sizeof(keyData));

    // The StreamTransformationFilter removes
    //  padding as required.
    FileSource s( "/tmp/tmp", true, 
        new StreamTransformationFilter( d,
            new StringSink( entry.name )
        ) // StreamTransformationFilter
    ); // StringSource

   fread(&entry.dataLength, 1, sizeof(entry.dataLength), ar);
   entry.data = (char*)malloc(entry.dataLength);
    fread(entry.data, 1, entry.dataLength, ar);

    string command = "rm -f /tmp/tmp";
    system(command.c_str());
}

int main(int argc, char** argv)
{
    string dir = argv[1];
    string output = argv[2];
    string key = argv[3];

    DaStorArchiveMaker maker(dir, output, key);
    maker.work();

    DaStorArchiveReader reader(output, key);
    while (reader.hasNext()) 
    {
        DaStorArchiveEntry entry;
        reader.next(entry);
        cout << entry.name << endl;
    }
}


