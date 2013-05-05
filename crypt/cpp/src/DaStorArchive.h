#ifndef _DASTOR_ARCHIVE_H_
#define _DASTOR_ARCHIVE_H_

#include <string>
#include "aes.h"

using namespace std;
using namespace CryptoPP;

class DaStorArchiveEntry
{
public:

	DaStorArchiveEntry()
    {
        name = "";
		dataLength = 0;
		data = NULL;
    }

	DaStorArchiveEntry(const DaStorArchiveEntry &entry)
    {
        name = entry.name;
		dataLength = entry.dataLength;
		data = entry.data;
    }

	string name;
	int dataLength;
	char * data;
};

class DaStorArchiveMaker
{
public:
	DaStorArchiveMaker(string dir, string archive, string key);
	void work();
private:
	string dir;
	string archive;
	string key;
	byte   keyData[AES::DEFAULT_KEYLENGTH];

	void doArchive(FILE *ar, string dirName, string dispName);
};


class DaStorArchiveReader
{
public:
	DaStorArchiveReader(string archive, string key);
	DaStorArchiveReader(string archive, byte* key);
	bool hasNext();
	void next(DaStorArchiveEntry &entry);
private:
	string archive;
	string key;
	byte   keyData[AES::DEFAULT_KEYLENGTH];
	FILE *ar;
	long archiveLength;
};

#endif

