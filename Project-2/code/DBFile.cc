#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;

//Looked at COP6726 A1.pdf for details on how functions are used
DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	//cout << fileName << endl;

}

void DBFile::Load (Schema& schema, char* textFile) {
}

int DBFile::Close () {
}

void DBFile::MoveFirst () {
}

void DBFile::AppendRecord (Record& rec) {
}

int DBFile::GetNext (Record& rec) {
}

string DBFile::GetFile (){
	return fileName;
}