#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;

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
	file.Open (10,f_path);
	//cout << fileName << endl;

}

void DBFile::Load (Schema& schema, char* textFile) {
	FILE* f = fopen("rt",textFile);
	Record rec;
	while(rec.ExtractNextRecord(schema,*f)){
		page.Append(rec);
	}
}

int DBFile::Close () {
	file.Close();
}

void DBFile::MoveFirst () {
	currPage = 0;
	file.GetPage(page,currPage);
}

void DBFile::AppendRecord (Record& rec) {
}

int DBFile::GetNext (Record& rec) {
	int ret = page.GetFirst(rec);
	if(ret){
		return true;
	}else{
		if(currPage == file.GetLength()){
			return false;
		}else{
			currPage++;
			file.GetPage(page,currPage);
			ret = page.GetFirst(rec);
			return true;
		}
	}
}

string DBFile::GetFile (){
	return fileName;
}