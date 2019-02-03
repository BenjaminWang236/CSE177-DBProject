#include <iostream>
#include "sqlite3.h"
#include <stdio.h>
#include <stdlib.h>
#include "Schema.h"
#include "Catalog.h"

using namespace std;



Catalog::Catalog(string& _fileName) {
	zErrMsg = 0;
	sent = false;
	tempN = "sentinel";
	rc = sqlite3_open(_fileName.c_str(),&db);

	if( rc ) {
    	fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
   	} else {
    	fprintf(stderr, "Opened database successfully\n");
    	string selAtt = "SELECT a.name,a.type,a.numDistVal,a.tName,t.numTuples,t.fileLoc FROM attributes a,tables t WHERE a.tName = t.name";
    	//string selAtt = "SELECT * FROM attributes a,tables t WHERE a.tName = t.name";
   		rc = sqlite3_exec(db, selAtt.c_str(), putStuffIn, this, &zErrMsg);
	   	if( rc != SQLITE_OK ){
	      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
	      	sqlite3_free(zErrMsg);
	   }else {
	     	fprintf(stdout, "Table copied successfully\n");
	   }
   }


}
int Catalog::putStuffIn(void* catalog, int argc, char** argv, char ** azColName){
	Catalog data = *(Catalog *) (catalog);
	string name = argv[0]; 
	string type = argv[1];
	unsigned int numDistinct = atoi(argv[2]);
	string tName = argv[3];
	if(tName != data.tempN){
		data.tempN = tName;
		if(!data.sent){
			data.schemas.push_back(Schema(data.attributes,data.types,data.distincts));
			data.schemaN.push_back(tName);
			data.schemaT.push_back(atoi(argv[4]));
			data.schemaL.push_back(argv[5]);
			data.attributes.clear();
			data.types.clear();
			data.distincts.clear();
		}
		data.sent = true;
	}
	data.attributes.push_back(name);
	data.types.push_back(type);
	data.distincts.push_back(numDistinct);
	return 0;



}
Catalog::~Catalog() {
	Save();

}

bool Catalog::Save() {
	string delAllT = "DELETE FROM tables";
	string delAllA = "DELETE FROM attributes";
	string insAllT = "INSERT INTO tables VALUES(?,?,?)";
	string insAllA = "INSERT INTO attributes VALUES(?,?,?,?)";
	rc = sqlite3_exec(db, delAllT.c_str(), 0, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
      	sqlite3_free(zErrMsg);
    }else {
     	fprintf(stdout, "tables emptied successfully\n");
    }
	rc = sqlite3_exec(db, delAllA.c_str(), 0, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
      	sqlite3_free(zErrMsg);
    }else {
     	fprintf(stdout, "attributes emptied successfully\n");
    }	
	rc = sqlite3_prepare_v2( db, insAllT.c_str(), -1, &stmt, 0 );
	for(int i = 0; i < schemaN.size();i++){
		rc = sqlite3_bind_text( stmt, 1, schemaN[i].c_str(),schemaN[i].size(), SQLITE_STATIC);
		rc = sqlite3_bind_int( stmt, 2, schemaT[i]);
		rc = sqlite3_bind_text( stmt, 3, schemaL[i].c_str(),schemaL[i].size(), SQLITE_STATIC);
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
		    printf("ERROR inserting data: %s\n", sqlite3_errmsg(db));
		}
		sqlite3_finalize(stmt);
	}
	rc = sqlite3_prepare_v2( db, insAllA.c_str(), -1, &stmt, 0 );
	for(int i = 0; i < attributes.size();i++){
		rc = sqlite3_bind_text( stmt, 1, attributes[i].c_str(),attributes[i].size(), SQLITE_STATIC);
		rc = sqlite3_bind_text( stmt, 2, types[i].c_str(),types[i].size(),SQLITE_STATIC);
		rc = sqlite3_bind_int( stmt, 3, distincts[i]);
		rc = sqlite3_step(stmt);
		if (rc != SQLITE_DONE) {
		    printf("ERROR inserting data: %s\n", sqlite3_errmsg(db));
		}
		sqlite3_finalize(stmt);
	}
   

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table && schemaT[i] == _noTuples){
			return true;
		}
	}
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			schemaT[i] == _noTuples;
		}
	}
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table && schemaL[i] == _path){
			return true;
		}
	}
	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			schemaL[i] == _path;
		}
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	return true;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
}

void Catalog::GetTables(vector<string>& _tables) {
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	return true;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	return true;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {
	return true;
}

bool Catalog::DropTable(string& _table) {
	return true;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	return _os;
}
