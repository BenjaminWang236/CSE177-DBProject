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
    	rc = sqlite3_prepare_v2( db, selAtt.c_str(), -1, &stmt, 0 );
    	while ( (rc = sqlite3_step(stmt)) == SQLITE_ROW) {                                              
			string name = reinterpret_cast<const char*> (sqlite3_column_text(stmt,0)); 
			string type = reinterpret_cast<const char*> (sqlite3_column_text(stmt,1));

			unsigned int numDistinct = sqlite3_column_int(stmt,2);
			string tName = reinterpret_cast<const char*> (sqlite3_column_text(stmt,3));
			if(tName != tempN){
				tempN = tName;
				//cout << tempN << " " <<sqlite3_column_int(stmt,4) << " "<< reinterpret_cast<const char*> (sqlite3_column_text(stmt,5)) <<endl;
				schemaN.push_back(tempN);
				schemaT.push_back(sqlite3_column_int(stmt,4));
				schemaL.push_back(reinterpret_cast<const char*> (sqlite3_column_text(stmt,5)));
				

			}

			//cout << name << " " << type << " " << numDistinct << endl;
			tNames.push_back(tName);
			attributes.push_back(name);
			types.push_back(type);
			distincts.push_back(numDistinct);


    	}	   	
    	sqlite3_finalize(stmt);
    	if( rc != SQLITE_DONE ){
    		cout << rc << endl;
	      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
	      	sqlite3_free(zErrMsg);
	   }else {
	  		for(int i = 0; i < schemaN.size();i++){
   				vector<string> aN;
   				vector<string> aT;
   				vector<unsigned int> aD;
   				//cout << schemaN[i] << " ";
	   			for(int j = 0; j < attributes.size();j++){
	   				if(tNames[j] == schemaN[i]){
	   					// cout << attributes[j] << " ";
	   					// cout << types[j] << " ";
	   					// cout << distincts[j] << endl;
	   					aN.push_back(attributes[j]);
	   					aT.push_back(types[j]);
	   					aD.push_back(distincts[j]);

	   				}

	   			}
	   			if(!aN.empty()){
	   				schemas.push_back(Schema(aN,aT,aD));
	   			}
	   		}
	     	fprintf(stdout, "Table copied successfully\n");
	     	print();


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


			//data.schemas.push_back(Schema(data.attributes,data.types,data.distincts));
			data.schemaN.push_back(data.tempN);

			data.schemaT.push_back(atoi(argv[4]));
			data.schemaL.push_back(argv[5]);
			data.tempN = tName;

	}
	data.tNames.push_back(tName);
	data.attributes.push_back(name);
	data.types.push_back(type);
	data.distincts.push_back(numDistinct);
	//cout << data.tNames.size() << endl;
	return 0;

}
Catalog::~Catalog() {
	cout << "Destructor" << endl;
	Save();


}

bool Catalog::Save() {	
	string delAllT = "DELETE FROM tables";
	string delAllA = "DELETE FROM attributes";
	string insAllT = "INSERT INTO tables VALUES(?1,?2,?3)";
	string insAllA = "INSERT INTO attributes VALUES(?1,?2,?3,?4)";
	rc = sqlite3_exec(db, delAllT.c_str(), 0, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
      	sqlite3_free(zErrMsg);
    }else {
     	//fprintf(stdout, "tables emptied successfully\n");
    }
	rc = sqlite3_exec(db, delAllA.c_str(), 0, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
      	fprintf(stderr, "SQL error: %s\n", zErrMsg);
      	sqlite3_free(zErrMsg);
    }else {
     	//fprintf(stdout, "attributes emptied successfully\n");

    }
	
	rc = sqlite3_prepare_v2( db, insAllT.c_str(), -1, &stmt, 0 );

	for(int i = 0; i < schemaN.size();i++){
		sqlite3_reset(stmt);
		rc = sqlite3_bind_text( stmt, 1, schemaN[i].c_str(),schemaN[i].size(), SQLITE_STATIC);

		rc = sqlite3_bind_int( stmt, 2, schemaT[i]);

		rc = sqlite3_bind_text( stmt, 3, schemaL[i].c_str(),schemaL[i].size(), SQLITE_STATIC);

		rc = sqlite3_step(stmt);

		if (rc != SQLITE_DONE) {
			printf("ERROR inserting data: %s\n", sqlite3_errmsg(db));
		}


	}
	sqlite3_finalize(stmt);
	rc = sqlite3_prepare_v2( db, insAllA.c_str(), -1, &stmt, 0 );
	for(int i = 0; i < schemas.size();i++){
		vector<Attribute> temp = schemas[i].GetAtts();
		for(int j = 0; j < temp.size();j++){
			sqlite3_reset(stmt);
			rc = sqlite3_bind_text( stmt, 1, temp[j].name.c_str(),temp[j].name.size(), SQLITE_TRANSIENT);

			string typeT;
			if (temp[j].type == Integer) typeT = "INTEGER";
			else if (temp[j].type == Float) typeT = "FLOAT";
			else if (temp[j].type == String) typeT = "STRING";
			rc = sqlite3_bind_text( stmt, 2, typeT.c_str(),typeT.size(),SQLITE_STATIC);

			rc = sqlite3_bind_int( stmt, 3, temp[j].noDistinct);
			rc = sqlite3_bind_text( stmt, 4, schemaN[i].c_str(),schemaN[i].size(),SQLITE_STATIC);

			rc = sqlite3_step(stmt);
			if (rc != SQLITE_DONE) {
				
				printf("ERROR inserting data: %s\n", sqlite3_errmsg(db));

			}				   			
		}
	}
	sqlite3_finalize(stmt);

}

bool Catalog::GetNoTuples(string& _table, unsigned int& _noTuples) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			_noTuples = schemaT[i];
			return true;
		}
	}
	return false;
}

void Catalog::SetNoTuples(string& _table, unsigned int& _noTuples) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			schemaT[i] = _noTuples;
		}
	}
}

bool Catalog::GetDataFile(string& _table, string& _path) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			_path = schemaL[i];
			return true;
		}
	}
	return false;
}

void Catalog::SetDataFile(string& _table, string& _path) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			schemaL[i] = _path;
		}
	}
}

bool Catalog::GetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			_noDistinct = schemas[i].GetDistincts(_attribute);
			return true;
		}
	}
	return false;
}
void Catalog::SetNoDistinct(string& _table, string& _attribute,
	unsigned int& _noDistinct) {

	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			int temp = schemas[i].Index(_attribute);
			schemas[i].GetAtts()[temp].noDistinct = _noDistinct;

		}
	}
}

void Catalog::GetTables(vector<string>& _tables) {
	_tables = schemaN;
}

bool Catalog::GetAttributes(string& _table, vector<string>& _attributes) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			vector<string> temp;
			for(int j = 0; j < schemas[i].GetAtts().size();j++){
				temp.push_back(schemas[i].GetAtts()[j].name);
			}
			_attributes = temp;
			return true;
		}
	}
	return false;
}

bool Catalog::GetSchema(string& _table, Schema& _schema) {
	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			_schema = schemas[i];
			return true;
		}
	}
	return false;
}

bool Catalog::CreateTable(string& _table, vector<string>& _attributes,
	vector<string>& _attributeTypes) {

	for(int i = 0; i < schemaN.size();i++){
		if(schemaN[i] == _table){
			cout << "Table with same name" << endl;
			return false;
		}
	}
	if(_attributes.size() == 0){
		cout << "0 Attributes" << endl;
		return false;
	}	
	if(_attributeTypes.size()!= _attributes.size()){
		cout << "NoTypes don't match NoAttributes" << endl;
		return false;
	}
	for(int i = 0; i < _attributes.size();i++){
		for(int j = 0; j < _attributes.size();j++){
			if(_attributes[i] == _attributes[j] && i!= j){
				cout << "Two attributes same name" << endl;
				return false;
			}
		}
	}

	for(int i = 0; i < _attributeTypes.size();i++){

		if(_attributeTypes[i]== "INTEGER"|| _attributeTypes[i]== "STRING"|| _attributeTypes[i]== "FLOAT"){

		}else{
			cout << _attributeTypes[i] << endl;			
			return false;
		}
	}

	schemaN.push_back(_table);
	vector<unsigned int> _noDistinct;
	for(int i = 0; i < _attributes.size();i++){
		_noDistinct.push_back(0);
		schemaT.push_back(0);
		schemaL.push_back("Unknown");
	}
	schemas.push_back(Schema(_attributes,_attributeTypes,_noDistinct));
	return true;
	
}

bool Catalog::DropTable(string& _table) {
	for(int i = 0; i < schemas.size();i++){
		if(schemaN[i] == _table){
			vector<string> sN = schemaN;
			vector<int> sT = schemaT;
			vector<string> sL = schemaL;
			vector<Schema> s = schemas;
			schemaN.clear();
			schemaT.clear();
			schemaL.clear();
			schemas.clear();
			for(int j = 0; j < sN.size();j++){
				if(j != i){
					schemaN.push_back(sN[j]);
					schemaT.push_back(sT[j]);
					schemaL.push_back(sL[j]);
					schemas.push_back(s[j]);
				}
			}
			return true;
		}
	}
	return false;
}

ostream& operator<<(ostream& _os, Catalog& _c) {
	for(int i = 0; i < _c.getSchemaN().size();i++){
		fprintf(stdout,"%s \t %u \t %s\n",_c.getSchemaN()[i].c_str(),_c.getSchemaT()[i],_c.getSchemaL()[i].c_str());
		vector<Attribute> temp = _c.getSchemas()[i].GetAtts();
		for(int j = 0; j < temp.size();j++){
			string typeT;
			if (temp[j].type == Integer) typeT = "INTEGER";
			else if (temp[j].type == Float) typeT = "FLOAT";
			else if (temp[j].type == String) typeT = "STRING";
			fprintf(stdout,"\t %s \t %s \t %u\n",temp[j].name.c_str(),typeT.c_str(),temp[j].noDistinct);
		}
	}
	return _os;
}
void Catalog::print(){
	cout << "START PRINT" << endl;
	for(int i = 0; i < getSchemaN().size();i++){
		fprintf(stdout,"%s \t %u \t %s\n",getSchemaN()[i].c_str(),getSchemaT()[i],getSchemaL()[i].c_str());
		vector<Attribute> temp = getSchemas()[i].GetAtts();
		for(int j = 0; j < temp.size();j++){
			string typeT;
			if (temp[j].type == Integer) typeT = "INTEGER";
			else if (temp[j].type == Float) typeT = "FLOAT";
			else if (temp[j].type == String) typeT = "STRING";
			fprintf(stdout,"\t %s \t %s \t %u\n",temp[j].name.c_str(),typeT.c_str(),temp[j].noDistinct);
		}
	}	
	cout << "END PRINT" << endl;
}
