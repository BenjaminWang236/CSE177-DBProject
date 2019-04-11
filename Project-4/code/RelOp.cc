#include <iostream>
#include <string.h>
#include <sstream>
#include "EfficientMap.h"
#include "RelOp.h"
#include <map>

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}
bool Scan::GetNext(Record& _record){
	if(file.GetNext(_record)==0){
		//if there is a record, return
		return true;
	}else{
		//no records
 		return false;
	}
}
Scan::~Scan() {
	//printf("Deconstructor Scan\n");
}

ostream& Scan::print(ostream& _os) {
	return _os << "SCAN[" << file.GetFile()<<"]" << endl;
}


Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;	
}

Select::~Select() {
	//printf("Deconstructor Select\n");
}

bool Select::GetNext(Record& _record){
	//While there are still records from the producer
	while(true){
		//Get the records
		bool ret = producer->GetNext(_record);
		if(!ret){
			//No more records
			return false;
		}else{
			//If there are records
			//compare the record to the constants gotten from the predicates
			ret = predicate.Run(_record,constants);
			if(ret){
				//Success, found a matching tuple
				return true;
			}
		}
	}
}

ostream& Select::print(ostream& _os) {
	_os << "SELECT[ Schema:{";
	vector<Attribute> a = schema.GetAtts();
	int j = 0;
	for(int i = 0; i < a.size() ;i++){
		_os << a[i].name;
		if(i != a.size() - 1 ){
			_os << ", ";
		}
	}
	_os << "}; Predicate (";
	
	for(int i = 0; i < predicate.numAnds;i++){
		Comparison c = predicate.andList[j];
		if(c.operand1 != Literal){
			_os << a[c.whichAtt1].name;
		}else{
			int pointer = ((int *) constants.GetBits())[i + 1];
			if (c.attType == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
			}
			// then is a character string
			else if (c.attType == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
			} 
		}
		if(c.op == Equals){
			_os << " = ";
		}else if(c.op == GreaterThan){
			_os << " > ";
		}else if(c.op == LessThan){
			_os << " < ";
		}
		
		if(c.operand2 != Literal){
			_os << a[c.whichAtt2].name;
		}else{
			int pointer = ((int *) constants.GetBits())[i + 1];
			if (c.attType == Integer) {
				int *myInt = (int *) &(constants.GetBits()[pointer]);
				_os << *myInt;
			}
			// then is a double
			else if (c.attType == Float) {
				double *myDouble = (double *) &(constants.GetBits()[pointer]);
				_os << *myDouble;
				//_os << "issafloat";
			}
			// then is a character string
			else if (c.attType == String) {
				char *myString = (char *) &(constants.GetBits()[pointer]);
				_os << myString;
				//_os << "issastring";
			} 
		}
		j++;

		if(i < predicate.numAnds -1){
			_os << " AND ";
		}
	}
	return _os << ")] \t--" << *producer<< endl;
}


Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;
}

Project::~Project() {
	//printf("Deconstructor Project\n");
}

bool Project::GetNext(Record& _record)
{
	//Get the produces record
	bool ret = producer->GetNext(_record);
	if (ret)
	{
		//Success project the schema
		_record.Project(keepMe,numAttsOutput,numAttsInput);
		return true;
	}
	else
	{
		//Nothing left to project
		//cout << "false project" << endl;
		return false;
	}
}

ostream& Project::print(ostream& _os) {
	_os << "PROJECT[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]"<< endl;
	return _os << "\t\n\t--"<< *producer;
}


Join::Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right; 

}

Join::~Join() {
	//printf("Deconstructor Join\n");
}

ostream& Join::print(ostream& _os) {
	_os << "JOIN[ schemaLeft: {"; 
	for(int i = 0; i < schemaLeft.GetAtts().size();i++){
		_os << schemaLeft.GetAtts()[i].name;
		if(i != schemaLeft.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "},\n";
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	} 
	_os << "schemaRight: {";
	for(int i = 0; i < schemaRight.GetAtts().size();i++){
		_os << schemaRight.GetAtts()[i].name;
		if(i != schemaRight.GetAtts().size()-1){
			_os << ", ";
		}
	}	
	_os << "},\n";
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	} 
	_os << "schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]\tNumber of Tuples: " << size << endl; 
	for(int i = 0; i < push+1;i++){
		_os << "\t";
	}
 	_os <<"--"<<*right ;
 	for(int i = 0; i < push+1;i++){
		_os << "\t";
	}
 	return _os <<"--" << *left;
}


DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}

DuplicateRemoval::~DuplicateRemoval() {
	//printf("Deconstructor DuplicateRemoval\n");
}

bool DuplicateRemoval::GetNext(Record& _record){
	KeyString key;							//hashtable key
	stringstream data;						//string containing all data from tuple/record
	vector<Attribute> att = schema.GetAtts();	//attributes from schema
	while (producer->GetNext(_record))			//while there are still records 
	{
		for (int i = 0; i < schema.GetNumAtts(); i++) {		//for all attributes check if in hashmap
			int pointer = ((int *)_record.GetBits())[i + 1];	//pointer to current record
			//is a int
			if (att[i].type == Integer) {
				data << att[i].name << " "<< *(int *) &(_record.GetBits()[pointer]);		//gets value in pointer and puts into data
			}
			// then is a double
			else if (att[i].type == Float) {
				data << att[i].name << " " << *(double *) &(_record.GetBits()[pointer]);	//gets value in pointer and puts into data

			}
			//is a string
			else if (att[i].type == String) {
				string dataString = (char *) &(_record.GetBits()[pointer]);					//gets value in pointer and puts into data
				data << att[i].name << " " << dataString;
			}
			data << " ";					//for debugging readability
			key = data.str();				//set hashtable key to data
			

		}
		//cout << key << endl;
		if (!map.IsThere(key))				//check if already in hashmap; if not then insert
		{
			map.Insert(key, key);
			return true;
		}
	}
	return false;
}
ostream& DuplicateRemoval::print(ostream& _os) {
	_os << "DISTINCT[{";
	for(int i = 0; i < schema.GetAtts().size();i++){
		_os << schema.GetAtts()[i].name;
		if(i != schema.GetAtts().size()-1){
			_os << ", ";
		}
	}
	return _os<<"}]"<< "\n\t\n\t--" << *producer;
}


Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
	computed = false;
}

Sum::~Sum() {
	//printf("Deconstructor Sum\n");
}

bool Sum::GetNext(Record& _record){
	Type type;
	double runningDouble = 0;
	int runningInt = 0;
	char* value = new char[100];

	if(!computed){
		while(producer->GetNext(_record)){
			int tempInt = 0;
			double tempDouble = 0;
			type = compute.Apply(_record,tempInt,tempDouble);
			runningInt+=tempInt;
			runningDouble+=tempDouble;			
		}
		//cout << runningDouble << endl;

		//Current position in record is sizeof(int) * (number of atts + 1), number of atts = 1
		//Therefore current position is 2*sizeof(int)
		int currentPos = 2*sizeof(int);

		//set up the pointer to the current attribute in the record
		//i = 0, i + 1 = 1
		((int*)value)[1] = currentPos;	
		//Convert data to correct binary representation
		if(type == Integer){
			*((int*)&value[currentPos]) = runningInt;
			currentPos+=sizeof(int);

		}else if(type == Float){
			*((double*)&value[currentPos]) = runningDouble;
		    currentPos+=sizeof(double);			
		}
		//set up the pointer to just past the end of the record
		((int*)value)[0] = currentPos;

		//copy bits		
		char* runningSum = new char[currentPos];	
		memcpy(runningSum,value,currentPos);
		_record.Consume(runningSum);

		computed = true;

		// _record.print(cout,schemaOut);
		// cout << endl;

		return true;
	}
	return false;


}

ostream& Sum::print(ostream& _os) {
	 _os << "SUM:[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	return _os << "}]" << "\n\t\n\t--" <<*producer;
}


GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts; 
	compute = _compute;
	producer = _producer;	
}

GroupBy::~GroupBy() {
	//printf("Deconstructor GroupBy\n");
}

// bool GroupBy:: GetNext(Record& _record){
// 	//Record rec = _record;
// 	_record.Project(groupingAtts.whichAtts,groupingAtts.numAtts,schemaIn.GetNumAtts());

// 	Schema copy = schemaOut;
// 	vector <Attribute> atts;
// 	vector <string> attNames;
// 	atts = copy.GetAtts();
// 	for(int i = 1; i < copy.GetNumAtts();i++){
// 		attNames.push_back(atts[i].name);
// 	}
// 	int iter = 0;
// 	int viter = 1;
// 	//rec.print(cout, schemaOut);
// 	while(producer->GetNext(_record)){

// 		int pointer = ((int *) _record.GetBits())[iter + 1];
// 		if (atts[viter].type == Integer) {
// 			int *myInt = (int *) &(_record.GetBits()[pointer]);
// 			cout << *myInt << endl;
// 		}
// 		// then is a double
// 		else if (atts[viter].type == Float) {
// 			double *myDouble = (double *) &(_record.GetBits()[pointer]);
// 			cout << *myDouble << endl;
// 		}
// 		// then is a character string
// 		else if (atts[viter].type == String) {
// 			char *myString = (char *) &(_record.GetBits()[pointer]);
// 			cout << myString << endl;
// 		} 
// 		viter++;
// 		//if(groups.IsThere())
// 		return true;
// 	}
// 	return false;
// }
bool GroupBy:: GetNext(Record& _record){
	//Record rec = _record;
	_record.Project(groupingAtts.whichAtts,groupingAtts.numAtts,schemaIn.GetNumAtts());

	Schema copy = schemaOut;
	vector <Attribute> atts;
	vector <string> attNames;
	atts = copy.GetAtts();
	for(int i = 1; i < copy.GetNumAtts();i++){
		attNames.push_back(atts[i].name);
	}
	int iter = 0;
	int viter = 1;
	int runningInt = 0;
	int runningDouble = 0;
	//rec.print(cout, schemaOut);
	while(producer->GetNext(_record)){
		KeyString name = atts[viter].name;
		KeyDouble value;
		int pointer = ((int *) _record.GetBits())[iter + 1];
		if(groups.IsThere(name)){
			//cout << "name is found" << endl;
			if (atts[viter].type == Integer) {
				int *myInt = (int *) &(_record.GetBits()[pointer]);
				//cout << *myInt << endl;
				runningInt+=*myInt;
				value = groups.Find(name);
				groups.Remove(name,name,value);
				value = runningInt;
				groups.Insert(name,value);
			}
			// then is a double
			else if (atts[viter].type == Float) {
				double *myDouble = (double *) &(_record.GetBits()[pointer]);
				//cout << *myDouble << endl;
				runningDouble+=*myDouble;
				value = groups.Find(name);
				groups.Remove(name,name,value);
				value = runningDouble;
				groups.Insert(name,value);				
			}
			// then is a character string
			// else if (atts[viter].type == String) {
			// 	char *myString = (char *) &(_record.GetBits()[pointer]);
			// 	//cout << myString << endl;
			// } 
		}else{
			cout << "name not found" << endl;
			cout << name << endl;
			if (atts[viter].type == Integer) {
				int *myInt = (int *) &(_record.GetBits()[pointer]);
				//cout << *myInt << endl;
				value = *myInt;
				groups.Insert(name,value);
			}
			// then is a double
			else if (atts[viter].type == Float) {
				double *myDouble = (double *) &(_record.GetBits()[pointer]);
				//cout << *myDouble << endl;
				value = *myDouble;
				groups.Insert(name,value);
			}			
		}
		viter++;
		//if(groups.IsThere())
		return true;
	}
	groups.MoveToStart();
	for(int i = 0; i < groups.Length();i++){
		cout << groups.CurrentKey()<< ": "<< groups.CurrentData()<< endl;
		groups.Advance();
	}
	return false;
}
ostream& GroupBy::print(ostream& _os) {
	_os << "GROUP BY[ schemaIn: {";
	for(int i = 0; i < schemaIn.GetAtts().size();i++){
		_os << schemaIn.GetAtts()[i].name;
		if(i != schemaIn.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"},schemaOut: {";
	for(int i = 0; i < schemaOut.GetAtts().size();i++){
		_os << schemaOut.GetAtts()[i].name;
		if(i != schemaOut.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os << "}]"<< endl;
	return _os << "\t\n\t--"<< *producer;
}


WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	outFile = _outFile;
	//Open the file stream
	out.open(outFile.c_str());

}

WriteOut::~WriteOut() {
	//printf("Deconstructor WriteOut\n");
	//if filestream is open, close it
	if(out.is_open()){
		out.close();
	}
}

bool WriteOut::GetNext(Record& _record) {
	//Get record from producer
	bool ret = producer->GetNext(_record);
	if (ret)
	{	
		//Write to the outfile all the records matching the schema
		_record.print(out,schema);
		out << endl;
		//For demo only, need to comment out all other cout statments
		// _record.print(cout,schema);
		// cout << endl;
		return true;
	}
	else
	{		
		//Close the file stream
		out.close();
		return false;
	}
}

ostream& WriteOut::print(ostream& _os) {
	_os << "WRITE OUT [{";
	for(int i = 0; i < schema.GetAtts().size();i++){
		_os << schema.GetAtts()[i].name;
		if(i != schema.GetAtts().size()-1){
			_os << ", ";
		}
	}
	_os<<"}]"<< endl; 
	return _os << "\t--"<< *producer;
}


ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	_os << "QUERY EXECUTION TREE " <<endl; 
	return _os << "--"<<*_op.root;
}

void QueryExecutionTree::ExecuteQuery(){
		Record rec;
		while(root->GetNext(rec)){
		}
}