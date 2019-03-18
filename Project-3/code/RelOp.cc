#include <iostream>
#include "RelOp.h"

using namespace std;


ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


Scan::Scan(Schema& _schema, DBFile& _file) {
	schema = _schema;
	file = _file;
}

Scan::~Scan() {
	printf("Deconstructor Scan\n");
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
	printf("Deconstructor Select\n");
}

bool Select::GetNext(Record& _record){
	while(true){
		bool ret = producer->GetNext(_record);
		if(!ret){
			return false;
		}else{
			ret = predicate.Run(_record,constants);
			if(ret){
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
	printf("Deconstructor Project\n");
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
	printf("Deconstructor Join\n");
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
	printf("Deconstructor DuplicateRemoval\n");
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
}

Sum::~Sum() {
	printf("Deconstructor Sum\n");
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
	printf("Deconstructor GroupBy\n");
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
}

WriteOut::~WriteOut() {
	printf("Deconstructor WriteOut\n");
}

bool GetNext(Record& _record) {
	
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
