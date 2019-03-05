#include <vector>
#include <string>
#include <iostream>

#include "Schema.h"
#include "Catalog.h"

using namespace std;

void menu(Catalog& c){
	int choice;

	cout << "==============================" << endl;
	cout << "\t1: Create table" << endl;
	cout << "\t2: Drop table" << endl;
	cout << "\t3: Display Database Content" << endl;
	cout << "\t4: Save content" << endl;
	cout << "\t5: Set number of distincts of an attribute" << endl;
	cout << "\t6: Set number of tuples of a table" << endl;
	cout << "\t7: Set Datafile of a table" << endl;
	cout << "\t8: Get number of distincts for an attribute" << endl;
	cout << "\t9: Get number of tuples from a table" << endl;
	cout << "\t10: Get Datafile of a table" << endl;
	cout << "\t11: Exit" << endl;
	cout << "==============================" << endl;
	cout << "Your Choice: ";
	cin >> choice;
	switch(choice){

		case 1:
		{
			string tName;
			cout << "Table name: ";
			cin >> tName;
			cout << "Enter number of attributes: ";
			int numA;
			cin >> numA;
			vector<string> aN;
			vector<string> aT;
			for (int i = 0; i < numA;i++){
				cout << "Enter attribute name: ";
				string tN;
				cin >>tN;
				aN.push_back(tN);
				cout << "Enter attribute type: ";
				string tT;
				cin >> tT;
				aT.push_back(tT);
			}
			c.CreateTable(tName,aN,aT);
			menu(c);
		}
			break;
		case 2:
		{
			string tName;
			cout << "Enter table to drop: ";
			cin >> tName;
			c.DropTable(tName);
			menu(c);
		}
			break;
		case 3:
		{
			cout << c << endl;
			menu(c);
		}
			break;
		case 4:
		{
			c.Save();
			menu(c);
		}
			break;
		case 5:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			cout << "Enter attribute name: ";
			string aName;
			cin >> aName;
			cout << "Enter number of distincts: ";
			unsigned int tD;
			cin >> tD;
			c.SetNoDistinct(tName,aName,tD);
			menu(c);
		}
			break;
		case 6:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			cout << "Enter number of tuples: ";
			unsigned int nTuple;
			cin >> nTuple;
			c.SetNoTuples(tName,nTuple);
			menu(c);			
		}
			break;
		case 7:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			cout << "Enter File name ";
			string fName;
			cin >> fName;
			c.SetDataFile(tName,fName);
			menu(c);			
		}
			break;
		case 8:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			cout << "Enter attribute name: ";
			string aName;
			cin >> aName;
			unsigned int tD;
			c.GetNoDistinct(tName,aName,tD);
			cout << "Number of distincts is: " << tD << endl;
			menu(c);			
		}
			break;
		case 9:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			unsigned int nTuple;
			c.GetNoTuples(tName,nTuple);
			cout << "Number of tuples is: " << nTuple << endl;
			menu(c);
		}			
			break;
		case 10:
		{
			cout << "Enter table name: ";
			string tName;
			cin >> tName;
			string fName;
			c.GetDataFile(tName,fName);
			cout << "Datafile is: " << fName << endl;
			menu(c);				
		}
			break;
		case 11:{
			cout << "Closing menu" << endl;
		}
	}

}

int main () {
	// string table = "region", attribute, type;
	// vector<string> attributes, types;
	// vector<unsigned int> distincts;

	// attribute = "r_regionkey"; attributes.push_back(attribute);
	// type = "INTEGER"; types.push_back(type);
	// distincts.push_back(5);
	// attribute = "r_name"; attributes.push_back(attribute);
	// type = "STRING"; types.push_back(type);
	// distincts.push_back(5);
	// attribute = "r_comment"; attributes.push_back(attribute);
	// type = "STRING"; types.push_back(type);
	// distincts.push_back(5);

	// Schema s(attributes, types, distincts);
	// Schema s1(s), s2; s2 = s1;

	// string a1 = "r_regionkey", b1 = "regionkey";
	// string a2 = "r_name", b2 = "name";
	// string a3 = "r_commen", b3 = "comment";

	// s1.RenameAtt(a1, b1);
	// s1.RenameAtt(a2, b2);
	// s1.RenameAtt(a3, b3);

	// s2.Append(s1);

	// vector<int> keep;
	// keep.push_back(5);
	// keep.push_back(0);
	// s2.Project(keep);

	// cout << s << endl;
	// cout << s1 << endl;
	// cout << s2 << endl;


	 string dbFile = "catalog.sqlite";
	// Catalog c(dbFile);

	// c.CreateTable(table, attributes, types);
	// string filepath = "urmomma";
	// c.SetDataFile(table,filepath);
	// unsigned int sigh = 6;
	// c.SetNoTuples(table,sigh);
	// cout << c << endl;

	Catalog c(dbFile);
	menu(c);
	return 0;
}
