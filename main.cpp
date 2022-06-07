/**
 * @file main.cpp
 * @author Hong Xu 9081571920
 * @author Tongyu Shen 9079821006
 * @author Hongru Zhou 9081228554
 * @brief Core functions of the buffer manager implemented with the clock algorithm.
 * @version 0.1
 * @date 2021-04-25
 *
 *
 *
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <vector>
#include "btree.h"
#include "page.h"
#include "filescan.h"
#include "page_iterator.h"
#include "file_iterator.h"
#include "exceptions/insufficient_space_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/bad_index_info_exception.h"

#define checkPassFail(a, b) 																				\
{																																		\
	if(a == b)																												\
		std::cout << "\nTest passed at line no:" << __LINE__ << "\n";		\
	else																															\
	{																																	\
		std::cout << "\nTest FAILS at line no:" << __LINE__;						\
		std::cout << "\nExpected no of records:" << b;									\
		std::cout << "\nActual no of records found:" << a;							\
		std::cout << std::endl;																					\
		exit(1);																												\
	}																																	\
}

using namespace badgerdb;

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
const std::string relationName = "relA";
//If the relation size is changed then the second parameter 2 chechPassFail may need to be changed to number of record that are expected to be found during the scan, else tests will erroneously be reported to have failed.
const int	relationSize = 5000;
std::string intIndexName, doubleIndexName, stringIndexName;


const int myRelationSize = 20000;

// This is the structure for tuples in the base relation

typedef struct tuple {
	int i;
	double d;
	char s[64];
} RECORD;

PageFile* file1;
RecordId rid;
RECORD record1;
std::string dbRecord1;

BufMgr * bufMgr = new BufMgr(100);

// -----------------------------------------------------------------------------
// Forward declarations
// -----------------------------------------------------------------------------

void createRelationForward();
void createRelationBackward();
void createRelationRandom();
void myCreateRelationForward();
void myCreateRelationBackward();
void myCreateRelationInSpecialOrder();
void myIntTests();
void myIndexTests();
void intTests();
bool equalitySearch(BTreeIndex *index, int searchKey);
void checkLeafNodesSequence(int relationSize);
int intScan(BTreeIndex *index, int lowVal, Operator lowOp, int highVal, Operator highOp);
void indexTests();
void test1();
void test2();
void test3();
void test4();
void test5();
void test6();
void test7();
void test8();
void test9();
void test10();
void errorTests();
void deleteRelation();

int main(int argc, char **argv)
{

  // Clean up from any previous runs that crashed.
  try
	{
    File::remove(relationName);
  }
	catch(const FileNotFoundException &)
	{
  }

	{
		// Create a new database file.
		PageFile new_file = PageFile::create(relationName);

		// Allocate some pages and put data on them.
		for (int i = 0; i < 20; ++i)
		{
			PageId new_page_number;
			Page new_page = new_file.allocatePage(new_page_number);

    	sprintf(record1.s, "%05d string record", i);
    	record1.i = i;
    	record1.d = (double)i;
    	std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			new_page.insertRecord(new_data);
			new_file.writePage(new_page_number, new_page);
		}

	}
	// new_file goes out of scope here, so file is automatically closed.

	{
		FileScan fscan(relationName, bufMgr);

		try
		{
			RecordId scanRid;
			while(1)
			{
				fscan.scanNext(scanRid);
				//Assuming RECORD.i is our key, lets extract the key, which we know is INTEGER and whose byte offset is also know inside the record.
				std::string recordStr = fscan.getRecord();
				const char *record = recordStr.c_str();
				int key = *((int *)(record + offsetof (RECORD, i)));
				std::cout << "Extracted : " << key << std::endl;
			}
		}
		catch(const EndOfFileException &e)
		{
			std::cout << "Read all records" << std::endl;
		}
	}
	// filescan goes out of scope here, so relation file gets closed.

	File::remove(relationName);

	test1();
	test2();
	test3();
	test4();
	test5();
	test6();
	test7();
	test8();
	test9();
	test10();
	errorTests();

	delete bufMgr;

  return 1;
}

void test1()
{
	// Create a relation with tuples valued 0 to relationSize and perform index tests
	// on attributes of all three types (int, double, string)
	std::cout << "---------------------" << std::endl;
	std::cout << "createRelationForward" << std::endl;
	createRelationForward();
	indexTests();
	deleteRelation();
}

void test2()
{
	// Create a relation with tuples valued 0 to relationSize in reverse order and perform index tests
	// on attributes of all three types (int, double, string)
	std::cout << "----------------------" << std::endl;
	std::cout << "createRelationBackward" << std::endl;
	createRelationBackward();
	indexTests();
	deleteRelation();
}

void test3()
{
	// Create a relation with tuples valued 0 to relationSize in random order and perform index tests
	// on attributes of all three types (int, double, string)
	std::cout << "--------------------" << std::endl;
	std::cout << "createRelationRandom" << std::endl;
	createRelationRandom();
	indexTests();
	deleteRelation();
}

/**
  * Insert 20000 records in increasing order into an index file and perform self defined index tests on INTEGER type
  *
 **/
void test4() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 4 begins" << std::endl;
	myCreateRelationForward();
	myIndexTests();
	deleteRelation();
}

/**
  * Insert 20000 records in decreasing order into an index file and perform self defined index tests on INTEGER type
  *
 **/
void test5() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 5 begins" << std::endl;
	myCreateRelationBackward();
	myIndexTests();
	deleteRelation();
}

/**
  * Insert 20000 records in an special order into an index file and perform self defined index tests on INTEGER type.
  * The special order is specified in test design
  *
 **/
void test6() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 6 begins" << std::endl;
	myCreateRelationInSpecialOrder();
	myIndexTests();
	deleteRelation();
}

/**
  * Insert 20000 records in increasing order into an index file and check if each of the inserted key is on the desired
  * position
  *
 **/
void test7() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 7 begins" << std::endl;

	myCreateRelationForward();

	BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
	PageId pageNo;
	int pos;
	int total_key;
	PageId currLeafPageNo;
	index.findLeafNode(0, currLeafPageNo, pos, total_key);
	pageNo = currLeafPageNo;
	int pageCnt = 0;

	std::string outIndexName;
	std::ostringstream idxStr;
	idxStr << relationName << '.' << offsetof(tuple,i);
	outIndexName = idxStr.str();
	BlobFile *file = new BlobFile(outIndexName, false);

	for (int i = 0; i < myRelationSize; i += (INTARRAYLEAFSIZE / 2 + 1)) {
		int posCnt = 0;

		if (myRelationSize - pageCnt * (INTARRAYLEAFSIZE / 2 + 1) <= INTARRAYLEAFSIZE) {
			for (int j = i; j < i + (myRelationSize - pageCnt * (INTARRAYLEAFSIZE / 2 + 1)); j++) {
				index.findLeafNode(j, pageNo, pos, total_key);
				if (!(pageNo == currLeafPageNo && pos == posCnt && total_key == myRelationSize - pageCnt * (INTARRAYLEAFSIZE / 2 + 1))) {
					std::cout << "Key " << j << " is at Page " << pageNo << " position " << pos << "\n";
					std::cout << "Key " << j << " is at expected Page " << currLeafPageNo << " position " << posCnt << "\n";
					std::cout << "findLeafNode fails to get the correct information of key " << j << " in current index file." << std::endl;
					exit(1);
				}
				posCnt++;
			}
			break;
		}

		for (int j = i; j < i + (INTARRAYLEAFSIZE / 2 + 1); j++) {
			index.findLeafNode(j, pageNo, pos, total_key);
			if (!(pageNo == currLeafPageNo && pos == posCnt && total_key == (INTARRAYLEAFSIZE / 2 + 1))) {
				std::cout << "Key " << j << " is at Page " << pageNo << " position " << pos << "\n";
				std::cout << "Key " << j << " is at expected Page " << currLeafPageNo << " position " << posCnt << "\n";
				std::cout << "findLeafNode fails to get the correct information of key " << j << " in current index file." << std::endl;
				exit(1);
			}
			posCnt++;
		}

		Page* leaf_page;
		bufMgr->readPage(file, currLeafPageNo, leaf_page);
		bufMgr->unPinPage(file, currLeafPageNo, false);
		LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(leaf_page);
		currLeafPageNo = leaf_node->rightSibPageNo;

		pageCnt++;
	}

	std::cout << "\nTest passed at line no:" << __LINE__ << "\n";

	bufMgr->flushFile(file);
	delete file;
	file = NULL;

	deleteRelation();
}

/**
  * Insert 20000 records in decreasing order into an index file and check if each of the inserted key is on the desired
  * position by calling another function checkLeafNodesSequence
  *
 **/
void test8() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 8 begins" << std::endl;
	myCreateRelationBackward();
	checkLeafNodesSequence(myRelationSize);
	std::cout << "\nTest passed at line no:" << __LINE__ << "\n";
	deleteRelation();
}

/**
  * Insert 20000 records in an special order into an index file and check if each of the inserted key is on the desired
  * position by calling another function checkLeafNodesSequence. The special order is specified in test design
  *
 **/
void test9() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 9 begins" << std::endl;
	myCreateRelationInSpecialOrder();
	checkLeafNodesSequence(myRelationSize);
	std::cout << "\nTest passed at line no:" << __LINE__ << "\n";
	deleteRelation();
}

/**
  * Insert 5000 records in an random order into an index file and check if each of the inserted key is on the desired
  * position by calling another function checkLeafNodesSequence. The special order is specified in test design
  *
 **/
void test10() {
	std::cout << "--------------------" << std::endl;
	std::cout << "My test: test 10 begins" << std::endl;
	createRelationRandom();
	checkLeafNodesSequence(relationSize);
	std::cout << "\nTest passed at line no:" << __LINE__ << "\n";
	deleteRelation();
}

/**
  * Given the relationSize, this function builds a index file on current relation file. Then, it calls
  * findLeafnode function to get the first leaf page. Relying on the fact that all leaf nodes are connected,
  * this function iterates every key in the leaf nodes of the index file. If every key is on the correct
  * position of correct leaf node. This function will not produce any output.
  * @param relationSize the size of the relation file
  *
 **/
void checkLeafNodesSequence(int relationSize) {

	BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);
	int pos;
	int total_key;
	PageId currLeafPageNo;
	index.findLeafNode(0, currLeafPageNo, pos, total_key);

	std::string outIndexName;
	std::ostringstream idxStr;
	idxStr << relationName << '.' << offsetof(tuple,i);
	outIndexName = idxStr.str();
	BlobFile *file = new BlobFile(outIndexName, false);

	int keyToCheck = 0;

	for (int i = 0; i < relationSize; i += total_key) {

		Page* leaf_page;
		bufMgr->readPage(file, currLeafPageNo, leaf_page);
		bufMgr->unPinPage(file, currLeafPageNo, false);
		LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(leaf_page);

		for (int j = 0; j < leaf_node->keySize; j++) {
			if (keyToCheck++ != leaf_node->keyArray[j]) {
				std::cout << "Key " << leaf_node->keyArray[j] << " is at Page " << currLeafPageNo << " position " << j << std::endl;
				std::cout << "The order of keys is not sorted correctly." << std::endl;
				exit(1);
			}
		}

		total_key = leaf_node->keySize;
		currLeafPageNo = leaf_node->rightSibPageNo;
	}

	bufMgr->flushFile(file);
	delete file;
	file = NULL;
}

/**
  * Create a relation file with records with keys from 0 to 19999 (in increasing order of keys)
  *
 **/
void myCreateRelationForward() {
    // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}

	file1 = new PageFile(relationName, true);

	// initialize all of record1.s to keep purify happy
	memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
	Page new_page = file1->allocatePage(new_page_number);

	// Insert a bunch of tuples into the relation.
	for(int i = 0; i < myRelationSize; i++ )
		{
		sprintf(record1.s, "%05d string record", i);
		record1.i = i;
		record1.d = (double)i;
		std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			while(1)
			{
				try
				{
				new_page.insertRecord(new_data);
					break;
				}
				catch(const InsufficientSpaceException &e)
				{
					file1->writePage(new_page_number, new_page);
					new_page = file1->allocatePage(new_page_number);
				}
			}
		}

	file1->writePage(new_page_number, new_page);
	std::cout << "Done create relation" << std::endl;
}

/**
  * Create a relation file with records with keys from 19999 to 0 (in decreasing order of keys)
  *
 **/
void myCreateRelationBackward() {
    // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}

	file1 = new PageFile(relationName, true);

	// initialize all of record1.s to keep purify happy
	memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
	Page new_page = file1->allocatePage(new_page_number);

	// Insert a bunch of tuples into the relation.
	for(int i = myRelationSize - 1; i >= 0; i-- )
		{
		sprintf(record1.s, "%05d string record", i);
		record1.i = i;
		record1.d = (double)i;
		std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			while(1)
			{
				try
				{
				new_page.insertRecord(new_data);
					break;
				}
				catch(const InsufficientSpaceException &e)
				{
					file1->writePage(new_page_number, new_page);
					new_page = file1->allocatePage(new_page_number);
				}
			}
		}

	file1->writePage(new_page_number, new_page);
}

/**
  * Create a relation file with records with keys from 0 to 19999 in a special ordering of keys.
  * It inserts records in this order: 19999, 0, 19998, 1, ...... , 10000, 9999.
  *
 **/
void myCreateRelationInSpecialOrder() {
	// destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}

	file1 = new PageFile(relationName, true);

	// initialize all of record1.s to keep purify happy
	memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
	Page new_page = file1->allocatePage(new_page_number);

	// Insert a bunch of tuples into the relation.
	for(int i = myRelationSize - 1; i >= myRelationSize / 2; i--)
	{
		sprintf(record1.s, "%05d string record", i);
		record1.i = i;
		record1.d = (double)i;
		std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
				new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
				new_page = file1->allocatePage(new_page_number);
			}
		}

		sprintf(record1.s, "%05d string record", myRelationSize - 1 - i);
		record1.i = myRelationSize - 1 - i;
		record1.d = (double)(myRelationSize - 1 - i);
		std::string new_data_2(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
				new_page.insertRecord(new_data_2);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
				new_page = file1->allocatePage(new_page_number);
			}
		}
	}

	file1->writePage(new_page_number, new_page);
}

/**
  * Call myIntTests() and remove an existing index file
  *
 **/
void myIndexTests() {
	myIntTests();
	try
	{
		File::remove(intIndexName);
	}
	catch(const FileNotFoundException &e)
	{
	}
}

/**
  * This function is developed based on intTests(). Except for run some range scan tests, it
  * uses equalitySearch() to check if the index file has every key of records in the relation files
  *
 **/
void myIntTests() {
	std::cout << "myIntTests begin" << std::endl;
	BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)

	checkPassFail(intScan(&index,0,GTE,19999,LTE), 20000)
	checkPassFail(intScan(&index,0,GT,19999,LTE), 19999)
	checkPassFail(intScan(&index,0,GTE,19999,LT), 19999)
	checkPassFail(intScan(&index,0,GT,19999,LT), 19998)

	checkPassFail(intScan(&index,0,GTE,341,LTE), 342)

	// run some random scan tests
	checkPassFail(intScan(&index,182,GTE,287,LTE), 106)
	checkPassFail(intScan(&index,-1232,GTE,-445,LTE), 0)
	checkPassFail(intScan(&index,-1000,GTE,10000,LTE), 10001)
	checkPassFail(intScan(&index,123,GT,700,LTE), 577)
	checkPassFail(intScan(&index,100,GT,300,LT), 199)
	checkPassFail(intScan(&index,50,GT,70,LT), 19)
	checkPassFail(intScan(&index,11000,GT,12321,LTE), 1321)

	for (int i = 0; i < myRelationSize; i++) {
		if (!equalitySearch(&index, i)) {
			std::cout << "Equality search of key " << i << " fails." << std::endl;
			exit(1);
			break;
		}
	}
}

/**
  * This equality search is simply calling range scan in an index file with low operator GTE
  * and high operator LTE, and the lower bound equals to the higher bound in this scan.Suppose
  * we want to find a key X in the index file. We want to find something which is less than or
  * equal to X and bigger than or equal to X, which is the same as saying we want to find something
  * equal to X
  *
 **/
bool equalitySearch(BTreeIndex * index, int searchKey) {
	try
	{
		index->startScan(&searchKey, GTE, &searchKey, LTE);
	}
	catch(const NoSuchKeyFoundException &e)
	{
		std::cout << "No Key Found satisfying the equality search criteria." << std::endl;
		return false;
	}

	RecordId currRid;
	Page *currPage;
	index->scanNext(currRid);
	bufMgr->readPage(file1, currRid.page_number, currPage);
	RECORD myRec = *(reinterpret_cast<const RECORD*>(currPage->getRecord(currRid).data()));
	bufMgr->unPinPage(file1, currRid.page_number, false);

	try
	{
		index->scanNext(currRid);
		std::cout << "The scan ends but IndexScanCompletedException is not thrown." << std::endl;
		return false;
	}
	catch(const IndexScanCompletedException &e)
	{
	}

	index->endScan();
	if (myRec.i == searchKey)
		return true;
	else
		return false;
}

// -----------------------------------------------------------------------------
// createRelationForward
// -----------------------------------------------------------------------------

void createRelationForward()
{
	std::vector<RecordId> ridVec;
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}

  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = 0; i < relationSize; i++ )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = (double)i;
    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationBackward
// -----------------------------------------------------------------------------

void createRelationBackward()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // Insert a bunch of tuples into the relation.
  for(int i = relationSize - 1; i >= 0; i-- )
	{
    sprintf(record1.s, "%05d string record", i);
    record1.i = i;
    record1.d = i;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
				file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}
  }

	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// createRelationRandom
// -----------------------------------------------------------------------------

void createRelationRandom()
{
  // destroy any old copies of relation file
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
  file1 = new PageFile(relationName, true);

  // initialize all of record1.s to keep purify happy
  memset(record1.s, ' ', sizeof(record1.s));
	PageId new_page_number;
  Page new_page = file1->allocatePage(new_page_number);

  // insert records in random order

  std::vector<int> intvec(relationSize);
  for( int i = 0; i < relationSize; i++ )
  {
    intvec[i] = i;
  }

  long pos;
  int val;
	int i = 0;
  while( i < relationSize )
  {
    pos = random() % (relationSize-i);
    val = intvec[pos];
    sprintf(record1.s, "%05d string record", val);
    record1.i = val;
    record1.d = val;

    std::string new_data(reinterpret_cast<char*>(&record1), sizeof(RECORD));

		while(1)
		{
			try
			{
    		new_page.insertRecord(new_data);
				break;
			}
			catch(const InsufficientSpaceException &e)
			{
      	file1->writePage(new_page_number, new_page);
  			new_page = file1->allocatePage(new_page_number);
			}
		}

		int temp = intvec[relationSize-1-i];
		intvec[relationSize-1-i] = intvec[pos];
		intvec[pos] = temp;
		i++;
  }

	file1->writePage(new_page_number, new_page);
}

// -----------------------------------------------------------------------------
// indexTests
// -----------------------------------------------------------------------------

void indexTests()
{
  intTests();
	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
}

// -----------------------------------------------------------------------------
// intTests
// -----------------------------------------------------------------------------

void intTests()
{
  std::cout << "Create a B+ Tree index on the integer field" << std::endl;
  BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

	// run some tests
	checkPassFail(intScan(&index,25,GT,40,LT), 14)
	checkPassFail(intScan(&index,20,GTE,35,LTE), 16)
	checkPassFail(intScan(&index,-3,GT,3,LT), 3)
	checkPassFail(intScan(&index,996,GT,1001,LT), 4)
	checkPassFail(intScan(&index,0,GT,1,LT), 0)
	checkPassFail(intScan(&index,300,GT,400,LT), 99)
	checkPassFail(intScan(&index,3000,GTE,4000,LT), 1000)
}

int intScan(BTreeIndex * index, int lowVal, Operator lowOp, int highVal, Operator highOp)
{
  RecordId scanRid;
	Page *curPage;

  std::cout << "Scan for ";
  if( lowOp == GT ) { std::cout << "("; } else { std::cout << "["; }
  std::cout << lowVal << "," << highVal;
  if( highOp == LT ) { std::cout << ")"; } else { std::cout << "]"; }
  std::cout << std::endl;

  int numResults = 0;

	try
	{
  	index->startScan(&lowVal, lowOp, &highVal, highOp);
	}
	catch(const NoSuchKeyFoundException &e)
	{
    std::cout << "No Key Found satisfying the scan criteria." << std::endl;
		return 0;
	}

	while(1)
	{
		try
		{
			index->scanNext(scanRid);
			bufMgr->readPage(file1, scanRid.page_number, curPage);
			RECORD myRec = *(reinterpret_cast<const RECORD*>(curPage->getRecord(scanRid).data()));
			bufMgr->unPinPage(file1, scanRid.page_number, false);

			if( numResults < 5 )
			{
				std::cout << "at:" << scanRid.page_number << "," << scanRid.slot_number;
				std::cout << " -->:" << myRec.i << ":" << myRec.d << ":" << myRec.s << ":" <<std::endl;
			}
			else if( numResults == 5 )
			{
				std::cout << "..." << std::endl;
			}
		}
		catch(const IndexScanCompletedException &e)
		{
			break;
		}

		numResults++;
	}

  if( numResults >= 5 )
  {
    std::cout << "Number of results: " << numResults << std::endl;
  }
  index->endScan();
  std::cout << std::endl;

	return numResults;
}

// -----------------------------------------------------------------------------
// errorTests
// -----------------------------------------------------------------------------

void errorTests()
{
	{
		std::cout << "Error handling tests" << std::endl;
		std::cout << "--------------------" << std::endl;
		// Given error test

		try
		{
			File::remove(relationName);
		}
		catch(const FileNotFoundException &e)
		{
		}

		file1 = new PageFile(relationName, true);

		// initialize all of record1.s to keep purify happy
		memset(record1.s, ' ', sizeof(record1.s));
		PageId new_page_number;
		Page new_page = file1->allocatePage(new_page_number);

		// Insert a bunch of tuples into the relation.
		for(int i = 0; i <10; i++ )
		{
		  sprintf(record1.s, "%05d string record", i);
		  record1.i = i;
		  record1.d = (double)i;
		  std::string new_data(reinterpret_cast<char*>(&record1), sizeof(record1));

			while(1)
			{
				try
				{
		  		new_page.insertRecord(new_data);
					break;
				}
				catch(const InsufficientSpaceException &e)
				{
					file1->writePage(new_page_number, new_page);
					new_page = file1->allocatePage(new_page_number);
				}
			}
		}

		file1->writePage(new_page_number, new_page);

		BTreeIndex index(relationName, intIndexName, bufMgr, offsetof(tuple,i), INTEGER);

		int int2 = 2;
		int int5 = 5;

		// Scan Tests
		std::cout << "Call endScan before startScan" << std::endl;
		try
		{
			index.endScan();
			std::cout << "ScanNotInitialized Test 1 Failed." << std::endl;
		}
		catch(const ScanNotInitializedException &e)
		{
			std::cout << "ScanNotInitialized Test 1 Passed." << std::endl;
		}

		std::cout << "Call scanNext before startScan" << std::endl;
		try
		{
			RecordId foo;
			index.scanNext(foo);
			std::cout << "ScanNotInitialized Test 2 Failed." << std::endl;
		}
		catch(const ScanNotInitializedException &e)
		{
			std::cout << "ScanNotInitialized Test 2 Passed." << std::endl;
		}

		std::cout << "Scan with bad lowOp" << std::endl;
		try
		{
			index.startScan(&int2, LTE, &int5, LTE);
			std::cout << "BadOpcodesException Test 1 Failed." << std::endl;
		}
		catch(const BadOpcodesException &e)
		{
			std::cout << "BadOpcodesException Test 1 Passed." << std::endl;
		}

		std::cout << "Scan with bad highOp" << std::endl;
		try
		{
			index.startScan(&int2, GTE, &int5, GTE);
			std::cout << "BadOpcodesException Test 2 Failed." << std::endl;
		}
		catch(const BadOpcodesException &e)
		{
			std::cout << "BadOpcodesException Test 2 Passed." << std::endl;
		}


		std::cout << "Scan with bad range" << std::endl;
		try
		{
			index.startScan(&int5, GTE, &int2, LTE);
			std::cout << "BadScanrangeException Test 1 Failed." << std::endl;
		}
		catch(const BadScanrangeException &e)
		{
			std::cout << "BadScanrangeException Test 1 Passed." << std::endl;
		}

		deleteRelation();
	}

	try
	{
		File::remove(intIndexName);
	}
  catch(const FileNotFoundException &e)
  {
  }
}

void deleteRelation()
{
	if(file1)
	{
		bufMgr->flushFile(file1);
		delete file1;
		file1 = NULL;
	}
	try
	{
		File::remove(relationName);
	}
	catch(const FileNotFoundException &e)
	{
	}
}
