/**
 * @file btree.cpp
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

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    // Add your code below. Please do not remove this line.

    // generate index file name given relation name and attribute offset
    std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	//initialize members of BTreeIndex
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;
	this->leafOccupancy = INTARRAYLEAFSIZE;
	this->nodeOccupancy = INTARRAYNONLEAFSIZE;
	this->headerPageNum = (PageId)1;
	this->scanExecuting = false;

	// If the corresponding index file exists, open the file and check the meta data in its header page.
	// If the meta data does not match the values received through constructor parameters, then throw
	// BadIndexInfoException
	Page* header_page;
	Page* root_page;
	if (BlobFile::exists(outIndexName)){

        // Open the existing index file
        file = new BlobFile(outIndexName, false);
        bufMgr->readPage((BlobFile*)file, headerPageNum, header_page);
        bufMgr->unPinPage((BlobFile*)file, headerPageNum, false);
        IndexMetaInfo* treeHeader = reinterpret_cast<IndexMetaInfo*>(header_page);
        rootPageNum = treeHeader->rootPageNo;

        // Check the meta data of the existing index file
        if(treeHeader->attrByteOffset != attrByteOffset || treeHeader->attrType != attrType ||
           (strcmp(treeHeader->relationName, relationName.c_str()) != 0)){
               throw BadIndexInfoException("Error: The index file is a bad file!");
           }
        return;
	}
	else{
        // If not exist, create a new index file
        file = new BlobFile(outIndexName, true);
	}

	// If the index file does not exist, allocate header page and first root page
	bufMgr->allocPage((BlobFile*)file, headerPageNum, header_page);
	bufMgr->allocPage((BlobFile*)file, rootPageNum, root_page);
	initializeLeaf(root_page);
	IndexMetaInfo* treeHeader = reinterpret_cast<IndexMetaInfo*>(header_page);
	treeHeader->attrByteOffset = attrByteOffset;
	treeHeader->attrType = attrType;
	strcpy(treeHeader->relationName, relationName.c_str());
	treeHeader->rootPageNo = rootPageNum;

	// Unpin header page and root page and set dirty bits
	bufMgr->unPinPage((BlobFile*)file, headerPageNum, true);
	bufMgr->unPinPage((BlobFile*)file, rootPageNum, true);

	{
	    // Scan the relation file to insert key&rid pairs
	    FileScan fscan(relationName, bufMgrIn);
	    try{
	        RecordId scanRid;
	        while(1){
                fscan.scanNext(scanRid);
                std::string recordStr = fscan.getRecord();
                const char *record = recordStr.c_str();
                int key = *((int*)(record + attrByteOffset));
                insertEntry(&key, scanRid);
	        }

	    }
	    // Reach the end of the relation file, exit the while loop
	    catch(const EndOfFileException &e){
	    }

	}
	// Close the relation file automatically
}

// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::initializeNonLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::initializeNonLeaf(Page* page){
    // This function imply initializes a non-leaf node through setting its level to 0, and the number of keys to be 0
    NonLeafNodeInt* non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(page);
    non_leaf_node->level = 0;
    non_leaf_node->keySize = 0;
}

// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::initializeLeaf
// -----------------------------------------------------------------------------
void BTreeIndex::initializeLeaf(Page* page){
    // This function simply initializes a leaf node through setting the PageId of its right sibling to be an invalid page number
    // and the number of keys to be zero
    LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(page);
    leaf_node->rightSibPageNo = Page::INVALID_NUMBER;
    leaf_node->keySize = 0;
}


// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::findLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::findLeafNode(int key, PageId& page_num, int& position, int& total_key){
    // This function gets the entry and page for insertion

    // Set up the root page number through the meta info from header page
    Page* header_page;
    IndexMetaInfo* treeHeader;
    bufMgr->readPage((BlobFile*)file, headerPageNum, header_page);
    bufMgr->unPinPage((BlobFile*)file, headerPageNum, false);
    treeHeader = reinterpret_cast<IndexMetaInfo*>(header_page);
    rootPageNum = treeHeader->rootPageNo;


    // If the root page number is still 2, then the root node is the only (leaf) node in the tree.
    // Treat the root page as a leaf node and locate the entry for insertion.
    if(rootPageNum == (PageId)2){

        // Return the page number of the root page, which is 2
        page_num = rootPageNum;
        Page* root_page;
        bufMgr->readPage((BlobFile*)file, rootPageNum, root_page);
        bufMgr->unPinPage((BlobFile*)file, rootPageNum, false);
        LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(root_page);

        // Return the total number of keys in the root (leaf) node
        total_key = leaf_node->keySize;

        // Locate the entry for insertion from left to right, assuming the keys are sorted
        int i;
        for(i = 0; i < leaf_node->keySize; i++){
            if(leaf_node->keyArray[i] >= key){
                break;
            }
        }

        // Return the location of the entry
        position = i;
        return;
    }

    // If the root page number is not 2, it means the root node is non-leaf node, which must be non-empty
    // Treat the root page as an non-leaf node, and try to find the location for insertion recursively from
    // root to bottom
    PageId temp_num = rootPageNum;
    while(1){
        Page* temp_page;
        bufMgr->readPage((BlobFile*)file, temp_num, temp_page);
        bufMgr->unPinPage((BlobFile*)file, temp_num, false);
        NonLeafNodeInt* non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(temp_page);
        int i;
        for(i = 0; i < non_leaf_node->keySize; i++){
            if(non_leaf_node->keyArray[i] >= key){
                break;
            }
        }
        temp_num = non_leaf_node->pageNoArray[i];

        // If the non-leaf node is above leaf node, treat its child as a leaf nodes
        // The temp_num currently store the page number of the leaf node
        if(non_leaf_node->level == 1){
            page_num = temp_num;
            Page* leaf_page;
            bufMgr->readPage((BlobFile*)file, temp_num, leaf_page);
            bufMgr->unPinPage((BlobFile*)file, temp_num, false);
            LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(leaf_page);

            // Return the total number of keys in the leaf node before insertion
            total_key = leaf_node->keySize;

            int j;
            for(j = 0; j < leaf_node->keySize; j++){
                if(leaf_node->keyArray[j] >= key){
                    break;
                }
            }

            // Return the entry
            position = j;
            return;
        }
    }
}


// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::findParentNode
// -----------------------------------------------------------------------------
void BTreeIndex::findParentNode(PageId child_page_num, int key, PageId& parent_page_num, int& position, int& total_key){
    // This function gets the entry to insert the pushing-up key from a child node, assuming the child node splits

    // Set up the root page number through the meta info from header page
    Page* header_page;
    IndexMetaInfo* treeHeader;
    bufMgr->readPage((BlobFile*)file, headerPageNum, header_page);
    bufMgr->unPinPage((BlobFile*)file, headerPageNum, false);
    treeHeader = reinterpret_cast<IndexMetaInfo*>(header_page);
    rootPageNum = treeHeader->rootPageNo;

    //If the child node is already the root node, return a invalid page number and exit
    if(child_page_num == rootPageNum){
        parent_page_num = Page::INVALID_NUMBER;
        position = 0;
        total_key = 0;
        return;
    }

    //if child node is not root node, then find the parent node recursively from root to bottom
    PageId temp_num = rootPageNum;
    while(1){
        Page* temp_page;
        NonLeafNodeInt* non_leaf_node;
        bufMgr->readPage((BlobFile*)file, temp_num, temp_page);
        bufMgr->unPinPage((BlobFile*)file, temp_num, false);
        non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(temp_page);
        int i;
        for(i = 0; i < non_leaf_node->keySize; i++){
            if(non_leaf_node->keyArray[i] > key){
                break;
            }
        }

        //if the temp node is the parent of the current node, return the information of the parent node and exit
        if(non_leaf_node->pageNoArray[i] == child_page_num){
            parent_page_num = temp_num;
            total_key = non_leaf_node->keySize;
            position = i;
            return;
        }
        else{
            // If the temp node is not the parent, then goes one depth down
            temp_num = non_leaf_node->pageNoArray[i];
        }

    }

}


// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::modifyLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::modifyLeafNode(PageId page_num, int key, RecordId rid, int position, int total_key,
                        PageId& left_node_num, PageId& right_node_num, int& push_up_key){
    // This function modifies a specific leaf node when a pair of key&rid inserts into a given position

    // If the leaf node is not full before insertion, do not split, just insert, increment its size and exit
    if(total_key < INTARRAYLEAFSIZE){
        Page* leaf_page;
        LeafNodeInt* leaf_node;
        bufMgr->readPage((BlobFile*)file, page_num, leaf_page);
        leaf_node = reinterpret_cast<LeafNodeInt*>(leaf_page);

        // Shift keys and rids to the right of the given position one position to the right
        for(int i = total_key; i > position; i--){
            leaf_node->keyArray[i] = leaf_node->keyArray[i-1];
            leaf_node->ridArray[i] = leaf_node->ridArray[i-1];
        }
        leaf_node->keyArray[position] = key;
        leaf_node->ridArray[position] = rid;
        leaf_node->keySize = leaf_node->keySize + 1;
        bufMgr->unPinPage((BlobFile*)file, page_num, true);

        left_node_num = page_num;
        right_node_num = Page::INVALID_NUMBER;
        push_up_key = -1;
        return;
    }

    //if the leaf node is full before insertion, then split
    else{
        Page* left_page;
        Page* right_page;
        LeafNodeInt* left_node;
        LeafNodeInt* right_node;
        PageId temp_right_num;

        bufMgr->readPage((BlobFile*)file, page_num, left_page);
        bufMgr->allocPage((BlobFile*)file, temp_right_num, right_page);//allocate a new page as the right node after the splitting

        initializeLeaf(right_page);
        left_node = reinterpret_cast<LeafNodeInt*>(left_page);
        right_node = reinterpret_cast<LeafNodeInt*>(right_page);

        right_node->rightSibPageNo = left_node->rightSibPageNo;
        left_node->rightSibPageNo = temp_right_num;

        left_node_num = page_num;
        right_node_num = temp_right_num;

        // Store the keys are rids including inserted key&rid pair into temporary arrays
        int temp_key_array[INTARRAYLEAFSIZE+1];
        RecordId temp_rid_array[INTARRAYLEAFSIZE+1];
        for(int i = 0; i < position; i++){
            temp_key_array[i] = left_node->keyArray[i];
            temp_rid_array[i] = left_node->ridArray[i];
        }
        temp_key_array[position] = key;
        temp_rid_array[position] = rid;
        for(int i = position+1; i < INTARRAYLEAFSIZE+1; i++){
            temp_key_array[i] = left_node->keyArray[i-1];
            temp_rid_array[i] = left_node->ridArray[i-1];
        }
        push_up_key = temp_key_array[MIDDLELEAF];

        // Redistribute keys and rids into left and right nodes
        initializeLeaf(left_page);
        left_node->rightSibPageNo = temp_right_num;
        left_node->keySize = MIDDLELEAF + 1;
        for(int i = 0; i < left_node->keySize; i++){
            left_node->keyArray[i] = temp_key_array[i];
            left_node->ridArray[i] = temp_rid_array[i];
        }

        right_node->keySize = INTARRAYLEAFSIZE - MIDDLELEAF;
        for(int i = 0; i < right_node->keySize; i++){
            right_node->keyArray[i] = temp_key_array[i+MIDDLELEAF+1];
            right_node->ridArray[i] = temp_rid_array[i+MIDDLELEAF+1];
        }

        // Unpin right and left node and set dirty bits
        bufMgr->unPinPage((BlobFile*)file, page_num, true);
        bufMgr->unPinPage((BlobFile*)file, temp_right_num, true);
    }


}

// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::modifyNonLeafNode
// -----------------------------------------------------------------------------
void BTreeIndex::modifyNonLeafNode(PageId page_num, int key, PageId left_child_num, PageId right_child_num, int position, int total_key,
                        PageId& left_node_num, PageId& right_node_num, int& push_up_key){
    // This function modifies a specific non-leaf node when a pushing-up key is inserted into the node. Split the non-leaf node if
    // necessary

    // If the non-leaf node is not full before insertion, then just insert the key into the given position without
    // splitting and exit
    if(total_key < INTARRAYNONLEAFSIZE){
        Page* non_leaf_page;
        NonLeafNodeInt* non_leaf_node;
        bufMgr->readPage((BlobFile*)file, page_num, non_leaf_page);
        non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(non_leaf_page);

        // Shift one position to right
        for(int i = total_key; i > position; i--){
            non_leaf_node->keyArray[i] = non_leaf_node->keyArray[i-1];
            non_leaf_node->pageNoArray[i+1] = non_leaf_node->pageNoArray[i];
        }
        non_leaf_node->keyArray[position] = key;
        non_leaf_node->pageNoArray[position+1] = right_child_num;
        non_leaf_node->pageNoArray[position] = left_child_num;
        // Increment the node size
        non_leaf_node->keySize = non_leaf_node->keySize+1;
        // Unpin the node and set the dirty bit
        bufMgr->unPinPage((BlobFile*)file, page_num, true);

        left_node_num = page_num;
        right_node_num = Page::INVALID_NUMBER;
        push_up_key = -1;
    }
    // If the non-leaf node is full before insertion, split it into left and right nodes, and push up
    // the middle key
    else{
       Page* left_non_leaf_page;
       Page* right_non_leaf_page;
       PageId temp_right_num;
       NonLeafNodeInt* left_non_leaf_node;
       NonLeafNodeInt* right_non_leaf_node;
       bufMgr->readPage((BlobFile*)file, page_num, left_non_leaf_page);
       bufMgr->allocPage((BlobFile*)file, temp_right_num, right_non_leaf_page); // allocate a new page as the right page
       left_non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(left_non_leaf_page);
       right_non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(right_non_leaf_page);

       initializeNonLeaf(right_non_leaf_page);
       right_non_leaf_node->level = left_non_leaf_node->level;

       // Store keys and page-ids including the inserted key&pageId pair into temporary arrays
       int temp_key_array[INTARRAYNONLEAFSIZE+1];
       PageId temp_pageid_array[INTARRAYNONLEAFSIZE+2];
       for(int i = 0; i < position; i++){
           temp_key_array[i] = left_non_leaf_node->keyArray[i];
           temp_pageid_array[i] = left_non_leaf_node->pageNoArray[i];
       }
       temp_key_array[position] = key;
       temp_pageid_array[position] = left_child_num;
       temp_pageid_array[position+1] = right_child_num;
       for(int i = position+1; i < INTARRAYNONLEAFSIZE+1; i++){
            temp_key_array[i] = left_non_leaf_node->keyArray[i-1];
            temp_pageid_array[i+1] = left_non_leaf_node->pageNoArray[i];
       }

       // Return the page-id of the right page and the pushing-up key from splitting
       push_up_key = temp_key_array[MIDDLENONLEAF];
       left_node_num = page_num;
       right_node_num = temp_right_num;

       // Redistribute keys and page-ids into left and right nodes, updates their node size
       initializeNonLeaf(left_non_leaf_page);
       left_non_leaf_node->level = right_non_leaf_node->level;
       left_non_leaf_node->keySize = MIDDLENONLEAF;
       for(int i = 0; i < left_non_leaf_node->keySize; i++){
            left_non_leaf_node->keyArray[i] = temp_key_array[i];
            left_non_leaf_node->pageNoArray[i] = temp_pageid_array[i];
       }
       left_non_leaf_node->pageNoArray[left_non_leaf_node->keySize] = temp_pageid_array[MIDDLENONLEAF];
       right_non_leaf_node->keySize = INTARRAYNONLEAFSIZE-MIDDLENONLEAF;
       right_non_leaf_node->pageNoArray[0] = temp_pageid_array[MIDDLENONLEAF+1];
       for(int i = 0; i < right_non_leaf_node->keySize; i++){
            right_non_leaf_node->keyArray[i] = temp_key_array[i + MIDDLENONLEAF+1];
            right_non_leaf_node->pageNoArray[i + 1] = temp_pageid_array[i+ MIDDLENONLEAF+2];
       }

       // Unpin the left and right page and set dirty bits
       bufMgr->unPinPage((BlobFile*)file, page_num, true);
       bufMgr->unPinPage((BlobFile*)file, temp_right_num, true);
    }

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
BTreeIndex::~BTreeIndex()
{
    // Add your code below. Please do not remove this line.

    try{
        bufMgr->flushFile((BlobFile*)file); // Flush index file
        delete file;                       // Delete file instance thereby closing the index file
        file = NULL;
    }
    catch(std::exception &e){             // Catch all possible exceptions inside the destructor
        std::cout<<"Error: fail to deallocate"<<std::endl;
    }

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    // Add your code below. Please do not remove this line.

    int target_key = *((int*)key);
    int push_up_key;
    PageId leaf_num;
    PageId parent_num;
    PageId left_child_num;
    PageId right_child_num;
    int position;
    int total_key;

    // Locate the leaf node to insert the key&rid pair and modify the leaf node
    // return the page-id of the right page and pushing-up key if necessary
    findLeafNode(target_key, leaf_num, position, total_key);
    modifyLeafNode(leaf_num, target_key, rid, position, total_key, left_child_num, right_child_num, push_up_key);

    // If page-id of the right page is invalid, the leaf node did not split, then finish the insert
    if(right_child_num == Page::INVALID_NUMBER){
        return;
    }

    // If the right child number is valid, the leaf node did split, then recursively insert the pushing keys to its ancestors
    else{
        while(1){

        // Get the page node of the leaf node
        findParentNode(left_child_num, push_up_key, parent_num, position, total_key);

        // If the page-id of the parent node is invalid, it means the current node is the root.
        // then allocate a new root page to insert the pushing-up key from insertting
        if(parent_num == Page::INVALID_NUMBER){
            Page* root_page;
            Page* header_page;
            IndexMetaInfo* tree_header;
            NonLeafNodeInt* root_node;
            bufMgr->allocPage((BlobFile*)file, rootPageNum, root_page);
            bufMgr->readPage((BlobFile*)file, headerPageNum, header_page);
            tree_header = reinterpret_cast<IndexMetaInfo*>(header_page);
            root_node = reinterpret_cast<NonLeafNodeInt*>(root_page);

            // Update the meta data in header and data in the new root node
            tree_header->rootPageNo = rootPageNum;
            root_node->keySize = 1;
            root_node->keyArray[0] = push_up_key;
            root_node->pageNoArray[0] = left_child_num;
            root_node->pageNoArray[1] = right_child_num;
            if(left_child_num == leaf_num){ // If the newly created root is a parent of a leaf node, then set level to 1, if not, set level to 0
                root_node->level = 1;
            }
            else{
                root_node->level = 0;
            }

            // Unpin header and root node and set dirty bit
            bufMgr->unPinPage((BlobFile*)file, headerPageNum, true);
            bufMgr->unPinPage((BlobFile*)file, rootPageNum, true);
            return;

        }

        // If the parent-id of parent node is valid, it means there is an existing parent node
        // of the child node, then modify the parent node through inserting the pushing-up key
        else{
            int temp_key = push_up_key;
            PageId temp_left_child_num = left_child_num;
            PageId temp_right_child_num = right_child_num;
            int temp_position = position;
            int temp_total_key = total_key;
            modifyNonLeafNode(parent_num, temp_key, temp_left_child_num, temp_right_child_num, temp_position, temp_total_key, left_child_num,
                              right_child_num, push_up_key);
            if(right_child_num == Page::INVALID_NUMBER){ // If modifying the parent node does not cause splitting, then return
                return;
            }
            else{
                continue; // If modifying the parent node causes splitting, recursively modify the parent node of this parent node
            }
        }
        }
    }
}


// -----------------------------------------------------------------------------
// Helper Function: BTreeIndex::findScanPage
// -----------------------------------------------------------------------------
void BTreeIndex::findScanPage(int low_value, int high_value, PageId& page_num, int& entry){
    // This function is similar to BTreeIndex::findLeafNode, which finds the smallest entry in a
    // leaf node that greater or equal to the given low value


    // If the low value is greater than the high value, then there is no page that an entry that satisfies the scan criteria
    if(low_value > high_value){
        page_num = Page::INVALID_NUMBER;
        entry = -1;
        return;
    }

    // Get the root page number through the meta data in header page
    Page* header_page;
    IndexMetaInfo* treeHeader;
    bufMgr->readPage((BlobFile*)file, headerPageNum, header_page);
    bufMgr->unPinPage((BlobFile*)file, headerPageNum, false);
    treeHeader = reinterpret_cast<IndexMetaInfo*>(header_page);
    rootPageNum = treeHeader->rootPageNo;


    // If the root page number is still 2, then the root node is the only (leaf) node in the tree.
    // So treat the root as a leaf node, and find the entry from leaf to right
    if(rootPageNum == (PageId)2){
        Page* root_page;
        bufMgr->readPage((BlobFile*)file, rootPageNum, root_page);
        bufMgr->unPinPage((BlobFile*)file, rootPageNum, false);
        LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(root_page);

        // Locate the entry in the root(leaf) node
        int i;
        for(i = 0; i < leaf_node->keySize; i++){
            if(leaf_node->keyArray[i] >= low_value){
                break;
            }
        }
        entry = i;

        // If there is no entry in the root(leaf) node which is greater than or equal to the given low value,
        // return an invalid page number, otherwise, return the page-id of the page where is entry is currently in
        if(entry == leaf_node->keySize){
            page_num = Page::INVALID_NUMBER;
        }
        else{
            page_num = rootPageNum;
        }
        return;
    }

    // If the rootPageNum is not 2, it means the root node is a non-leaf node, which must be non-empty.
    // Recursively find the entry from root to bottom
    PageId temp_num = rootPageNum;
    while(1){
        Page* temp_page;
        bufMgr->readPage((BlobFile*)file, temp_num, temp_page);
        bufMgr->unPinPage((BlobFile*)file, temp_num, false);
        NonLeafNodeInt* non_leaf_node = reinterpret_cast<NonLeafNodeInt*>(temp_page);
        int i;
        for(i = 0; i < non_leaf_node->keySize; i++){
            if(non_leaf_node->keyArray[i] >= low_value){
                break;
            }
        }
        temp_num = non_leaf_node->pageNoArray[i];

        // If the non-leaf node is above leaf nodes, check its children
        if(non_leaf_node->level == 1){

            Page* leaf_page;
            bufMgr->readPage((BlobFile*)file, temp_num, leaf_page);
            bufMgr->unPinPage((BlobFile*)file, temp_num, false);
            LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(leaf_page);

            // Locate the entry
            int j;
            for(j = 0; j < leaf_node->keySize; j++){
                if(leaf_node->keyArray[j] >= low_value){
                    break;
                }
            }
            entry = j;

            // If there is no entry which is greater than or equal to the given low value,
            // return an invalid page number, otherwise, return the page-id of the page
            // where is entry is currently in
            if(entry == leaf_node->keySize){
                page_num = Page::INVALID_NUMBER;
            }
            else if(leaf_node->keyArray[entry] > high_value){
                page_num = Page::INVALID_NUMBER;
            }
            else{
                page_num = temp_num;
            }
            return;
        }
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
    // Add your code below. Please do not remove this line.

    // If another scan is already executing, that needs to be ended here
    if(scanExecuting == true){
        endScan();
    }

    num_pinned_page = 0; // Set the number of pinned page for scanning to 0


    // Handle exceptions before scanning
    if(!(lowOpParm == GT || lowOpParm == GTE)){ // If lowOp does not contain one of their their expected values, throw BadOpcodesException
        throw BadOpcodesException();
    }
    if(!(highOpParm == LT || highOpParm == LTE)){ // If highOp does not contain one of their their expected values, throw BadOpcodesException
        throw BadOpcodesException();
    }
    if(*(int*)lowValParm > *(int*)highValParm){ // If lowVal > highval, throw BadScanrangeException
        throw BadScanrangeException();
    }

    // Set up lowValInt and highValInt according the given values and opcodes
    if(lowOpParm == GT){
        lowValInt = *(int *)lowValParm + 1;
    }
    else{
        lowValInt = *(int *)lowValParm;
    }
    if(highOpParm == LT){
        highValInt = *(int *)highValParm - 1;
    }
    else{
        highValInt = *(int *)highValParm;
    }
    lowOp = lowOpParm;
    highOp = highOpParm;

    // Find the entry in the B+ tree that satisfies the scan criteria
    findScanPage(lowValInt, highValInt, currentPageNum, nextEntry);
    if(currentPageNum != Page::INVALID_NUMBER){
        // Pin the page for scanning
        bufMgr->readPage((BlobFile*)file, currentPageNum, currentPageData);

        page_nums[num_pinned_page] = currentPageNum; // Updates the page-ids of pages which are pinned for scanning
        num_pinned_page++; // Increment the number of pinned pages
        scanExecuting = true; // Set the scanExecuting to true since the scan is successfully started
    }
    else{ // Throw NoSuchKeyFoundException if there is no key in the B+ tree that satisfies the scan criteria
        throw NoSuchKeyFoundException();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid)
{
    // Add your code below. Please do not remove this line.

    // Handle exception before return the next rid
    if(scanExecuting == false){ // If no scan has been initialized, throw ScanNotInitializedException
        throw ScanNotInitializedException();
    }
    if(currentPageNum == Page::INVALID_NUMBER){ // If no more records, satisfying the scan criteria, are left to be scanned, throw IndexScanCompletedException
        throw IndexScanCompletedException();
    }
    LeafNodeInt* leaf_node = reinterpret_cast<LeafNodeInt*>(currentPageData);
    if(leaf_node->keyArray[nextEntry] > highValInt){ // If no more records, satisfying the scan criteria, are left to be scanned, throw IndexScanCompletedException
        throw IndexScanCompletedException();
    }

    // Fetch the record id of the next index entry that matches the scan
    else{
        // Return the rid of next entry
        outRid = leaf_node->ridArray[nextEntry];

        // Update the next entry
        if(leaf_node->keySize-1 > nextEntry){ // If the entry does not reach the end of the leaf node, just increment it
            nextEntry++;
        }
        else if(leaf_node->keySize-1 == nextEntry && leaf_node->rightSibPageNo != Page::INVALID_NUMBER){
            // If the entry reaches the end of the current leaf node, and there is a right sibling of the current leaf node,
            // then update next entry to 0, and the current page to the right sibling
            nextEntry = 0;
            currentPageNum = leaf_node->rightSibPageNo;
            bufMgr->readPage((BlobFile*)file, currentPageNum, currentPageData);
            page_nums[num_pinned_page] = currentPageNum;
            num_pinned_page++;
        }
        else{
            // If the entry reaches the end of the current leaf node, and there is no right sibling of the current leaf node,
            // Set the current page number to an invalid number
            currentPageNum = Page::INVALID_NUMBER;
        }
        return;
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------

void BTreeIndex::endScan()
{
    // Add your code below. Please do not remove this line.

    if(scanExecuting == false){// If no scan has been initialized, throw the ScanNotInitializedException and exit
        throw ScanNotInitializedException();
    }

    // Set the scanExecuting to false since the scan is ended
    scanExecuting = false;

    // Unpin the pinned pages
    for(int i = 0; i < num_pinned_page; i++){
        bufMgr->unPinPage((BlobFile*)file, page_nums[i], false);
    }
}

}
