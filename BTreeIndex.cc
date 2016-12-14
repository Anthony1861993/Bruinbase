/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <stdio.h>
#include <string.h>
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    treeHeight = 0; 
    memset(buffer, 0, 1024); 
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
	RC error; 
	if (error = pf.open(indexname, mode))
		return error; 

	// If this is an empty pagefile, just initialize the first page with 0 
	if (!pf.endPid()) {
		//if (error = pf.write(0, buffer))
		//	return error; 
		return 0; 
	}
	
	if (error = pf.read(0, buffer))
		return error; 

	
	// Check if the values we read are valid for rootPid and treeHeight
	if ((int)(*buffer) > 0 && (int)(*(buffer+4)) > 0 ) {
		memcpy(&rootPid, buffer, sizeof(int) );
		memcpy(&treeHeight, buffer + 4, sizeof(int)); 
	}
	
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
	memcpy(buffer, &rootPid, sizeof(int) );
	memcpy(buffer + 4, &treeHeight, sizeof(int) );

	RC error;
	// write to disk 
	if (error = pf.write(0, buffer))
		return error;

    return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
	// empty tree
	if (!treeHeight) {
		BTLeafNode myLeaf;
		myLeaf.insert(key, rid);

		++treeHeight;
		rootPid = pf.endPid()? pf.endPid() : 1;

		return myLeaf.write(rootPid, pf);
	}

	//else do the recursive function 
	int keyToInsert = -1;
	PageId pidToInsert = -1; 
	return insert_helper(key, rid, rootPid, 1, keyToInsert, pidToInsert); 
}

// recursive function 
RC BTreeIndex::insert_helper(int key, const RecordId& rid, PageId currentPid, int currentHeight, int& keyToInsert, PageId& pidToInsert)
{
	RC error; 
	// this is somewhere in the middle of the tree
	if (currentHeight != treeHeight) {
		BTNonLeafNode myNonLeaf; 
		if (error = myNonLeaf.read(currentPid, pf))
			return error; 

		PageId thePid; 
		myNonLeaf.locateChildPtr(key, thePid);

		int myKeyToInsert = -1;
		PageId myPidToInsert = -1; 

		insert_helper(key, rid, thePid, currentHeight+1, myKeyToInsert, myPidToInsert);

		if ((myKeyToInsert != -1) && (myPidToInsert != -1)) { 

			if (!myNonLeaf.insert(myKeyToInsert, myPidToInsert))  // if insert is successful, meaning no overflow 
				return myNonLeaf.write(currentPid, pf);
			// if not, then we have to do insertAndSplit
			BTNonLeafNode mySecondNonLeaf; 
			int myMidKey;
			myNonLeaf.insertAndSplit(myKeyToInsert, myPidToInsert, mySecondNonLeaf, myMidKey);
			// return key to insert (for parent to process)
			keyToInsert = myMidKey;
			// return pointer to new non-leaf node (for parent to process)
			PageId lastPid = pf.endPid();
			pidToInsert = lastPid; 

			if (error = mySecondNonLeaf.write(lastPid, pf))
				return error;
			if (error = myNonLeaf.write(currentPid, pf))
				return error; 

			if (treeHeight == 1) {
				BTNonLeafNode myRoot; 
				if (error = myRoot.initializeRoot(currentPid, myMidKey, lastPid))
					return error; 
				++treeHeight; 
				rootPid = pf.endPid(); 
				if (error = myRoot.write(rootPid, pf))
					return error; 
			}

		}

		return 0; 

	}
	else {   // this is when we reached the leaf level 
		BTLeafNode myLeaf; 
		if (error = myLeaf.read(currentPid, pf))
			return error; 

		if (!myLeaf.insert(key, rid))  // if insert is successful, meaning no overflow
			return myLeaf.write(currentPid, pf); 

		// if not, then we have to do insertAndSplit 
		BTLeafNode mySecondLeaf; 
		int theKey; 
		if (error = myLeaf.insertAndSplit(key, rid, mySecondLeaf, theKey))
			return error; 
		// return the key to insert (for parent to process)
		keyToInsert = theKey; 
		// return pointer to new leaf node (for parent to process)
		PageId lastPid = pf.endPid();
		pidToInsert = lastPid; 
		// set this leaf to point to the new leaf 
		myLeaf.setNextNodePtr(lastPid); 

		if (error = mySecondLeaf.write(lastPid, pf))
			return error;
		if (error = myLeaf.write(currentPid, pf))
			return error;

		// in case we just split the root 
		if (treeHeight == 1) {
			BTNonLeafNode myRoot; 
			if (error = myRoot.initializeRoot(currentPid, theKey, lastPid))
				return error; 
			++treeHeight; 
			rootPid = pf.endPid(); 
			if (error = myRoot.write(rootPid, pf))
				return error; 

		}

		return 0; 
	}

}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
	BTNonLeafNode myNonLeafNode; 
	RC error;
	int nextPid = rootPid; 
	for (int i = 1; i < treeHeight; ++i) {
		if (error = myNonLeafNode.read(nextPid, pf))
			return error; 

		if (error = myNonLeafNode.locateChildPtr(searchKey, nextPid))
			return error; 
	}

	BTLeafNode myLeafNode; 
	if (error = myLeafNode.read(nextPid, pf))
		return error; 
	int myEid; 
	error = myLeafNode.locate(searchKey, myEid);
	cursor.pid = nextPid;
	cursor.eid = myEid; 
		 
    return error;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
	BTLeafNode myLeaf;
	RC error; 

	if (cursor.pid <= 0)
		return RC_INVALID_CURSOR; 

	if (error = myLeaf.read(cursor.pid, pf))
		return error; 
	if (error = myLeaf.readEntry(cursor.eid, key, rid))
		return error; 

	if (cursor.eid + 1 == myLeaf.getKeyCount()) {
		cursor.eid = 0; 
		cursor.pid = myLeaf.getNextNodePtr(); 
	}
	else {
		int dummy = cursor.eid; 
		cursor.eid = dummy + 1; 
	}

    return 0;
}
