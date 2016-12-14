#include "BTreeNode.h"
#include <iostream>
#include <cstring>
#include <stdio.h>
#include <cstdlib>

using namespace std;

BTLeafNode::BTLeafNode()
{

	memset(buffer, 0, 1024);
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{ 
	int entrySize = sizeof(int) + sizeof(RecordId); 
	int count = 0; 
	char *temp = buffer; 

	int theKey; 
	for (int i = 0; i < (1024 - sizeof(PageId))/entrySize; ++i) {
		// 4 is sizeof(int)
		memcpy(&theKey, temp, 4); 
		if (!theKey)
			break; 
		++count; 
		temp += entrySize; 
	}


	return count; 
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
	// this is 4 + (4 + 4) = 12 
	int entrySize = sizeof(int) + sizeof(RecordId);

	int maxNumberOfEntries = (1024 - sizeof(PageId)) / entrySize;   // this is (1024 - 4)/12 = 85 
	int keyCount = getKeyCount();
	if (keyCount == maxNumberOfEntries)
		return RC_NODE_FULL; 

	char *temp = buffer; 
	int i = 0, theKey; 
	for (; i < (1024 - sizeof(PageId))/entrySize; ++i) {
		memcpy(&theKey, temp, sizeof(int)); 
		if ((!theKey) || (theKey > key) )
			break; 
		temp += entrySize;
	}
	// i is the number of items "key" is > than 

	char *newBuffer = (char *)malloc(1024); 
	memset(newBuffer, 0, 1024); 

	// copy i items 
	memcpy(newBuffer, buffer, i*entrySize); 
	// copy "key" and "rid" 
	memcpy(newBuffer + (i*entrySize), &key, sizeof(int)); 
	memcpy(newBuffer + (i*entrySize) + sizeof(int), &rid, sizeof(RecordId)); 

	// copy the rest (items that "key" is < than)
	memcpy(newBuffer + (i+1)*entrySize, buffer + (i*entrySize), (keyCount - i) * entrySize); 
	// copy the pageID of next page 
	PageId myPageID = getNextNodePtr();
	memcpy(newBuffer + 1024 - sizeof(PageId), &myPageID, sizeof(PageId)); 

	// copy the newBuffer back into buffer 
	memcpy(buffer, newBuffer, 1024);

	free(newBuffer);

	return 0; 
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
	int entrySize = sizeof(int) + sizeof(RecordId);
	int maxNumberOfEntries = (1024 - sizeof(PageId)) / entrySize;   // this is (1024-4)/12 = 85 
	int keyCount = getKeyCount();
	if (keyCount < maxNumberOfEntries)
		return RC_INVALID_FILE_FORMAT;

	if (sibling.getKeyCount())
		return RC_INVALID_ATTRIBUTE; 

	memset(sibling.buffer, 0, 1024);

	// This is the number of keys that remain in this node 
	int firstHalf = (keyCount + 1) / 2;

	// Copy the secondHalf to the sibling node 
	memcpy(sibling.buffer, buffer + (firstHalf*entrySize), 1024 - sizeof(PageId) - (firstHalf*entrySize)); 
	// Set the pageid of the sibling node 
	sibling.setNextNodePtr(getNextNodePtr()); 

	// Erase the secondHalf from this node 
	std::fill(buffer + (firstHalf*entrySize), buffer + 1024 - sizeof(PageId), 0); 

	// Now we insert the new (key, rid) pair
	int theKey;
	memcpy(&theKey, sibling.buffer, sizeof(int));
	if (key >= theKey)
		sibling.insert(key, rid);
	else
		insert(key, rid);

	// Now we return the first key of the sibling node 
	memcpy(&siblingKey, sibling.buffer, sizeof(int)); 

	// Should we set the "next node pointer" of this node to the sibling node ???
	// ... 

	return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
	int entrySize = sizeof(int) + sizeof(RecordId);

	char *temp = buffer; 

	int i = 0;	// i = index entry 
	int theKey; 
	for (; i < getKeyCount(); ++i) {
		memcpy(&theKey, temp, sizeof(int));
		if (theKey == searchKey) {
			eid = i;
			return 0; 
		}
		else if (theKey > searchKey) {
			eid = i; 
			return RC_NO_SUCH_RECORD;  
		}
		temp += entrySize;
	}

	// At this point, searchKey is > all keys in the node 
	eid = i;
	return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
	// first check if eid is valid
	if (eid < 0 || eid >= getKeyCount())
		return RC_NO_SUCH_RECORD; 

	int entrySize = sizeof(int) + sizeof(RecordId);

	memcpy(&key, buffer + (eid*entrySize), sizeof(int));
	memcpy(&rid, buffer + (eid*entrySize) + sizeof(int), sizeof(RecordId));

	return 0; 
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
	PageId myPageID;
	memcpy(&myPageID, buffer + 1024 - sizeof(PageId), sizeof(PageId));
	return myPageID; 
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{ 
	// first check if pid is valid 
	if (pid < 0)
		return RC_INVALID_PID; 

	memcpy(buffer + 1024 - sizeof(PageId), &pid, sizeof(PageId)); 
	return 0; 
}

void BTLeafNode::print() 
{
	int entrySize = sizeof(int) + sizeof(RecordId);

	char *temp = buffer; 

	int theKey;
	for (int i = 0; i < getKeyCount(); ++i) {
		memcpy(&theKey, temp, sizeof(int));
		cout << theKey << " | ";
		temp += entrySize; 
	}
	cout << "[pid]" << endl;
}




BTNonLeafNode::BTNonLeafNode()
{
	memset(buffer, 0, 1024);
}


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
	return pf.read(pid, buffer); 
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
	return pf.write(pid, buffer); 
}

// For leaf: Structure is: (key,rid) | (key, rid) | ... | pid 
// For non-leaf: Structure is: pid (4 empty bytes) | (key, pid) | (key, pid) | ... | (key, pid) 
/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{  
	int count = 0; 

	int entrySize = sizeof(PageId) + sizeof(int); 	// this is 4 + 4 = 8 

	char *temp = buffer + 4 + sizeof(PageId); // according to the structure above 

	int theKey; 
	for (int i = 0; i < (1024 - sizeof(PageId))/entrySize; ++i) {	// this is (1024 - 4)/8 = 127
		// 4 is sizeof(int) and also sizeof(PageID)
		memcpy(&theKey, temp, sizeof(int)); 
		if (!theKey)
			break; 
		++count; 
		temp += entrySize; 
	}

	return count; 
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
	// this is 4 + 4 = 8 
	int entrySize = sizeof(int) + sizeof(PageId);

	int maxNumberOfEntries = (1024 - sizeof(PageId)) / entrySize;   // this is (1024 - 4)/8 = 127 
	int keyCount = getKeyCount();
	if (keyCount == maxNumberOfEntries)
		return RC_NODE_FULL; 

	char *temp = buffer + 4 + sizeof(PageId); 
	int i = 0, theKey; 
	for (; i < (1024 - sizeof(PageId))/entrySize; ++i) {
		memcpy(&theKey, temp, sizeof(int)); 
		if ((!theKey) || (theKey > key) )
			break; 
		temp += entrySize;
	}
	// i is the number of items "key" is > than 

	char *newBuffer = (char *)malloc(1024); 
	memset(newBuffer, 0, 1024); 

	// copy i items (and the initial 8 bytes)
	memcpy(newBuffer, buffer, 8 + i*entrySize); 
	// copy "key" and "pid"
	memcpy(newBuffer + 8 + (i*entrySize), &key, sizeof(int));
	memcpy(newBuffer + 8 + (i*entrySize) + sizeof(int), &pid, sizeof(PageId)); 

	// copy the rest (items that "key" is < than)
	memcpy(newBuffer + 8 + (i+1)*entrySize, buffer + 8 + (i*entrySize), (keyCount - i) * entrySize); 

	// copy the newBuffer back into buffer 
	memcpy(buffer, newBuffer, 1024);

	free(newBuffer);

	return 0; 
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
	int entrySize = sizeof(int) + sizeof(PageId);
	int maxNumberOfEntries = (1024 - sizeof(PageId)) / entrySize;   // this is (1024-4)/8 = 127 
	int keyCount = getKeyCount();
	if (keyCount < maxNumberOfEntries)
		return RC_INVALID_FILE_FORMAT;

	if (sibling.getKeyCount())
		return RC_INVALID_ATTRIBUTE; 

	memset(sibling.buffer, 0, 1024);

	// This is the number of keys that remain in this node 
	int firstHalf = (keyCount + 1) / 2;

	// Now we have to find the middle key 
	// Since the keys are sorted, 3 candidates for middle key are:
	// Last key of the first node, first key of the sibling node, and the one
	// we are about to insert in this function 
	int lastKeyOfFirst, firstKeyOfSibling; 
	memcpy(&lastKeyOfFirst, buffer + 8 + (firstHalf*entrySize) - 8, sizeof(int));
	memcpy(&firstKeyOfSibling, buffer + 8 + (firstHalf*entrySize), sizeof(int)); 

	if (key < lastKeyOfFirst) {		// lastKeyOfFirst = middle key 
		// set the midKey
		midKey = lastKeyOfFirst; 

		// copy the secondHalf to the sibling node
		memcpy(sibling.buffer + 8, buffer + 8 + (firstHalf * entrySize), 1024 - 8 - (firstHalf * entrySize) );
		// set the head pid of the sibling node to the pid of the (lastKeyOfFirst, pid) pair of the first node 
		memcpy(sibling.buffer, buffer + (firstHalf*entrySize) + sizeof(int), sizeof(PageId)); 

		// erase the secondHalf from this node, including the lastKeyOfFirst 
		std::fill(buffer + (firstHalf*entrySize), buffer + 1024, 0); 

		insert(key, pid); 
	}
	else if (key > firstKeyOfSibling) {		// firstKeyOfSibling = middle key 
		// set the midKey
		midKey = firstKeyOfSibling; 

		// copy the secondHalf to the sibling node, except the (firstKeyOfSibling, pid) pair 
		memcpy(sibling.buffer + 8, buffer + 8 + (firstHalf*entrySize) + entrySize, 1024 - 8 - (firstHalf*entrySize) - entrySize); 
		// set the head pid of the sibling node to the pid of the (firstKeyOfSibling, pid) pair 
		memcpy(sibling.buffer, buffer + 8 + (firstHalf*entrySize) + sizeof(int), sizeof(PageId));

		// erase the secondHalf from this node 
		std::fill(buffer + 8 + (firstHalf*entrySize), buffer + 1024, 0); 

		sibling.insert(key, pid); 
	}
	else {	// key = middle key 
		// set the midKey
		midKey = key; 

		// copy the secondHalf to the sibling node
		memcpy(sibling.buffer + 8, buffer + 8 + (firstHalf*entrySize), 1024 - 8 - (firstHalf*entrySize)); 
		// set the head pid of the sibling node to the pid of the (key, pid) pair we are to insert in this function 
		memcpy(sibling.buffer, &pid, sizeof(PageId));

		// erase the secondHalf from this node 
		std::fill(buffer + 8 + (firstHalf*entrySize), buffer + 1024, 0); 

	}

	return 0; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
	int entrySize = sizeof(int) + sizeof(PageId);

	char *temp = buffer + 8; 

	int theKey; 
	memcpy(&pid, buffer, sizeof(PageId));
	for (int i = 0; i < getKeyCount(); ++i) {
		memcpy(&theKey, temp, sizeof(int));
		if (searchKey >= theKey)
			memcpy(&pid, temp + sizeof(int), sizeof(PageId));
		else 
			break; 

		temp += entrySize;
	}

	return 0; 
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
	memset(buffer, 0, 1024); 

	memcpy(buffer, &pid1, sizeof(PageId));

	return insert(key, pid2); 
}

void BTNonLeafNode::print() 
{
	int entrySize = sizeof(int) + sizeof(PageId);

	char *temp = buffer + 8; 

	int theKey;
	for (int i = 0; i < getKeyCount(); ++i) {
		memcpy(&theKey, temp, sizeof(int));
		cout << theKey << " | ";
		temp += entrySize; 
	}
	cout << endl;
}