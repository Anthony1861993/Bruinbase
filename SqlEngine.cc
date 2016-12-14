/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <vector>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc = 0;
  int    key;     
  string value;
  int    count = 0;
  int    diff;
  BTreeIndex myTree;
  int dummy = 0; 

 

  bool conditionForIndex = false, valueCondition = false; 
  int myMin = -1, myMax = -1, targetValue = -1; 
  vector<int> myV;    // store the key <> .....
  vector<string> myV2;  // store the value <> .....
  string targetValue2 = "", myValMin = "", myValMax = ""; 

  // analyze the conditions 
  for (int i = 0; i < cond.size(); ++i) {
      int compareValue = atoi(cond[i].value);
      if (cond[i].attr == 1) {  // compare key
          if (cond[i].comp == SelCond::NE) {
              myV.push_back(compareValue); 
          }
          else {
              conditionForIndex = true; 
              switch (cond[i].comp) {
                  case SelCond::EQ:
                      if (targetValue == -1)
                          targetValue = compareValue;
                      else if (targetValue != compareValue)
                          goto exit_select_2; 
                      break;
                  case SelCond::GE:
                      if (myMin == -1 || compareValue > myMin)
                        myMin = compareValue;
                      break;
                  case SelCond::GT:
                      if (myMin == -1 || compareValue >= myMin)
                        myMin = compareValue + 1;
                      break;
                  case SelCond::LE:
                      if (myMax == -1 || compareValue < myMax)
                        myMax = compareValue;
                      break;
                  case SelCond::LT:
                      if (myMax == -1 || compareValue <= myMax)
                        myMax = compareValue - 1;
                      break;
              }

          }

      }
      else if (cond[i].attr == 2) {  // compare value 
          valueCondition = true; 
          switch (cond[i].comp) {
              case SelCond::EQ:
                  if (targetValue2 == "") 
                    targetValue2 = cond[i].value; 
                  else if (strcmp(targetValue2.c_str(), cond[i].value))
                    goto exit_select_2; 
                  break;
              case SelCond::NE:
                  myV2.push_back(cond[i].value);
                  break;
              default:
                  break;
          }
      }
  }


  // early failure for case like: key = 9 AND key <> 9 
  // early failure for case like: key = 9 AND key > 20
  // early failure for case like: key = 9 AND key < 4 
  if (targetValue != -1) {
    if (myV.size())
      for (int i = 0; i < myV.size(); ++i)
        if (myV[i] == targetValue)
          goto exit_select_2;

    if (myMin != -1 && targetValue < myMin) goto exit_select_2;
    if (myMax != -1 && targetValue > myMax) goto exit_select_2; 
  }
  
  // early failure for Min-Max conflict
  if (myMin != -1 && myMax != -1)
    if (myMin > myMax)
      goto exit_select_2;  


  // early failure for case like: value = 'hehe' AND value <> 'hehe'
  if (targetValue2 != "" && myV2.size())
    for (int i = 0; i < myV2.size(); ++i)
      if (myV2[i] == targetValue2)
        goto exit_select_2;

  // Now we have checked for all silly cases, lets get to business...

   // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  if (myTree.open(table + ".idx", 'r') || (!conditionForIndex && attr != 4)) {   // do the usual way 
      // scan the table file from the beginning
      rid.pid = rid.sid = 0;
      while (rid < rf.endRid()) {
        // read the tuple
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }

        // check the conditions on the tuple
        for (unsigned i = 0; i < cond.size(); i++) {
          // compute the difference between the tuple value and the condition value
          switch (cond[i].attr) {
              case 1:
	               diff = key - atoi(cond[i].value);
	               break;
              case 2:
	               diff = strcmp(value.c_str(), cond[i].value);
	               break;
          }

          // skip the tuple if any condition is not met
          switch (cond[i].comp) {
              case SelCond::EQ:
	               if (diff != 0) goto next_tuple;
	               break;
              case SelCond::NE:
	               if (diff == 0) goto next_tuple;
	               break;
              case SelCond::GT:
	               if (diff <= 0) goto next_tuple;
	               break;
              case SelCond::LT:
	               if (diff >= 0) goto next_tuple;
	               break;
              case SelCond::GE:
	               if (diff < 0) goto next_tuple;
	               break;
              case SelCond::LE:
	               if (diff > 0) goto next_tuple;
	               break;
          }
        }

        // the condition is met for the tuple. 
        // increase matching tuple counter
        count++;

        // print the tuple 
        switch (attr) {
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
        }

        // move to the next tuple
        next_tuple:
        ++rid;
      }
  }
  else {    // do the B+ tree style 
      
      IndexCursor cursor; 
      rid.pid = rid.sid = 0;

      // now set the starting point 
      if (targetValue != -1)
        myTree.locate(targetValue, cursor);
      else if (myMin != -1)
        myTree.locate(myMin, cursor);
      else myTree.locate(0, cursor);     // or start from the beginning

      while (!myTree.readForward(cursor, key, rid)) {

          // check each tuple 
          if (targetValue != -1 && key != targetValue) break;
          if (myMax != -1 && key > myMax) break;

          // if query is count(*) without condition for value 
          if (attr == 4 && !valueCondition) {
              ++count;
              continue;
          }

          // read the value only when we have to
          if (valueCondition || attr == 2 || attr == 3)
            if ((rc = rf.read(rid, key, value)) < 0) {
              fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
              goto exit_select;
            }

          for (unsigned i = 0; i < cond.size(); i++) {
              // compute the difference between the tuple value and the condition value
              switch (cond[i].attr) {
                  case 1:
                    diff = key - atoi(cond[i].value);
                    break;
                  case 2:
                    diff = strcmp(value.c_str(), cond[i].value);
                    break;
              }

              // skip the tuple if any condition is not met
              switch (cond[i].comp) {
                  case SelCond::EQ:
                    if (diff != 0) goto my_exit;
                    break;
                  case SelCond::NE:
                    if (diff == 0) goto my_exit;
                    break;
                  case SelCond::GT:
                    if (diff <= 0) goto my_exit;
                    break;
                  case SelCond::LT:
                    if (diff >= 0) goto my_exit;
                    break;
                  case SelCond::GE:
                    if (diff < 0) goto my_exit;
                    break;
                  case SelCond::LE:
                    if (diff > 0) goto my_exit;
                    break;
              }
          }

          ++count;
          switch (attr) {
            case 1:  // SELECT key
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  // SELECT value
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  // SELECT *
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
          }


          my_exit:
          ++dummy;
      }
      myTree.close();
      
  }

  exit_select_2:
  // print matching tuple count if "select count(*)"
  if (attr == 4) 
    fprintf(stdout, "%d\n", count);
  
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    RecordFile rf; 
    RecordId rid; 
    RC rc; 
    string value;
    int key;
    

    ifstream theData(loadfile.c_str()); 
    if (!theData.is_open())
      fprintf (stderr, "Error: cannot open file %s", loadfile.c_str()); 

    rc = rf.open(table + ".tbl", 'w'); 

    string line; 
    if (index) {
        // do sth here
        BTreeIndex myTree;
        myTree.open(table + ".idx", 'w');

        while (getline(theData, line)) {
            parseLoadLine(line, key, value);
            if (rc = rf.append(key, value, rid))
              return rc; 

            if (rc = myTree.insert(key, rid))
              return rc; 

        }
        myTree.close();
    }
    else {
        while (getline(theData, line)) {
            parseLoadLine(line, key, value); 
            if (rc = rf.append(key, value, rid))
                return rc;  
        }
    }

    theData.close(); 
    rf.close();
    return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
