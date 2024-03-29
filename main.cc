/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "Bruinbase.h"
#include "SqlEngine.h"
#include <cstdio>
#include <iostream>
#include "BTreeNode.h"
#include <stdio.h>
#include <string.h>
#include "BTreeIndex.h"

using namespace std;

int main()
{

  // run the SQL engine taking user commands from standard input (console).
  SqlEngine::run(stdin);

  return 0;
}
