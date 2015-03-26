/*

   Derby - Class com.splicemachine.db.impl.store.access.btree.index.B2ICostController

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.store.access.btree.index;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;

import com.splicemachine.db.iapi.store.raw.Transaction;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.store.access.btree.BTreeCostController;


/**
 * Controller used to provide cost estimates to optimizer about secondary index
 * data access.
 *
 * Implements the StoreCostController interface for the B-Tree index
 * implementation.  The primary use of this interface is to provide costs
 * used by the query optimizer to use when choosing query plans. Provides
 * costs of things like fetch one row, how many rows in conglomerate, how
 * many rows between these 2 keys.
 *
 * Note most work of this class is inherited from the generic btree 
 * implementation.  This class initializes the top level object and deals with 
 * locking information specific to a secondary index implementation of a btree.
 */
public class B2ICostController extends BTreeCostController
{
	/*
	** Fields of B2IController.
	*/

	/*
	** Methods of B2IController.
	*/

	B2ICostController()
	{
		// Perform the generic b-tree construction.
		super();
	}

//    @Override
    public double cardinalityFraction(int columnNumber){
        return 1d; //not really implemented
    }

    void init(
    TransactionManager  xact_manager,
    B2I                 conglomerate,
    Transaction         rawtran) 
		throws StandardException
	{
		// Do generic b-tree initialization.
		super.init(xact_manager, conglomerate, rawtran);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(conglomerate != null);
	}
}
