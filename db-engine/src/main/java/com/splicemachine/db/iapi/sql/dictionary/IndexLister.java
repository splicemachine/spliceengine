/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;


/**
 * This interface gathers up some tasty information about the indices on a
 * table from the DataDictionary.
 */
public class IndexLister{
    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    private TableDescriptor tableDescriptor;
    private IndexRowGenerator[] indexRowGenerators;
    private long[] indexConglomerateNumbers;
    private String[] indexNames;
    // the following 3 are the compact arrays, without duplicate indexes
    private IndexRowGenerator[] distinctIndexRowGenerators;
    private long[] distinctIndexConglomerateNumbers;
    private String[] distinctIndexNames;

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTORS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Make an IndexLister
     *
     * @param    tableDescriptor    Describes the table in question.
     */
    public IndexLister(TableDescriptor tableDescriptor){
        this.tableDescriptor=tableDescriptor;
    }


    ////////////////////////////////////////////////////////////////////////
    //
    //	INDEXLISTER METHODS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Returns an array of all the index row generators on a table.
     *
     * @throws StandardException Thrown on error
     * @return an array of index row generators
     */
    public IndexRowGenerator[] getIndexRowGenerators() throws StandardException{
        if(indexRowGenerators==null){
            getAllIndexes();
        }
        return indexRowGenerators;
    }

    /**
     * Returns an array of all the index conglomerate ids on a table.
     *
     * @throws StandardException Thrown on error
     * @return an array of index conglomerate ids
     */
    public long[] getIndexConglomerateNumbers() throws StandardException{
        if(indexConglomerateNumbers==null){
            getAllIndexes();
        }
        return indexConglomerateNumbers;
    }

    /**
     * Returns an array of all the index names on a table.
     *
     * @throws StandardException Thrown on error
     * @return an array of index names
     */
    public String[] getIndexNames() throws StandardException{
        if(indexNames==null){
            getAllIndexes();
        }
        return indexNames;
    }

    /**
     * Returns an array of distinct index row generators on a table,
     * erasing entries for duplicate indexes (which share same conglomerate).
     *
     * @throws StandardException Thrown on error
     * @return an array of index row generators
     */
    public IndexRowGenerator[] getDistinctIndexRowGenerators() throws StandardException{
        if(distinctIndexRowGenerators==null){
            getAllIndexes();
        }
        return distinctIndexRowGenerators;
    }

    /**
     * Returns an array of distinct index conglomerate ids on a table.
     * erasing entries for duplicate indexes (which share same conglomerate).
     *
     * @throws StandardException Thrown on error
     * @return an array of index conglomerate ids
     */
    public long[] getDistinctIndexConglomerateNumbers() throws StandardException{
        if(distinctIndexConglomerateNumbers==null){
            getAllIndexes();
        }
        return distinctIndexConglomerateNumbers;
    }

    /**
     * Returns an array of index names for all distinct indexes on a table.
     * erasing entries for duplicate indexes (which share same conglomerate).
     *
     * @throws StandardException Thrown on error
     * @return an array of index names
     */
    public String[] getDistinctIndexNames() throws StandardException{
        if(indexNames==null){
            getAllIndexes();
        }
        return indexNames;
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /**
     * Reads all the indices on the table and populates arrays with the
     * corresponding index row generators and index conglomerate ids.
     *
     * @throws StandardException Thrown on error
     */
    private void getAllIndexes() throws StandardException{
        int indexCount=0;

        ConglomerateDescriptor[] cds= tableDescriptor.getConglomerateDescriptors();

		/* from one end of work space, we record distinct conglomerate
         * numbers for comparison while we iterate; from the other end of
		 * work space, we record duplicate indexes' indexes in "cds" array,
		 * so that we can skip them in later round.
		 */
        long[] workSpace=new long[cds.length-1];  // 1 heap
        int distinctIndexCount=0, duplicateIndex=workSpace.length-1;

        for(int i=0;i<cds.length;i++){
            // first count the number of indices.
            ConglomerateDescriptor cd=cds[i];

            if(!cd.isIndex())
                continue;

            int k;
            long thisCongNum=cd.getConglomerateNumber();

            for(k=0;k<distinctIndexCount;k++){
                if(workSpace[k]==thisCongNum){
                    workSpace[duplicateIndex--]=i;
                    break;
                }
            }
            if(k==distinctIndexCount)            // first appearence
                workSpace[distinctIndexCount++]=thisCongNum;

            indexCount++;
        }

        indexRowGenerators=new IndexRowGenerator[indexCount];
        indexConglomerateNumbers=new long[indexCount];
        indexNames=new String[indexCount];
        distinctIndexRowGenerators=new IndexRowGenerator[distinctIndexCount];
        distinctIndexConglomerateNumbers=new long[distinctIndexCount];
        distinctIndexNames=new String[distinctIndexCount];

        int duplicatePtr=workSpace.length-1;
        for(int i=0, j=-1, k=-1;i<cds.length;i++){
            ConglomerateDescriptor cd=cds[i];

            if(!cd.isIndex())
                continue;

            indexRowGenerators[++j]= cd.getIndexDescriptor();
            indexConglomerateNumbers[j]=cd.getConglomerateNumber();
            if(!(cd.isConstraint())){
                // only fill index name if it is not a constraint.
                indexNames[j]=cd.getConglomerateName();
            }

            if(duplicatePtr>duplicateIndex && i==(int)workSpace[duplicatePtr])
                duplicatePtr--;
            else{
                distinctIndexRowGenerators[++k]=indexRowGenerators[j];
                distinctIndexConglomerateNumbers[k]=indexConglomerateNumbers[j];
                distinctIndexNames[k]=indexNames[j];
            }
        }
    }
}
