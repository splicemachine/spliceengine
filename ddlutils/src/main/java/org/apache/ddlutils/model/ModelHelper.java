/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Contains some utility functions for working with the model classes.
 *
 * @version $Revision: $
 */
public class ModelHelper {
    /**
     * Determines whether one of the tables in the list has a foreign key to a table outside of the list,
     * or a table outside of the list has a foreign key to one of the tables in the list.
     *
     * @param model  The database model
     * @param tables The tables
     * @throws ModelException If such a foreign key exists
     */
    public void checkForForeignKeysToAndFromTables(Database model, Table[] tables) throws ModelException {
        List tableList = Arrays.asList(tables);

        for (Schema schema : model.getSchemas()) {
            for (Table curTable : schema.getTables()) {
                boolean curTableIsInList = tableList.contains(curTable);

                for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++) {
                    ForeignKey curFk = curTable.getForeignKey(fkIdx);

                    if (curTableIsInList != tableList.contains(curFk.getForeignTable())) {
                        throw new ModelException("The table " + curTable.getName() + " has a foreign key to table " + curFk
                            .getForeignTable().getName());
                    }
                }
            }
        }
    }

    /**
     * Removes all foreign keys from the tables in the list to tables outside of the list,
     * or from tables outside of the list to tables in the list.
     *
     * @param model  The database model
     * @param tables The tables
     */
    public void removeForeignKeysToAndFromTables(Database model, Table[] tables) {
        List tableList = Arrays.asList(tables);

        for (Schema schema : model.getSchemas()) {
            for (Table curTable : schema.getTables()) {
                boolean curTableIsInList = tableList.contains(curTable);
                ArrayList<ForeignKey> fksToRemove = new ArrayList<>();

                for (int fkIdx = 0; fkIdx < curTable.getForeignKeyCount(); fkIdx++) {
                    ForeignKey curFk = curTable.getForeignKey(fkIdx);

                    if (curTableIsInList != tableList.contains(curFk.getForeignTable())) {
                        fksToRemove.add(curFk);
                    }
                    for (Iterator fkIt = fksToRemove.iterator(); fkIt.hasNext(); ) {
                        curTable.removeForeignKey((ForeignKey) fkIt.next());
                    }
                }
            }
        }
    }
}
