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

package org.apache.ddlutils.task;

import java.util.ArrayList;

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.platform.CreationParameters;

/**
 * Base type for database commands that use creation parameters.
 *
 * @version $Revision: 289996 $
 * @ant.type ignore="true"
 */
public abstract class DatabaseCommandWithCreationParameters extends DatabaseCommand {
    /**
     * The additional creation parameters.
     */
    private ArrayList _parameters = new ArrayList();

    /**
     * Adds a parameter which is a name-value pair.
     *
     * @param param The parameter
     */
    public void addConfiguredParameter(TableSpecificParameter param) {
        _parameters.add(param);
    }

    /**
     * Filters the parameters for the given model and platform.
     *
     * @param model           The database model
     * @param platformName    The name of the platform
     * @param isCaseSensitive Whether case is relevant when comparing names of tables
     * @return The filtered parameters
     */
    protected CreationParameters getFilteredParameters(Database model, String platformName, boolean isCaseSensitive) {
        CreationParameters parameters = new CreationParameters();

        for (Object _parameter : _parameters) {
            TableSpecificParameter param = (TableSpecificParameter) _parameter;

            if (param.isForPlatform(platformName)) {
                for (Schema schema : model.getSchemas()) {
                    schema.getTables().stream().filter(table -> param.isForTable(table, isCaseSensitive))
                          .forEach(table -> parameters.addParameter(table, param.getName(), param.getValue()));
                }
            }
        }
        return parameters;
    }
}
