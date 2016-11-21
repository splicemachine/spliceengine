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

package org.apache.ddlutils.platform.derby;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.ForeignKey;
import org.apache.ddlutils.model.Index;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TypeMap;
import org.apache.ddlutils.platform.DatabaseMetaDataWrapper;
import org.apache.ddlutils.platform.JdbcModelReader;

/**
 * Reads a database model from a Derby database.
 *
 * @version $Revision: $
 */
public class DerbyModelReader extends JdbcModelReader {

    /**
     * Creates a new model reader for Derby databases.
     *
     * @param platform The platform that this model reader belongs to
     */
    public DerbyModelReader(Platform platform) {
        super(platform);
    }

    /**
     * Determines whether the index is an internal index, i.e. one created by Derby.
     *
     * @param index The index to check
     * @return <code>true</code> if the index seems to be an internal one
     */
    private boolean isInternalIndex(Index index) {
        String name = index.getName();

        // Internal names normally have the form "SQL051228005030780"
        if ((name != null) && name.startsWith("SQL")) {
            try {
                //noinspection ResultOfMethodCallIgnored
                Long.parseLong(name.substring(3));
                return true;
            } catch (NumberFormatException ex) {
                // we ignore it
            }
        }
        return false;
    }

    private final static String SCHEMA_QUERY = "select SCHEMANAME, SCHEMAID, AUTHORIZATIONID from SYS.SYSSCHEMAS";
    private final static String SCHEMA_REFINE = " WHERE SCHEMANAME LIKE '%s'";
    @Override
    protected Iterable<? extends Schema> readSchemas(String catalog, String schemaPattern) throws SQLException {
        // We need to specialize reading schemas because DatabaseMetadata does not contain AUTHORIZATIONID,
        // which we need to create the schema by owner.
        // TODO: JC - ignoring catalog here
        List<Schema> schemas = new ArrayList<>();
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery((schemaPattern == null ? SCHEMA_QUERY : String.format(SCHEMA_QUERY + SCHEMA_REFINE,
                                                                                                      schemaPattern)))) {
                while (rs.next()) {
                    schemas.add(new Schema(rs.getString(1), rs.getString(2), rs.getString(3)));
                }
                Collections.sort(schemas);
            }
        }
        return schemas;
    }

    private final static String TABLE_ID_QUERY = "SELECT TABLEID FROM SYS.SYSTABLES WHERE SCHEMAID = '%s' AND TABLENAME = '%s'";
    @Override
    protected void setTableInternalId(Table table) throws SQLException {
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(String.format(TABLE_ID_QUERY, table.getSchema().getSchemaId(), table.getName()))) {
                if (rs.next()) {
                    table.setInternalId(rs.getString(1));
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isInternalPrimaryKeyIndex(DatabaseMetaDataWrapper metaData, Table table, Index index) {
        return isInternalIndex(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isInternalForeignKeyIndex(DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, Index index) {
        return isInternalIndex(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);

        String defaultValue = column.getDefaultValue();

        if (defaultValue != null) {
            // we check for these strings
            //   GENERATED_BY_DEFAULT               -> 'GENERATED BY DEFAULT AS IDENTITY'
            //   AUTOINCREMENT: start 1 increment 1 -> 'GENERATED ALWAYS AS IDENTITY'
            if ("GENERATED_BY_DEFAULT".equals(defaultValue) || defaultValue.startsWith("AUTOINCREMENT:")) {
                column.setDefaultValue(null);
                column.setAutoIncrement(true);
            } else if (TypeMap.isTextType(column.getTypeCode())) {
                column.setDefaultValue(unescape(defaultValue, "'", "''"));
            }
        }

        // add column check constraints, if any
        if (column.getSchemaId() != null && column.getTableId() != null) {
            addCheckConstraint(column);
        }
        return column;
    }

    private static final String VIEW_DFN_QUERY = "SELECT V.VIEWDEFINITION FROM SYS.SYSVIEWS V, SYS.SYSTABLES T WHERE T.SCHEMAID = '%s' AND T.TABLENAME = '%s' AND T.TABLEID = V.TABLEID";
    @Override
    protected String readViewDefinition(Table view) throws SQLException {
        String viewDefn = "";
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(String.format(VIEW_DFN_QUERY, view.getSchema().getSchemaId(), view.getName()))) {
                while (rs.next()) {
                    viewDefn = rs.getString(1);
                }
            }
        }
        return viewDefn;
    }

    private final static String CHECK_QUERY = "select b.REFERENCEDCOLUMNS, a.CONSTRAINTNAME, b.CHECKDEFINITION from sys.sysconstraints a, " +
        "SYS.SYSCHECKS b where a.schemaid = '%s' AND a.tableid = '%s' AND a.CONSTRAINTID = b.CONSTRAINTID AND A.TYPE = 'C' AND A.STATE = 'E'";
    private void addCheckConstraint(Column column) throws SQLException {
        try (Statement st = _connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(String.format(CHECK_QUERY, column.getSchemaId(), column.getTableId()))) {
                while (rs.next()) {
                    Object referencedColumns = rs.getObject(1);
                    if (referencedColumns != null) {
                        if (containsOrdinalPosition(column.getOrdinalPosition(), referencedColumns.toString())) {
                            String constraintName = rs.getString(2);
                            String checkDefinition = rs.getString(3);
                            column.createCheckConstraint(constraintName, checkDefinition);
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void postProcessTable(Table table) {
        // There's no way to tell a table check constraint from a column constraint.
        // A table check constraint is created by including ALL column positions in its definition.
        // We'll have to post-process the table after it's been completely created and, if all columns contain
        // a given constraint, pull that up to the table and remove all from the columns so that the proper
        // SQL create stmt can be generated.
        table.pullUpCheckConstraintIfNeeded();
    }

    private static boolean containsOrdinalPosition(int oridinalPosition, String columnPositionsString) {
        // Check constraint column position values are a user defined type, which all we can do is call toString() on.
        // The returned string is in the for "(1)" or "(1,2)", etc. It has to be parsed to see if our column is
        // a member.  Thus all this gyration.
        // A further annoyance, there's no way to tell a table check constraint from a column constraint.
        // A table check constraint is created by including ALL column positions in its definition.
        // We'll have to post-process the table after it's been completely created and, if all columns contain
        // a given constraint, pull that up to the table so that the proper SQL create stmt can be generated.
        String cos = columnPositionsString;
        // strip parens
        if (cos.indexOf('(') == 0 && cos.indexOf(')') == cos.length()-1) {
            cos = cos.replace("(", "");
            cos = cos.replace(")", "");
        }
        // turn comma-separated string into Set<Integer>
        return createIntSet(cos).contains(oridinalPosition);
    }

    private static Set<Integer> createIntSet(String intString) {
        Set<Integer> intSet = new HashSet<>();
        if (intString != null && ! intString.isEmpty()) {
            if (intString.contains(",")) {
                for (String anInt : intString.split(",")) {
                    intSet.add(Integer.parseInt(anInt));
                }
            } else {
                intSet.add(Integer.parseInt(intString));
            }
        }
        return intSet;
    }
}
