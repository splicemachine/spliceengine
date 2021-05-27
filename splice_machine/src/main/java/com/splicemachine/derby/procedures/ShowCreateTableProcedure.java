package com.splicemachine.derby.procedures;

import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.UserType;
import com.splicemachine.db.impl.sql.catalog.Procedure;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.procedures.ProcedureUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static com.splicemachine.db.iapi.sql.StatementType.*;

public class ShowCreateTableProcedure extends BaseAdminProcedures {

    public static Procedure getProcedure() {
        return Procedure.newBuilder().name("SHOW_CREATE_TABLE")
                .numOutputParams(0)
                .numResultSets(1)
                .varchar("schemaName", 128)
                .varchar("tableName", 128)
                .ownerClass(ShowCreateTableProcedure.class.getCanonicalName())
                .build().debugCheck();
    }

    @SuppressFBWarnings(value="SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", justification="Intentional")
    public static void SHOW_CREATE_TABLE(String schemaName, String tableName, ResultSet[] resultSet)
            throws SQLException, StandardException
    {
        List<String> ddls = SHOW_CREATE_TABLE_CORE(schemaName, tableName, false);
        resultSet[0] = ProcedureUtils.generateResult("DDL", ddls.get(0) + ";");
    }


    public static List<String> SHOW_CREATE_TABLE_CORE(String schemaName, String tableName, boolean separateFK) throws SQLException {
        Connection connection = getCurrentConnection();
        schemaName = EngineUtils.validateSchema(schemaName);
        tableName = EngineUtils.validateTable(tableName);
        List<String> ddls = Lists.newArrayList();
        try {
            TableDescriptor td = EngineUtils.verifyTableExists(connection, schemaName, tableName);

            String tableTypeString = "";
            StringBuilder extTblString = new StringBuilder();

            ColumnDescriptorList cdl = td.getColumnDescriptorList();
            //Process external table definition
            if (td.isExternal()) {
                tableTypeString = "EXTERNAL ";

                List<ColumnDescriptor> partitionColumns = cdl.stream()
                        .filter(columnDescriptor -> columnDescriptor.getPartitionPosition() > -1)
                        .sorted(Comparator.comparing(ColumnDescriptor::getPartitionPosition))
                        .collect(Collectors.toList());

                String tmpStr;
                tmpStr = td.getCompression();
                if (tmpStr != null && !tmpStr.equals("none"))
                    extTblString.append("\nCOMPRESSED WITH " + tmpStr);

                // Partitioned Columns
                boolean firstCol = true;
                for (ColumnDescriptor col: partitionColumns) {
                    extTblString.append(firstCol ? "\nPARTITIONED BY (\"" + col.getColumnName()+"\"" : ",\"" + col.getColumnName()+"\"");
                    firstCol = false;
                }

                if (!firstCol)
                    extTblString.append(")");

                // Row Format
                if (td.getDelimited() != null || td.getLines() != null) {
                    extTblString.append("\nROW FORMAT DELIMITED");
                    if ((tmpStr = td.getDelimited()) != null)
                        extTblString.append(" FIELDS TERMINATED BY '" + tmpStr + "'");
                    if ((tmpStr = td.getEscaped()) != null)
                        extTblString.append(" ESCAPED BY '" + tmpStr + "'");
                    if ((tmpStr = td.getLines()) != null)
                        extTblString.append(" LINES TERMINATED BY '" + tmpStr + "'");
                }
                // Storage type
                if ((tmpStr = td.getStoredAs()) != null) {
                    extTblString.append("\nSTORED AS ");
                    switch (tmpStr) {
                        case "T":
                            extTblString.append("TEXTFILE");
                            break;
                        case "P":
                            extTblString.append("PARQUET");
                            break;
                        case "A":
                            extTblString.append("AVRO");
                            break;
                        case "O":
                            extTblString.append("ORC");
                            break;
                        default:
                            throw new SQLException("Invalid stored format");
                    }
                }
                // Location
                if ((tmpStr = td.getLocation()) != null) {
                    extTblString.append("\nLOCATION '" + tmpStr + "'");
                }
            }//End External Table
            else if (td.getTableType() == TableDescriptor.VIEW_TYPE) {
                //Target table is a View
                throw ErrorState.LANG_INVALID_OPERATION_ON_VIEW.newException("SHOW CREATE TABLE", "\"" +
                        schemaName + "\".\"" + tableName + "\"");
            } else if (td.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE) {
                //Target table is a system table
                throw ErrorState.LANG_NO_USER_DDL_IN_SYSTEM_SCHEMA.newException("SHOW CREATE TABLE", schemaName);
            } else if (td.getTableType() == TableDescriptor.LOCAL_TEMPORARY_TABLE_TYPE) {
                tableTypeString = "LOCAL TEMPORARY ";
            }

            // Get column list, and write DDL for each column.
            StringBuilder colStringBuilder = new StringBuilder("");
            String createColString = "";

            boolean firstCol = true;
            cdl.sort(Comparator.comparing(columnDescriptor -> columnDescriptor.getPosition()));
            for (ColumnDescriptor col: cdl) {
                createColString = createColumn(col);
                colStringBuilder.append(firstCol ? createColString : "," + createColString).append("\n");
                firstCol = false;
            }


            colStringBuilder.append(createConstraint(td, schemaName, tableName, separateFK));

            String DDL = "CREATE " + tableTypeString + "TABLE \"" + schemaName + "\".\"" + tableName + "\" (\n" + colStringBuilder.toString() + ") ";
            String extStr = extTblString.toString();
            if (extStr.length() > 0)
                DDL += extStr;

            ddls.add(DDL);

            if (separateFK) {
                List<String> fks = getForeignKeyConstraints(td, schemaName, tableName);
                ddls.addAll(fks);
            }
            return ddls;
        } catch (StandardException e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    private static String createColumn(ColumnDescriptor columnDescriptor) throws SQLException
    {
        StringBuffer colDef = new StringBuffer();

        colDef.append("\"" + columnDescriptor.getColumnName() + "\"");
        colDef.append(" ");
        String colType = (new UserType(columnDescriptor.getType().getCatalogType())).toString();
        colDef.append(colType);

        Object defaultSerializable;
        if(columnDescriptor.getDefaultInfo() != null) {
            defaultSerializable = columnDescriptor.getDefaultInfo();
        }else{
            defaultSerializable = columnDescriptor.getDefaultValue();
        }
        String defaultText = defaultSerializable == null? null:defaultSerializable.toString();

        if (!reinstateAutoIncrement(columnDescriptor, colDef) &&
                defaultText != null) {
            if (defaultText.startsWith("GENERATED ALWAYS AS")) {
                colDef.append(" ");
            } else {
                colDef.append(" DEFAULT ");
            }
            colDef.append(defaultText);
        }

        return colDef.toString();
    }

    private static List<String> getForeignKeyConstraints(TableDescriptor td,
                                                         String schemaName,
                                                         String tableName) throws SQLException, StandardException {

        List<String> fks = Lists.newArrayList();
        Map<Integer, ColumnDescriptor> columnDescriptorMap = td.getColumnDescriptorList()
                .stream()
                .collect(Collectors.toMap(ColumnDescriptor::getStoragePosition, columnDescriptor -> columnDescriptor));

        ConstraintDescriptorList constraintDescriptorList = td.getDataDictionary().getConstraintDescriptors(td);
        for (ConstraintDescriptor cd: constraintDescriptorList) {
            int type = cd.getConstraintType();
            if (type == DataDictionary.FOREIGNKEY_CONSTRAINT) {
                StringBuffer fkKeys = new StringBuffer();
                int[] keyColumns = cd.getKeyColumns();
                String fkName = cd.getConstraintName();
                ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor = (ForeignKeyConstraintDescriptor) cd;
                ConstraintDescriptor referencedCd = foreignKeyConstraintDescriptor.getReferencedConstraint();
                TableDescriptor referencedTableDescriptor = referencedCd.getTableDescriptor();
                int[] referencedKeyColumns = referencedCd.getKeyColumns();
                String refTblName = referencedTableDescriptor.getQualifiedName();
                ColumnDescriptorList referencedTableCDL = referencedTableDescriptor.getColumnDescriptorList();
                Map<Integer, ColumnDescriptor> referencedTableCDM = referencedTableCDL
                        .stream()
                        .collect(Collectors.toMap(ColumnDescriptor::getStoragePosition, columnDescriptor -> columnDescriptor));

                int updateType = foreignKeyConstraintDescriptor.getRaUpdateRule();
                int deleteType = foreignKeyConstraintDescriptor.getRaDeleteRule();

                List<String> referencedColNames = new LinkedList<>();
                List<String> fkColNames = new LinkedList<>();
                for (int index = 0; index < keyColumns.length; index++) {
                    fkColNames.add("\"" + columnDescriptorMap.get(keyColumns[index]).getColumnName() + "\"");
                    referencedColNames.add("\"" + referencedTableCDM.get(referencedKeyColumns[index]).getColumnName() + "\"");
                }
                String s = String.format("ALTER TABLE \"%s\".\"%s\" ADD ", schemaName, tableName);
                fkKeys.append(s + ShowCreateTableProcedure.buildForeignKeyConstraint(fkName, refTblName,
                        referencedColNames, fkColNames, updateType, deleteType));
                fks.add(fkKeys.toString() + "\n");
            }
        }
        return fks;
    }
    @SuppressFBWarnings(value="OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", justification="Intentional")
    private static String createConstraint(TableDescriptor td, String schemaName, String tableName,
                                           boolean separateFK) throws SQLException, StandardException {
        Map<Integer, ColumnDescriptor> columnDescriptorMap = td.getColumnDescriptorList()
                .stream()
                .collect(Collectors.toMap(ColumnDescriptor::getStoragePosition, columnDescriptor -> columnDescriptor));
        ConstraintDescriptorList constraintDescriptorList = td.getDataDictionary().getConstraintDescriptors(td);

        StringBuffer priKeys = new StringBuffer();
        TreeMap<String, String> uniqueMap = new TreeMap<>();
        TreeMap<String, String> fkMap = new TreeMap();
        TreeMap<String, String> checkMap = new TreeMap<>();

        for (ConstraintDescriptor cd: constraintDescriptorList) {
            switch (cd.getConstraintType()) {
                //Check
                case DataDictionary.CHECK_CONSTRAINT:
                    if (!cd.isEnabled())
                        break;
                    String name = cd.getConstraintName();
                    checkMap.put(name, ", CONSTRAINT " + name + " CHECK " + cd.getConstraintText());
                    break;
                case DataDictionary.PRIMARYKEY_CONSTRAINT:
                    int[] keyColumns = cd.getKeyColumns();
                    boolean pkFirstCol = true;
                    for (int index=0; index<keyColumns.length; index ++) {
                        String colName = columnDescriptorMap.get(keyColumns[index]).getColumnName();
                        priKeys.append(pkFirstCol ? ", CONSTRAINT " + cd.getConstraintName() + " PRIMARY KEY(\""
                                + colName + "\"": ",\"" + colName + "\"");
                        pkFirstCol = false;
                    }
                    if (!pkFirstCol)
                        priKeys.append(")");
                    break;
                case DataDictionary.UNIQUE_CONSTRAINT:
                    if (!cd.isEnabled())
                        break;
                    keyColumns = cd.getKeyColumns();
                    boolean uniqueFirstCol = true;
                    StringBuffer uniqueStr = new StringBuffer();
                    StringBuffer columnNames = new StringBuffer();
                    String constraintName = cd.getConstraintName();
                    for (int index=0; index<keyColumns.length; index ++) {
                        String colName = columnDescriptorMap.get(keyColumns[index]).getColumnName();
                        uniqueStr.append(uniqueFirstCol ? ", CONSTRAINT " + constraintName + " UNIQUE (\"" +
                                colName + "\"": ",\"" + colName + "\"");
                        columnNames.append(uniqueFirstCol ? "\"" + colName + "\"": ",\"" + colName + "\"");

                        uniqueFirstCol = false;
                    }
                    if (!uniqueFirstCol)
                        uniqueStr.append(")");
                    uniqueMap.put(columnNames.toString(), uniqueStr.toString());
                    break;
                case DataDictionary.FOREIGNKEY_CONSTRAINT:
                    if (!separateFK) {
                        keyColumns = cd.getKeyColumns();
                        String fkName = cd.getConstraintName();
                        ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor = (ForeignKeyConstraintDescriptor) cd;
                        ConstraintDescriptor referencedCd = foreignKeyConstraintDescriptor.getReferencedConstraint();
                        TableDescriptor referencedTableDescriptor = referencedCd.getTableDescriptor();
                        int[] referencedKeyColumns = referencedCd.getKeyColumns();
                        String refTblName = referencedTableDescriptor.getQualifiedName();
                        ColumnDescriptorList referencedTableCDL = referencedTableDescriptor.getColumnDescriptorList();
                        Map<Integer, ColumnDescriptor> referencedTableCDM = referencedTableCDL
                                .stream()
                                .collect(Collectors.toMap(ColumnDescriptor::getStoragePosition, columnDescriptor -> columnDescriptor));

                        int updateType = foreignKeyConstraintDescriptor.getRaUpdateRule();
                        int deleteType = foreignKeyConstraintDescriptor.getRaDeleteRule();

                        List<String> referencedColNames = new LinkedList<>();
                        List<String> fkColNames = new LinkedList<>();
                        for (int index = 0; index < keyColumns.length; index++) {
                            fkColNames.add("\"" + columnDescriptorMap.get(keyColumns[index]).getColumnName() + "\"");
                            referencedColNames.add("\"" + referencedTableCDM.get(referencedKeyColumns[index]).getColumnName() + "\"");
                        }
                        fkMap.put(buildColumnsFromList(fkColNames), ", " +
                                buildForeignKeyConstraint(fkName, refTblName, referencedColNames, fkColNames, updateType, deleteType));

                    }
                    break;
                default:
                    break;
            }
        }

        StringBuffer chkStr = new StringBuffer();
        for (Map.Entry<String, String> entry : checkMap.entrySet()) {
            String value = entry.getValue();
            chkStr.append(value);
        }
        StringBuffer uniqueStr = new StringBuffer();
        for(Map.Entry<String, String> entry : uniqueMap.entrySet()) {
            String value = entry.getValue();
            uniqueStr.append(value);
        }
        StringBuffer fkKeys = new StringBuffer();
        for(Map.Entry<String, String> entry : fkMap.entrySet()) {
            String value = entry.getValue();
            fkKeys.append(value);
        }

        return  chkStr.toString() + uniqueStr.toString() + priKeys.toString() + fkKeys.toString();
    }


    static String buildForeignKeyConstraint(String fkName, String refTblName, List<String> pkCols, List<String> fkCols,
                                            int updateRule, int deleteRule) throws SQLException
    {
        StringBuffer fkStr = new StringBuffer("CONSTRAINT " + fkName + " FOREIGN KEY " + buildColumnsFromList(fkCols));
        fkStr.append(" REFERENCES " + refTblName + buildColumnsFromList(pkCols));

        fkStr.append(" ON UPDATE ");
        switch (updateRule) {
            case RA_RESTRICT:
                fkStr.append("RESTRICT");
                break;
            case RA_NOACTION:
                fkStr.append("NO ACTION");
                break;
            default:  // shouldn't happen
                throw new SQLException("INTERNAL ERROR: unexpected 'on-update' action: " + updateRule);
        }
        fkStr.append(" ON DELETE ");
        switch (deleteRule) {
            case RA_RESTRICT:
                fkStr.append("RESTRICT");
                break;
            case RA_NOACTION:
                fkStr.append("NO ACTION");
                break;
            case RA_CASCADE:
                fkStr.append("CASCADE");
                break;
            case RA_SETNULL:
                fkStr.append("SET NULL");
                break;
            default:  // shouldn't happen
                throw new SQLException("INTERNAL ERROR: unexpected 'on-delete' action: " + deleteRule);
        }
        return fkStr.toString();
    }

    private static String buildColumnsFromList(List<String> cols)
    {
        StringBuffer sb = new StringBuffer("(");
        boolean firstCol = true;
        for (String c : cols) {
            sb.append(firstCol ? "" : ",");
            sb.append(c);
            firstCol = false;
        }
        sb.append(")");
        return sb.toString();
    }

    public static boolean reinstateAutoIncrement(ColumnDescriptor columnDescriptor, StringBuffer colDef) throws SQLException {
        if (columnDescriptor.getAutoincInc() != 0) {
            colDef.append(" GENERATED ");
            colDef.append(columnDescriptor.getDefaultInfo() == null ?
                    "ALWAYS " : "BY DEFAULT ");
            colDef.append("AS IDENTITY (START WITH ");
            colDef.append(columnDescriptor.getAutoincStart());
            colDef.append(", INCREMENT BY ");
            colDef.append(columnDescriptor.getAutoincInc());
            colDef.append(")");
            return true;
        }
        return false;
    }
}
