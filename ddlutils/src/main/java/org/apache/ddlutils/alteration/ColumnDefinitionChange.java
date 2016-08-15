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

package org.apache.ddlutils.alteration;

import org.apache.ddlutils.PlatformInfo;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.util.StringUtilsExt;

/**
 * Represents the change of one or more aspects of the definition of a column, such as
 * data type or size, whether it is required or not, etc. Note that primary key status
 * is not part of the definition.
 *
 * @version $Revision: $
 */
public class ColumnDefinitionChange extends ColumnChangeImplBase {
    /**
     * The target column definition.
     */
    private Column _newColumnDef;

    /**
     * Creates a new change object.
     *
     * @param table        The name of the table owning the changed column
     * @param columnName   The name of the changed column
     * @param newColumnDef The new column definition
     */
    public ColumnDefinitionChange(Table table, String columnName, Column newColumnDef) {
        super(table, columnName);
        _newColumnDef = newColumnDef;
    }

    /**
     * Determines whether the definition of the given target column is different from the one of the given source column.
     *
     * @param platformInfo The info object for the current platform
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the definitions differ
     */
    public static boolean isChanged(PlatformInfo platformInfo, Column sourceColumn, Column targetColumn) {
        return isTypeChanged(platformInfo, sourceColumn, targetColumn) ||
            isSizeChanged(platformInfo, sourceColumn, targetColumn) ||
            isDefaultValueChanged(sourceColumn, targetColumn) ||
            isRequiredStatusChanged(sourceColumn, targetColumn) ||
            isAutoIncrementChanged(sourceColumn, targetColumn);
    }

    /**
     * Determines whether the jdbc type of the given target column is different from the one of the given source column.
     * This method uses the platform info object to determine the actual jdbc type that the target column would have
     * in the database, and compares that to the type of he source column.
     *
     * @param platformInfo The info object for the current platform
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the jdbc types differ
     */
    public static boolean isTypeChanged(PlatformInfo platformInfo, Column sourceColumn, Column targetColumn) {
        int targetTypeCode = platformInfo.getTargetJdbcType(targetColumn.getTypeCode());

        return targetTypeCode != sourceColumn.getTypeCode();
    }

    /**
     * Determines whether the size or precision/scale of the given target column is different from that of the given source
     * column.
     * If size and precision/scale do not matter for the target column's type, then <code>false</code> is returned.
     *
     * @param platformInfo The info object for the current platform
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the sizes or precisions/scales differ
     */
    public static boolean isSizeChanged(PlatformInfo platformInfo, Column sourceColumn, Column targetColumn) {
        int targetTypeCode = platformInfo.getTargetJdbcType(targetColumn.getTypeCode());
        boolean sizeMatters = platformInfo.hasSize(targetTypeCode);
        boolean scaleMatters = platformInfo.hasPrecisionAndScale(targetTypeCode);

        if (sizeMatters && !StringUtilsExt.equals(sourceColumn.getSize(), targetColumn.getSize())) {
            return true;
        } else return scaleMatters &&
            ((sourceColumn.getPrecisionRadix() != targetColumn.getPrecisionRadix()) ||
                (sourceColumn.getScale() != targetColumn.getScale()));
    }

    /**
     * Determines whether the size or precision/scale of the given target column is smaller than that of the given source column.
     * If size and precision/scale do not matter for the target column's type, then <code>false</code> is returned. Note that for
     * columns with precision & scale, it also counted as being smaller if the scale of the target column is smaller than the
     * one of the source column, regardless of whether the precision of the target column is smaller than precision of the source
     * column or equal to it or even bigger. The reason for this is that the reduced scale would still potentially lead to
     * truncation
     * errors.
     *
     * @param platformInfo The info object for the current platform
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the size of the target column is smaller
     */
    public static boolean isSizeReduced(PlatformInfo platformInfo, Column sourceColumn, Column targetColumn) {
        int targetTypeCode = platformInfo.getTargetJdbcType(targetColumn.getTypeCode());
        boolean sizeMatters = platformInfo.hasSize(targetTypeCode);
        boolean scaleMatters = platformInfo.hasPrecisionAndScale(targetTypeCode);

        if (sizeMatters && (sourceColumn.getSizeAsInt() > targetColumn.getSizeAsInt())) {
            return true;
        } else return scaleMatters &&
            ((sourceColumn.getPrecisionRadix() > targetColumn.getPrecisionRadix()) ||
                (sourceColumn.getScale() > targetColumn.getScale()));
    }

    /**
     * Determines whether the default value of the given target column is different from the one of the given source column.
     * This method compares the parsed default values instead of their representations in the columns.
     *
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the default values differ
     */
    public static boolean isDefaultValueChanged(Column sourceColumn, Column targetColumn) {
        Object sourceDefaultValue = sourceColumn.getParsedDefaultValue();
        Object targetDefaultValue = targetColumn.getParsedDefaultValue();

        return ((sourceDefaultValue == null) && (targetDefaultValue != null)) ||
            ((sourceDefaultValue != null) && !sourceDefaultValue.equals(targetDefaultValue));
    }

    /**
     * Determines whether the required status of the given target column is different from that of the given source column.
     *
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the required status is different in the target column
     */
    public static boolean isRequiredStatusChanged(Column sourceColumn, Column targetColumn) {
        return sourceColumn.isRequired() != targetColumn.isRequired();
    }

    /**
     * Determines whether the auto increment status of the given target column is different from that of the given source column.
     *
     * @param sourceColumn The source column
     * @param targetColumn The target column
     * @return <code>true</code> if the auto increment status is different in the target column
     */
    public static boolean isAutoIncrementChanged(Column sourceColumn, Column targetColumn) {
        return sourceColumn.isAutoIncrement() != targetColumn.isAutoIncrement();
    }

    /**
     * Returns the new column definition.
     *
     * @return The new column
     */
    public Column getNewColumn() {
        return _newColumnDef;
    }

    /**
     * {@inheritDoc}
     */
    public void apply(Database model, boolean caseSensitive) {
        Column column = findChangedColumn(model, caseSensitive);

        column.setTypeCode(_newColumnDef.getTypeCode());
        column.setSize(_newColumnDef.getSize());
        column.setAutoIncrement(_newColumnDef.isAutoIncrement());
        column.setRequired(_newColumnDef.isRequired());
        column.setDescription(_newColumnDef.getDescription());
        column.setDefaultValue(_newColumnDef.getDefaultValue());
    }
}
