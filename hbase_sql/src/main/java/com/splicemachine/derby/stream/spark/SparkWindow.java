/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.window.FrameDefinition;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import static com.splicemachine.derby.stream.spark.SparkUtils.convertPartitions;
import static com.splicemachine.derby.stream.spark.SparkUtils.convertSortColumns;
import static org.apache.spark.sql.functions.*;
import static com.splicemachine.db.iapi.sql.compile.AggregateDefinition.*;
/**
 * Created by jfilali on 10/4/16.
 * Spark Window Builder.
 * Start by providing a partition and then use a set of convenient
 * method to specify the Spark Window
 */
public class SparkWindow {

    private WindowSpec spec;
    private int[] inputColumnIds;
    private ColumnOrdering[] partitionIds;
    private FunctionType functionType;
    private int resultColumn;
    private  DataType resultDataType;
    private FormatableHashtable specificArgs;

    private SparkWindow(ColumnOrdering[] partitionIds) {
        this.partitionIds = partitionIds;
        this.spec = Window.partitionBy(convertPartitions(partitionIds));
    }

    /**
     * Define the Window Partition and initiate that builder
     * @param partitionIds
     * @return
     */
    public static SparkWindow partitionBy(ColumnOrdering[] partitionIds) {
        return new SparkWindow(partitionIds);
    }

    /**
     * Specify the window ordering
     * @param sortColumns
     * @return
     */

    public SparkWindow orderBy(ColumnOrdering[] sortColumns) {
        //not  ordering provided but spark need one,
        // for backward compatibility order by partition
        // Current IT except that not to fail but is not possible to
        // have nor ordering with Spark.
        if(sortColumns.length == 0){
            spec = spec.orderBy(convertSortColumns(partitionIds));
            return this;
        }
        spec = spec.orderBy(convertSortColumns(sortColumns));
        return this;
    }

    public SparkWindow frameBoundary(FrameDefinition definition) {

        // special cases for some specifics functions because there
        // is a difference  between Spark and our derby specification
        // Spark will complain if you provide a frame for those functions

        switch (functionType) {
            case LAG_FUNCTION:
            case ROW_NUMBER_FUNCTION:
            case LEAD_FUNCTION:
            return this;

        }

        // other cases
        switch (definition.getFrameMode()) {
            case ROWS:
                spec = this.spec.rowsBetween(
                        definition.getFrameStart().getValue(),
                        definition.getFrameEnd().getValue());
                break;

            case RANGE:
                spec = this.spec.rangeBetween(
                        definition.getFrameStart().getValue(),
                        definition.getFrameEnd().getValue());
                break;

        }
        return this;
    }

    /**
     * Define the input column. Winow function will be
     * executed on those columns.
     * @param inputColumnIds
     * @return
     */
    public SparkWindow inputs(int[] inputColumnIds) {
        this.inputColumnIds = inputColumnIds;
        return this;
    }

    /**
     * This is how you define which function you want to run
     * @param functionType
     * @return
     */
    public SparkWindow function(FunctionType functionType) {
        this.functionType = functionType;
        return this;
    }

    /**
     * We need to defnie the result Column type because there is a difference
     * between what Spark return and what our logic said.
     * So some of the functions require some conversions like avg
     * @param resultDataType
     * @return
     */
    public  SparkWindow resultDataType(DataType resultDataType){
        this.resultDataType = resultDataType;
        return  this;
    }

    /**
     * This is used to define some specific args
     * @param specificArgs
     * @return
     */

    public SparkWindow specificArgs(FormatableHashtable specificArgs){
        this.specificArgs = specificArgs;
        return this;
    }

    /**
     * The reference to the column that hold the result
     * @param resultColumn
     * @return
     */
    public SparkWindow resultColumn(int resultColumn) {
        this.resultColumn = resultColumn;
        return this;
    }

    /**
     *  The final method used to generate the Spark column definition.
     *  Based on all the previous info provided
     * @return
     */

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Column toColumn() {
        Column column = null;
        switch (functionType) {
            case MAX_FUNCTION:
                column = max(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case MIN_FUNCTION:
                column = min(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case SUM_FUNCTION:
                column = sum(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;


            case LAST_VALUE_FUNCTION:
                column = last(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)),(Boolean) specificArgs.getOrDefault("IGNORE_NULLS",false))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case AVG_FUNCTION:
                column = avg(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1))
                        .cast(resultDataType);
                break;

            case COUNT_FUNCTION:
                column = count(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;
            case COUNT_STAR_FUNCTION:
                column = count("*")
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case DENSE_RANK_FUNCTION:
                column = dense_rank()
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1))
                        .cast(resultDataType);
                break;

            case RANK_FUNCTION:
                column = rank()
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1))
                        .cast(resultDataType);
                break;

            case FIRST_VALUE_FUNCTION:
                column = first(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)),(Boolean) specificArgs.getOrDefault("IGNORE_NULLS",false))
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case LAG_FUNCTION:
                column = lag(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)),1)
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case LEAD_FUNCTION:
                column = lead(col(ValueRow.getNamedColumn(inputColumnIds[0] - 1)),1)
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1));
                break;

            case ROW_NUMBER_FUNCTION:
                column = row_number()
                        .over(spec)
                        .as(ValueRow.getNamedColumn(resultColumn - 1))
                        .cast(resultDataType);
                break;
        }
        return column;
    }





}
