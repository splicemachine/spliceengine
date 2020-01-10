/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
package com.splicemachine.orc.predicate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.orc.OrcPredicate;
import com.splicemachine.orc.metadata.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.types.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



/**
 * Qualifiers in Conjunctive Normal Form...
 *
 *
 */
public class SpliceORCPredicate implements OrcPredicate, Externalizable {
    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    private Qualifier[][] qualifiers;
    int[] baseColumnMap;
    StructType structType;

    public SpliceORCPredicate() {

    }

    public SpliceORCPredicate(Qualifier[][] qualifiers, int[] baseColumnMap, StructType structType) {
        this.qualifiers = qualifiers;
        this.baseColumnMap = baseColumnMap;
        this.structType = structType;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeIntArray(out,baseColumnMap);
        out.writeBoolean(qualifiers!=null);
        if (qualifiers != null) {
            out.writeInt(qualifiers.length);
            out.writeInt(qualifiers[0].length);
            for (int i = 0; i < qualifiers[0].length; i++) {
                out.writeObject(qualifiers[0][i]);
            }
            for (int and_idx = 1; and_idx < qualifiers.length; and_idx++) {
                out.writeInt(qualifiers[and_idx].length);
                for (int or_idx = 0; or_idx < qualifiers[and_idx].length; or_idx++) {
                    out.writeObject(qualifiers[and_idx][or_idx]);
                }
            }
        }
        out.writeObject(structType.json());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseColumnMap = ArrayUtil.readIntArray(in);
        if (in.readBoolean()) {
            qualifiers = new Qualifier[in.readInt()][];
            qualifiers[0] = new Qualifier[in.readInt()];
            for (int i = 0; i < qualifiers[0].length; i++) {
                qualifiers[0][i] = (Qualifier) in.readObject();
            }
            for (int and_idx = 1; and_idx < qualifiers.length; and_idx++) {
                qualifiers[and_idx] = new Qualifier[in.readInt()];
                for (int or_idx = 0; or_idx < qualifiers[and_idx].length; or_idx++) {
                    qualifiers[and_idx][or_idx] = (Qualifier) in.readObject();
                }
            }
        }
        structType = (StructType) StructType.fromJson((String)in.readObject());
    }

    @Override
    public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex) {
        try {
            boolean row_qualifies = true;
            if (qualifiers == null)
                return numberOfRows > 0;
            for (int i = 0; i < qualifiers[0].length; i++) {
                Qualifier q = qualifiers[0][i];
                if (q.getVariantType() == Qualifier.VARIANT)
                    continue; // Cannot Push Down Qualifier
                StatsEval statsEval = statsEval(numberOfRows, statisticsByColumnIndex.get(q.getStoragePosition()),
                        structType.fields()[baseColumnMap[q.getStoragePosition()]].dataType());
                if (statsEval == null || statsEval.maximumDVD == null || statsEval.minimumDVD == null) {
                    // predicate on non-partitioning column or no stats info, continue to evaluate other predicates
                    continue;
                }

                if (q.getOrderable() == null || q.getOrderable().isNull()) {
                    if (!statsEval.hasNulls)
                        return false;
                    continue;
                }

                /* min/max value in Date column stats are stored as days from 1970-01-01, while predicate is a date
                   string, so need to convert the date string to days also
                 */
                DataValueDescriptor orderable = q.getOrderable();
                switch (q.getOperator()) {
                    case com.splicemachine.db.iapi.types.DataType.ORDER_OP_LESSTHAN:
                    case com.splicemachine.db.iapi.types.DataType.ORDER_OP_LESSOREQUALS:
                        if (q.negateCompareResult()) {
                            row_qualifies =
                                    statsEval.maximumDVD.compare(
                                            q.getOperator(),
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV());
                        } else {
                            row_qualifies =
                                    statsEval.minimumDVD.compare(
                                            q.getOperator(),
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV());
                        }
                        break;
                    case com.splicemachine.db.iapi.types.DataType.ORDER_OP_GREATERTHAN:
                    case com.splicemachine.db.iapi.types.DataType.ORDER_OP_GREATEROREQUALS:
                        if (q.negateCompareResult()) {
                            row_qualifies =
                                    statsEval.minimumDVD.compare(
                                            q.getOperator(),
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV());
                        } else {
                            row_qualifies =
                                    statsEval.maximumDVD.compare(
                                            q.getOperator(),
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV());
                        }
                        break;
                    case com.splicemachine.db.iapi.types.DataType.ORDER_OP_EQUALS:
                        if (q.negateCompareResult()) {
                            row_qualifies =
                                    statsEval.minimumDVD.compare(
                                            com.splicemachine.db.iapi.types.DataType.ORDER_OP_EQUALS,
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV())
                                            &&
                                            statsEval.maximumDVD.compare(
                                                    com.splicemachine.db.iapi.types.DataType.ORDER_OP_EQUALS,
                                                    orderable,
                                                    q.getOrderedNulls(),
                                                    q.getUnknownRV());
                        } else {
                            row_qualifies =
                                    statsEval.minimumDVD.compare(
                                            com.splicemachine.db.iapi.types.DataType.ORDER_OP_LESSOREQUALS,
                                            orderable,
                                            q.getOrderedNulls(),
                                            q.getUnknownRV())
                                            &&
                                            statsEval.maximumDVD.compare(
                                                    com.splicemachine.db.iapi.types.DataType.ORDER_OP_GREATEROREQUALS,
                                                    orderable,
                                                    q.getOrderedNulls(),
                                                    q.getUnknownRV());
                        }
                        break;
                }
                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;
                if (!row_qualifies)
                    return (false);
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
            /*
            // all the qual[0] and terms passed, now process the OR clauses
            for (int and_idx = 1; and_idx < qual_list.length; and_idx++) {
                Column orCols = null;
                for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++) {
                    Qualifier q = qual_list[and_idx][or_idx];
                    if (q.getVariantType() == Qualifier.VARIANT)
                        continue; // Cannot Push Down Qualifier
                    q.clearOrderableCache();
                    Column orCol = dataset.col(allColIdInSpark[(baseColumnMap != null ? baseColumnMap[q.getStoragePosition()] : q.getStoragePosition())]);
                    Object value = q.getOrderable().getObject();
                    switch (q.getOperator()) {
                        case DataType.ORDER_OP_LESSTHAN:
                            orCol = q.negateCompareResult() ? orCol.geq(value) : orCol.lt(value);
                            break;
                        case DataType.ORDER_OP_LESSOREQUALS:
                            orCol = q.negateCompareResult() ? orCol.gt(value) : orCol.leq(value);
                            break;
                        case DataType.ORDER_OP_GREATERTHAN:
                            orCol = q.negateCompareResult() ? orCol.leq(value) : orCol.gt(value);
                            break;
                        case DataType.ORDER_OP_GREATEROREQUALS:
                            orCol = q.negateCompareResult() ? orCol.lt(value) : orCol.geq(value);
                            break;
                        case DataType.ORDER_OP_EQUALS:
                            orCol = q.negateCompareResult() ? orCol.notEqual(value) : orCol.equalTo(value);
                            break;
                    }
                    if (orCols == null)
                        orCols = orCol;
                    else
                        orCols = orCols.or(orCol);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        */
    }

    public String serialize() throws IOException {
        return Base64.encodeBase64String(SerializationUtils.serialize(this));
    }

    public static SpliceORCPredicate deserialize(String base64String) throws IOException {
        return (SpliceORCPredicate) SerializationUtils.deserialize(Base64.decodeBase64(base64String));
    }

    public static Map<Integer, ColumnStatistics> partitionStatsEval(List<Integer> baseColumnMap,
                   StructType rowStruct, List<Integer> partitionColumns, String[] values, boolean isCollectStats) {
        try {
            Map<Integer, ColumnStatistics> partitionStatistics = new HashMap<>(partitionColumns.size());

            for (int i = 0; i< partitionColumns.size(); i++) {
                int storagePos = partitionColumns.get(i);
                if (storagePos >= baseColumnMap.size())
                    continue;
                int j = baseColumnMap.get(storagePos);
                if (j==-1) // Partition Column Not In List...
                    continue;

                DataType dataType = rowStruct.fields()[j].dataType();
                if (dataType instanceof BooleanType) {
                    partitionStatistics.put(storagePos, BooleanStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof DecimalType) {
                    partitionStatistics.put(storagePos, DecimalStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof StringType) {
                    partitionStatistics.put(storagePos, StringStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof DateType) {
                    partitionStatistics.put(storagePos, DateStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof IntegerType) {
                    partitionStatistics.put(storagePos, IntegerStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof LongType) {
                    partitionStatistics.put(storagePos, IntegerStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof ShortType) {
                    partitionStatistics.put(storagePos, IntegerStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof DoubleType) {
                    partitionStatistics.put(storagePos, DoubleStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else if (dataType instanceof FloatType) {
                    partitionStatistics.put(storagePos, DoubleStatistics.getPartitionColumnStatistics(!isDefaultPartition(values[i]) ? values[i] : null));
                } else {
                }
            }
            return partitionStatistics;
        } catch (Exception se) {
            throw new RuntimeException(se);
        }
    }

    public static boolean isDefaultPartition(String value) {
        return value.equals(HIVE_DEFAULT_PARTITION);
    }

    public StatsEval statsEval(long numberOfRows, ColumnStatistics columnStatistics, DataType dataType) throws StandardException {
        StatsEval statsEval = new StatsEval();
        if (numberOfRows == 0) {
            statsEval.alwaysFalse = true;
            return statsEval;
        }

        if (columnStatistics == null) {
            statsEval.alwaysTrue = true;
            return statsEval;
        }

        if (columnStatistics.hasNumberOfValues() && columnStatistics.getNumberOfValues() == 0) {
            statsEval.allNulls = true;
            return statsEval;
        }

        statsEval.hasNulls = columnStatistics.getNumberOfValues() != numberOfRows;

        if (dataType instanceof BooleanType && columnStatistics.getBooleanStatistics() != null) {
            BooleanStatistics booleanStatistics = columnStatistics.getBooleanStatistics();
            boolean hasTrueValues = (booleanStatistics.getTrueValueCount() != 0);
            boolean hasFalseValues = (columnStatistics.getNumberOfValues() != booleanStatistics.getTrueValueCount());
            if (hasTrueValues && hasFalseValues) {
                statsEval.minimumDVD = new SQLBoolean(false);
                statsEval.maximumDVD = new SQLBoolean(true);
            }
            if (hasTrueValues) {
                statsEval.minimumDVD = new SQLBoolean(true);
                statsEval.maximumDVD = new SQLBoolean(true);
            }
            if (hasFalseValues) {
                statsEval.minimumDVD = new SQLBoolean(false);
                statsEval.maximumDVD = new SQLBoolean(false);
            }
            return statsEval;
        }
        else if (dataType instanceof DecimalType) {
            DecimalStatistics decimalStatistics = columnStatistics.getDecimalStatistics();
            statsEval.minimumDVD = new SQLDecimal(decimalStatistics.getMin());
            statsEval.maximumDVD = new SQLDecimal(decimalStatistics.getMax());
        }
        else if (dataType instanceof StringType) {
            StringStatistics stringStatistics = columnStatistics.getStringStatistics();
            statsEval.minimumDVD = new SQLVarchar(stringStatistics.getMin().toStringUtf8());
            statsEval.maximumDVD = new SQLVarchar(stringStatistics.getMax().toStringUtf8());
        }
        else if (dataType instanceof DateType) {
            DateStatistics dateStatistics = columnStatistics.getDateStatistics();
            statsEval.minimumDVD = new SQLDate(new Date(DateWritable.daysToMillis(dateStatistics.getMin())));
            statsEval.maximumDVD = new SQLDate(new Date(DateWritable.daysToMillis(dateStatistics.getMax())));
        }
        else if (dataType instanceof IntegerType) {
            IntegerStatistics integerStatistics = columnStatistics.getIntegerStatistics();
            statsEval.minimumDVD = new SQLInteger(integerStatistics.getMin().intValue());
            statsEval.maximumDVD = new SQLInteger(integerStatistics.getMax().intValue());
        }
        else if (dataType instanceof LongType) {
            IntegerStatistics integerStatistics = columnStatistics.getIntegerStatistics();
            statsEval.minimumDVD = new SQLLongint(integerStatistics.getMin());
            statsEval.maximumDVD = new SQLLongint(integerStatistics.getMax());
        }
        else if (dataType instanceof ShortType) {
            IntegerStatistics integerStatistics = columnStatistics.getIntegerStatistics();
            statsEval.minimumDVD = new SQLSmallint(integerStatistics.getMin().shortValue());
            statsEval.maximumDVD = new SQLSmallint(integerStatistics.getMax().shortValue());
        }
        else if (dataType instanceof DoubleType) {
            DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics();
            statsEval.minimumDVD = new SQLDouble(doubleStatistics.getMin());
            statsEval.maximumDVD = new SQLDouble(doubleStatistics.getMax());
        }

        else if (dataType instanceof FloatType) {
            DoubleStatistics doubleStatistics = columnStatistics.getDoubleStatistics();
            statsEval.minimumDVD = new SQLReal(doubleStatistics.getMin().floatValue());
            statsEval.maximumDVD = new SQLReal(doubleStatistics.getMax().floatValue());
        }
        else {
            statsEval.alwaysTrue = true;
        }
        return statsEval;
    }



}
