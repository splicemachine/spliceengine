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
package com.splicemachine.orc.block;

import org.apache.spark.sql.types.DataType;
import static java.util.Objects.requireNonNull;

/**
 *
 *
 */
public class LazyPartitionColumnBlockLoaderImpl implements LazyColumnBlockLoader {
    private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
    private int size;
    private DataType type;
    private String partitionValue;
    private boolean loaded;
    private ColumnBlock columnBlock;

    public LazyPartitionColumnBlockLoaderImpl(DataType type, int size, String partitionValue) {
        this.type = requireNonNull(type, "type is null");
        this.size = size;
        this.partitionValue = partitionValue;
    }

    @Override
    public ColumnBlock getColumnBlock() {
        if (loaded) {
            return columnBlock;
        }
        try {
            columnBlock = BlockFactory.getColumnBlock(null,type);
            if(!partitionValue.equals(HIVE_DEFAULT_PARTITION))
                columnBlock.setPartitionValue(partitionValue,size);
            else
                columnBlock.setPartitionNull(size);
            loaded = true;
            return columnBlock;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isLoaded() {
        return isLoaded();
    }

}
