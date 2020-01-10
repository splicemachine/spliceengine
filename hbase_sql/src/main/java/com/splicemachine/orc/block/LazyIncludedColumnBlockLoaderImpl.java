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

import com.splicemachine.orc.reader.StreamReader;
import org.apache.spark.sql.types.DataType;
import java.io.IOException;
import static java.util.Objects.requireNonNull;

/**
 *
 *
 *
 */
public class LazyIncludedColumnBlockLoaderImpl implements LazyColumnBlockLoader {
    private final DataType type;
    private boolean loaded;
    private StreamReader streamReader;
    private ColumnBlock columnBlock;

    public LazyIncludedColumnBlockLoaderImpl(StreamReader streamReader, DataType type) {
        this.type = requireNonNull(type, "type is null");
        this.streamReader = streamReader;
    }

    @Override
    public ColumnBlock getColumnBlock() {
        if (loaded) {
            return columnBlock;
        }
        try {
            columnBlock = BlockFactory.getColumnBlock(streamReader.readBlock(type),type);
            loaded = true;
            return columnBlock;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isLoaded() {
        return isLoaded();
    }

}
