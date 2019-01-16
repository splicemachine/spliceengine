/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configuration;

/**
 * Allow access to certain package private methods of WriterOptions,
 * primarily used for providing the memory manager to the writer
 */
public class OrcWriterOptions
        extends OrcFile.WriterOptions
{
    public OrcWriterOptions(Configuration conf)
    {
        super(conf);
    }

    @Override
    public OrcWriterOptions memory(MemoryManager value)
    {
        super.memory(value);
        return this;
    }
}
