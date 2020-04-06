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
 *
 */

package com.splicemachine.derby.stream.output;

import org.apache.spark.sql.types.StructType;
import java.util.Optional;

public abstract class KafkaDataSetWriterBuilder<V> {
    protected String topicName;
    protected Optional<StructType> schema;

    public KafkaDataSetWriterBuilder<V> topicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    public KafkaDataSetWriterBuilder<V> schema(Optional<StructType> schema) {
        this.schema = schema;
        return this;
    }

    abstract public DataSetWriter build();
}
