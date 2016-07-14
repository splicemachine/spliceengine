/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter.Type;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.mrio.MRConstants;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

/**
 *  Utilities for encoding and decoding writers.
 *
 * Created by jleach on 5/20/15.
 */
public class TableWriterUtils {

    public static void serializeInsertTableWriterBuilder(Configuration conf, InsertTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.getInsertTableWriterBuilderBase64String());
        conf.set(MRConstants.TABLE_WRITER_TYPE, Type.INSERT.toString());
    }

    public static void serializeHTableWriterBuilder(Configuration conf, DirectTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.base64Encode());
        conf.set(MRConstants.TABLE_WRITER_TYPE, Type.INDEX.toString());
    }

    public static void serializeUpdateTableWriterBuilder(Configuration conf, UpdateTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.getUpdateTableWriterBuilderBase64String());
        conf.set(MRConstants.TABLE_WRITER_TYPE, Type.UPDATE.toString());
    }

    public static void serializeDeleteTableWriterBuilder(Configuration conf, DeleteTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.getDeleteTableWriterBuilderBase64String());
        conf.set(MRConstants.TABLE_WRITER_TYPE, Type.DELETE.toString());
    }


    public static DataSetWriterBuilder deserializeTableWriter(Configuration conf) throws IOException, StandardException {
        String typeString = conf.get(MRConstants.TABLE_WRITER_TYPE);
        if (typeString == null)
            throw new IOException("Table Writer Type Missing");
        Type type = Type.valueOf(typeString);
        String base64 = conf.get(MRConstants.TABLE_WRITER);
        if (base64 == null)
            throw new IOException("record Writer Failed");
        switch (type) {
            case INSERT:
                return InsertTableWriterBuilder.getInsertTableWriterBuilderFromBase64String(base64);
            case UPDATE:
                return UpdateTableWriterBuilder.getUpdateTableWriterBuilderFromBase64String(base64);
            case DELETE:
                return DeleteTableWriterBuilder.getDeleteTableWriterBuilderFromBase64String(base64);
            case INDEX:
                return DirectTableWriterBuilder.decodeBase64(base64);
            default:
                throw new IOException("Type Incorrect");
        }
    }


}
