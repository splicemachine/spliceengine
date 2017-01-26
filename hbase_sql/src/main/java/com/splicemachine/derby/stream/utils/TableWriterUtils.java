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

package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter.Type;
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
