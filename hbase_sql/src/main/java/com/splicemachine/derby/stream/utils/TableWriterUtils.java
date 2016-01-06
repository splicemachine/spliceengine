package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.iapi.TableWriter.Type;
import com.splicemachine.stream.index.HTableWriterBuilder;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.mrio.MRConstants;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;

/**
 * Created by jleach on 5/20/15.
 */
public class TableWriterUtils {

    public static void serializeInsertTableWriterBuilder(Configuration conf, InsertTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.getInsertTableWriterBuilderBase64String());
        conf.set(MRConstants.TABLE_WRITER_TYPE, Type.INSERT.toString());
    }

    public static void serializeHTableWriterBuilder(Configuration conf, HTableWriterBuilder builder) throws IOException, StandardException {
        conf.set(MRConstants.TABLE_WRITER, builder.getHTableWriterBuilderBase64String());
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


    public static TableWriter deserializeTableWriter(Configuration conf) throws IOException, StandardException {
        String typeString = conf.get(MRConstants.TABLE_WRITER_TYPE);
        if (typeString == null)
            throw new IOException("Table Writer Type Missing");
        Type type = Type.valueOf(typeString);
        String base64 = conf.get(MRConstants.TABLE_WRITER);
        if (base64 == null)
            throw new IOException("record Writer Failed");
        switch (type) {
            case INSERT:
                return InsertTableWriterBuilder.getInsertTableWriterBuilderFromBase64String(base64).build();
            case UPDATE:
                return UpdateTableWriterBuilder.getUpdateTableWriterBuilderFromBase64String(base64).build();
            case DELETE:
                return DeleteTableWriterBuilder.getDeleteTableWriterBuilderFromBase64String(base64).build();
            case INDEX:
                return HTableWriterBuilder.getHTableWriterBuilderFromBase64String(base64).build();
            default:
                throw new IOException("Type Incorrect");
        }
    }


}
