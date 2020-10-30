package com.splicemachine.homeless;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoObjectInput;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.CursorInfo;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.GenericResultDescription;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.catalog.BaseDataDictionary;
import com.splicemachine.db.impl.sql.execute.TriggerInfo;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.spark.sql.types.StructField;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.sql.Types;
import java.util.Arrays;

public class SerializationTest {
    KryoPool kryoPool = SpliceKryoRegistry.getInstance();

    @BeforeClass
    public static void init() {
        BaseDataDictionary.READ_NEW_FORMAT = false;
        BaseDataDictionary.WRITE_NEW_FORMAT = false;
    }

    byte[] serializeKryo(Object o, int size) throws IOException {
        Output out = new Output(size);
        KryoObjectOutput koo = new KryoObjectOutput(out, kryoPool.get());
        koo.writeObject(o);
        return out.getBuffer();
    }

    Object deserializeKryo(byte[] data) throws IOException, ClassNotFoundException {
        KryoObjectInput in = new KryoObjectInput(kryoPool.get(), new Input(data));
        return in.readObject();
    }

    void testSerDe(Object o, int size) throws IOException, ClassNotFoundException {
        byte[] buf = serializeKryo(o, size);
        // comment this out if you need a binary string of an object
        //System.out.println(com.splicemachine.primitives.Bytes.toStringBinary(buf));
        Object o2 = deserializeKryo(buf);
        Assert.assertEquals(o.toString(), o2.toString());
    }
    @Test
    public void testTriggerInfo() throws IOException, ClassNotFoundException {
        byte[] data = Bytes.toBytesBinary("\\xFF\\x01" +
                "\\x00\\x00\\x00\\x01\\xD1\\x02W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\x90G\\x81T\\x03TG\\xB2W\\x00\\x00\\x00\\x0A\\x0AA,\\x00\\x00\\x00\\x00\\xD2\\xB3\\x8FL\\xDA\\x80\\x00\\x00\\x00W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0UU\\xC1K\\x00\\x00\\x00\\x04\\x00\\x01\\x01W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\x19GAW\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x00\\x01\\x00\\x03NEW_RO\\xD7\\x03NEW_ROW.i = \\xB1\\x00\\x00\\x00\\x03p\\x02\\x03insert into b values \\xB1\\x03insert into b values \\xB2p\\x02W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0x\\x9C\\x81UW\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\xA0\\xF1\\xC1V\\x00\\x00\\x00\\x00\\x00");
        KryoObjectInput in = new KryoObjectInput(SpliceKryoRegistry.getInstance().get(), new Input(data));
        TriggerInfo o = (TriggerInfo) in.readObject();
        Assert.assertNotEquals(null, o);
        Assert.assertEquals("TriggerInfo{columnIds=null, triggerDescriptors=[name=TG2\n" +
                "id=90478154-0175-d076-fcc0-000000f33448\n" +
                "oldReferencingName=NULL\n" +
                "newReferencingName=NEW_ROW\n" +
                "triggerDefinition = insert into b values 1\n" +
                "triggerDefinition = insert into b values 2\n" +
                "triggerDML=INSERT\n" +
                "whenClauseText=NEW_ROW.i = 1\n" +
                "version=3\n" +
                "], columnNames=null}", o.toString());
        testSerDe(o, 1000);
    }

    @Test
    public void testCursorInfo() throws IOException, ClassNotFoundException {
        byte[] data = Bytes.toBytesBinary("g\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00");
        KryoObjectInput in = new KryoObjectInput(SpliceKryoRegistry.getInstance().get(), new Input(data));
        CursorInfo o = (CursorInfo) in.readObject();
        Assert.assertNotEquals(null, o);
        Assert.assertEquals("CursorInfo\n" +
                "\tupdateMode: 0\n" +
                "\ttargetTable: null\n" +
                "\tupdateColumns: NULL\n" +
                "\tTargetColumnDescriptors: \n" +
                "NULL", o.toString());
        testSerDe(o, 15);
    }

    @Test
    public void testGenericStorablePreparedStatement() throws IOException, ClassNotFoundException {
        byte[] data = Bytes.toBytesBinary("\\x0Dg\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00z\\x00\\x00\\x00\\x00\\x00\\x00\\x06\\x80\\x16\\x01\\xD3\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x06\\x80\\x00\\x00\\x00\\x01\\x00P\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0UU\\xC1K\\x00\\x00\\x00\\x06\\x00\\x00\\x00\\x00\\xFF\\x01\\x00\\x00\\x00\\x01\\xD1\\x02W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\x90G\\x81T\\x03TG\\xB2W\\x00\\x00\\x00\\x0A\\x0AA,\\x00\\x00\\x00\\x00\\xD2\\xB3\\x8FL\\xDA\\x80\\x00\\x00\\x00W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0UU\\xC1K\\x00\\x00\\x00\\x04\\x00\\x01\\x01W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\x19GAW\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x00\\x01\\x00\\x03NEW_RO\\xD7\\x03NEW_ROW.i = \\xB1\\x00\\x00\\x00\\x03p\\x02\\x03insert into b values \\xB1\\x03insert into b values \\xB2p\\x02W\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0x\\x9C\\x81UW\\x00\\x00\\x00\\x00\\x00\\xF34H\\x00\\x00\\x01u\\xD0v\\xFC\\xC0\\xA0\\xF1\\xC1V\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x03SPLIC\\xC5\\x03\\x82A\\x00\\x00\\x00\\x01\\x03\\x82I\\x00\\x00\\x00\\x00h\\x03INSER\\xD4\\x00\\x00\\x00\\x01i\\x82I\\x00\\x00\\x00\\x00\\x00\\x01\\x14\\x0E\\x0F\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x13INTEGE\\xD2\\x00\\x00\\x00\\x0A\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x04\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x03\\xC1\\x01com.splicemachine.db.exe.ac34b10149x0175xd076xfcc0x000000f334484\\x01\\x00\\x00\\x0E\\x12\\xCA\\xFE\\xBA\\xBE\\x00\\x03\\x00-\\x00\\x8A\\x01\\x00@com/splicemachine/db/exe/ac34b10149x0175xd076xfcc0x000000f334484\\x07\\x00\\x01\\x01\\x004com/splicemachine/db/impl/sql/execute/BaseActivation\\x07\\x00\\x03\\x01\\x00\\x06<init>\\x01\\x00\\x03()V\\x0C\\x00\\x05\\x00\\x06\\x0A\\x00\\x04\\x00\\x07\\x01\\x00\\x04Code\\x01\\x00\\x0FpostConstructor\\x01\\x00\\x07execute\\x01\\x00+()Lcom/splicemachine/db/iapi/sql/ResultSet;\\x08\\x00\\x0B\\x01\\x00\\x0DthrowIfClosed\\x01\\x00\\x15(Ljava/lang/String;)V\\x0C\\x00\\x0E\\x00\\x0F\\x0A\\x00\\x04\\x00\\x10\\x01\\x00\\x0EstartExecution\\x0C\\x00\\x12\\x00\\x06\\x0A\\x00\\x04\\x00\\x13\\x01\\x00\\x0Bmaterialize\\x08\\x00\\x15\\x01\\x00\\x15getSubqueryResultSets\\x01\\x00\\x14()Ljava/util/Vector;\\x08\\x00\\x17\\x01\\x00\\x10java/util/Vector\\x07\\x00\\x1A\\x0A\\x00\\x1B\\x00\\x07\\x01\\x00\\x14datasetProcessorType\\x01\\x00\\x01I\\x0C\\x00\\x1D\\x00\\x1E\\x09\\x00\\x04\\x00\\x1F\\x01\\x00\\x0DfillResultSet\\x01\\x00\\x14setParameterValueSet\\x01\\x00\\x05(IZ)V\\x0C\\x00\"\\x00#\\x0A\\x00\\x04\\x00$\\x01\\x00\\x13throwIfMissingParms\\x0C\\x00&\\x00\\x06\\x0A\\x00\\x04\\x00'\\x01\\x00\\x13getResultSetFactory\\x01\\x00:()Lcom/splicemachine/db/iapi/sql/execute/ResultSetFactory;\\x0C\\x00)\\x00*\\x0A\\x00\\x04\\x00+\\x01\\x00\\x02e0\\x01\\x00\\x14()Ljava/lang/Object;\\x01\\x00/Lcom/splicemachine/db/iapi/sql/execute/ExecRow;\\x0C\\x00-\\x00/\\x09\\x00\\x02\\x000\\x01\\x00\\x13getExecutionFactory\\x01\\x00:()Lcom/splicemachine/db/iapi/sql/execute/ExecutionFactory;\\x0C\\x002\\x003\\x0A\\x00\\x04\\x004\\x01\\x006com/splicemachine/db/iapi/sql/execute/ExecutionFactory\\x07\\x006\\x01\\x00\\x0BgetValueRow\\x01\\x002(I)Lcom/splicemachine/db/iapi/sql/execute/ExecRow;\\x0C\\x008\\x009\\x0B\\x007\\x00:\\x01\\x00\\x13getDataValueFactory\\x01\\x004()Lcom/splicemachine/db/iapi/types/DataValueFactory;\\x0C\\x00<\\x00=\\x0A\\x00\\x04\\x00>\\x01\\x000com/splicemachine/db/iapi/types/DataValueFactory\\x07\\x00@\\x01\\x00\\x0CgetDataValue\\x01\\x00e(ILcom/splicemachine/db/iapi/types/NumberDataValue;)Lcom/splicemachine/db/iapi/types/NumberDataValue;\\x0C\\x00B\\x00C\\x0B\\x00A\\x00D\\x01\\x003com/splicemachine/db/iapi/types/DataValueDescriptor\\x07\\x00F\\x01\\x00!com/splicemachine/db/iapi/sql/Row\\x07\\x00H\\x01\\x00\\x09setColumn\\x01\\x009(ILcom/splicemachine/db/iapi/types/DataValueDescriptor;)V\\x0C\\x00J\\x00K\\x0B\\x00I\\x00L\\x01\\x001com/splicemachine/db/iapi/error/StandardException\\x07\\x00N\\x01\\x00\\x13java/lang/Exception\\x07\\x00P\\x01\\x00\\x0AExceptions\\x08\\x00-\\x01\\x00;com/splicemachine/db/iapi/services/loader/GeneratedByteCode\\x07\\x00T\\x01\\x00\\x09getMethod\\x01\\x00O(Ljava/lang/String;)Lcom/splicemachine/db/iapi/services/loader/GeneratedMethod;\\x0C\\x00V\\x00W\\x0B\\x00U\\x00X\\x06?\\xF0\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x006com/splicemachine/db/iapi/sql/execute/ResultSetFactory\\x07\\x00\\x5C\\x01\\x00\\x0FgetRowResultSet\\x01\\x00\\xA1(Lcom/splicemachine/db/iapi/sql/Activation;Lcom/splicemachine/db/iapi/services/loader/GeneratedMethod;ZIDD)Lcom/splicemachine/db/iapi/sql/execute/NoPutResultSet;\\x0C\\x00^\\x00_\\x0B\\x00]\\x00`\\x01\\x00\\x06INSERT\\x08\\x00b\\x01\\x00\\x034.0\\x08\\x00d\\x01\\x00:Insert(n=0,\\x0AtotalCost=0.001,insertedRows=1,\\x0AtargetTable=A)\\x08\\x00f\\x01\\x00\\x04none\\x08\\x00h\\x01\\x00\\x12getInsertResultSet\\x01\\x01\\xB9(Lcom/splicemachine/db/iapi/sql/execute/NoPutResultSet;Lcom/splicemachine/db/iapi/services/loader/GeneratedMethod;Lcom/splicemachine/db/iapi/services/loader/GeneratedMethod;Ljava/lang/String;Ljava/lang/String;IZZDDLjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;ZZZDLjava/lang/String;)Lcom/splicemachine/db/iapi/sql/ResultSet;\\x0C\\x00j\\x00k\\x0B\\x00]\\x00l\\x01\\x00\\x09resultSet\\x01\\x00)Lcom/splicemachine/db/iapi/sql/ResultSet;\\x0C\\x00n\\x00o\\x09\\x00\\x04\\x00p\\x0C\\x00!\\x00\\x0C\\x0A\\x00\\x02\\x00r\\x01\\x00\\x11getExecutionCount\\x0C\\x00t\\x00\\x1E\\x09\\x00\\x02\\x00u\\x01\\x00\\x03()I\\x01\\x00\\x11setExecutionCount\\x01\\x00\\x04(I)V\\x01\\x00\\x16getRowCountCheckVector\\x01\\x00\\x12Ljava/util/Vector;\\x0C\\x00z\\x00{\\x09\\x00\\x02\\x00|\\x01\\x00\\x16setRowCountCheckVector\\x01\\x00\\x15(Ljava/util/Vector;)V\\x01\\x00\\x19getStalePlanCheckInterval\\x0C\\x00\\x80\\x00\\x1E\\x09\\x00\\x02\\x00\\x81\\x01\\x00\\x19setStalePlanCheckInterval\\x01\\x00-com/splicemachine/db/iapi/sql/execute/ExecRow\\x07\\x00\\x84\\x01\\x00\\x03row\\x01\\x000[Lcom/splicemachine/db/iapi/sql/execute/ExecRow;\\x0C\\x00\\x86\\x00\\x87\\x09\\x00\\x04\\x00\\x88\\x001\\x00\\x02\\x00\\x04\\x00\\x00\\x00\\x04\\x00\\x02\\x00-\\x00/\\x00\\x00\\x00\\x0A\\x00t\\x00\\x1E\\x00\\x00\\x00\\x0A\\x00z\\x00{\\x00\\x00\\x00\\x0A\\x00\\x80\\x00\\x1E\\x00\\x00\\x00\\x0D\\x00\\x01\\x00\\x05\\x00\\x06\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x11\\x00\\x01\\x00\\x01\\x00\\x00\\x00\\x05*\\xB7\\x00\\x08\\xB1\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x0A\\x00\\x06\\x00\\x02\\x00R\\x00\\x00\\x00\\x04\\x00\\x01\\x00O\\x00\\x09\\x00\\x00\\x00L\\x00\\x05\\x00\\x01\\x00\\x00\\x00@*\\x03Z\\xB5\\x00 W*\\x03\\x03\\xB6\\x00%*\\xB6\\x005\\x04\\xB9\\x00;\\x02\\x00*_\\xB5\\x001*\\xB4\\x001\\x04\\x04*\\xB6\\x00?_\\x01\\xB9\\x00E\\x03\\x00\\xC0\\x00G\\xB9\\x00M\\x03\\x00*\\x04\\xBD\\x00\\x85Z\\xB5\\x00\\x89W\\xB1\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x0B\\x00\\x0C\\x00\\x02\\x00R\\x00\\x00\\x00\\x04\\x00\\x01\\x00O\\x00\\x09\\x00\\x00\\x003\\x00\\x03\\x00\\x01\\x00\\x00\\x00'*\\x12\\x0D\\xB6\\x00\\x11*\\xB6\\x00\\x14*\\xB6\\x00(*\\xB4\\x00q\\xC7\\x00\\x10*\\xB6\\x00s*_Z\\xB5\\x00q\\xA7\\x00\\x07*\\xB4\\x00q\\xB0\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x15\\x00\\x06\\x00\\x02\\x00R\\x00\\x00\\x00\\x04\\x00\\x01\\x00O\\x00\\x09\\x00\\x00\\x00\\x13\\x00\\x02\\x00\\x01\\x00\\x00\\x00\\x07*\\x12\\x16\\xB6\\x00\\x11\\xB1\\x00\\x00\\x00\\x00\\x00\\x01\\x00\\x17\\x00\\x18\\x00\\x02\\x00R\\x00\\x00\\x00\\x04\\x00\\x01\\x00O\\x00\\x09\\x00\\x00\\x00\\x1A\\x00\\x02\\x00\\x01\\x00\\x00\\x00\\x0E*\\x12\\x19\\xB6\\x00\\x11\\xBB\\x00\\x1BY\\xB7\\x00\\x1C\\xB0\\x00\\x00\\x00\\x00\\x00\\x02\\x00!\\x00\\x0C\\x00\\x02\\x00R\\x00\\x00\\x00\\x04\\x00\\x01\\x00O\\x00\\x09\\x00\\x00\\x00P\\x00\\x1D\\x00\\x01\\x00\\x00\\x00D*\\xB6\\x00,*\\xB6\\x00,**\\x12S\\xB9\\x00Y\\x02\\x00\\x04\\x03\\x14\\x00Z\\x14\\x00Z\\xB9\\x00a\\x09\\x00\\x01\\x01\\x12c\\x01\\x03\\x03\\x03\\x14\\x00Z\\x14\\x00Z\\x12e\\x12g\\x01\\x01\\x01\\x01\\x01\\x12i\\x02\\x01\\x03\\x03\\x03\\x0E\\x01\\xB9\\x00m\\x1D\\x00\\xB0\\x00\\x00\\x00\\x00\\x00\\x01\\x00-\\x00.\\x00\\x02\\x00R\\x00\\x00\\x00\\x06\\x00\\x02\\x00O\\x00Q\\x00\\x09\\x00\\x00\\x00\\x11\\x00\\x01\\x00\\x01\\x00\\x00\\x00\\x05*\\xB4\\x001\\xB0\\x00\\x00\\x00\\x00\\x00\\x14\\x00t\\x00w\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x10\\x00\\x01\\x00\\x01\\x00\\x00\\x00\\x04\\xB2\\x00v\\xAC\\x00\\x00\\x00\\x00\\x00\\x14\\x00x\\x00y\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x11\\x00\\x01\\x00\\x02\\x00\\x00\\x00\\x05\\x1B\\xB3\\x00v\\xB1\\x00\\x00\\x00\\x00\\x00\\x14\\x00z\\x00\\x18\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x10\\x00\\x01\\x00\\x01\\x00\\x00\\x00\\x04\\xB2\\x00}\\xB0\\x00\\x00\\x00\\x00\\x00\\x14\\x00~\\x00\\x7F\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x11\\x00\\x01\\x00\\x02\\x00\\x00\\x00\\x05+\\xB3\\x00}\\xB1\\x00\\x00\\x00\\x00\\x00\\x14\\x00\\x80\\x00w\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x10\\x00\\x01\\x00\\x01\\x00\\x00\\x00\\x04\\xB2\\x00\\x82\\xAC\\x00\\x00\\x00\\x00\\x00\\x14\\x00\\x83\\x00y\\x00\\x01\\x00\\x09\\x00\\x00\\x00\\x11\\x00\\x01\\x00\\x02\\x00\\x00\\x00\\x05\\x1B\\xB3\\x00\\x82\\xB1\\x00\\x00\\x00\\x00\\x00\\x00\\x00");
        KryoObjectInput in = new KryoObjectInput(SpliceKryoRegistry.getInstance().get(), new Input(data));
        GenericStorablePreparedStatement o = (GenericStorablePreparedStatement) in.readObject();
        Assert.assertNotEquals(null, o);
        Assert.assertEquals( "GenericStorablePreparedStatement activationClassName=null " +
                "className=com.splicemachine.db.exe.ac34b10149x0175xd076xfcc0x000000f334484", o.toString());
        testSerDe(o, 5000);
    }


    static class CustomResultColumnDescriptor implements ResultColumnDescriptor {

        String sourceTableName, sourceSchemaName, name;
        int iColPos;
        DataTypeDescriptor type;
        boolean bUpdateableByCursor, bAutoIncrement, bHasGenerationClause;

        public CustomResultColumnDescriptor(String name, String sourceTableName, String sourceSchemaName, int iColPos,
                                            DataTypeDescriptor type,
                                            boolean bAutoIncrement, boolean bUpdateableByCursor, boolean bHasGenerationClause) {
            this.name = name;
            this.sourceTableName = sourceTableName;
            this.sourceSchemaName = sourceSchemaName;
            this.iColPos = iColPos;
            this.type = type;
            this.bAutoIncrement = bAutoIncrement;
            this.bUpdateableByCursor = bUpdateableByCursor;
            this.bHasGenerationClause = bHasGenerationClause;
        }

        @Override
        public DataTypeDescriptor getType() {
            return type;
        }

        @Override
        public StructField getStructField() {
            Assert.fail("not expected call of getStructField");
            return null;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getSourceSchemaName() {
            return sourceSchemaName;
        }

        @Override
        public String getSourceTableName() {
            return sourceTableName;
        }

        @Override
        public boolean updatableByCursor() {
            return bUpdateableByCursor;
        }

        @Override
        public int getColumnPosition() {
            return iColPos;
        }

        @Override
        public boolean isAutoincrement() {
            return bAutoIncrement;
        }

        @Override
        public boolean hasGenerationClause() {
            return bHasGenerationClause;
        }

        public static DataTypeDescriptor sampleDTD1()
        {
            return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DECIMAL), true, 10);
        }

        public static DataTypeDescriptor sampleDTD2()
        {
            return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), false, 5);
        }

        public static GenericColumnDescriptor sample1() {
            return new GenericColumnDescriptor(new CustomResultColumnDescriptor(
                    "name", "tabName", "schemaName", 3,
                    sampleDTD1(), true, true, true)
            );
        }
        public static GenericColumnDescriptor sample2() {
            return new GenericColumnDescriptor(new CustomResultColumnDescriptor(
                    "sample2name", "atable", "SPLICE", 9,
                    sampleDTD2(), false, false, false)
            );
        }

        public static void assertEquals(ResultColumnDescriptor rcd1, ResultColumnDescriptor rcd2)
        {
            Assert.assertEquals(rcd1.getSourceSchemaName(), rcd2.getSourceSchemaName());
            Assert.assertEquals(rcd1.getSourceTableName(), rcd2.getSourceTableName());
            Assert.assertEquals(rcd1.getName(), rcd2.getName());

            Assert.assertEquals(rcd1.getType(), rcd2.getType());
            Assert.assertEquals(rcd1.getColumnPosition(), rcd2.getColumnPosition());
            Assert.assertEquals(rcd1.isAutoincrement(), rcd2.isAutoincrement());
            Assert.assertEquals(rcd1.updatableByCursor(), rcd2.updatableByCursor());

            // check this in DB-10582
            // Assert.assertEquals(rcd1.hasGenerationClause(), rcd2.hasGenerationClause());
        }
    }

    ByteArrayOutputStream serialize(Externalizable obj) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(stream);
        obj.writeExternal(oos);
        oos.flush();
        return stream;
    }

    long writeObjHash(Externalizable obj) throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(stream);
        // writeObj will also incorporate the object's serialVersionUID, see DB-10655
        oos.writeObject(obj);
        oos.flush();
        return Arrays.hashCode(stream.toByteArray());
    }

    void deserialize(ByteArrayOutputStream stream, Externalizable obj) throws IOException, ClassNotFoundException {
        obj.readExternal(new ObjectInputStream(new ByteArrayInputStream(stream.toByteArray())));
    }

    void testSerdeTypeDescriptorImpl(int i, long expectedHash, long expectedHashKryo,
                                     TypeDescriptorImpl td) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream stream = serialize(td);

        Assert.assertEquals( "nr " + i , expectedHash, writeObjHash(td));

        TypeDescriptorImpl td2 = new TypeDescriptorImpl();
        deserialize(stream, td2);
        Assert.assertEquals(true, td.equals(td2));
        byte[] kryo = serializeKryo(td, 1000);
        testSerDe(td, 1000);
        Assert.assertEquals(expectedHashKryo, Arrays.hashCode(kryo));
    }

    @Test
    public void testSerdeTypeDescriptorImpl() throws IOException, ClassNotFoundException {

        // do NOT change these values unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(TypeDescriptorImpl.serialVersionUID, -366780836777876368l);

        testSerdeTypeDescriptorImpl( 0, 1632867878, 1527676321,
                (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD1().getCatalogType() );
        testSerdeTypeDescriptorImpl( 1, -401935666, -1051578936,
                (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD2().getCatalogType() );
        testSerdeTypeDescriptorImpl( 2, 461323613, 1680325579,
                new TypeDescriptorImpl(
                        new BaseTypeIdImpl(StoredFormatIds.CLOB_TYPE_ID_IMPL),
                        7, 0, // CLOB doesn't have scale
                        false, 8, 3) );

        testSerdeTypeDescriptorImpl( 3, 55041400, 718303979,
                new TypeDescriptorImpl(new DecimalTypeIdImpl(true),
                        4, 2,false, 3) );

        testSerdeTypeDescriptorImpl( 4, 1499050555, -1257734010,
                new TypeDescriptorImpl(new UserDefinedTypeIdImpl("a", "b", "c"),
                        3, 2,false, 3) );

        testSerdeTypeDescriptorImpl( 5, -261646406, 586328168,
                new TypeDescriptorImpl(
                        new BaseTypeIdImpl(StoredFormatIds.VARCHAR_TYPE_ID_IMPL),
                        0, 0, true, 32, 3) );

        testSerdeTypeDescriptorImpl( 6, -332625021, 1226312723,
                new TypeDescriptorImpl(
                        new RowMultiSetImpl(new String[]{"a", "b"},
                                new TypeDescriptor[] {TypeDescriptor.INTEGER, TypeDescriptor.DOUBLE} ),
                        0, 0, true, 32, 3) );

        TypeDescriptorImpl t = new TypeDescriptorImpl(new BaseTypeIdImpl(StoredFormatIds.ARRAY_TYPE_ID_IMPL),
                0, 0, true, 32, 3);
        t.setChildren( new TypeDescriptor[] {TypeDescriptor.INTEGER, TypeDescriptor.DOUBLE} );
        testSerdeTypeDescriptorImpl( 7, -1015081738, 1939197093, t );
    }

    @Test
    public void testSerdeTypeDescriptorImpl_2() throws IOException, ClassNotFoundException {
        TypeDescriptorImpl td = (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD1().getCatalogType();
        ByteArrayOutputStream stream = serialize(td);
        byte[] kryo = serializeKryo(td, 1000);
        testSerDe(td, 1000);

        // do NOT change value these unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(1527676321, Arrays.hashCode(kryo));
        Assert.assertEquals(-1651386200, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals(td.serialVersionUID, -366780836777876368l);
        Assert.assertEquals(1632867878, writeObjHash(td));

        TypeDescriptorImpl td2 = new TypeDescriptorImpl();
        deserialize(stream, td2);
        Assert.assertEquals(true, td.equals(td2));
    }

    @Test
    public void testSerdeDataTypeDescriptor() throws IOException, ClassNotFoundException {
        DataTypeDescriptor dtd1 = CustomResultColumnDescriptor.sampleDTD1();
        ByteArrayOutputStream stream = serialize(dtd1);
        byte[] kryo = serializeKryo(dtd1, 1000);
        testSerDe(dtd1, 1000);
        
        // do NOT change these values unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(-363698866, Arrays.hashCode(kryo));
        Assert.assertEquals(1665200532, Arrays.hashCode(stream.toByteArray()));
//        Assert.assertEquals(804804029538241393l, dtd1.serialVersionUID);
        Assert.assertEquals(34423717, writeObjHash(dtd1));

        DataTypeDescriptor dtd2 = new DataTypeDescriptor();
        deserialize(stream, dtd2);
        Assert.assertEquals(true, dtd1.equals(dtd2));
    }

    @Test
    public void testSerdeGenericColumnDescriptor() throws IOException, ClassNotFoundException {
        GenericColumnDescriptor gcd1 = CustomResultColumnDescriptor.sample1();
        ByteArrayOutputStream stream = serialize(gcd1);
        byte[] kryo = serializeKryo(gcd1, 1000);
        testSerDe(gcd1, 1000);

        // do NOT change these values unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(1354337419, Arrays.hashCode(kryo));
        Assert.assertEquals(87838571, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals( -7718734896813275598l, gcd1.serialVersionUID);
        Assert.assertEquals(614045928, writeObjHash(gcd1));

        GenericColumnDescriptor gcd2 = new GenericColumnDescriptor();
        deserialize(stream, gcd2);
        CustomResultColumnDescriptor.assertEquals(gcd1, gcd2);
    }

    @Test
    public void testSerdeGenericResultDescription() throws IOException, ClassNotFoundException {

        ResultColumnDescriptor[] columns = {
                CustomResultColumnDescriptor.sample1(),
                CustomResultColumnDescriptor.sample2()
        };
        GenericResultDescription grd = new GenericResultDescription(columns, "statementTypeName");
        ByteArrayOutputStream stream = serialize(grd);
        byte[] kryo = serializeKryo(grd, 1000);
        testSerDe(grd, 1000);

        // do NOT change these values unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(-831967788, Arrays.hashCode(kryo));
        Assert.assertEquals(15577122, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals(1, grd.serialVersionUID);
        Assert.assertEquals(171528844, writeObjHash(grd));

        GenericResultDescription grd2 = new GenericResultDescription();
        deserialize(stream, grd2);

        // 1-based index
        CustomResultColumnDescriptor.assertEquals(columns[0], grd2.getColumnDescriptor(1));
        CustomResultColumnDescriptor.assertEquals(columns[1], grd2.getColumnDescriptor(2));
    }
}
