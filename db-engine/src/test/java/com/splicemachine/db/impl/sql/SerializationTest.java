package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import org.apache.spark.sql.types.StructField;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.sql.Types;
import java.util.Arrays;

public class SerializationTest {

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

        public static GenericColumnDescriptor sample1() {
            return new GenericColumnDescriptor(new CustomResultColumnDescriptor(
                    "name", "tabName", "schemaName", 3,
                    new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DECIMAL), true, 10),
                    true, true, true)
            );
        }
        public static GenericColumnDescriptor sample2() {
            return new GenericColumnDescriptor(new CustomResultColumnDescriptor(
                    "sample2name", "atable", "SPLICE", 9,
                    new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), false, 5),
                    false, false, false)
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

    @Test
    public void testSerdeGenericColumnDescriptor() throws IOException, ClassNotFoundException {

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(stream);
        GenericColumnDescriptor gcd1 = CustomResultColumnDescriptor.sample1();
        gcd1.writeExternal(oos);
        oos.flush();

        // do NOT change value this unless you have an upgrade script, see DB-10566
        Assert.assertEquals(479938529, Arrays.hashCode(stream.toByteArray()));

        GenericColumnDescriptor gcd2 = new GenericColumnDescriptor();
        gcd2.readExternal(new ObjectInputStream(new ByteArrayInputStream(stream.toByteArray())));
        CustomResultColumnDescriptor.assertEquals(gcd1, gcd2);
    }

    @Test
    public void testSerdeGenericResultDescription() throws IOException, ClassNotFoundException {

        ResultColumnDescriptor[] columns = {
                CustomResultColumnDescriptor.sample1(),
                CustomResultColumnDescriptor.sample2()
        };
        GenericResultDescription grd = new GenericResultDescription(columns, "statementTypeName");

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(stream);
        grd.writeExternal(oos);
        oos.flush();

        // do NOT change value this unless you have an upgrade script, see DB-10566
        Assert.assertEquals(-975576838, Arrays.hashCode(stream.toByteArray()));

        GenericResultDescription grd2 = new GenericResultDescription();
        ByteArrayInputStream in = new ByteArrayInputStream(stream.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(in);
        grd2.readExternal(ois);

        // 1-based index
        CustomResultColumnDescriptor.assertEquals(columns[0], grd2.getColumnDescriptor(1));
        CustomResultColumnDescriptor.assertEquals(columns[1], grd2.getColumnDescriptor(2));
    }
}
