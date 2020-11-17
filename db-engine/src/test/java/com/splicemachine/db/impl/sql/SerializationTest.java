package com.splicemachine.db.impl.sql;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.types.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import org.apache.spark.sql.types.StructField;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.sql.Types;
import java.util.Arrays;

@Ignore
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

    void testSerdeTypeDescriptorImpl(int i, long expectedHash, TypeDescriptorImpl td) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream stream = serialize(td);

        Assert.assertEquals( "nr " + i , expectedHash, writeObjHash(td));

        TypeDescriptorImpl td2 = new TypeDescriptorImpl();
        deserialize(stream, td2);
        Assert.assertEquals(true, td.equals(td2));
    }

    @Test
    public void testSerdeTypeDescriptorImpl() throws IOException, ClassNotFoundException {

        // do NOT change these values unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(TypeDescriptorImpl.serialVersionUID, -366780836777876368l);
        testSerdeTypeDescriptorImpl( 0, -1294251152,
                (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD1().getCatalogType() );
        testSerdeTypeDescriptorImpl( 1, 93735511,
                (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD2().getCatalogType() );
        testSerdeTypeDescriptorImpl( 2, 1507648294,
                new TypeDescriptorImpl(
                        new BaseTypeIdImpl(StoredFormatIds.CLOB_TYPE_ID_IMPL),
                        7, 0, // CLOB doesn't have scale
                        false, 8, 3) );

        testSerdeTypeDescriptorImpl( 3, 1422889666,
                new TypeDescriptorImpl(new DecimalTypeIdImpl(true),
                        4, 2,false, 3) );

        testSerdeTypeDescriptorImpl( 4, -1099254617,
                new TypeDescriptorImpl(new UserDefinedTypeIdImpl("a", "b", "c"),
                        3, 2,false, 3) );

        testSerdeTypeDescriptorImpl( 5, -2075709103,
                new TypeDescriptorImpl(
                        new BaseTypeIdImpl(StoredFormatIds.VARCHAR_TYPE_ID_IMPL),
                        0, 0, true, 32, 3) );

        testSerdeTypeDescriptorImpl( 6, 2143857202,
                new TypeDescriptorImpl(
                        new RowMultiSetImpl(new String[]{"a", "b"},
                                new TypeDescriptor[] {TypeDescriptor.INTEGER, TypeDescriptor.DOUBLE} ),
                        0, 0, true, 32, 3) );

        TypeDescriptorImpl t = new TypeDescriptorImpl(new BaseTypeIdImpl(StoredFormatIds.ARRAY_TYPE_ID_IMPL),
            0, 0, true, 32, 3);
        t.setChildren( new TypeDescriptor[] {TypeDescriptor.INTEGER, TypeDescriptor.DOUBLE} );
        testSerdeTypeDescriptorImpl( 7, -1832431411, t );
    }

    @Test
    public void testSerdeTypeDescriptorImpl_2() throws IOException, ClassNotFoundException {
        TypeDescriptorImpl td = (TypeDescriptorImpl)CustomResultColumnDescriptor.sampleDTD1().getCatalogType();
        ByteArrayOutputStream stream = serialize(td);

        // do NOT change value these unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(-1191620066, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals(td.serialVersionUID, -366780836777876368l);
        Assert.assertEquals(-1294251152, writeObjHash(td));

        TypeDescriptorImpl td2 = new TypeDescriptorImpl();
        deserialize(stream, td2);
        Assert.assertEquals(true, td.equals(td2));
    }

    @Test
    public void testSerdeDataTypeDescriptor() throws IOException, ClassNotFoundException {
        DataTypeDescriptor dtd1 = CustomResultColumnDescriptor.sampleDTD1();
        ByteArrayOutputStream stream = serialize(dtd1);

        // do NOT change value this unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(1174013790, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals(804804029538241393l, dtd1.serialVersionUID);
        Assert.assertEquals(1987503899, writeObjHash(dtd1));

        DataTypeDescriptor dtd2 = new DataTypeDescriptor();
        deserialize(stream, dtd2);
        Assert.assertEquals(true, dtd1.equals(dtd2));
    }

    @Test
    public void testSerdeGenericColumnDescriptor() throws IOException, ClassNotFoundException {
        GenericColumnDescriptor gcd1 = CustomResultColumnDescriptor.sample1();
        ByteArrayOutputStream stream = serialize(gcd1);

        // do NOT change value this unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(479938529, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals( -7718734896813275598l, gcd1.serialVersionUID);
        Assert.assertEquals(-115757262, writeObjHash(gcd1));

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

        // do NOT change value this unless you have an upgrade script, see DB-10566/DB-10665
        Assert.assertEquals(-975576838, Arrays.hashCode(stream.toByteArray()));
        Assert.assertEquals(1, grd.serialVersionUID);
        Assert.assertEquals(-489472844, writeObjHash(grd));

        GenericResultDescription grd2 = new GenericResultDescription();
        deserialize(stream, grd2);

        // 1-based index
        CustomResultColumnDescriptor.assertEquals(columns[0], grd2.getColumnDescriptor(1));
        CustomResultColumnDescriptor.assertEquals(columns[1], grd2.getColumnDescriptor(2));
    }
}
