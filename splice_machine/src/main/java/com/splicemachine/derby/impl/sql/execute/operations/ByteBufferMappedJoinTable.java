package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
class ByteBufferMappedJoinTable implements JoinTable{
    private final Map<ByteBuffer, List<ExecRow>> table;
    private final KeyEncoder outerKeyEncoder;

    public ByteBufferMappedJoinTable(Map<ByteBuffer, List<ExecRow>> table,int[] outerHashkeys, ExecRow outerTemplateRow){
        this.table=table;
        DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(outerTemplateRow);
        this.outerKeyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE,
                BareKeyHash.encoder(outerHashkeys,null,serializers),NoOpPostfix.INSTANCE);
    }

    @Override
    public Iterator<ExecRow> fetchInner(ExecRow outer) throws IOException, StandardException{
        byte[] outerKey=outerKeyEncoder.getKey(outer);
        assert outerKey!=null: "Programmer error: outer row does not have row key";
        List<ExecRow> rows = table.get(ByteBuffer.wrap(outerKey));
        if(rows==null)
            return Collections.emptyIterator();
        else
            return rows.iterator();
    }

    //nothing to close
    @Override public void close(){}

    static class Factory implements JoinTable.Factory{
        private final Map<ByteBuffer,List<ExecRow>> table;
        private final int[] outerHashKeys;
        private final ExecRow outerTemplateRow;

        public Factory(Map<ByteBuffer, List<ExecRow>> table,int[] outerHashKeys,ExecRow outerTemplateRow){
            this.table=table;
            this.outerHashKeys=outerHashKeys;
            this.outerTemplateRow=outerTemplateRow;
        }

        @Override
        public JoinTable newTable(){
            return new ByteBufferMappedJoinTable(table,outerHashKeys,outerTemplateRow);
        }
    }
}
