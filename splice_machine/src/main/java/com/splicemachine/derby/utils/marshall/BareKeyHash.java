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

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class BareKeyHash{

    protected final int[] keyColumns;
    protected final boolean[] keySortOrder;
    protected final boolean sparse;
    protected final DescriptorSerializer[] serializers;

    protected final KryoPool kryoPool;

    protected BareKeyHash(int[] keyColumns,
                          boolean[] keySortOrder,
                          boolean sparse,
                          KryoPool kryoPool,
                          DescriptorSerializer[] serializers){
        this.keyColumns=keyColumns;
        this.keySortOrder=keySortOrder;
        this.sparse=sparse;
        this.kryoPool=kryoPool;
        this.serializers=serializers;
    }

    public static DataHash encoder(int[] keyColumns,boolean[] keySortOrder,DescriptorSerializer[] serializers){
        return encoder(keyColumns,keySortOrder,SpliceKryoRegistry.getInstance(),serializers);
    }

    public static DataHash encoder(int[] keyColumns,boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
        return new Encoder(keyColumns,keySortOrder,kryoPool,serializers);
    }

    public static KeyHashDecoder decoder(int[] keyColumns,boolean[] keySortOrder,DescriptorSerializer[] serializers){
        return decoder(keyColumns,keySortOrder,SpliceKryoRegistry.getInstance(),serializers);
    }

    public static KeyHashDecoder decoder(int[] keyColumns,boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
        return new Decoder(keyColumns,keySortOrder,kryoPool,serializers);
    }

    protected void pack(MultiFieldEncoder encoder,ExecRow currentRow) throws StandardException, IOException{
        encoder.reset();
        DataValueDescriptor[] dvds=currentRow.getRowArray();
        if(keySortOrder!=null){
            for(int i=0;i<keyColumns.length;i++){
                int pos=keyColumns[i];
                if(pos==-1) continue; //skip columns marked with a -1
                DescriptorSerializer serializer=serializers[pos];
                DataValueDescriptor dvd=dvds[pos];
                serializer.encode(encoder,dvd,!keySortOrder[i]);
            }
        }else if(keyColumns!=null){
            for(int keyColumn : keyColumns){
                if(keyColumn==-1) continue; //skip columns marked with a -1
                DescriptorSerializer serializer=serializers[keyColumn];
                DataValueDescriptor dvd=dvds[keyColumn];
                serializer.encode(encoder,dvd,false);
            }
        }else{
            for(int keyColumn=0;keyColumn<dvds.length;keyColumn++){
                DescriptorSerializer serializer=serializers[keyColumn];
                DataValueDescriptor dvd=dvds[keyColumn];
                serializer.encode(encoder,dvd,false);
            }
        }
    }

    protected void unpack(ExecRow destination,MultiFieldDecoder decoder) throws StandardException{
        DataValueDescriptor[] fields=destination.getRowArray();
        if(keySortOrder!=null){
            for(int i=0;i<keyColumns.length;i++){
                int pos=keyColumns[i];
                if(pos<0) continue;
                boolean desc=!keySortOrder[i];
                DescriptorSerializer serializer=serializers[pos];
                DataValueDescriptor field=fields[pos];
                serializer.decode(decoder,field,desc);
            }
        }else if(keyColumns!=null){
            for(int pos : keyColumns){
                if(pos==-1) continue;
                DescriptorSerializer serializer=serializers[pos];
                DataValueDescriptor field=fields[pos];
                serializer.decode(decoder,field,false);
            }
        }else{
            for(int pos=0;pos<fields.length;pos++){
                if(fields[pos]==null) continue;
                DescriptorSerializer serializer=serializers[pos];
                DataValueDescriptor field=fields[pos];
                serializer.decode(decoder,field,false);
            }
        }
    }

    public void close() throws IOException{
        for(DescriptorSerializer serializer : serializers){
            try{serializer.close();}catch(IOException ignored){}
        }
    }

    private static class Decoder extends BareKeyHash implements KeyHashDecoder{
        private MultiFieldDecoder decoder;

        private Decoder(int[] keyColumns,boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
            super(keyColumns,keySortOrder,false,kryoPool,serializers);
        }

        @Override
        public void set(byte[] bytes,int hashOffset,int length){
            if(decoder==null)
                decoder=MultiFieldDecoder.create();

            decoder.set(bytes,hashOffset,length);
        }

        @Override
        public void decode(ExecRow destination) throws StandardException{
            unpack(destination,decoder);
        }

    }

    private static class Encoder extends BareKeyHash implements DataHash<ExecRow>{
        private MultiFieldEncoder encoder;

        private ExecRow currentRow;

        public Encoder(int[] keyColumns,boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
            super(keyColumns,keySortOrder,false,kryoPool,serializers);
        }

        @Override
        public void setRow(ExecRow rowToEncode){
            this.currentRow=rowToEncode;
        }

        @Override
        public byte[] encode() throws StandardException, IOException{
            if(encoder==null){
                if(keyColumns==null)
                    encoder=MultiFieldEncoder.create(currentRow.nColumns());
                else{
                    int numFields=0;
                    for(int keyColumn : keyColumns){
                        if(keyColumn>=0) numFields++;
                    }
                    encoder=MultiFieldEncoder.create(numFields);
                }
            }

            pack(encoder,currentRow);
            return encoder.build();
        }

        @Override
        public KeyHashDecoder getDecoder(){
            return new Decoder(keyColumns,keySortOrder,kryoPool,serializers);
        }
    }
}
