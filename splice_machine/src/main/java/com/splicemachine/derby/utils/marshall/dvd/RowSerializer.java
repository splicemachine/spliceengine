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

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Serializes an ExecRow.
 *
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class RowSerializer implements Closeable{
    private boolean initialized;


    public interface Factory{
        RowSerializer forRow(ExecRow row,int[] columnMap,boolean[] columnSortOrder);

        EntryRowSerializer entryRow(ExecRow template,int[] keyColumns,boolean[] keySortOrder);

        DescriptorSerializer[] getDescriptors(ExecRow template);
    }

    protected final DescriptorSerializer[] serializers;
    protected final int[] columnMap;
    protected final boolean[] columnSortOrder;

    RowSerializer(DescriptorSerializer[] serializers,int[] columnMap,boolean[] columnSortOrder){
        this.serializers=serializers;
        this.columnMap=columnMap;
        this.columnSortOrder=columnSortOrder;
    }

    public int numCols(){
        return columnMap.length;
    }

    public void encode(MultiFieldEncoder fieldEncoder,ExecRow row) throws StandardException{
        initialize();
        if(columnMap==null){
            encodeAll(fieldEncoder,row);
        }else if(columnSortOrder==null){
            encodeAscending(fieldEncoder,row);
        }else{
            encodeFields(fieldEncoder,row);
        }
    }

    public void decode(MultiFieldDecoder decoder,ExecRow destination) throws StandardException{
        initialize();
        if(columnMap==null){
            decodeAll(decoder,destination);
        }else if(columnSortOrder==null)
            decodeAscending(decoder,destination);
        else{
            decodeFields(decoder,destination);
        }
    }

    public boolean isNull(DataValueDescriptor dvd,int position){
        return dvd==null || dvd.isNull() || (columnMap!=null && columnMap[position]==-1);
    }

    public void close() throws IOException{
        //serializers usually won't throw an IOException, but this makes it easy to integrate if it's closeable
        for(DescriptorSerializer serializer : serializers){
            serializer.close();
        }
    }

    public void initialize(){
        if(initialized) return;
        Calendar calendar=null;
        for(DescriptorSerializer serializer : serializers){
            if(serializer instanceof TimeValuedSerializer){
                if(calendar==null)
                    calendar=new GregorianCalendar();
                ((TimeValuedSerializer)serializer).setCalendar(calendar);
            }
        }
        initialized=true;
    }


    protected void decodeFields(MultiFieldDecoder decoder,ExecRow destination) throws StandardException{
        DataValueDescriptor[] dvds=destination.getRowArray();
        for(int col : columnMap){
            if(col==-1) continue;
            DescriptorSerializer serializer=serializers[col];
            serializer.decode(decoder,dvds[col],columnSortOrder[col]);
        }
    }

    protected void encodeFields(MultiFieldEncoder fieldEncoder,ExecRow row) throws StandardException{
        DataValueDescriptor[] dvds=row.getRowArray();
        for(int i=0;i<columnMap.length;i++){
            int column=columnMap[i];
            if(column==-1) continue;
            serializers[column].encode(fieldEncoder,dvds[column],!columnSortOrder[i]);
        }
    }

    protected void decodeAscending(MultiFieldDecoder decoder,ExecRow destination) throws StandardException{
        DataValueDescriptor[] dvs=destination.getRowArray();
        for(int col : columnMap){
            if(col==-1) continue;
            DescriptorSerializer serializer=serializers[col];
            serializer.decode(decoder,dvs[col],false);
        }
    }

    protected void decodeAll(MultiFieldDecoder decoder,ExecRow destination) throws StandardException{
        DataValueDescriptor[] dvs=destination.getRowArray();
        for(int i=0;i<serializers.length;i++){
            DescriptorSerializer serializer=serializers[i];
            serializer.decode(decoder,dvs[i],false);
        }
    }

    protected void encodeAscending(MultiFieldEncoder fieldEncoder,ExecRow row) throws StandardException{
        if(columnMap==null){
            encode(fieldEncoder,row);
            return;
        }
        DataValueDescriptor[] dvds=row.getRowArray();
        for(int column : columnMap){
            if(column==-1) continue;
            serializers[column].encode(fieldEncoder,dvds[column],false);
        }
    }

    protected void encodeAll(MultiFieldEncoder fieldEncoder,ExecRow row) throws StandardException{
        DataValueDescriptor[] dvds=row.getRowArray();
        for(int i=0;i<dvds.length;i++){
            serializers[i].encode(fieldEncoder,dvds[i],false);
        }
    }
}
