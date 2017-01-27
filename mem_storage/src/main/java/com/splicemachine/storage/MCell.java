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

package com.splicemachine.storage;

import com.splicemachine.access.util.ByteComparisons;
import org.spark_project.guava.primitives.Longs;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

/**
 * A cell representing an "In-Memory cell", which is really just a collection of byte arrays.
 *
 * @author Scott Fines
 *         Date: 12/15/15
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class MCell implements DataCell{
    private byte[] key;
    private byte[] value;
    private byte[] family;
    private byte[] qualifier;
    private CellType cellType;
    private long version;
    private long sequenceCounter;

    public MCell(){
    }

    public MCell(byte[] key,byte[] family,byte[] qualifier,long version,byte[] value,CellType cellType){
        set(key,family,qualifier,version,value,cellType);
    }

    public MCell(byte[] key,int keyOff,int keyLen,
                 byte[] family, int familyOff,int famLen,
                 byte[] qualifier,int qualOff,int qualLen,
                 long version,
                 byte[] value,int valueOff,int valueLen,CellType cellType){
        byte[] k=new byte[keyLen]; System.arraycopy(key,keyOff,k,0,keyLen);
        byte[] v=new byte[valueLen]; System.arraycopy(value,valueOff,v,0,valueLen);
        byte[] f = new byte[famLen];System.arraycopy(family,familyOff,f,0,famLen);
        byte[] q = new byte[qualLen];System.arraycopy(qualifier,qualOff,q,0,qualLen);
        set(k,f,q,version,v,cellType);
    }

    public MCell(byte[] key,int keyOff,int keyLen,byte[] family,byte[] qualifier,long version,byte[] value,int valueOff,int valueLen,CellType cellType){
        byte[] k=new byte[keyLen];
        System.arraycopy(key,keyOff,k,0,keyLen);
        byte[] v=new byte[valueLen];
        System.arraycopy(value,valueOff,v,0,valueLen);
        set(k,family,qualifier,version,v,cellType);
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public void set(byte[] key,byte[] family,byte[] qualifier,long version,byte[] value,CellType cellType){
        if(key==null){
            if(value!=null)
                throw new IllegalArgumentException("Cannot have a null key and a non-null value!");
        }
        assert cellType!=null:"Programmer error: cannot NOT specify a cell type";

        this.key=key;
        this.family=family;
        this.qualifier=qualifier;
        this.value=value;
        this.cellType=cellType;
        this.version=version;
    }

    @Override
    public boolean matchesFamily(byte[] family){
        return Bytes.equals(family,this.family);
    }

    @Override
    public byte[] family(){
        return family;
    }

    @Override
    public long familyLength(){
        return family.length;
    }

    @Override
    public byte[] qualifier(){
        return qualifier;
    }

    @Override
    public byte[] valueArray(){
        return value;
    }

    @Override
    public int valueOffset(){
        return 0;
    }

    @Override
    public int valueLength(){
        return value==null?0:value.length;
    }

    @Override
    public byte[] keyArray(){
        return key;
    }

    @Override
    public int keyOffset(){
        return 0;
    }

    @Override
    public int keyLength(){
        return key==null?0:key.length;
    }

    @Override
    public CellType dataType(){
        return cellType;
    }

    @Override
    public DataCell getClone(){
        return new MCell(key,family,qualifier,version,value,cellType);
    }

    @Override
    public boolean matchesQualifier(byte[] family,byte[] dataQualifierBytes){
        if(this.family!=family && !Bytes.equals(this.family,family)) return false;
        return this.qualifier==dataQualifierBytes || Bytes.equals(this.qualifier,dataQualifierBytes);
    }

    @Override
    public long version(){
        return version;
    }

    @Override
    public long valueAsLong(){
        return Bytes.bytesToLong(value,0);
    }

    @Override
    public DataCell copyValue(byte[] newValue,CellType newCellType){
        return new MCell(key,family,qualifier,version,newValue,newCellType);
    }

    @Override
    public int encodedLength(){
        return key.length+family.length+qualifier.length+value.length;
    }

    @Override
    public byte[] value(){
        return value;
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;
        if(!(o instanceof DataCell)) return false;

        return compareTo((DataCell)o)==0;
    }

    @Override
    public int hashCode(){
        int r = 17;
        r+=31*r+Arrays.hashCode(key);
        r+=31*r+Arrays.hashCode(family);
        r+=31*r+Arrays.hashCode(qualifier);
        r+=31*r+Longs.hashCode(version);
        r+=31*r+Arrays.hashCode(value);
        return r;
    }

    @Override
    public int compareTo(DataCell o){
        if(o==this) return 0;

        ByteComparator bc=ByteComparisons.comparator();
        int compare=bc.compare(key,0,key.length,o.keyArray(),o.keyOffset(),o.keyLength());
        if(compare!=0) return compare;
        compare=bc.compare(family,o.family());
        if(compare!=0) return compare;
        compare=bc.compare(qualifier,o.qualifier());
        if(compare!=0) return compare;
        return version<o.version()?1:version==o.version()?0:-1;
    }

    @Override
    public byte[] key(){
        return key;
    }

    @Override
    public byte[] qualifierArray(){
        return qualifier;
    }

    @Override
    public int qualifierOffset(){
        return 0;
    }

    public void sequence(long sequence){
       this.sequenceCounter = sequence;
    }

    public long getSequence(){
        return sequenceCounter;
    }
}
