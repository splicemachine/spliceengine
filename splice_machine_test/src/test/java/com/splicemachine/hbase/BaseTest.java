package com.splicemachine.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
//import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

import junit.framework.TestCase;

// TODO: Auto-generated Javadoc
/**
 * The Class HBaseBaseTest.
 */
public class BaseTest extends TestCase{

  /** The Constant LOG. */
   static final Log LOG = LogFactory.getLog(BaseTest.class);  
  /** The table a. */
  public byte[] TABLE_A = "TABLE_A".getBytes();
  
  /** The block size. */
  public static int BLOCK_SIZE = 64 * 1024;

  
  /* Families */
  /** The families. */
  public byte[][] FAMILIES = new byte[][]
        {"fam_a".getBytes()/*, "fam_b".getBytes(), "fam_c".getBytes()*/};
  
  
  /* Columns */
  /** The columns. */
  public  byte[][] COLUMNS = 
  {"col_a".getBytes(), "col_b".getBytes(),  "col_c".getBytes()};
  
    
  /** The versions. */
  public int VERSIONS = 10;
    
  /* All CF are cacheable */
  /** The table a. */
  public static HTableDescriptor tableA;
      
  
  /**
   * Constract family map.
   *
   * @param families the families
   * @param columns the columns
   * @return the map
   */
  protected Map<byte[], NavigableSet<byte[]>> constructFamilyMap(List<byte[]> families, List<byte[]> columns)
  {
    Map<byte[], NavigableSet<byte[]>> map = new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
    NavigableSet<byte[]> colSet = getColumnSet(columns);
    for(byte[] f: families){
      map.put(f, colSet);
    }
    return map;
  }
  
  /**
   * Gets the row.
   *
   * @param i the i
   * @return the row
   */
  protected byte[] getRow (int i){
    return ("row"+formatValue(i, 12)).getBytes();
  }
  
  protected byte[] getRow (byte[] row, int offset)
  {
	  String number = new String(row, 3, 12);
	  int n = Integer.parseInt(number);
	  return getRow(n + offset);
  }
  
  private static final String formatValue(int value, int pos)
  {
	  StringBuffer sb = new StringBuffer(pos);
	  int n = value;
	  while(n > 0){
		  sb.insert(0,  n % 10);
		  n /= 10;
	  }
	  int nn = pos - sb.length();
	  int i = 0;
	  while(i++ <  nn){
		  sb.insert(0, 0);
	  }
	  return sb.toString();
  }
  
  /**
   * Gets the value.
   *
   * @param i the i
   * @return the value
   */
  protected byte[] getValue (int i){
    return ("value"+i).getBytes();
  }
  
  
  /**
   * Generate row data.
   *
   * @param i the i
   * @return the list
   */
  protected List<KeyValue> generateRowData(int i){
    byte[] row = getRow(i);
    byte[] value = getValue(i);
    long startTime = System.currentTimeMillis();
    List<KeyValue> list = new ArrayList<KeyValue>();
    int count = 0;
    for(byte[] f: FAMILIES){
      for(byte[] c: COLUMNS){
        count = 0;
        for(; count < VERSIONS; count++){
          KeyValue kv = new KeyValue(row, f, c, startTime + (count),  value); 
          list.add(kv);
        }
      }
    }
    
    Collections.sort(list, KeyValue.COMPARATOR);
    
    return list;
  }
  
  
  /**
   * Generate data.
   *
   * @param n the n
   * @return the list
   */
  protected  List<List<KeyValue>> generateData(int n)
  {
    List<List<KeyValue>> data = new ArrayList<List<KeyValue>>();
    for(int i=0; i < n; i++){
      data.add(generateRowData(i));
    }
    return data;
  }
  
  /**
   * Creates the tables.
   *
   * @param versions the versions
   */
  protected void createTables(int versions) {
    
    HColumnDescriptor famA = new HColumnDescriptor(FAMILIES[0]);    
    famA.setMaxVersions(versions);
    famA.setBlockCacheEnabled(false);
    famA.setBlocksize(BLOCK_SIZE);
    //famA.setBloomFilterType(org.apache.hadoop.hbase.regionserver.StoreFile.BloomType.ROW);

    
   /* HColumnDescriptor famB = new HColumnDescriptor(FAMILIES[1]);
    famB.setCacheDataOnWrite(true);
    famB.setMaxVersions(versions); 
    famA.setBlockCacheEnabled(false);
    famB.setBlocksize(BLOCK_SIZE);
    famB.setBloomFilterType(BloomType.ROW);
    
    HColumnDescriptor famC = new HColumnDescriptor(FAMILIES[2]);
    famC.setMaxVersions(versions);  
    famA.setBlockCacheEnabled(false);
    famC.setBlocksize(BLOCK_SIZE);
    famC.setBloomFilterType(BloomType.ROW);
*/
    tableA = new HTableDescriptor(TABLE_A);
    tableA.addFamily(famA);
   //tableA.addFamily(famB);
   // tableA.addFamily(famC);
    // set 100G to avoid region split
    tableA.setMaxFileSize(100000000000L);
           
    
  }
  
  
  /**
   * Creates the get.
   *
   * @param row the row
   * @param familyMap the family map
   * @param tr the tr
   * @param f the f
   * @return the gets the
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected Get createGet(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, Filter f ) throws IOException
  {
    Get get = new Get(row);
    if(tr != null){
      get.setTimeRange(tr.getMin(), tr.getMax());
    }
    if(f != null) get.setFilter(f);
    
    if(familyMap != null){
      for(byte[] fam: familyMap.keySet())
      {
        NavigableSet<byte[]> cols = familyMap.get(fam);
        if( cols == null || cols.size() == 0){
          get.addFamily(fam);
        } else{
          for(byte[] col: cols)
          {
            get.addColumn(fam, col);
          }
        }
      }
    }
    return get;
  }
  
  
  /**
   * Creates the get.
   *
   * @param i the i
   * @return the gets the
   */
  protected Get createGet(int i)
  {
    Get get = new Get(getRow(i));
    for(int k = 0; k < FAMILIES.length; k++)
    {
       get.addFamily(FAMILIES[k]);
    }
    return get;
  }
  
  /**
   * Creates the put.
   *
   * @param values the values
   * @return the put
   */
  protected Put createPut(List<KeyValue> values)
  {
    Put put = new Put(values.get(0).getRow());
    for(KeyValue kv: values)
    {
      put.add(kv.getFamily(), kv.getQualifier(), kv.getTimestamp(), kv.getValue());
    }
    return put;
  }
  
  
  /**
   * Creates the increment.
   *
   * @param row the row
   * @param familyMap the family map
   * @param tr the tr
   * @param value the value
   * @return the increment
   * @throws IOException Signals that an I/O exception has occurred.
   */
  protected Increment createIncrement(byte[] row, Map<byte[], NavigableSet<byte[]>> familyMap, TimeRange tr, long value) 
  throws IOException
  {
    Increment incr = new Increment(row);
    if(tr != null){
      incr.setTimeRange(tr.getMin(), tr.getMax());
    }

    
    if(familyMap != null){
      for(byte[] fam: familyMap.keySet())
      {
        NavigableSet<byte[]> cols = familyMap.get(fam);

          for(byte[] col: cols)
          {
            incr.addColumn(fam, col, value);
          }
        
      }
    }
    return incr;    
  }
  
  /**
   * Creates the append.
   *
   * @param row the row
   * @param families the families
   * @param columns the columns
   * @param value the value
   * @return the append
   */
  protected Append createAppend(byte[] row, List<byte[]> families, List<byte[]> columns, byte[] value){
    
    Append op = new Append(row);
    
    for(byte[] f: families){
      for(byte[] c: columns){
        op.add(f, c, value);
      }
    }
    return op;
  }
  /**
   * Creates the delete.
   *
   * @param values the values
   * @return the delete
   */
  protected Delete createDelete(List<KeyValue> values)
  {
    Delete del = new Delete(values.get(0).getRow());
    for(KeyValue kv: values)
    {
      del.deleteColumns(kv.getFamily(), kv.getQualifier());
    }
    return del;
  }
  
  /**
   * Creates the delete.
   *
   * @param row the row
   * @return the delete
   */
  protected Delete createDelete(byte[] row)
  {
    Delete del = new Delete(row);
    return del;
  }
  
  /**
   * Creates the delete.
   *
   * @param row the row
   * @param families the families
   * @return the delete
   */
  protected Delete createDelete(byte[] row, List<byte[]> families)
  {
    Delete del = new Delete(row);
    for(byte[] f: families)
    {
      del.deleteFamily(f);
    }
    return del;
  }

  /**
   * Equals.
   *
   * @param list1 the list1
   * @param list2 the list2
   * @return true, if successful
   */
  protected boolean equals(List<KeyValue> list1, List<KeyValue> list2)
  {
    if(list1.size() != list2.size()) return false;
    Collections.sort(list1, KeyValue.COMPARATOR);
    Collections.sort(list2, KeyValue.COMPARATOR); 
    for(int i=0; i < list1.size(); i++){
      if(list1.get(i).equals(list2.get(i)) == false) return false;
    }
    return true;
  }
  protected boolean equalsNoTS(List<KeyValue> list1, List<KeyValue> list2)
  {
    if(list1.size() != list2.size()) return false;
    Collections.sort(list1, KeyValue.COMPARATOR);
    Collections.sort(list2, KeyValue.COMPARATOR); 
    for(int i=0; i < list1.size(); i++){
      KeyValue first = list1.get(i);
      KeyValue second = list2.get(i);
      //LOG.info(i +" ["+ new String(first.getRow())+"]"+ "["+new String(second.getRow())+"]");
      int r1 = Bytes.compareTo(first.getRow(), second.getRow());
      if(r1 != 0) return false;
      // LOG.info(i+ " ["+ new String(first.getFamily())+"]"+ "["+new String(second.getFamily())+"]");
      int r2 = Bytes.compareTo(first.getFamily(), second.getFamily());
      if(r2 != 0) return false;
      // LOG.info(i+" ["+ new String(first.getQualifier())+"]"+ "["+new String(second.getQualifier())+"]");

      int r3 = Bytes.compareTo(first.getQualifier(), second.getQualifier());
      if(r3 != 0) return false;
      // LOG.info(i +" ["+ new String(first.getValue())+"]"+ "["+new String(second.getValue())+"]");

      int r4 = Bytes.compareTo(first.getValue(), second.getValue());
      if(r4 != 0) {
        //LOG.info(i +" ["+ new String(first.getValue())+"]"+ "["+new String(second.getValue())+"] ***************");
        return false;
      } 
    }
    return true;
  } 
  /**
   * Sub list.
   *
   * @param list the list
   * @param family the family
   * @return the list
   */
  protected List<KeyValue> subList(List<KeyValue> list, byte[] family){
    List<KeyValue> result = new ArrayList<KeyValue>();
    for(KeyValue kv : list){
      if(Bytes.equals(family, kv.getFamily())){
        result.add(kv);
      }
    }
    return result;
  }
  
  /**
   * Sub list.
   *
   * @param list the list
   * @param family the family
   * @param cols the cols
   * @return the list
   */
  protected List<KeyValue> subList(List<KeyValue> list, byte[] family, List<byte[]> cols){
    List<KeyValue> result = new ArrayList<KeyValue>();
    for(KeyValue kv : list){
      if(Bytes.equals(family, kv.getFamily())){
        byte[] col = kv.getQualifier();
        for(byte[] c: cols){          
          if(Bytes.equals(col, c)){
            result.add(kv); break;
          }
        }
        
      }
    }
    return result;
  }
  
  /**
   * Sub list.
   *
   * @param list the list
   * @param families the families
   * @param cols the cols
   * @return the list
   */
  protected List<KeyValue> subList(List<KeyValue> list, List<byte[]> families, List<byte[]> cols){
    List<KeyValue> result = new ArrayList<KeyValue>();
    for(KeyValue kv : list){
      for(byte[] family: families){       
        if(Bytes.equals(family, kv.getFamily())){
          byte[] col = kv.getQualifier();
          for(byte[] c: cols){          
            if(Bytes.equals(col, c)){
              result.add(kv); break;
            }
          }       
        }
      }
    }
    return result;
  }
  
  
  /**
   * Sub list.
   *
   * @param list the list
   * @param families the families
   * @param cols the cols
   * @param max the max
   * @return the list
   */
  protected List<KeyValue> subList(List<KeyValue> list, List<byte[]> families, List<byte[]> cols, int max){
    List<KeyValue> result = new ArrayList<KeyValue>();
    for(KeyValue kv : list){
      for(byte[] family: families){       
        if(Bytes.equals(family, kv.getFamily())){
          byte[] col = kv.getQualifier();
          for(byte[] c: cols){          
            if(Bytes.equals(col, c)){
              result.add(kv); break;
            }
          }       
        }
      }
    }
    
    int current = 0;
    byte[] f = result.get(0).getFamily();
    byte[] c = result.get(0).getQualifier();
    
    List<KeyValue> ret = new ArrayList<KeyValue>();
    
    for(KeyValue kv : result ){
      byte[] fam = kv.getFamily();
      byte[] col = kv.getQualifier();
      if(Bytes.equals(f, fam) ){
        if( Bytes.equals(c, col)){
          if( current < max){
            ret.add(kv);
          }
          current++;
        } else{
          c = col; current = 1;
          ret.add(kv);
        }
      } else {
        f = fam; c = col; current = 1;
        ret.add(kv);
      }
    }
    return ret;
  }
  /**
   * Gets the column set.
   *
   * @param cols the cols
   * @return the column set
   */
  protected NavigableSet<byte[]> getColumnSet(List<byte[]> cols)
  {
    if(cols == null) return null;
    TreeSet<byte[]> set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for(byte[] c: cols){
      set.add(c);
    }
    return set;
  }
  
  /**
   * Dump.
   *
   * @param list the list
   */
  protected void dump(List<KeyValue> list)
  {
    for( KeyValue kv : list){
      dump(kv);
    }
  }
  
  /**
   * Dump.
   *
   * @param kv the kv
   */
  protected void dump(KeyValue kv)
  {
    LOG.info("row="+new String(kv.getRow())+" family="+ new String(kv.getFamily())+
        " column="+new String(kv.getQualifier()) + " ts="+ kv.getTimestamp());
  }
  
  /**
   * Patch row.
   *
   * @param kv the kv
   * @param patch the patch
   */
  protected void patchRow(KeyValue kv, byte[] patch)
  {
    int off = kv.getRowOffset();
    System.arraycopy(patch, 0, kv.getBuffer(), off, patch.length);
  }
  
}
