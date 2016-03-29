package com.splicemachine.hbase.client;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 3/28/16
 */
public class ScanAdjustingResultScanner implements ResultScanner{
    private final HTableInterface table;
    private final Scan baseScan;

    private ResultScanner currentScanner;
    private Scan currentScan;
    private ByteSlice currentRowKey = new ByteSlice();
    private ByteSlice latestCellQualifier = new ByteSlice();

    public ScanAdjustingResultScanner(HTableInterface table,Scan baseScan){
        this.table=table;
        this.baseScan=baseScan;
    }

    @Override
    public Result next() throws IOException{
        initializeIfNeeded(baseScan.getStartRow());
        Result r=nextInternal();
        if(r!=null && r.size()>0){
            setCurrentRow(r);
        }
        return r;
    }



    @Override
    public Result[] next(int nbRows) throws IOException{
        List<Result> results = new ArrayList<>(nbRows);
        for(int i=0;i<nbRows;i++){
            Result n = next();
            if(n!=null && n.size()>0){
                results.add(n);
            }else break;
        }
        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close(){
        if(currentScanner!=null)
            currentScanner.close();
    }

    @Override
    public Iterator<Result> iterator(){
        return new CallingIter();
    }


    /* ****************************************************************************************************************/
    /*private helper methods and classes*/
    private void setCurrentRow(Result r){
        currentRowKey.set(r.getRow());
        Cell[] cells=r.rawCells();
        Cell lastCell=cells[cells.length-1];
        latestCellQualifier.set(lastCell.getRowArray(),lastCell.getRowOffset(),lastCell.getRowLength());
    }

    private void initializeIfNeeded(byte[] startKey) throws IOException{
        if(currentScanner==null){
            currentScanner = reopenScanner(baseScan,startKey,baseScan.getCaching());
        }
    }

    private ResultScanner reopenScanner(Scan baseScan,
                                        byte[] newStartKey,
                                        int newCaching) throws IOException{
        Scan s = new Scan(baseScan);
        s.setStartRow(newStartKey);
        s.setCaching(newCaching);
        currentScan = s;
        return table.getScanner(s);
    }

    private Result nextInternal() throws IOException{
        boolean checkInOrder=false;
        int numTries =SpliceConstants.numRetries;
        IOException lastError = null;
        Result r;
        do{
            try{
                r=currentScanner.next();
                if(r==null||r.size()<0) break;
                else if(checkInOrder){
                    r = nextInOrder(r);
                }//otherwise we skip it
            }catch(OutOfOrderScannerNextException oosne){
                r=null;
                lastError=oosne;
                checkInOrder=true;
                /*
                 * An OutOfOrderScannerNextException is called when the server takes too
                 * long to process one request, so the next request is called before the old one is
                 * returned. Primarily it tends to happen at the start of a scan, when it takes too
                 * long to return all the rows that the caching would like to see (although it may occur
                 * in other places as well). We will restart the scan at the last known good row and skip
                 * just like other Retry-able exceptions, but we will also reduce the scan size in order to
                 * prevent a recurrance (as long as the current scan size allows for reduction at any rate).
                 */
                int newCaching=Math.max(1,currentScan.getCaching()/2);
                reopenScanner(currentScan,currentRowKey.getByteCopy(),newCaching);
                numTries--;
            }catch(LeaseExpiredException lee){
                r=null;
                lastError=lee;
                checkInOrder=true;
                /*
                 * A LeaseExpiration is when we took too long in between subsequent calls to next(). This
                 * tends to happen when something beyond the client's control causes a latency spike (i.e. serializing
                 * that data over to a JDBC client or performing intensive CPU processing). There's really nothing
                 * wrong with the scan setup itself, so we use the same data that we currently have.
                 */
                reopenScanner(currentScan,currentRowKey.getByteCopy(),currentScan.getCaching());
                numTries--;
            } catch(RemoteException re){
                r=null;
                checkInOrder=true;
                IOException ioe=re.unwrapRemoteException();
                lastError=ioe;
                numTries--;

                int caching;
                if(ioe instanceof OutOfOrderScannerNextException){
                    caching=Math.max(1,currentScan.getCaching()/2);
                }else
                    caching = currentScan.getCaching();
                reopenScanner(currentScan,currentRowKey.getByteCopy(),caching);
            }
        }while(r==null && numTries>0);
        if(numTries<0)
            throw lastError;
        else return r;
    }

    private Result nextInOrder(Result r){
        /*
         * Check that this result set is the next in sequence.
         *
         * First we compare row keys:
         *
         * 1. if currentRowKey < row => We are at a row past the last known good row, so we are good and can proceed.
         * 2. currentRowKey > row => We went backwards in the scan, so we need to skip this entry
         * 3. currentRowKey == row => rows match. Make sure that this batch occurs after the last known good cell
         *
         * Of course, if the scan is reversed, then we apply these comparisons in reverse order.
         */
        byte[] row=r.getRow();
        int compare=currentRowKey.compareTo(row,0,row.length);
        if(currentScan.isReversed()){
            if(compare>0) return r;
            else if(compare<0) return null;
            else{
                return filterColumns(r);
            }
        }else{
            if(compare<0) return r;
            else if(compare>0) return null;
            else{
                return filterColumns(r);
            }
        }
    }

    private Result filterColumns(Result r){
        Cell[] cells=r.rawCells();
        Cell lastInResult = cells[cells.length-1];
        Cell firstInResult = cells[0];
        int compare=latestCellQualifier.compareTo(firstInResult.getQualifierArray(),
                firstInResult.getQualifierOffset(),
                firstInResult.getQualifierLength());
        if(currentScan.isReversed()){
            if(compare>0) return r; //the first in this result happens after the last known good result, so we are okay
            else{
                /*
                 * The first element in this result happens at or before the last known good result,
                 * so filter out any cell which wasn't. If the last in the result is also before, then
                 * the entire range is fitered out and we skip this result. Otherwise, we filter down
                 */
                int c=latestCellQualifier.compareTo(lastInResult.getQualifierArray(),
                        lastInResult.getQualifierOffset(),
                        lastInResult.getQualifierLength());
                if(c<=0){
                    //the entire range is filtered out
                    return null;
                }else{
                    //use binary search to find the first element which compares after this qualifier
                    int a=0,b=cells.length-1;
                    while(a<=b){
                        int p = (a+b)>>>1;
                        Cell cell = cells[p];
                        c = latestCellQualifier.compareTo(cell.getQualifierArray(),
                                cell.getQualifierOffset(),cell.getQualifierLength());
                        if(c>0){
                            Cell[] copy = new Cell[cells.length-p];
                            System.arraycopy(cells,p,copy,0,copy.length);
                            return Result.create(copy);
                        }
                    }
                    throw new IllegalStateException("Programmer Error: did not expect to get here");
                }
            }
        }else{
            if(compare<0) return r; //the first in this result happens after the last known good result, so we are okay
            else{
                /*
                 * The first element in this result happens at or before the last known good result,
                 * so filter out any cell which wasn't. If the last in the result is also before, then
                 * the entire range is fitered out and we skip this result. Otherwise, we filter down
                 */
                int c=latestCellQualifier.compareTo(lastInResult.getQualifierArray(),
                        lastInResult.getQualifierOffset(),
                        lastInResult.getQualifierLength());
                if(c>=0){
                    //the entire range is filtered out
                    return null;
                }else{
                    //use binary search to find the first element which compares after this qualifier
                    int a=0,b=cells.length-1;
                    while(a<=b){
                        int p = (a+b)>>>1;
                        Cell cell = cells[p];
                        c = latestCellQualifier.compareTo(cell.getQualifierArray(),
                                cell.getQualifierOffset(),cell.getQualifierLength());
                        if(c<0){
                            Cell[] copy = new Cell[cells.length-p];
                            System.arraycopy(cells,p,copy,0,copy.length);
                            return Result.create(copy);
                        }
                    }
                    throw new IllegalStateException("Programmer Error: did not expect to get here");
                }
            }
        }
    }

    private class CallingIter implements Iterator<Result>{
        private Result r;
        @Override
        public boolean hasNext(){
            if(r==null)
                r = next();
            return r!=null && r.size()>0;
        }

        @Override
        public Result next(){
            if(!hasNext()) throw new NoSuchElementException();
            Result n = r;
            r = null;
            return n;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException("Remove is not supported by client iterators");
        }
    }
}
