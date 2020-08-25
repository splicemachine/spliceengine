/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.access.client;

import com.google.common.collect.Sets;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.SkeletonHBaseClientPartition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Throwables;

import java.io.IOException;
import java.util.*;

/**
 * 
 * 
 */
public abstract class SkeletonClientSideRegionScanner implements RegionScanner{
    private boolean isClosed = false;
    private static final Logger LOG = Logger.getLogger(SkeletonClientSideRegionScanner.class);
    private HRegion region;
    private RegionScanner scanner;
    private Configuration conf;
    private FileSystem fs;
    private Path rootDir;
    private TableDescriptor htd;
    private HRegionInfo hri;
    private Scan scan;
    private String hostAndPort;
    private Cell topCell;
    private List<KeyValueScanner>    memScannerList = new ArrayList<>(1);
    private boolean flushed;
    private long numberOfRows = 0;
    private FileSystem customFilesystem;
    private List<Cell> rowBuffer = null;
    private boolean noMoreRecords = false;


    public SkeletonClientSideRegionScanner(Configuration conf,
                                           FileSystem fs,
                                           Path rootDir,
                                           TableDescriptor htd,
                                           HRegionInfo hri,
                                           Scan scan, String hostAndPort) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "init for regionInfo=%s, scan=%s", hri,scan);
        scan.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
        this.conf = conf;
        this.fs = fs;
        this.rootDir = rootDir;
        this.htd = htd;
        this.hri = new SpliceHRegionInfo(hri);
        this.scan = scan;
        this.hostAndPort = hostAndPort;
    }

    @Override
    public void close() throws IOException {
        if (isClosed)
            return;
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        if (scanner != null)
            scanner.close();
        memScannerList.get(0).close();
        region.close();
        if (customFilesystem != null)
            customFilesystem.close();
        isClosed = true;
    }


    public HRegionInfo getRegionInfo() {
        return (HRegionInfo) scanner.getRegionInfo();
    }

    @Override
    public boolean reseek(byte[] row) throws IOException {
        return scanner.reseek(row);
    }

    @Override
    public long getMvccReadPoint() {
        return scanner.getMvccReadPoint();
    }

    public boolean next(List<Cell> result,int limit) throws IOException{
        return nextRaw(result,limit);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException{
        return nextRaw(results);
    }

    public boolean nextRaw(List<Cell> result,int limit) throws IOException{
        return nextRaw(result);
    }

    @Override
    public long getMaxResultSize(){
        return scanner.getMaxResultSize();
    }

    @Override
    public boolean isFilterDone() throws IOException{
        return scanner.isFilterDone();
    }

    private void fetchNextAndGiveBuffer(List<Cell> result) throws IOException {
        assert result.isEmpty();
        List<Cell> nextResult = new ArrayList<>();
        boolean response = nextMerged(nextResult);
        if (matchingFamily(nextResult, ClientRegionConstants.FLUSH)) {
            // A flush should be returned before a potential partial result in the buffer
            result.addAll(nextResult);
        } else {
            result.addAll(rowBuffer);
            rowBuffer = nextResult;
            if (!response) {
                noMoreRecords = true;
            }
        }
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        boolean firstCall = rowBuffer == null;
        boolean response;
        if (firstCall) {
            rowBuffer = new ArrayList<>();
            response = nextMerged(rowBuffer);
            if (!response) {
                result.addAll(rowBuffer);
                rowBuffer = null;
            } else {
                fetchNextAndGiveBuffer(result);
            }
        } else if (noMoreRecords) {
            result.addAll(rowBuffer);
            rowBuffer = null;
            response = false;
        } else {
            fetchNextAndGiveBuffer(result);
            response = true;
        }
        boolean returnValue = updateTopCell(response, result);
        if (returnValue)
            numberOfRows++;
        return returnValue;
    }


    /**
     * refresh underlying RegionScanner we call this when new store file gets
     * created by MemStore flushes or current scanner fails due to compaction
     */
    void updateScanner() throws IOException {
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG,
                        "updateScanner with hregionInfo=%s, tableName=%s, rootDir=%s, scan=%s",
                        hri, htd.getTableName().getNameAsString(), rootDir, scan);
            }
            if (flushed) {
                if (LOG.isDebugEnabled())
                    SpliceLogUtils.debug(LOG, "Flush occurred");
                byte[] restartRow = null;
                if (rowBuffer != null && !rowBuffer.isEmpty()) {
                    restartRow = CellUtil.cloneRow(rowBuffer.get(0));
                    rowBuffer = null;
                } else if (this.topCell != null) {
                    restartRow = Bytes.add(CellUtil.cloneRow(topCell), new byte[]{0});
                }
                if (restartRow != null) {
                    if (LOG.isDebugEnabled())
                        SpliceLogUtils.debug(LOG, "setting start row to %s", Hex.encodeHexString(restartRow));
                    //noinspection deprecation
                    scan.setStartRow(restartRow);
                }
            }
            memScannerList.add(getMemStoreScanner());
            this.region = openHRegion();
            RegionScanner regionScanner = new CountingRegionScanner(HRegionUtil.getScanner(region, scan, memScannerList), region, scan);
            if (flushed) {
                if (scanner != null)
                    scanner.close();
            }
            scanner = regionScanner;
    }

    public HRegion getRegion(){
        return region;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    private boolean updateTopCell(boolean response, List<Cell> results) throws IOException {
        if (matchingFamily(results, ClientRegionConstants.FLUSH)) {
            if (LOG.isDebugEnabled())
                SpliceLogUtils.debug(LOG,"received flush message " + results.get(0));
            flushed = true;
            updateScanner();
            flushed = false;
            results.clear();
            return nextRaw(results);
        } else
        if (response)
            topCell = results.get(results.size() - 1);
        return response;
    }

    private boolean matchingFamily(List<Cell> result, byte[] family) {
        return !result.isEmpty() && CellUtil.matchingFamily(result.get(0), family);
    }

    private boolean nextMerged(List<Cell> result) throws IOException {
        boolean res = scanner.nextRaw(result);
        // Drain HoldTimestamps
        if (matchingFamily(result,ClientRegionConstants.HOLD)) {
            // Second Hold, null out scanner
            if (result.get(0).getTimestamp()== HConstants.LATEST_TIMESTAMP) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Second hold, close scanner");
                }
                result.clear();
                return false;
            }
            else { // First Hold, traverse to real records.
                if (LOG.isDebugEnabled()) {
                    LOG.debug("First hold, skip to real records");
                }
                result.clear();
                return nextMerged(result);
            }
        }
        return res;
    }

    @SuppressWarnings("unchecked")
    private Set<String> getCompactedFilesPathsFromHBaseRegionServer() {
        try {
            String regionName = hri.getRegionNameAsString();
            try (Partition partition = SIDriver.driver().getTableFactory().getTable(htd.getTableName())) {
                Map<byte[], List<String>> results = ((SkeletonHBaseClientPartition) partition).coprocessorExec(
                        SpliceMessage.SpliceDerbyCoprocessorService.class,
                        hri.getStartKey(),
                        hri.getStartKey(),
                        instance -> {
                            ServerRpcController controller = new ServerRpcController();
                            SpliceMessage.GetCompactedHFilesRequest message = SpliceMessage.GetCompactedHFilesRequest
                                    .newBuilder()
                                    .setRegionEncodedName(regionName)
                                    .build();

                            CoprocessorRpcUtils.BlockingRpcCallback<SpliceMessage.GetCompactedHFilesResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
                            instance.getCompactedHFiles(controller, message, rpcCallback);
                            if (controller.failed()) {
                                Throwable t = Throwables.getRootCause(controller.getFailedOn());
                                if (t instanceof IOException) throw (IOException) t;
                                else throw new IOException(t);
                            }
                            SpliceMessage.GetCompactedHFilesResponse response = rpcCallback.get();
                            return response.getFilePathList();
                        });
                //assert results.size() == 1: results;
                return Sets.newHashSet(results.get(hri.getRegionName()));
            }
        } catch (Throwable e) {
            SpliceLogUtils.error(LOG, "Unable to set Compacted Files from HBase region server", e);
            throw new RuntimeException(e);
        }
    }

    private HRegion openHRegion() throws IOException {
        Path tableDir = FSUtils.getTableDir(rootDir, hri.getTable());
        Set<String> compactedFilesPaths = getCompactedFilesPathsFromHBaseRegionServer();
        return new SpliceHRegion(
                tableDir, null, fs, conf, hri, htd, null, compactedFilesPaths);
    }

    private KeyValueScanner getMemStoreScanner() throws IOException {
        Scan memScan = new Scan(scan);
        memScan.setFilter(null);   // Remove SamplingFilter if the scan has it
        memScan.setAsyncPrefetch(false); // async would keep buffering rows indefinitely
        memScan.setReadType(Scan.ReadType.PREAD);
        memScan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_ONLY,SIConstants.TRUE_BYTES);
        memScan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_BEGIN_KEY, hri.getStartKey());
        memScan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_END_KEY, hri.getEndKey());
        memScan.setAttribute(ClientRegionConstants.SPLICE_SCAN_MEMSTORE_PARTITION_SERVER,Bytes.toBytes(hostAndPort));
        memScan.setAttribute(SIConstants.SI_NEEDED,null);
        ResultScanner scanner=newScanner(memScan);
        // We want to request the first row from the Memstore scanner to make sure the region is
        // open and possible pending edits have been replayed. The MemstoreKeyValueScanner doest that for us.
        // Make sure the reply is ClientRegionConstants.MEMSTORE_BEGIN
        MemstoreKeyValueScanner memScanner = new MemstoreKeyValueScanner(scanner);
        Cell current = memScanner.current();
        assert current != null;
        assert matchingFamily(Arrays.asList(current), ClientRegionConstants.HOLD);
        assert current.getTimestamp() == 0;
        return memScanner;
    }

    protected abstract ResultScanner newScanner(Scan memScan) throws IOException;

    @Override
    public String toString() {
        return String.format("SkeletonClienSideregionScanner[scan=%s,region=%s,numberOfRows=%d",scan,region.getRegionInfo(),numberOfRows);
    }

    public Cell getTopCell() {
        return topCell;
    }

}
