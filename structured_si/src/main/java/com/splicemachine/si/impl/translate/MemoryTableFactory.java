package com.splicemachine.si.impl.translate;

import com.splicemachine.si.api.Clock;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.light.IncrementingClock;
import com.splicemachine.si.data.light.LDataLib;
import com.splicemachine.si.data.light.LStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MemoryTableFactory implements HTableInterfaceFactory {
    final LDataLib memoryDataLib;
    final LStore memoryStore;
    final Transcoder transcoder;
    final Transcoder transcoder2;
    final Translator translator;

    final Set<String> memoryTableNames;
    final Set<String> loadedTables = new HashSet<String>();
    final HTableInterfaceFactory delegate;
    final SDataLib dataLib;
    final STableReader reader;

    public MemoryTableFactory(final SDataLib dataLib, STableReader reader, Set<String> memoryTableNames, HTableInterfaceFactory delegate) {
        this.memoryTableNames = memoryTableNames;
        this.delegate = delegate;
        this.dataLib = dataLib;
        this.reader = reader;

        memoryDataLib = new LDataLib();
        final Clock lClock = new IncrementingClock(1000);
        memoryStore = new LStore(lClock);
        transcoder = new Transcoder() {
            @Override
            public Object transcode(Object data) {
                return data;
            }

            @Override
            public Object transcodeKey(Object key) {
                return bytesToString((byte[]) key);
            }

            @Override
            public Object transcodeFamily(Object family) {
                return dataLib.decode(family, String.class);
            }

            @Override
            public Object transcodeQualifier(Object qualifier) {
                return dataLib.decode(qualifier, String.class);
            }
        };
        transcoder2 = new Transcoder() {
            @Override
            public Object transcode(Object data) {
                return data;
            }

            @Override
            public Object transcodeKey(Object key) {
                return stringToBytes((String) key);
            }

            @Override
            public Object transcodeFamily(Object family) {
                return dataLib.encode(family);
            }

            @Override
            public Object transcodeQualifier(Object qualifier) {
                return dataLib.encode(qualifier);
            }
        };
        translator = new Translator(dataLib, reader, memoryDataLib, memoryStore, memoryStore, transcoder, transcoder2);
    }

    public static String bytesToString(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for(int i=0; i<bytes.length; i++) {
            builder.append(String.format("%02X", bytes[i]));
        }
        return builder.toString();
    }

    public static byte[] stringToBytes(String s) {
        byte[] result = new byte[s.length()/2];
        for(int i=0; i<result.length; i++) {
            result[i] = Byte.parseByte(s.substring(i*2, (i+1)*2), 16);
        }
        return result;
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, byte[] tableNameBytes) {
        final HTableInterface table = delegate.createHTableInterface(config, tableNameBytes);
        if (isMemoryTable(tableNameBytes)) {
            synchronized (this) {
                try {
                    loadTable(tableNameBytes);
                } catch(IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return new MemoryHTable(table, translator, reader, tableName(tableNameBytes), memoryStore);
        } else {
            return table;
        }
    }

    private void loadTable(byte[] tableNameBytes) throws IOException {
        final String tableName = tableName(tableNameBytes);
        if (!loadedTables.contains(tableName)) {
            translator.translate(tableName);
            loadedTables.add(tableName);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        delegate.releaseHTableInterface(table);
    }

    private boolean isMemoryTable(byte[] tableNameBytes) {
        return memoryTableNames.contains(tableName(tableNameBytes));
    }

    private String tableName(byte[] tableNameBytes) {
        return Bytes.toString(tableNameBytes);
    }
}
