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
 *
 */

package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CipherSuite;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.authorize.AuthorizationException;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX;

public class ProxiedDFSClient extends DFSClient {
    private final Connection connection;
    private static volatile boolean proxyDecryptEEK = false;

    public ProxiedDFSClient(URI nameNodeUri, Configuration conf, FileSystem.Statistics stats, Connection connection) throws IOException {
        super(nameNodeUri, conf, stats);
        this.connection = connection;
    }

    @Override
    public LocatedBlocks getLocatedBlocks(String src, long start, long length) throws IOException {
        try {
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_HDFS_OPERATION(?, ?)")) {
                statement.setString(1, src);
                statement.setString(2, "blocks");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for getFileStatus");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    HdfsProtos.LocatedBlocksProto lbp = HdfsProtos.LocatedBlocksProto.parseFrom(bytes);

// TODO                    return PBHelper.convert(lbp);
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public DFSInputStream open(String src, int buffersize, boolean verifyChecksum) throws IOException {
        checkOpen();
        //    Get block info from namenode

        LocatedBlocks locatedBlocks = getLocatedBlocks(src, 0);
        return new CustomDFSInputStream(this, src, verifyChecksum, locatedBlocks);
    }

    public HdfsDataInputStream createWrappedInputStream(DFSInputStream dfsis)
            throws IOException {
        final FileEncryptionInfo feInfo = dfsis.getFileEncryptionInfo();
        if (feInfo != null) {
            // File is encrypted, wrap the stream in a crypto stream.
            // Currently only one version, so no special logic based on the version #
            getCryptoProtocolVersion(feInfo);
            final CryptoCodec codec = getCryptoCodec(getConfiguration(), feInfo);
            final KeyProvider.KeyVersion decrypted = decryptEncryptedDataEncryptionKey(dfsis, feInfo);
            final CryptoInputStream cryptoIn =
                    new CryptoInputStream(dfsis, codec, decrypted.getMaterial(),
                            feInfo.getIV());
            return new HdfsDataInputStream(cryptoIn);
        } else {
            // No FileEncryptionInfo so no encryption.
            return new HdfsDataInputStream(dfsis);
        }
    }

    /**
     * Copied private methods from DFSClient
     */

    /**
     * Obtain the crypto protocol version from the provided FileEncryptionInfo,
     * checking to see if this version is supported by.
     *
     * @param feInfo FileEncryptionInfo
     * @return CryptoProtocolVersion from the feInfo
     * @throws IOException if the protocol version is unsupported.
     */
    private static CryptoProtocolVersion getCryptoProtocolVersion
    (FileEncryptionInfo feInfo) throws IOException {
        final CryptoProtocolVersion version = feInfo.getCryptoProtocolVersion();
        if (!CryptoProtocolVersion.supports(version)) {
            throw new IOException("Client does not support specified " +
                    "CryptoProtocolVersion " + version.getDescription() + " version " +
                    "number" + version.getVersion());
        }
        return version;
    }
    /**
     * O
     * btain a CryptoCodec based on the CipherSuite set in a FileEncryptionInfo
     * and the available CryptoCodecs configured in the Configuration.
     *
     * @param conf   Configuration
     * @param feInfo FileEncryptionInfo
     * @return CryptoCodec
     * @throws IOException if no suitable CryptoCodec for the CipherSuite is
     *                     available.
     */
    private static CryptoCodec getCryptoCodec(Configuration conf,
                                              FileEncryptionInfo feInfo) throws IOException {
        final CipherSuite suite = feInfo.getCipherSuite();
        if (suite.equals(CipherSuite.UNKNOWN)) {
            throw new IOException("NameNode specified unknown CipherSuite with ID "
                    + suite.getUnknownValue() + ", cannot instantiate CryptoCodec.");
        }
        final CryptoCodec codec = CryptoCodec.getInstance(conf, suite);
        if (codec == null) {
            throw new UnknownCipherSuiteException(
                    "No configuration found for the cipher suite "
                            + suite.getConfigSuffix() + " prefixed with "
                            + HADOOP_SECURITY_CRYPTO_CODEC_CLASSES_KEY_PREFIX
                            + ". Please see the example configuration "
                            + "hadoop.security.crypto.codec.classes.EXAMPLECIPHERSUITE "
                            + "at core-default.xml for details.");
        }
        return codec;
    }


    /**
     * Decrypts a EDEK by consulting the KeyProvider.
     */
    private KeyProvider.KeyVersion decryptEncryptedDataEncryptionKey(DFSInputStream dfsis, FileEncryptionInfo feInfo) throws IOException {
        if (!proxyDecryptEEK) {
            KeyProvider provider = getKeyProvider();
            if (provider == null) {
                throw new IOException("No KeyProvider is configured, cannot access" +
                        " an encrypted file");
            }
            KeyProviderCryptoExtension.EncryptedKeyVersion ekv = KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption(
                    feInfo.getKeyName(), feInfo.getEzKeyVersionName(), feInfo.getIV(),
                    feInfo.getEncryptedDataEncryptionKey());
            try {
                KeyProviderCryptoExtension cryptoProvider = KeyProviderCryptoExtension
                        .createKeyProviderCryptoExtension(provider);
                try {
                    return cryptoProvider.decryptEncryptedKey(ekv);
                } catch (AuthorizationException ae) {
                    LOG.info("Got AuthorizationException trying to decrypt EEK, enabling proxy: " + ae.getMessage());
                    proxyDecryptEEK = true;
                }
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }
        }

        try {
            CustomDFSInputStream cis = (CustomDFSInputStream) dfsis;
            try (PreparedStatement statement = connection.prepareStatement("call SYSCS_UTIL.SYSCS_HDFS_OPERATION(?, ?)")) {
                statement.setString(1, cis.getPath());
                statement.setString(2, "decrypt");
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next()) {
                        throw new IOException("No results for decrypt");
                    }
                    Blob blob = rs.getBlob(1);
                    byte[] bytes = blob.getBytes(1, (int) blob.length());
                    Text text = new Text();
                    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                    text.readFields(dis);
                    String name = text.toString();
                    text.readFields(dis);
                    String versionName = text.toString();
                    if (!rs.next()) {
                        throw new IOException("Only one result for decrypt");
                    }
                    blob = rs.getBlob(1);
                    byte[] material = blob.getBytes(1, (int) blob.length());
                    return new CustomKeyVersion(name, versionName, material);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private static class CustomKeyVersion extends KeyProvider.KeyVersion {
        protected CustomKeyVersion(String name, String versionName, byte[] material) {
            super(name, versionName, material);
        }
    }
}
