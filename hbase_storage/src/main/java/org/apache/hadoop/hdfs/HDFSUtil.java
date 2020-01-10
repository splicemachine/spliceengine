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

import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import java.io.IOException;
import java.security.GeneralSecurityException;

public class HDFSUtil {
    public static LocatedBlocks getBlocks(DistributedFileSystem dfs, String path) throws IOException {
        return dfs.dfs.getLocatedBlocks(path, 0);
    }

    public static KeyProvider.KeyVersion decrypt(DistributedFileSystem dfs, String path) throws IOException {
        return decryptEncryptedDataEncryptionKey(dfs, dfs.dfs.getLocatedBlocks(path, 0).getFileEncryptionInfo());
    }

    private static KeyProvider.KeyVersion decryptEncryptedDataEncryptionKey(DistributedFileSystem dfs, FileEncryptionInfo feInfo) throws IOException {
        KeyProvider provider = dfs.dfs.getKeyProvider();
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
            return cryptoProvider.decryptEncryptedKey(ekv);
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
    }

}
