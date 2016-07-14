package com.splicemachine.encryption;

/**
 * Created by jleach on 7/14/16.
 */
public interface EncryptionManager {
    /**
     *
     * Kerberos or other encryption mechanism (Per Platform config properties)
     *
     * @return
     */
    boolean encryptedMessaging();

    /**
     *
     * File System is encrypted (Per Platform config properties)
     *
     * @return
     */
    boolean encryptedFileSystem();

}
