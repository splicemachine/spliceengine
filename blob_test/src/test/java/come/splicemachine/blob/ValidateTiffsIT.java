package come.splicemachine.blob;

import java.io.File;
import java.util.Map;

import org.junit.Test;

import com.splicemachine.blob.RunTiffBlobs;

/**
 * Requires a running splice server.
 *
 * @author Jeff Cunningham
 *         Date: 5/22/14
 */
public class ValidateTiffsIT {
    private static final String FILE_DIRECTORY = System.getProperty("user.dir")+ "/blob_test/target/classes/tiff";

    @Test
    public void testTifs() throws Exception {
        RunTiffBlobs vts = new RunTiffBlobs();
        try {
            vts.createInvoicesTable(RunTiffBlobs.DEFAULT_SCHEMA_NAME, RunTiffBlobs.DEFAULT_TABLE_NAME, RunTiffBlobs.DEFAULT_TABLE_SCHEMA);
            Map<String,File> files = vts.getFiles(FILE_DIRECTORY, RunTiffBlobs.DEFAULT_TIFF_EXT);
            vts.insertTiffFiles(RunTiffBlobs.DEFAULT_SCHEMA_NAME, RunTiffBlobs.DEFAULT_TABLE_NAME,
                                RunTiffBlobs.DEFAULT_BATCH_INSERT_SIZE, files);
            vts.validateTiffBlobStream(RunTiffBlobs.DEFAULT_SCHEMA_NAME, RunTiffBlobs.DEFAULT_TABLE_NAME, files);
        } finally {
            vts.dropInvoicesTable(RunTiffBlobs.DEFAULT_SCHEMA_NAME, RunTiffBlobs.DEFAULT_TABLE_NAME);
        }
    }
}
