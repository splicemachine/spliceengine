/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.task;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;

/**
 *  For given model, write the database export and analogous import statements to SQL command files.
 */
public class WriteExportImportSqlToFileCommand extends DatabaseCommand {
    /**
     * Not used when running as an ANT task.
     * Useful when running pragmatically.
     */
    private Platform _platform;
    /**
     * The file to output the export script to.
     */
    private File _exportFile;
    /**
     * The file to output the import script to.
     */
    private File _importFile;
    /**
     * The directory under which the export scripts should export data
     */
    private String _exportDirectory;

    /**
     * Specifies the the file to write the SQL commands to.
     *
     * @param exportFile The output file
     * @ant.required
     */
    public void setExportScriptFile(File exportFile) {
        _exportFile = exportFile;
    }

    /**
     * Specifies the the file to write the SQL commands to.
     *
     * @param importFile The output file
     * @ant.required
     */
    public void setImportScriptFile(File importFile) {
        _importFile = importFile;
    }

    /**
     * Specifies the name of directory to which the export data should be written.
     *
     * @param exportDirectory The output file
     * @ant.required
     */
    public void setExportDirectory(String exportDirectory) {
        _exportDirectory = exportDirectory;
    }

    /**
     * If not set pragmatically, will return that which was set by ANT.
     * @return the database platform
     */
    @Override
    public Platform getPlatform() {
        return _platform;
    }

    /**
     * Set the platform to use.<br/>
     * Used when calling pragmatically when creating ones own platform.
     * @param platform the platform to use
     * @ant.not-required
     */
    public void setPlatform(Platform platform) {
        this._platform = platform;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(DatabaseTaskBase task, Database model) throws BuildException {
        if (_exportFile == null) {
            throw new BuildException("No export script file specified");
        }
        if (_exportFile.exists() && !_exportFile.canWrite()) {
            throw new BuildException("Cannot overwrite export script file " + _exportFile.getAbsolutePath());
        }
        if (_exportDirectory == null || _exportDirectory.isEmpty()) {
            throw new BuildException("No export directory specified");
        }

        if (_importFile == null) {
            throw new BuildException("No import script file specified");
        }
        if (_importFile.exists() && !_importFile.canWrite()) {
            throw new BuildException("Cannot overwrite import script file " + _importFile.getAbsolutePath());
        }

        Platform platform = getPlatform();
        Map<String, String> params = getExportImportParameters(platform);
        platform.setScriptModeOn(true);
        if (platform.getPlatformInfo().isSqlCommentsSupported()) {
            // we're generating SQL comments if possible
            platform.setSqlCommentsOn(true);
        }

        // Do export
        try (FileWriter exportWriter = new FileWriter(_exportFile)) {
            exportWriter.write(platform.createExportModelSql(model, _exportDirectory, params, !isFailOnError()));
            _log.info("Model export SQL written to " + _exportFile.getAbsolutePath());
        } catch (Exception ex) {
            handleException(ex, ex.getMessage());
        }

        // Do import
        try (FileWriter importWriter = new FileWriter(_importFile)) {
            importWriter.write(platform.createImportModelSql(model, _exportDirectory, params, !isFailOnError()));
            _log.info("Model import SQL written to " + _importFile.getAbsolutePath());
        } catch (Exception ex) {
            handleException(ex, ex.getMessage());
        }
    }

    protected Map<String, String> getExportImportParameters(Platform platform) {
        Map<String, String> params = new HashMap<>();
        boolean isCaseSensitive = platform.isDelimitedIdentifierModeOn();
        params.put(Platform.IS_CASE_SENSITIVE, Boolean.toString(isCaseSensitive));
        return params;
    }
}
