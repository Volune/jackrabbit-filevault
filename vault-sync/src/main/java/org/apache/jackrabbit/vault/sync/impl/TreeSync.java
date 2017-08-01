/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.vault.sync.impl;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NodeType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.util.Text;
import org.apache.jackrabbit.vault.fs.VaultFileCopy;
import org.apache.jackrabbit.vault.fs.api.SerializationType;
import org.apache.jackrabbit.vault.fs.api.VaultFile;
import org.apache.jackrabbit.vault.fs.api.WorkspaceFilter;
import org.apache.jackrabbit.vault.util.FileInputSource;
import org.apache.jackrabbit.vault.util.LineOutputStream;
import org.apache.jackrabbit.vault.util.MimeTypes;
import org.apache.jackrabbit.vault.util.PlatformNameFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <code>TreeSync</code>...
 */
public class TreeSync {

    /**
     * default logger
     */
    private static final Logger log = LoggerFactory.getLogger(TreeSync.class);

    private final SyncLog syncLog;

    public TreeSync(SyncLog syncLog) {
        this.syncLog = syncLog;
    }

    public void syncVaultFile(SyncResult res, File parentFile, VaultFile vaultFile) throws RepositoryException, IOException {
        if (!parentFile.exists()) {
            VaultFile parentVaultFile = vaultFile.getParent();
            if (parentVaultFile != null) {
                syncVaultFile(res, parentFile.getParentFile(), parentVaultFile);
            }
        }
        for (VaultFile related : vaultFile.getRelated()) {
            if (related.isDirectory()) {
                createDirectory(res, getChildFile(parentFile, related.getArtifact().getPlatformPath()), related);
            } else {
                writeFile(res, getChildFile(parentFile, related.getArtifact().getPlatformPath()), related);
            }
        }
    }

    public void syncChildVaultFile(SyncResult res, File parentFile, VaultFile parentVaultFile, File deletedFile) throws RepositoryException, IOException {
        deleteFile(res, parentVaultFile, deletedFile);
        syncVaultFile(res, parentFile.getParentFile(), parentVaultFile);
    }

    private File getChildFile(File parentFile, String name) {
        String[] segs = Text.explode(name, '/');
        File file = parentFile;
        for (String seg : segs) {
            file = new File(file, seg);
        }
        return file;
    }

    private void deleteFile(SyncResult res, VaultFile parentVaultFile, File file) throws IOException, RepositoryException {
        String fsPath = file.getAbsolutePath();
        String jcrPath = parentVaultFile.getAggregatePath() + "/" + file.getName();
        FileUtils.forceDelete(file);
        syncLog.log("D file://%s", fsPath);
        res.addEntry(jcrPath, file.getAbsolutePath(), SyncResult.Operation.DELETE_FS);
    }

    private void createDirectory(SyncResult res, File file, VaultFile vaultFile) throws RepositoryException {
        if (file.mkdir()) {
            String fsPath = file.getAbsolutePath();
            String jcrPath = vaultFile.getAggregatePath();
            syncLog.log("A file://%s/", fsPath);
            res.addEntry(jcrPath, fsPath, SyncResult.Operation.UPDATE_FS);
        } else if (!file.isDirectory()) {
            log.error("sync cannot create directory " + file.getAbsolutePath());
        }
    }

    private void writeFile(SyncResult res, File file, VaultFile vaultFile) throws IOException, RepositoryException {
        String action = file.exists() ? "U" : "A";
        byte[] lineFeed = MimeTypes.isBinary(vaultFile.getContentType())
                ? null
                : LineOutputStream.LS_NATIVE;
        VaultFileCopy.copy(vaultFile, file, lineFeed);
        String fsPath = file.getAbsolutePath();
        String jcrPath = vaultFile.getAggregatePath();
        syncLog.log("%s file://%s", action, fsPath);
        res.addEntry(jcrPath, fsPath, SyncResult.Operation.UPDATE_FS);
    }
}