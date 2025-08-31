/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.tar.io;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.specialized.AppendBlobClient;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.file.tar.IOWriteAccess;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class AzureIOWriteAccess extends AzureIOReadAccess implements IOWriteAccess {

    /*
     * Access to "writeBuffer" and "length" is synchronized on "writeBuffer".
     */
    private final Buffer writeBuffer = Buffer.allocate(4 * 1024 * 1024);

    private final AtomicInteger length;

    private final WriteAccessController writeAccessController;

    private AppendBlobClient writeClient;

    public AzureIOWriteAccess(String archiveName, BlobClient blobClient, WriteAccessController writeAccessController) {
        super(archiveName, blobClient);
        this.writeAccessController = writeAccessController;
        this.length = new AtomicInteger(0) ;
    }

    private void initIfNeeded() {
        if (writeClient == null) {
            writeClient = readClient.getAppendBlobClient();
            writeClient.createIfNotExists();
        }
    }

    @Override
    public void write(byte[] data) throws IOException {
        write(data, 0, data.length);
    }

    @Override
    public void write(byte[] data, int offset, int length) throws IOException {
        writeAccessController.checkWritingAllowed();
        initIfNeeded();
        synchronized (writeBuffer) {
            if (writeBuffer.remaining() <= length) {
                sync();
            }
            writeBuffer.put(data, offset, length);
            this.length.addAndGet(length);
        }
    }

    @Override
    public Buffer read(int position, int length) throws IOException {
        int persistedOffset;
        synchronized (writeBuffer) {
            persistedOffset = this.length.get() - writeBuffer.position();

            int lastBytePos = position + length;

            // read entirely from persistence
            if (lastBytePos < persistedOffset) {
                return super.read(position, length);
            }

            // read entirely from write buffer
            if (persistedOffset < position) {
                return Buffer.wrap(writeBuffer.array(), position - persistedOffset, length);
            }

            // read partially from persistence and from write buffer
            Buffer bytes = Buffer.allocate(length);
            bytes.put(super.read(position, persistedOffset - position));
            bytes.put(writeBuffer.array(), 0, position + length - persistedOffset);
            bytes.rewind();
            return bytes;
        }
    }

    @Override
    public void sync() throws IOException {
        if (isDirty()) {
            synchronized (writeBuffer) {
                if (writeBuffer.position() > 0) {
                    writeAccessController.checkWritingAllowed();
                    writeClient.appendBlock(
                            new ByteArrayInputStream(writeBuffer.array(), 0, writeBuffer.position()),
                            writeBuffer.position()
                    );
                    writeBuffer.position(0);
                }
            }
        }
    }

    @Override
    public boolean isDirty() {
        return writeClient != null;
    }

    @Override
    public int length() {
        synchronized (writeBuffer) {
            return length.get();
        }
    }

    @Override
    public void close() throws IOException {
        sync();
    }
}
