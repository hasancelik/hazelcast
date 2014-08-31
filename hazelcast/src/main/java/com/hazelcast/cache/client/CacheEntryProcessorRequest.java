/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.client;

import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.operation.CacheEntryProcessorOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.Operation;

import javax.cache.processor.EntryProcessor;
import java.io.IOException;

public class CacheEntryProcessorRequest
        extends AbstractCacheRequest {

    protected Data key;
    private EntryProcessor entryProcessor;
    private Object[] arguments;

    public CacheEntryProcessorRequest() {
    }

    public CacheEntryProcessorRequest(String name, Data key, javax.cache.processor.EntryProcessor entryProcessor,
                                      Object... arguments) {
        super(name);
        this.key = key;
        this.entryProcessor = entryProcessor;
        this.arguments = arguments;
    }

    public int getClassId() {
        return CachePortableHook.ENTRY_PROCESSOR;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        return new CacheEntryProcessorOperation(name, key, -1, entryProcessor, arguments);
    }

    public void write(PortableWriter writer)
            throws IOException {
        writer.writeUTF("n", name);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        out.writeObject(entryProcessor);
        out.writeBoolean(arguments != null);
        if (arguments != null) {
            out.writeInt(arguments.length);
            for (Object arg : arguments) {
                out.writeObject(arg);
            }
        }
    }

    public void read(PortableReader reader)
            throws IOException {
        name = reader.readUTF("n");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        entryProcessor = in.readObject();
        final boolean hasArguments = in.readBoolean();
        if (hasArguments) {
            final int size = in.readInt();
            arguments = new Object[size];
            for (int i = 0; i < size; i++) {
                arguments[i] = in.readObject();
            }
        }
    }

}
