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

import com.hazelcast.cache.CacheClearResponse;
import com.hazelcast.cache.CachePortableHook;
import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.operation.CacheLoadAllOperationFactory;
import com.hazelcast.client.impl.client.AllPartitionsClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import java.io.IOException;
import java.security.Permission;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CacheLoadAllRequest
        extends AllPartitionsClientRequest implements Portable, RetryableRequest, SecureRequest {

    protected String name;
    private Set<Data> keys = new HashSet<Data>();
    private boolean replaceExistingValues;

    public CacheLoadAllRequest() {
    }

    public CacheLoadAllRequest(String name, Set<Data> keys, boolean replaceExistingValues) {
        this.name = name;
        this.keys = keys;
        this.replaceExistingValues = replaceExistingValues;
    }

    public int getFactoryId() {
        return CachePortableHook.F_ID;
    }

    public int getClassId() {
        return CachePortableHook.LOAD_ALL;
    }

    @Override
    protected OperationFactory createOperationFactory() {
        return new CacheLoadAllOperationFactory(name, keys, replaceExistingValues);
    }

    @Override
    protected Object reduce(Map<Integer, Object> map) {
        for (Object result : map.values()) {
            if (result != null && result instanceof CacheClearResponse) {
                final Object response = ((CacheClearResponse) result).getResponse();
                if (response instanceof Exception) {
                    if (completionListener != null) {
                        completionListener.onException((Exception) response);
                        return;
                    }
                }
            }
        }

        MapEntrySet resultSet = new MapEntrySet();
        CacheService service = getService();
        for (Map.Entry<Integer, Object> entry : map.entrySet()) {
            MapEntrySet mapEntrySet = (MapEntrySet) service.toObject(entry.getValue());
            Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
            for (Map.Entry<Data, Data> dataEntry : entrySet) {
                resultSet.add(dataEntry);
            }
        }
        return resultSet;
    }

    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("r", replaceExistingValues);
        writer.writeInt("size", keys.size());
        if (!keys.isEmpty()) {
            ObjectDataOutput output = writer.getRawDataOutput();
            for (Data key : keys) {
                key.writeData(output);
            }
        }
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        replaceExistingValues = reader.readBoolean("r");
        int size = reader.readInt("size");
        if (size > 0) {
            ObjectDataInput input = reader.getRawDataInput();
            for (int i = 0; i < size; i++) {
                Data key = new Data();
                key.readData(input);
                keys.add(key);
            }
        }

    }

    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "getAll";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{keys};
    }
}
