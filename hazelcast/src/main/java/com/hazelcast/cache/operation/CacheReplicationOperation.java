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

package com.hazelcast.cache.operation;

import com.hazelcast.cache.CachePartitionSegment;
import com.hazelcast.cache.CacheService;
import com.hazelcast.cache.ICacheRecordStore;
import com.hazelcast.cache.record.CacheRecord;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author mdogan 05/02/14
 */
public final class CacheReplicationOperation extends AbstractOperation {

    final Map<String, Map<Data, CacheRecord>> source;

    final Map<String, Map<Data, CacheRecord>> destination;

//    public CacheReplicationOperation() {
//        source = null;
//        destination = new HashMap<String, Map<Data, CacheRecord>>();
//    }

    public CacheReplicationOperation(CachePartitionSegment segment, int replicaIndex) {
        source = new HashMap<String, Map<Data, CacheRecord>>();
        destination = null;

        Iterator<ICacheRecordStore> iter = segment.cacheIterator();
        while (iter.hasNext()) {
            ICacheRecordStore next = iter.next();
            CacheConfig cacheConfig = next.getConfig();
            if (cacheConfig.getAsyncBackupCount() + cacheConfig.getBackupCount() >= replicaIndex) {
                source.put(next.getName(), next.getReadOnlyRecords());
            }
        }
    }

    @Override
    public final void beforeRun() throws Exception {

    }

    @Override
    public void run() throws Exception {
        CacheService service = getService();
        for (Map.Entry<String, Map<Data, CacheRecord>> entry : destination.entrySet()) {
            ICacheRecordStore cache = service.getOrCreateCache(entry.getKey(), getPartitionId());
            Map<Data, CacheRecord> map = entry.getValue();

            Iterator<Map.Entry<Data, CacheRecord>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<Data, CacheRecord> next = iter.next();
                Data key = next.getKey();
                CacheRecord record = next.getValue();
                iter.remove();
                cache.own(key, record);
            }
        }
        destination.clear();
    }

    @Override
    public String getServiceName() {
        return CacheService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int count = source.size();
        out.writeInt(count);
        if (count > 0) {
            long now = Clock.currentTimeMillis();
            for (Map.Entry<String, Map<Data, CacheRecord>> entry : source.entrySet()) {
                Map<Data, CacheRecord> cacheMap = entry.getValue();
                int subCount = cacheMap.size();
                out.writeInt(subCount);
                if (subCount > 0) {
                    out.writeUTF(entry.getKey());
                    for (Map.Entry<Data, CacheRecord> e : cacheMap.entrySet()) {
                        final Data key = e.getKey();
                        final CacheRecord record = e.getValue();
                        final long expirationTime = record.getExpirationTime();
                        if (expirationTime > now) {
                            key.writeData(out);
                            out.writeObject(record);
                        }
                        subCount--;
                    }
                    if (subCount != 0) {
                        throw new AssertionError("Cache iteration error, count is not zero!" + subCount);
                    }
                }
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int count = in.readInt();
        if (count > 0) {
            for (int i = 0; i < count; i++) {
                int subCount = in.readInt();
                if (subCount > 0) {
                    String name = in.readUTF();
                    Map<Data, CacheRecord> m = new HashMap<Data, CacheRecord>(subCount);
                    destination.put(name, m);
                    for (int j = 0; j < subCount; j++) {
                        final Data key = new Data();
                        key.readData(in);
                        final CacheRecord record = in.readObject();
                        m.put(key, record);
                    }
                }
            }
        }
    }

    public boolean isEmpty() {
        return source == null || source.isEmpty();
    }

}
