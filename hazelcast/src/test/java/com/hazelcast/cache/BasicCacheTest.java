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

package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.CacheConcurrentHashMap;
import com.hazelcast.util.ConcurrentReferenceHashMap;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BasicCacheTest extends HazelcastTestSupport {

    HazelcastInstance hz;
    HazelcastInstance hz2;

    @Before
    public void init() {

//        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        hz=Hazelcast.newHazelcastInstance(config);
        hz2=Hazelcast.newHazelcastInstance(config);
//        Hazelcast.newHazelcastInstance(config);
//        hz= factory.newHazelcastInstance(config);
//        hz2= factory.newHazelcastInstance(config);
    }

    @Test
    public void testJSRExample1() throws InterruptedException {
        final String cacheName = "simpleCache";

        final CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Thread.sleep(2000);

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);

        String value2 = cache.get(key);
        assertEquals(value1,value2);
        cache.remove(key);
        assertNull(cache.get(key));

        Cache<Integer, String> cache2 = cacheManager.getCache(cacheName);
        assertNotNull(cache2);

        key = 1;
        value1 = "value";
        cache.put(key, value1);

        value2 = cache.get(key);
        assertEquals(value1,value2);
        cache.remove(key);
        assertNull(cache.get(key));


        cacheManager.destroyCache(cacheName);
    }

    @Test
    public void testJSRCreateDestroyCreate() throws InterruptedException {
        final String cacheName = "simpleCache";

        final CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        assertNotNull(cacheManager);

        assertNull(cacheManager.getCache(cacheName));

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        Cache<Integer, String> cache = cacheManager.createCache(cacheName, config);
        assertNotNull(cache);

        Thread.sleep(1000);

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);

        String value2 = cache.get(key);
        assertEquals(value1,value2);
        cache.remove(key);
        assertNull(cache.get(key));

        cacheManager.destroyCache(cacheName);
        assertNull(cacheManager.getCache(cacheName));

        final Cache<Integer, String> cache1 = cacheManager.createCache(cacheName, config);
        assertNotNull(cache1);
    }

    @Test
    public void testCaches_NotEmpty() {
        final CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        ArrayList<String> caches1 = new ArrayList<String>();
        cacheManager.createCache("c1", new MutableConfiguration());
        cacheManager.createCache("c2", new MutableConfiguration());
        cacheManager.createCache("c3", new MutableConfiguration());
        caches1.add(cacheManager.getCache("c1").getName());
        caches1.add(cacheManager.getCache("c2").getName());
        caches1.add(cacheManager.getCache("c3").getName());

        final Iterable<String> cacheNames = cacheManager.getCacheNames();
        final Iterator<String> iterator = cacheNames.iterator();
        while(iterator.hasNext()){
            System.out.println(iterator.next());
        }
//        checkCollections(caches1, cacheNames);
    }

    @Test
    public void testIterator(){

        final CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        config.setName("SimpleCache");
//        config.setInMemoryFormat(InMemoryFormat.OBJECT);

        ICache<Integer, String> cache = (ICache<Integer, String>) cacheManager.createCache("simpleCache", config);

        int testSize=1007;
        for(int i=0;i<testSize;i++){
            Integer key = i;
            String value1 = "value"+i;
            cache.put(key, value1);
        }

        assertEquals(testSize, cache.size());

        final Iterator<Cache.Entry<Integer, String>> iterator = cache.iterator();
        assertNotNull(iterator);

        HashMap<Integer, String> resultMap= new HashMap<Integer, String>();

        int c=0;
        while (iterator.hasNext()){
            final Cache.Entry<Integer, String> next = iterator.next();
            final Integer key = next.getKey();
            final String value = next.getValue();
//            System.out.println("ITERATOR ITEMS:" + key + " c:"+c );
            resultMap.put(key, value);
            c++;
//            if(c>98){
//                System.out.print("ready");
//            }
        }
        assertEquals(testSize, c);

    }

    @Test
    public void testInitableIterator(){
        int testSize=3007;
        SerializationService ss = new SerializationServiceBuilder().build();
        for(int fetchSize=1;fetchSize < 102;fetchSize++) {
            final CacheConcurrentHashMap<Data, String> cmap = new CacheConcurrentHashMap<Data, String>(1000);
            for (int i = 0; i < testSize; i++) {
                Integer key = i;
                final Data data = ss.toData(key);
                String value1 = "value" + i;
                cmap.put(data, value1);
            }

            int nti = Integer.MAX_VALUE;
            int total = 0;
            int remainig = testSize;
            while (remainig > 0 && nti > 0) {
                int fsize = remainig > fetchSize ? fetchSize : remainig;
                final CacheKeyIteratorResult iteratorResult = cmap.fetchNext(nti, fsize);
                final List<Data> keys = iteratorResult.getKeys();
                nti = iteratorResult.getTableIndex() ;
                remainig -= keys.size();
                total += keys.size();
            }
            assertEquals(testSize, total);
        }
    }

    @Test
    @Ignore
    public void testCacheMigration(){
        HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();

        HazelcastServerCacheManager cacheManager = new HazelcastServerCacheManager(hcp,hz,hcp.getDefaultURI(),hcp.getDefaultClassLoader(),null);

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        config.setName("SimpleCache");
        config.setInMemoryFormat(InMemoryFormat.OBJECT);

        Cache<Integer, String> simpleCache = cacheManager.createCache("simpleCache", config);

        Cache<Integer, String> cache = cacheManager.getCache("simpleCache");

        for(int i=0;i<100;i++){
            cache.put(i,"value"+i);
        }

        hz2.shutdown();

        for(int i=0;i<100;i++){
            String val = cache.get(i);
            assertEquals(val,"value" + i);
        }

    }


}
