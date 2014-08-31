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

package com.hazelcast.client.proxy;

import com.hazelcast.cache.CacheClearResponse;
import com.hazelcast.cache.CacheEntryEventImpl;
import com.hazelcast.cache.CacheEntryProcessorResult;
import com.hazelcast.cache.CacheEventListenerAdaptor;
import com.hazelcast.cache.ICache;
import com.hazelcast.cache.client.CacheAddEntryListenerRequest;
import com.hazelcast.cache.client.CacheClearRequest;
import com.hazelcast.cache.client.CacheContainsKeyRequest;
import com.hazelcast.cache.client.CacheEntryProcessorRequest;
import com.hazelcast.cache.client.CacheGetAllRequest;
import com.hazelcast.cache.client.CacheGetAndRemoveRequest;
import com.hazelcast.cache.client.CacheGetAndReplaceRequest;
import com.hazelcast.cache.client.CacheGetRequest;
import com.hazelcast.cache.client.CacheLoadAllRequest;
import com.hazelcast.cache.client.CachePutIfAbsentRequest;
import com.hazelcast.cache.client.CachePutRequest;
import com.hazelcast.cache.client.CacheRemoveRequest;
import com.hazelcast.cache.client.CacheReplaceRequest;
import com.hazelcast.cache.client.CacheSizeRequest;
import com.hazelcast.client.cache.ClientClusterWideIterator;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.impl.client.ClientRequest;
import com.hazelcast.client.nearcache.ClientHeapNearCache;
import com.hazelcast.client.nearcache.ClientNearCache;
import com.hazelcast.client.nearcache.IClientNearCache;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.ClientCallFuture;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.map.MapEntrySet;
import com.hazelcast.map.client.MapAddEntryListenerRequest;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.DelegatingFuture;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.cache.CacheProxy.loadAllHelper;

public class ClientCacheProxy<K, V>
        implements ICache<K, V> {
    //WARNING:: this proxy do not extend ClientProxy because Cache and DistributedObject
    // has getName method which have different values a distributedObject delegate used to over come this

    private static final String NULL_KEY_IS_NOT_ALLOWED = "Null key is not allowed!";
    private static final String NULL_VALUE_IS_NOT_ALLOWED = "Null value is not allowed!";
    private final CacheConfig<K, V> cacheConfig;
    private final boolean cacheOnUpdate;
    protected ClientCacheDistributedObject delegate;
    //this will represent the name from the user perspective
    private String name;
    private boolean isClosed = false;

    //    private CacheLoader<K, V> cacheLoader;
    private HazelcastClientCacheManager cacheManager;
    private volatile IClientNearCache<Data, Object> nearCache;

    public ClientCacheProxy(CacheConfig<K, V> cacheConfig, ClientCacheDistributedObject delegate,
                            HazelcastClientCacheManager cacheManager) {
        this.name = cacheConfig.getName();
        this.cacheConfig = cacheConfig;
        this.delegate = delegate;
        this.cacheManager = cacheManager;
        //TODO DO WE NEED A CACHE LOADER HERE
        //        if (cacheConfig.getCacheLoaderFactory() != null) {
        //            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
        //            cacheLoader = cacheLoaderFactory.create();
        //        }

        NearCacheConfig nearCacheConfig = cacheConfig.getNearCacheConfig();
        if (nearCacheConfig != null) {
            nearCache = new ClientHeapNearCache<Data>(getDistributedObjectName(), delegate.getClientContext(), nearCacheConfig);
            cacheOnUpdate = nearCacheConfig.getLocalUpdatePolicy() == NearCacheConfig.LocalUpdatePolicy.CACHE;
        } else {
            nearCache = null;
            cacheOnUpdate = false;
        }
    }

    //region JAVAX.CACHE impl
    @Override
    public V get(K key) {
        return get(key, null);
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        return getAll(keys, null);
    }

    @Override
    public boolean containsKey(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return true;
        }
        CacheContainsKeyRequest request = new CacheContainsKeyRequest(getDistributedObjectName(), keyData);
        return toObject(invoke(request, keyData));
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        ensureOpen();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        for (K key : keys) {
            validateConfiguredTypes(false, key);
        }
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(toData(key));
        }
        CacheLoadAllRequest request = new CacheLoadAllRequest(getDistributedObjectName(), keysData, replaceExistingValues);
        try {
            final Map<Integer, Object> results = invoke(request);
            loadAllHelper(results);
            if (completionListener != null) {
                completionListener.onCompletion();
            }

        } catch (Exception e) {
            if (completionListener != null) {
                completionListener.onException(e);
            }
        }
    }

    @Override
    public void put(K key, V value) {
        put(key, value, null);
    }

    @Override
    public V getAndPut(K key, V value) {
        return getAndPut(key, value, null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        putAll(map, null);
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        return putIfAbsent(key, value, null);
    }

    @Override
    public boolean remove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Data keyData = delegate.toData(key);
        CacheRemoveRequest request = new CacheRemoveRequest(delegate.getName(), keyData);
        Boolean removed = toObject(invoke(request, keyData));
        if (removed == null) {
            return false;
        }
        if (removed) {
            invalidateNearCache(keyData);
        }
        return removed;
    }

    @Override
    public boolean remove(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheRemoveRequest request = new CacheRemoveRequest(getDistributedObjectName(), keyData, valueData);
        Boolean removed = toObject(invoke(request, keyData));
        if (removed == null) {
            return false;
        }
        if (removed) {
            invalidateNearCache(keyData);
        }
        return removed;
    }

    @Override
    public V getAndRemove(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(false, key);
        final Data keyData = toData(key);
        CacheGetAndRemoveRequest request = new CacheGetAndRemoveRequest(getDistributedObjectName(), keyData);
        V value = toObject(invoke(request, keyData));
        invalidateNearCache(keyData);
        return value;
    }

    @Override
    public boolean replace(K key, V currentValue, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, currentValue, value);
        final Data keyData = toData(key);
        final Data currentValueData = toData(currentValue);
        final Data valueData = toData(value);
        CacheReplaceRequest request = new CacheReplaceRequest(getDistributedObjectName(), keyData, currentValueData, valueData,
                null);
        Boolean replaced = toObject(invoke(request, keyData));
        if (replaced == null) {
            return false;
        }
        if (replaced) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return replaced;
    }

    @Override
    public boolean replace(K key, V value) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheReplaceRequest request = new CacheReplaceRequest(getDistributedObjectName(), keyData, valueData, null);
        Boolean replaced = toObject(invoke(request, keyData));
        if (replaced == null) {
            return false;
        }
        if (replaced) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return replaced;
    }

    @Override
    public V getAndReplace(K key, V value) {
        return getAndReplace(key, value, null);
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        ensureOpen();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        HashSet<Data> keysData = new HashSet<Data>();
        for (K key : keys) {
            keysData.add(toData(key));
        }
        removeAllInternal(keysData, true);
    }

    @Override
    public void removeAll() {
        ensureOpen();
        removeAllInternal(null, true);
    }

    @Override
    public void clear() {
        ensureOpen();
        removeAllInternal(null, false);
    }

    private void removeAllInternal(Set<Data> keysData, boolean isRemoveAll) {
        CacheClearRequest request = new CacheClearRequest(getDistributedObjectName(), keysData, isRemoveAll);
        try {
            final Map<Integer, Object> results = invoke(request);
            for (Object result : results.values()) {
                if (result != null && result instanceof CacheClearResponse) {
                    final Object response = ((CacheClearResponse) result).getResponse();
                    if (response instanceof Throwable) {
                        throw (Throwable) response;
                    }
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrowAllowedTypeFirst(t, CacheException.class);
        }
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        if (clazz.isInstance(cacheConfig)) {
            return clazz.cast(cacheConfig);
        }
        throw new IllegalArgumentException("The configuration class " + clazz +
                " is not supported by this implementation");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments)
            throws EntryProcessorException {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }
        final Data keyData = toData(key);
        final CacheEntryProcessorRequest request = new CacheEntryProcessorRequest(getDistributedObjectName(), keyData,
                entryProcessor, arguments);
        try {
            final Data resultData = (Data) invoke(request, keyData);
            return toObject(resultData);
        } catch (CacheException ce) {
            throw ce;
        } catch (Exception e) {
            throw new EntryProcessorException(e);
        }
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor,
                                                         Object... arguments) {
        //TODO implement a Multiple invoke operation and its factory
        ensureOpen();
        if (keys == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (entryProcessor == null) {
            throw new NullPointerException();
        }
        Map<K, EntryProcessorResult<T>> allResult = new HashMap<K, EntryProcessorResult<T>>();
        for (K key : keys) {
            CacheEntryProcessorResult<T> ceResult;
            try {
                final T result = this.invoke(key, entryProcessor, arguments);
                ceResult = result != null ? new CacheEntryProcessorResult<T>(result) : null;
            } catch (Exception e) {
                ceResult = new CacheEntryProcessorResult<T>(e);
            }
            if (ceResult != null) {
                allResult.put(key, ceResult);
            }
        }
        return allResult;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public void close() {
        //TODO CHECK this is valid
/*
        must close and release all resources being coordinated on behalf of the Cache by the
        CacheManager. This includes calling the close method on configured CacheLoader,
                CacheWriter, registered CacheEntryListeners and ExpiryPolicy instances that
        implement the java.io.Closeable interface,
*/
        isClosed = true;
        delegate.destroy();

        //close the configured CacheLoader
        //        if (cacheLoader instanceof Closeable) {
        //            try {
        //                ((Closeable) cacheLoader).close();
        //            } catch (IOException e) {
        //                //log
        //            }
        //        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(((Object) this).getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
//        final Factory<CacheEntryListener<? super K, ? super V>> factory = cacheEntryListenerConfiguration
//                .getCacheEntryListenerFactory();
//        final CacheEntryListener<? super K, ? super V> listener = factory.create();
//
//        CacheAddEntryListenerRequest registrationRequest = new CacheAddEntryListenerRequest(name, includeValue);
//
////        CacheEventListenerAdaptor<K,V> handler = new CacheEventListenerAdaptor<K, V>(meta, cacheEntryListenerConfiguration);
//
//        EventHandler<PortableEntryEvent> handler = createHandler(listener);
//
//        final String regId = getContext().getListenerService().listen(registrationRequest, null, handler);

        //        throw new UnsupportedOperationException("registerCacheEntryListener");
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        //        throw new UnsupportedOperationException("deregisterCacheEntryListener");
    }

    private EventHandler<PortableEntryEvent> createHandler(CacheEntryListener<? super K, ? super V> listener){
        return new EventHandler<PortableEntryEvent>() {
            @Override
            public void handle(PortableEntryEvent event) {
//                event
//                final CacheEntryEventImpl<K, V> event = new CacheEntryEventImpl<K, V>(source, eventType, key, newValue, oldValue);
//                switch (eventType) {
//                    case CREATED:
//                        if (this.cacheEntryCreatedListener != null) {
//                            this.cacheEntryCreatedListener.onCreated(createEventWrapper(event));
//                        }
//                        break;
//                    case UPDATED:
//                        if (this.cacheEntryUpdatedListener != null) {
//                            this.cacheEntryUpdatedListener.onUpdated(createEventWrapper(event));
//                        }
//                        break;
//                    case REMOVED:
//                        if (this.cacheEntryRemovedListener != null) {
//                            this.cacheEntryRemovedListener.onRemoved(createEventWrapper(event));
//                        }
//                        break;
//                    case EXPIRED:
//                        if (this.cacheEntryExpiredListener != null) {
//                            this.cacheEntryExpiredListener.onExpired(createEventWrapper(event));
//                        }
//                        break;
//                    default:
//                        throw new IllegalArgumentException("Invalid event type: " + eventType.name());
//                }
            }

            @Override
            public void onListenerRegister() {

            }

//            private EntryEvent<K, V> createEntryEvent(PortableEntryEvent event, Member member) {
//                V value = null;
//                V oldValue = null;
//                if (includeValue) {
//                    value = toObject(event.getValue());
//                    oldValue = toObject(event.getOldValue());
//                }
//                K key = toObject(event.getKey());
//                return new CacheEntryEventImpl<K, V>(source, eventType, key, newValue, oldValue);
//                return new EntryEvent<K, V>(name, member,event.getEventType().getType(), key, oldValue, value);
//            }
        };
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        ensureOpen();
        return new ClientClusterWideIterator<K, V>(this, getContext());
    }
    //endregion

    //region ICACHE imple
    @Override
    public Future<V> getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public Future<V> getAsync(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (shouldBeSync()) {
            V value = get(key, expiryPolicy);
            return createCompletedFuture(value);
        }
        final Data keyData = toData(key);
        Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return createCompletedFuture(cached);
        }

        CacheGetRequest request = new CacheGetRequest(getDistributedObjectName(), keyData, expiryPolicy);
        ClientCallFuture future;
        final ClientContext context = getContext();
        try {
            future = (ClientCallFuture) context.getInvocationService().invokeOnKeyOwner(request, keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        if (nearCache != null) {
            future.andThenInternal(new ExecutionCallback<Data>() {
                public void onResponse(Data valueData) {
                    storeInNearCache(keyData, valueData, null);
                }

                public void onFailure(Throwable t) {
                }
            });
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Void> putAsync(K key, V value) {
        return putAsync(key, value, null);
    }

    @Override
    public Future<Void> putAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }

        if (shouldBeSync()) {
            put(key, value, expiryPolicy);
            return createCompletedFuture(null);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, expiryPolicy, false);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Void>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> putIfAbsentAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);

        if (shouldBeSync()) {
            final Boolean put = putIfAbsent(key, value, expiryPolicy);
            return createCompletedFuture(put);
        }

        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(getDistributedObjectName(), keyData, valueData,
                expiryPolicy);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value) {
        return getAndPutAsync(key, value, null);
    }

    @Override
    public Future<V> getAndPutAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        if (shouldBeSync()) {
            V oldValue = getAndPut(key, value, expiryPolicy);
            return createCompletedFuture(oldValue);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, expiryPolicy, true);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> removeAsync(K key) {
        return removeAsync(key, null, false);

    }

    @Override
    public Future<Boolean> removeAsync(K key, V oldValue) {
        return removeAsync(key, oldValue, true);
    }

    private Future<Boolean> removeAsync(K key, V oldValue, boolean hasOldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (hasOldValue && oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(hasOldValue, key, oldValue);

        if (shouldBeSync()) {
            final Boolean isRemoved = remove(key, oldValue);
            return createCompletedFuture(isRemoved);
        }

        final Data keyData = toData(key);
        final Data valueData = oldValue != null ? toData(oldValue) : null;
        CacheRemoveRequest request = new CacheRemoveRequest(getDistributedObjectName(), keyData, valueData);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());
    }

    @Override
    public Future<V> getAndRemoveAsync(K key) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (shouldBeSync()) {
            V value = get(key);
            return createCompletedFuture(value);
        }
        final Data keyData = toData(key);
        CacheGetAndRemoveRequest request = new CacheGetAndRemoveRequest(getDistributedObjectName(), keyData);
        ClientCallFuture future;
        final ClientContext context = getContext();
        try {
            future = (ClientCallFuture) context.getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue) {
        return replaceAsyncInternal(key, oldValue, newValue, null, true);
    }

    @Override
    public Future<Boolean> replaceAsync(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceAsyncInternal(key, oldValue, newValue, expiryPolicy, true);
    }

    private Future<Boolean> replaceAsyncInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy, boolean hasOldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (newValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue && oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue) {
            validateConfiguredTypes(true, key, oldValue, newValue);
        } else {
            validateConfiguredTypes(true, key, newValue);
        }

        if (shouldBeSync()) {
            final Boolean isRemoved = replaceInternal(key, oldValue, newValue, expiryPolicy, true);
            return createCompletedFuture(isRemoved);
        }

        final Data keyData = toData(key);
        final Data currentValueData = oldValue != null ? toData(oldValue) : null;
        final Data valueData = newValue != null ? toData(newValue) : null;
        CacheReplaceRequest request = new CacheReplaceRequest(getDistributedObjectName(), keyData, currentValueData, valueData,
                expiryPolicy);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<Boolean>(future, getContext().getSerializationService());
    }

    private boolean replaceInternal(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy, boolean hasOldValue) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (newValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue && oldValue == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        if (hasOldValue) {
            validateConfiguredTypes(true, key, oldValue, newValue);
        } else {
            validateConfiguredTypes(true, key, newValue);
        }

        final Data keyData = toData(key);
        final Data currentValueData = oldValue != null ? toData(oldValue) : null;
        final Data valueData = newValue != null ? toData(newValue) : null;
        CacheReplaceRequest request = new CacheReplaceRequest(getDistributedObjectName(), keyData, currentValueData, valueData,
                expiryPolicy);
        Boolean replaced = toObject(invoke(request, keyData));
        if (replaced == null) {
            return false;
        }
        if (replaced) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, newValue);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return replaced;
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value) {
        return getAndReplaceAsync(key, value, null);
    }

    @Override
    public Future<V> getAndReplaceAsync(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        if (shouldBeSync()) {
            V oldValue = getAndPut(key, value, expiryPolicy);
            return createCompletedFuture(oldValue);
        }
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheGetAndReplaceRequest request = new CacheGetAndReplaceRequest(getDistributedObjectName(), keyData, valueData,
                expiryPolicy);
        ICompletableFuture future;
        try {
            future = getContext().getInvocationService().invokeOnKeyOwner(request, keyData);
            invalidateNearCache(keyData);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return new DelegatingFuture<V>(future, getContext().getSerializationService());
    }

    @Override
    public V get(K key, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        final Data keyData = toData(key);
        final Object cached = nearCache != null ? nearCache.get(keyData) : null;
        if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
            return (V) cached;
        }
        final CacheGetRequest request = new CacheGetRequest(getDistributedObjectName(), keyData, expiryPolicy);
        try {
            final Data resultData = (Data) invoke(request, keyData);
            final V result = toObject(resultData);
            storeInNearCache(keyData, resultData, result);
            return result;
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (keys == null || keys.contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (keys.isEmpty()) {
            return Collections.EMPTY_MAP;
        }

        Set<Data> keySet = new HashSet(keys.size());
        Map<K, V> result = new HashMap<K, V>();
        for (Object key : keys) {
            keySet.add(toData(key));
        }
        if (nearCache != null) {
            final Iterator<Data> iterator = keySet.iterator();
            while (iterator.hasNext()) {
                Data key = iterator.next();
                Object cached = nearCache.get(key);
                if (cached != null && !ClientNearCache.NULL_OBJECT.equals(cached)) {
                    result.put((K) toObject(key), (V) cached);
                    iterator.remove();
                }
            }
        }
        if (keySet.isEmpty()) {
            return result;
        }
        final CacheGetAllRequest request = new CacheGetAllRequest(getDistributedObjectName(), keySet, expiryPolicy);
        final MapEntrySet mapEntrySet = toObject(invoke(request));
        final Set<Map.Entry<Data, Data>> entrySet = mapEntrySet.getEntrySet();
        for (Map.Entry<Data, Data> dataEntry : entrySet) {
            final Data keyData = dataEntry.getKey();
            final Data valueData = dataEntry.getValue();
            final K key = toObject(keyData);
            final V value = toObject(valueData);
            result.put(key, value);
            storeInNearCache(keyData, valueData, value);
        }
        return result;
    }

    @Override
    public void put(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = delegate.toData(key);
        final Data valueData = delegate.toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, expiryPolicy);
        invoke(request, keyData);
        if (cacheOnUpdate) {
            storeInNearCache(keyData, valueData, value);
        } else {
            invalidateNearCache(keyData);
        }
    }

    @Override
    public V getAndPut(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutRequest request = new CachePutRequest(getDistributedObjectName(), keyData, valueData, expiryPolicy, true);
        final Object oldValue = invoke(request, keyData);
        if (cacheOnUpdate) {
            storeInNearCache(keyData, valueData, value);
        } else {
            invalidateNearCache(keyData);
        }
        return toObject(oldValue);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (map == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (map.keySet().contains(null)) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (map.values().contains(null)) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }

        final Iterator<? extends Map.Entry<? extends K, ? extends V>> iter = map.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<? extends K, ? extends V> next = iter.next();
            put(next.getKey(), next.getValue(), expiryPolicy);
        }
    }

    @Override
    public boolean putIfAbsent(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CachePutIfAbsentRequest request = new CachePutIfAbsentRequest(getDistributedObjectName(), keyData, valueData,
                expiryPolicy);
        Boolean isPut = toObject(invoke(request, keyData));
        if (isPut == null) {
            return false;
        }
        if (isPut) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return isPut;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue, ExpiryPolicy expiryPolicy) {
        return replaceInternal(key, oldValue, newValue, expiryPolicy, true);
    }

    @Override
    public boolean replace(K key, V value, ExpiryPolicy expiryPolicy) {
        return replaceInternal(key, value, null, expiryPolicy, false);
    }

    @Override
    public V getAndReplace(K key, V value, ExpiryPolicy expiryPolicy) {
        ensureOpen();
        if (key == null) {
            throw new NullPointerException(NULL_KEY_IS_NOT_ALLOWED);
        }
        if (value == null) {
            throw new NullPointerException(NULL_VALUE_IS_NOT_ALLOWED);
        }
        validateConfiguredTypes(true, key, value);
        final Data keyData = toData(key);
        final Data valueData = toData(value);
        CacheGetAndReplaceRequest request = new CacheGetAndReplaceRequest(getDistributedObjectName(), keyData, valueData,
                expiryPolicy);
        V currentValue = toObject(invoke(request, keyData));
        if (currentValue != null) {
            if (cacheOnUpdate) {
                storeInNearCache(keyData, valueData, value);
            } else {
                invalidateNearCache(keyData);
            }
        }
        return currentValue;
    }

    @Override
    public int size() {
        CacheSizeRequest request = new CacheSizeRequest(getDistributedObjectName());
        Integer result = invoke(request);
        if (result == null) {
            return 0;
        }
        return result;
    }
    //endregion

    private void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("Cache operations can not be performed. The cache closed");
        }
    }

    private Future createCompletedFuture(Object value) {
        return new CompletedFuture(getContext().getSerializationService(), value,
                getContext().getExecutionService().getAsyncExecutor());
    }

    private void validateConfiguredTypes(boolean validateValues, K key, V... values)
            throws ClassCastException {
        final Class keyType = cacheConfig.getKeyType();
        final Class valueType = cacheConfig.getValueType();
        if (Object.class != keyType) {
            //means type checks required
            if (!keyType.isAssignableFrom(key.getClass())) {
                throw new ClassCastException("Key " + key + "is not assignable to " + keyType);
            }
        }
        if (validateValues) {
            for (V value : values) {
                if (Object.class != valueType) {
                    //means type checks required
                    if (!valueType.isAssignableFrom(value.getClass())) {
                        throw new ClassCastException("Value " + value + "is not assignable to " + valueType);
                    }
                }
            }
        }
    }

    private ClientContext getContext() {
        return delegate.getClientContext();
    }

    private void storeInNearCache(Data key, Data valueData, V value) {
        if (nearCache != null) {
            final Object valueToStore;
            if (nearCache.getInMemoryFormat() == InMemoryFormat.OBJECT) {
                valueToStore = value != null ? value : valueData;
            } else {
                valueToStore = valueData != null ? valueData : value;
            }
            nearCache.put(key, valueToStore);
        }
    }

    private void invalidateNearCache(Data key) {
        if (nearCache != null) {
            nearCache.remove(key);
        }
    }

    private boolean shouldBeSync() {
        boolean sync = false;
        //TODO Implement a backpressure stuff here
        return sync;
    }

    public String getDistributedObjectName() {
        return delegate.getName();
    }

    private <T> T toObject(Object data) {
        return delegate.toObject(data);
    }

    private Data toData(Object o) {
        return delegate.toData(o);
    }

    private <T> T invoke(ClientRequest req) {
        return delegate.invoke(req);
    }

    private Object invoke(ClientRequest req, Object key) {
        try {
            final Future future = delegate.getClientContext().getInvocationService().invokeOnKeyOwner(req, key);
            return future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
