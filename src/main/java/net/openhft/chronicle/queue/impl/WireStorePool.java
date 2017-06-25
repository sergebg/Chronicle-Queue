/*
 * Copyright 2016 higherfrequencytrading.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.openhft.chronicle.queue.impl;

import net.openhft.chronicle.core.annotation.Nullable;
import net.openhft.chronicle.queue.RollDetails;
import net.openhft.chronicle.queue.TailerDirection;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WireStorePool {
    private static final Logger LOGGER = LoggerFactory.getLogger(WireStorePool.class);
    @NotNull
    private final WireStoreSupplier supplier;
    @NotNull
    private final Map<RollDetails, WireStore> stores;
    private final StoreFileListener storeFileListener;
    private boolean isClosed = false;

    private WireStorePool(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        this.supplier = supplier;
        this.storeFileListener = storeFileListener;
        this.stores = new ConcurrentHashMap<>();
    }

    @NotNull
    public static WireStorePool withSupplier(@NotNull WireStoreSupplier supplier, StoreFileListener storeFileListener) {
        return new WireStorePool(supplier, storeFileListener);
    }

    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;
            stores.values().stream().forEach(this::release);
        }
    }

    @org.jetbrains.annotations.Nullable
    @Nullable
    public synchronized WireStore acquire(final int cycle, final long epoch, boolean createIfAbsent) {
        RollDetails rollDetails = new RollDetails(cycle, epoch);
        WireStore store = stores.get(rollDetails);
        if (store != null) {
            if (store.tryReserve())
                return store;
            else
                /// this should never happen,
                // this method is synchronized
                // and this remove below, is only any use if the acquire method below that fails
                stores.remove(rollDetails);
        }

        store = this.supplier.acquire(cycle, createIfAbsent);
        if (store != null) {
            stores.put(rollDetails, store);
            storeFileListener.onAcquired(cycle, store.file());
        }
        return store;
    }

    public int nextCycle(final int currentCycle, @NotNull TailerDirection direction) throws ParseException {
        return supplier.nextCycle(currentCycle, direction);
    }

    public synchronized void release(@NotNull WireStore store) {
        store.release();
        long refCount = store.refCount();
        assert refCount >= 0;
        if (refCount == 0) {
            Iterator<Map.Entry<RollDetails, WireStore>> it = stores.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<RollDetails, WireStore> entry = it.next();
                if (entry.getValue() == store) {
                    it.remove();
                    storeFileListener.onReleased(entry.getKey().cycle(), store.file());
                    return;
                }
            }
            LOGGER.warn("Store was not registered {}", store.file().getName());
        }
    }

    /**
     * list cycles between ( inclusive )
     *
     * @param lowerCycle the lower cycle
     * @param upperCycle the upper cycle
     * @return an array including these cycles and all the intermediate cycles
     */
    public NavigableSet<Long> listCyclesBetween(int lowerCycle, int upperCycle) throws ParseException {
        return supplier.cycles(lowerCycle, upperCycle);
    }
}
