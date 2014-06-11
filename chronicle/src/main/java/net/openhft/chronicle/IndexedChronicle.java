/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.VanillaMappedBlocks;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.model.constraints.NotNull;
import net.openhft.lang.model.constraints.Nullable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * IndexedChronicle is a single-writer-multiple-reader
 * {@link net.openhft.chronicle.Chronicle} that you can put huge numbers of objects in,
 * having different sizes.
 *
 * <p>For each record, IndexedChronicle holds the memory-offset in another index cache
 * for random access. This means IndexedChronicle "knows" where the Nth object resides at
 * in memory, thus the name "Indexed". But this index is just sequential index,
 * first object has index 0, second object has index 1, and so on. If you want to access
 * objects with other logical keys you have to manage your own mapping from logical key
 * to index.</p>
 *
 * Indexing and data storage are achieved using two backing (memory-mapped) files:
 * <ul>
 * <li>a data file called &#60;base file name&#62;.data</li>
 * <li>an index file called &#60;base file name&#62;.index</li>
 * </ul>
 * , <tt>base file name</tt> (or <tt>basePath</tt>) is provided on construction.
 *
 * @author peter.lawrey
 */
public class IndexedChronicle implements Chronicle {

    @NotNull
    final VanillaMappedBlocks indexFileCache;
    @NotNull
    final VanillaMappedBlocks dataFileCache;

    @NotNull
    final ChronicleConfig config;
    private final String basePath;
    // todo consider making volatile to help detect bugs in calling code.
    private long lastWrittenIndex = -1;
    private volatile boolean closed = false;

    /**
     * Creates a new instance of IndexedChronicle having the specified <tt>basePath</tt> (the base name
     * of the two backing files).
     *
     * @param basePath the base name of the files backing this IndexChronicle
     *
     * @throws FileNotFoundException if the <tt>basePath</tt> string does not denote an existing,
     * writable regular file and a new regular file of that name cannot be created, or if some
     * other error occurs while opening or creating the file
     */
    public IndexedChronicle(@NotNull String basePath) throws IOException {
        this(basePath, ChronicleConfig.DEFAULT);
    }

    /**
     * Creates a new instance of IndexedChronicle as specified by the provided
     * {@link net.openhft.chronicle.ChronicleConfig} and having the specified <tt>basePath</tt> (the base name
     * of the two backing files).
     *
     * @param basePath the base name of the files backing this IndexChronicle
     * @param config the ChronicleConfig based on which the current IndexChronicle should be constructed
     *
     * @throws FileNotFoundException if the <tt>basePath</tt> string does not denote an existing,
     * writable regular file and a new regular file of that name cannot be created, or if some
     * other error occurs while opening or creating the file
     */
    public IndexedChronicle(@NotNull String basePath, @NotNull ChronicleConfig config) throws IOException {
        this.basePath = basePath;
        this.config = config.clone();

        File parentFile = new File(basePath).getParentFile();
        if (parentFile != null) {
            parentFile.mkdirs();
        }

        this.indexFileCache = VanillaMappedBlocks.readWrite(new File(basePath + ".index"),config.indexBlockSize());
        this.dataFileCache  = VanillaMappedBlocks.readWrite(new File(basePath + ".data" ),config.dataBlockSize());

        findTheLastIndex();
    }

    /**
     * Checks if this instance of IndexedChronicle is closed or not.
     * If closed an {@link java.lang.IllegalStateException} will be thrown.
     *
     * @throws java.lang.IllegalStateException if this IndexChronicle is close
     */
    public void checkNotClosed() {
        if (closed) throw new IllegalStateException(basePath + " is closed");
    }

    /**
     * Returns the {@link net.openhft.chronicle.ChronicleConfig} that has been used to create
     * the current instance of IndexedChronicle
     *
     * @return the ChronicleConfig used to create this IndexChronicle
     */
    public ChronicleConfig config() {
        return config; //todo: would be better to return a copy/clone, because like this the config can be changed from the outside
    }

    /**
     * Returns the index of the most recent {@link net.openhft.chronicle.Excerpt}s previously
     * written into this {@link net.openhft.chronicle.Chronicle}. Basically the same value as
     * returned by {@link IndexedChronicle#lastWrittenIndex()}, but does it by looking at the
     * content of the backing files and figuring it out from there.
     *
     * <p>A side effect of the method is that it also stores the obtained value and it can and
     * will be used by subsequent calls of {@link IndexedChronicle#lastWrittenIndex()}.</p>
     *
     * <p>The constructors of IndexedChronicle automatically call
     * this method so they properly handle the backing file being both empty or non-empty at the
     * start.</p>
     *
     * @return the index of the most recent Excerpt written into this Chronicle
     */
    public long findTheLastIndex() {
        return lastWrittenIndex = findTheLastIndex0();
    }

    private long findTheLastIndex0() {
        long size = 0;

        try {
            size = indexFileCache.size();
        } catch(Exception e) {
            return -1;
        }

        if (size <= 0) {
            return -1;
        }

        int indexBlockSize = config.indexBlockSize();
        for (long block = size / indexBlockSize - 1; block >= 0; block--) {
            VanillaMappedBytes mbb = null;
            try {
                mbb = indexFileCache.acquire(block);
            } catch (IOException e) {
                continue;
            }

            if (block > 0 && mbb.readLong(0) == 0) {
                continue;
            }

            int cacheLineSize = config.cacheLineSize();
            for (int pos = 0; pos < indexBlockSize; pos += cacheLineSize) {
                // if the next line is blank
                if (pos + cacheLineSize >= indexBlockSize || mbb.readLong(pos + cacheLineSize) == 0) {
                    // last cache line.
                    int pos2 = 8;
                    for (pos2 = 8; pos2 < cacheLineSize; pos2 += 4) {
                        if (mbb.readInt(pos + pos2) == 0) {
                            break;
                        }
                    }
                    return (block * indexBlockSize + pos) / cacheLineSize * (cacheLineSize / 4 - 2) + pos2 / 4 - 3;
                }
            }
            return (block + 1) * indexBlockSize / cacheLineSize * (cacheLineSize / 4 - 2);
        }
        return -1;
    }

    /**
     * Returns the number of {@link net.openhft.chronicle.Excerpt}s that have been written
     * into this {@link net.openhft.chronicle.Chronicle}.
     *
     * @return the number of Excerpts previously written into this Chronicle
     */
    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    @Override
    public void clear() {
        new File(basePath + ".index").delete();
        new File(basePath + ".data").delete();
    }

    /**
     * Returns the base file name backing this instance of IndexChronicle. Index chronicle uses two files:
     * <ul>
     * <li>a data file called &#60;base file name&#62;.data</li>
     * <li>an index file called &#60;base file name&#62;.index</li>
     * </ul>
     * @return the base file name backing this IndexChronicle
     */
    @Override
    public String name() {
        return basePath;
    }

    /**
     * Closes this instance of IndexedChronicle, including the backing files.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        this.indexFileCache.close();
        this.dataFileCache.close();
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.Excerpt} which can be used
     * for random access to the data stored in this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.Excerpt} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new IndexedExcerpt();
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.ExcerptTailer} which can be used
     * for sequential reads from this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.ExcerptTailer} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        return new IndexedExcerptTailer();
    }

    /**
     * Returns a new instance of {@link net.openhft.chronicle.ExcerptAppender} which can be used
     * for sequential writes into this Chronicle.
     *
     * @return new {@link net.openhft.chronicle.ExcerptAppender} for this Chronicle
     *
     * @throws IOException if an I/O error occurs
     */
    @NotNull
    @Override
    public ExcerptAppender createAppender() throws IOException {
        return new IndexedExcerptAppender();
    }

    /**
     * Returns the index of the most recent {@link net.openhft.chronicle.Excerpt}s previously
     * written into this {@link net.openhft.chronicle.Chronicle}. Basically <tt>size() - 1</tt>.
     *
     * @return the index of the most recent Excerpt written into this Chronicle
     */
    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    void incrSize() {
        lastWrittenIndex++;
    }

    // *************************************************************************
    //
    // *************************************************************************

    protected abstract class AbstractIndexedExcerpt extends NativeBytes implements ExcerptCommon {
        @NotNull
        final int cacheLineMask;
        final int dataBlockSize;
        final int indexBlockSize;
        final int indexEntriesPerLine;
        private final int indexEntriesPerBlock;
        private final int cacheLineSize;
        @Nullable
        @SuppressWarnings("FieldCanBeLocal")
        VanillaMappedBytes indexBuffer;
        @Nullable
        @SuppressWarnings("FieldCanBeLocal")
        VanillaMappedBytes dataBuffer;
        long index = -1;
        // relatively static
        // the start of the index block, as an address
        long indexStartAddr;
        // which index does this refer to?
        private long indexStartOffset;
        // the offset in data referred to the start of the line
        long indexBaseForLine;
        // the start of the data block, as an address
        long dataStartAddr;
        // which offset does this refer to.
        long dataStartOffset;
        // the position currently writing to in the index.
        long indexPositionAddr;
        boolean padding = true;

        // the start of this entry
        // inherited - long startAddr;
        // inherited - long positionAddr;
        // inherited - long limitAddr;

        protected AbstractIndexedExcerpt() throws IOException {
            super(NO_PAGE, NO_PAGE);
            cacheLineSize = config().cacheLineSize();
            cacheLineMask = (cacheLineSize - 1);
            dataBlockSize = config().dataBlockSize();
            indexBlockSize = config().indexBlockSize();
            indexEntriesPerLine = (cacheLineSize - 8) / 4;
            indexEntriesPerBlock = indexBlockSize * indexEntriesPerLine / cacheLineSize;
            loadIndexBuffer();
            loadDataBuffer();

            finished = true;
        }

        @Override
        public long index() {
            return index;
        }

        ExcerptCommon toStart() {
            index = -1;
            return this;
        }

        boolean indexForRead(long l) throws IOException {
            if (l < 0) {
                setIndexBuffer(0, true);
                index = -1;
                padding = true;
                return false;
            }
            long indexLookup = l / indexEntriesPerBlock;
            setIndexBuffer(indexLookup, true);

            long indexLookupMod = l % indexEntriesPerBlock;
            int indexLineEntry = (int) (indexLookupMod % indexEntriesPerLine);
            int indexLineStart = (int) (indexLookupMod / indexEntriesPerLine * cacheLineSize);
            int inLine = (indexLineEntry << 2) + 8;

            int dataOffsetEnd = UNSAFE.getInt(indexStartAddr + indexLineStart + inLine);

            indexBaseForLine = UNSAFE.getLong(indexStartAddr + indexLineStart);
            indexPositionAddr = indexStartAddr + indexLineStart + inLine;

            long dataOffsetStart = inLine == 0
                ? indexBaseForLine
                : (indexBaseForLine + Math.abs(UNSAFE.getInt(indexPositionAddr - 4)));

            long dataLookup = dataOffsetStart / dataBlockSize;
            long dataLookupMod = dataOffsetStart % dataBlockSize;
            setDataBuffer(dataLookup);

            startAddr = positionAddr = dataStartAddr + dataLookupMod;
            index = l;
            if (dataOffsetEnd > 0) {
                limitAddr = dataStartAddr + (indexBaseForLine + dataOffsetEnd - dataLookup * dataBlockSize);
                indexPositionAddr += 4;
                padding = false;
                return true;
            }
            else if (dataOffsetEnd == 0) {
                limitAddr = startAddr;
                padding = false;
                return false;
            }
            else /* if (dataOffsetEnd < 0) */ {
                padding = true;
                return false;
            }
        }

        private void setIndexBuffer(long index, boolean prefetch) throws IOException {
            if (indexBuffer != null) {
                indexBuffer.release();
            }

            indexBuffer = IndexedChronicle.this.indexFileCache.acquire(index);
            indexPositionAddr = indexStartAddr = indexBuffer.address();
        }

        void indexForAppender(long l) throws IOException {
            if (l < 0) {
                throw new IndexOutOfBoundsException("index: " + l);
            }
            else if (l == 0) {
                indexStartOffset = 0;
                loadIndexBuffer();
                dataStartOffset = 0;
                loadDataBuffer();
                return;
            }

            // We need the end of the previous Excerpt
            l--;
            long indexLookup = l / indexEntriesPerBlock;
            setIndexBuffer(indexLookup, true);

            long indexLookupMod = l % indexEntriesPerBlock;
            int indexLineEntry = (int) (indexLookupMod % indexEntriesPerLine);
            int indexLineStart = (int) (indexLookupMod / indexEntriesPerLine * cacheLineSize);
            int inLine = (indexLineEntry << 2) + 8;
            indexStartOffset = indexLookup * indexBlockSize + indexLineStart;

            indexBaseForLine = UNSAFE.getLong(indexStartAddr + indexLineStart);
            long dataOffsetEnd = indexBaseForLine + Math.abs(UNSAFE.getInt(indexStartAddr + indexLineStart + inLine));

            long dataLookup = dataOffsetEnd / dataBlockSize;
            long dataLookupMod = dataOffsetEnd % dataBlockSize;
            setDataBuffer(dataLookup);
            dataStartOffset = dataLookup * dataBlockSize;
            startAddr = positionAddr = dataStartAddr + dataLookupMod;
            index = l + 1;
            indexPositionAddr = indexStartAddr + indexLineStart + inLine + 4;
        }

        private void setDataBuffer(long dataLookup) throws IOException {
            if (dataBuffer != null) {
                dataBuffer.release();
            }

            dataBuffer = IndexedChronicle.this.dataFileCache.acquire(dataLookup);
            dataStartAddr = dataBuffer.address();
        }

        @Override
        public boolean wasPadding() {
            return padding;
        }

        @Override
        public long lastWrittenIndex() {
            return IndexedChronicle.this.lastWrittenIndex();
        }

        @Override
        public long size() {
            return IndexedChronicle.this.size();
        }

        @NotNull
        @Override
        public Chronicle chronicle() {
            return IndexedChronicle.this;
        }

        void loadNextIndexBuffer() throws IOException {
            indexStartOffset += indexBlockSize;
            loadIndexBuffer();
        }

        void loadNextDataBuffer() throws IOException {
            dataStartOffset += dataBlockSize;
            loadDataBuffer();
        }

        void loadNextDataBuffer(long offsetInThisBuffer) throws IOException {
            dataStartOffset += offsetInThisBuffer / dataBlockSize * dataBlockSize;
            loadDataBuffer();

        }

        void loadDataBuffer() throws IOException {
            setDataBuffer(dataStartOffset / dataBlockSize);
            startAddr = positionAddr = limitAddr = dataStartAddr;
        }

        void loadIndexBuffer() throws IOException {
            setIndexBuffer(indexStartOffset / indexBlockSize, true);
        }

        boolean index(long index) {
            throw new UnsupportedOperationException();
        }

        public long findMatch(@NotNull ExcerptComparator comparator) {
            long lo = 0, hi = lastWrittenIndex();
            while (lo <= hi) {
                long mid = (hi + lo) >>> 1;
                if (!index(mid)) {
                    if (mid > lo)
                        index(--mid);
                    else
                        break;
                }
                int cmp = comparator.compare((Excerpt) this);
                finish();
                if (cmp < 0)
                    lo = mid + 1;
                else if (cmp > 0)
                    hi = mid - 1;
                else
                    return mid; // key found
            }
            return ~lo; // -(lo + 1)
        }

        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            // lower search range
            long lo1 = 0, hi1 = lastWrittenIndex();
            // upper search range
            long lo2 = 0, hi2 = hi1;
            boolean both = true;
            // search for the low values.
            while (lo1 <= hi1) {
                long mid = (hi1 + lo1) >>> 1;
                if (!index(mid)) {
                    if (mid > lo1)
                        index(--mid);
                    else
                        break;
                }
                int cmp = comparator.compare((Excerpt) this);
                finish();

                if (cmp < 0) {
                    lo1 = mid + 1;
                    if (both)
                        lo2 = lo1;
                }
                else if (cmp > 0) {
                    hi1 = mid - 1;
                    if (both)
                        hi2 = hi1;
                }
                else {
                    hi1 = mid - 1;
                    if (both)
                        lo2 = mid + 1;
                    both = false;
                }
            }
            // search for the high values.
            while (lo2 <= hi2) {
                long mid = (hi2 + lo2) >>> 1;
                if (!index(mid)) {
                    if (mid > lo2)
                        index(--mid);
                    else
                        break;
                }
                int cmp = comparator.compare((Excerpt) this);
                finish();

                if (cmp <= 0) {
                    lo2 = mid + 1;
                }
                else {
                    hi2 = mid - 1;
                }
            }
            startEnd[0] = lo1; // inclusive
            startEnd[1] = lo2; // exclusive
        }

        /**
         * For compatibility with Java-Lang 6.2.
         */
        @Override
        public long capacity() {
            return limitAddr - startAddr;
        }
    }

    protected class IndexedExcerpt extends AbstractIndexedExcerpt implements Excerpt {
        private boolean padding = true;

        protected IndexedExcerpt() throws IOException {
        }

        public void startExcerpt(long capacity) {
            checkNotClosed();
            // if the capacity is to large, roll the previous entry, and there was one
            if (positionAddr + capacity > dataStartAddr + dataBlockSize) {
                // check we are the start of a block.
                checkNewIndexLine();

                writePaddedEntry();

                try {
                    loadNextDataBuffer();

                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            // check we are the start of a block.
            checkNewIndexLine();

            // update the soft limitAddr
            startAddr = positionAddr;
            limitAddr = positionAddr + capacity;
            finished = false;
        }

        private void writePaddedEntry() {
            int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
            assert size >= 0;
            if (size == 0) {
                return;
            }

            checkNewIndexLine();
            writePaddingIndexEntry(size);
            indexPositionAddr += 4;
        }

        private void writePaddingIndexEntry(int size) {
            UNSAFE.putInt(indexPositionAddr, -size);
        }

        @Override
        public boolean index(long l) {
            checkNotClosed();
            try {
                return indexForRead(l);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void finish() {
            super.finish();

            if (IndexedChronicle.this.config.synchronousMode()) {
                if (dataBuffer != null) {
                    dataBuffer.force();
                }
                if (indexBuffer != null) {
                    indexBuffer.force();
                }
            }
        }

        void checkNewIndexLine() {
            switch ((int) (indexPositionAddr & cacheLineMask)) {
                case 0:
                    newIndexLine();
                    break;
                case 4:
                    throw new AssertionError();
            }
        }

        void newIndexLine() {
            // check we have a valid index
            if (indexPositionAddr >= indexStartAddr + indexBlockSize) {
                try {
                    loadNextIndexBuffer();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            // sets the base address
            indexBaseForLine = positionAddr - dataStartAddr + dataStartOffset;

            assert indexBaseForLine >= 0 && indexBaseForLine < 1L << 48 : "dataPositionAtStartOfLine out of bounds, was " + indexBaseForLine;

            appendToIndex();
            // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);
            indexPositionAddr += 8;
        }

        private void appendToIndex() {
            UNSAFE.putLong(indexPositionAddr, indexBaseForLine);
        }

        @NotNull
        @Override
        public Excerpt toStart() {
            index = -1;
            return this;
        }

        @NotNull
        @Override
        public Excerpt toEnd() {
            index = chronicle().size();
            try {
                indexForRead(index);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return this;
        }

        @Override
        public boolean nextIndex() {
            checkNotClosed();
            try {
                long index2 = index;
                if (indexForRead(index() + 1)) {
                    return true;
                } else {
                    // rewind on a failure
                    index = index2;
                }
                if (wasPadding()) {
                    index++;
                    return indexForRead(index() + 1);
                }
                return false;
            } catch (IOException e) {
                return false;
            }
        }
    }

    public class IndexedExcerptAppender extends AbstractIndexedExcerpt implements ExcerptAppender {
        private boolean nextSynchronous;

        public IndexedExcerptAppender() throws IOException {
            toEnd();
        }

        @Override
        public void startExcerpt() {
            startExcerpt(config().messageCapacity());
        }

        public void startExcerpt(long capacity) {
            checkNotClosed();
            // in case there is more than one appender :P
            if (index != size()) {
                toEnd();
            }

            if (capacity >= config.dataBlockSize())
                throw new IllegalArgumentException("Capacity too large " + capacity + " >= " + config.dataBlockSize());
            // if the capacity is to large, roll the previous entry, and there was one
            if (positionAddr + capacity > dataStartAddr + dataBlockSize) {
                // check we are the start of a block.
                checkNewIndexLine();
                writePaddedEntry();

                try {
                    loadNextDataBuffer();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            // check we are the start of a block.
            checkNewIndexLine();

            // update the soft limitAddr
            startAddr = positionAddr;
            limitAddr = positionAddr + capacity;
            finished = false;
            nextSynchronous = config.synchronousMode();
        }

        public void nextSynchronous(boolean nextSynchronous) {
            this.nextSynchronous = nextSynchronous;
        }

        public boolean nextSynchronous() {
            return nextSynchronous;
        }

        @Override
        public void addPaddedEntry() {
            // in case there is more than one appender :P
            if (index != lastWrittenIndex()) {
                toEnd();
            }

            // check we are the start of a block.
            checkNewIndexLine();
            writePaddedEntry();

            try {
                loadNextDataBuffer();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }

            // check we are the start of a block.
            checkNewIndexLine();

            finished = true;
        }

        private void writePaddedEntry() {
            int size = (int) (dataBlockSize + dataStartOffset - indexBaseForLine);
            assert size >= 0;
            if (size == 0) {
                return;
            }

            appendIndexPaddingEntry(size);
            indexPositionAddr += 4;
            index++;
            IndexedChronicle.this.incrSize();
        }

        private void appendIndexPaddingEntry(int size) {
            assert index < this.indexEntriesPerLine || UNSAFE.getLong(indexPositionAddr & ~cacheLineMask) != 0 : "index: " + index + ", no start of line set.";
            UNSAFE.putInt(indexPositionAddr, -size);
        }

        @Override
        public void finish() {
            super.finish();
            if (index != IndexedChronicle.this.size())
                throw new ConcurrentModificationException("Chronicle appended by more than one Appender at the same time, index=" + index + ", size="
                    + chronicle().size());

            // push out the entry is available. This is what the reader polls.
            // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "= " + (int) (dataPosition() - dataPositionAtStartOfLine));
            long offsetInBlock = positionAddr - dataStartAddr;
            assert offsetInBlock >= 0 && offsetInBlock <= dataBlockSize;
            int relativeOffset = (int) (dataStartOffset + offsetInBlock - indexBaseForLine);
            assert relativeOffset > 0;
            writeIndexEntry(relativeOffset);
            indexPositionAddr += 4;
            index++;
            IndexedChronicle.this.incrSize();

            if ((indexPositionAddr & cacheLineMask) == 0 && indexPositionAddr - indexStartAddr < indexBlockSize) {
                indexBaseForLine += relativeOffset;
                appendStartOfLine();
            }

            if (nextSynchronous) {
                assert dataBuffer != null;
                dataBuffer.force();
                assert indexBuffer != null;
                indexBuffer.force();
            }
        }

        private void writeIndexEntry(int relativeOffset) {
            UNSAFE.putOrderedInt(null, indexPositionAddr, relativeOffset);
        }

        @NotNull
        @Override
        public ExcerptAppender toEnd() {
            index = chronicle().size();
            try {
                indexForAppender(index);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return this;
        }

        void checkNewIndexLine() {
            switch ((int) (indexPositionAddr & cacheLineMask)) {
                case 0:
                    newIndexLine();
                    break;
                case 4:
                    throw new AssertionError();
            }
        }

        void newIndexLine() {
            // check we have a valid index
            if (indexPositionAddr >= indexStartAddr + indexBlockSize) {
                try {
                    loadNextIndexBuffer();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            // sets the base address
            indexBaseForLine = positionAddr - dataStartAddr + dataStartOffset;

            assert (index == 0 || indexBaseForLine > 0) && indexBaseForLine < 1L << 48 : "dataPositionAtStartOfLine out of bounds, was " + indexBaseForLine;

            appendStartOfLine();

        }

        private void appendStartOfLine() {
            UNSAFE.putLong(indexPositionAddr, indexBaseForLine);
            // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + "=== " + dataPositionAtStartOfLine);
            indexPositionAddr += 8;
        }
    }

    public class IndexedExcerptTailer extends AbstractIndexedExcerpt implements ExcerptTailer {

        public static final long UNSIGNED_INT_MASK = 0xFFFFFFFFL;

        public IndexedExcerptTailer() throws IOException {
        }

        @Override
        public boolean index(long l) {
            checkNotClosed();
            try {
                return indexForRead(l);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            index = chronicle().size();
            try {
                indexForRead(index);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toStart() {
            super.toStart();
            return this;
        }

        public boolean nextIndex() {
            checkNotClosed();
            checkNextLine();
            long offset = UNSAFE.getInt(null, indexPositionAddr);
            if (offset == 0) {
                offset = UNSAFE.getIntVolatile(null, indexPositionAddr);
            }

            // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
            if (offset == 0) {
                return false;
            }

            index++;
            return nextIndex0(offset) || nextIndex1();
        }

        private boolean nextIndex1() {
            long offset;
            checkNextLine();
            offset = UNSAFE.getInt(null, indexPositionAddr);
            if (offset == 0) {
                offset = UNSAFE.getIntVolatile(null, indexPositionAddr);
            }
            // System.out.println(Long.toHexString(indexPositionAddr - indexStartAddr + indexStart) + " was " + offset);
            if (offset == 0) {
                return false;
            }

            index++;
            return nextIndex0(offset);
        }

        private void checkNextLine() {
            switch ((int) (indexPositionAddr & cacheLineMask)) {
                case 0:
                    newIndexLine();
                    // skip the base until we have the offset.
                    indexPositionAddr += 8;
                    break;
                case 4:
                    throw new AssertionError();
            }
        }

        private void newIndexLine() {
            if (indexPositionAddr >= indexStartAddr + indexBlockSize) {
                try {
                    loadNextIndexBuffer();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private boolean nextIndex0(long offset) {
            boolean present = true;
            padding = (offset < 0);
            if (padding) {
                present = false;
                offset = -offset;
            }

            checkNewIndexLine2();
            startAddr = positionAddr = limitAddr;
            setLimitAddr(offset);
            assert limitAddr >= startAddr || (!present && limitAddr == startAddr);
            indexPositionAddr += 4;
            return present;
        }

        private void setLimitAddr(long offset) {
            long offsetInThisBuffer = indexBaseForLine + offset - dataStartOffset;
            if (offsetInThisBuffer > dataBlockSize) {
                try {
                    loadNextDataBuffer(offsetInThisBuffer);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
                offsetInThisBuffer = indexBaseForLine + offset - dataStartOffset;
            }
            assert offsetInThisBuffer >= 0 && offsetInThisBuffer <= dataBlockSize : "index: " + index + ", offsetInThisBuffer: " + offsetInThisBuffer;
            limitAddr = dataStartAddr + offsetInThisBuffer;
        }

        void checkNewIndexLine2() {
            if ((indexPositionAddr & cacheLineMask) == 8) {
                indexBaseForLine = UNSAFE.getLongVolatile(null, indexPositionAddr - 8);
                assert index <= indexEntriesPerLine || indexBaseForLine > 0 : "index: " + index + " indexBaseForLine: " + indexBaseForLine;
                setLimitAddr(0);
            }
        }
    }
}
