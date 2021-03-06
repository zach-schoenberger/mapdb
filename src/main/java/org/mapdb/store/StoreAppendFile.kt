package org.mapdb.store

import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap
import org.mapdb.DBException
import org.mapdb.io.DataIO
import org.mapdb.io.DataInput2ByteArray
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.util.lockRead
import org.mapdb.util.lockWrite
import java.io.EOFException
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class StoreAppendFile(
    val file: Path,
    override val isThreadSafe: Boolean = true
) : MutableStore {

    protected val lock: ReadWriteLock? = if (isThreadSafe) ReentrantReadWriteLock() else null
    private var raf = RandomAccessFile(file.toFile(), "rw")

    /** key is recid, value is offset in channel */
    // protected val offsets = LongLongHashMap()
    protected var offsets = Long2LongRBTreeMap()
    protected var maxRecid: Long = 1L
    protected var curOffset = 0L

    init {
        val csize = raf.length()
        if (csize == 0L) {
            raf.write(ByteArray(8))
            curOffset = 8
        } else {
            var pos = 8L
            //read record positions
            while (pos < csize) {
                val offset = pos
                val (recid, size) = readRecidSize(offset)
                pos += 12 + size.coerceAtLeast(0)

                if (recid != TOMBSTONE_RECID) {
                    offsets[recid] = offset
                    maxRecid = recid.coerceAtLeast(maxRecid)
                }
            }
            curOffset = pos
        }
    }

    protected fun readRecidSize(offset: Long): Pair<Long, Int> {
        val b = ByteBuffer.allocate(12)
        raf.seek(offset)
        raf.read(b.array())
        val recid = DataIO.getLong(b.array(), 0)
        val size = DataIO.getInt(b.array(), 8)
        return Pair(recid, size)
    }

    protected fun readRecord(offset: Long, size: Int): ByteArray {
        val b = ByteBuffer.allocate(size)
        raf.seek(offset)
        raf.read(b.array())
        return b.array()
    }

    private fun append(recid: Long, serialized: ByteArray): Long {
        val offset = appendRecord(raf, recid, serialized)
        offsets[recid] = offset
        return offset
    }

    private fun appendRecord(raf: RandomAccessFile, recid: Long, serialized: ByteArray): Long {
        val b1 = ByteArray(12 + serialized.size)
        DataIO.putLong(b1, 0, recid)
        DataIO.putInt(b1, 8, serialized.size)
        serialized.copyInto(b1, 12)
        val offset = curOffset
        raf.seek(curOffset)
        raf.write(b1)
        curOffset += b1.size
        return offset
    }

    private fun tombStoneRecord(raf: RandomAccessFile, offset: Long) {
        raf.seek(offset)
        raf.writeLong(TOMBSTONE_RECID)
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K {
        lock.lockRead {
            return getNoLock(recid, serializer)
        }
    }

    private fun <K> getNoLock(recid: Long, serializer: Serializer<K>): K {
        val offset = offsets.getOrDefault(recid, Long.MIN_VALUE)
        if (offset == Long.MIN_VALUE) {
            throw DBException.RecidNotFound()
        }

        val (recid2, size) = readRecidSize(offset)
        assert(recid == recid2)
        val b = readRecord(offset + 12, size)

        val input = DataInput2ByteArray(b)
        return serializer.deserialize(input)
    }

    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        lock.lockRead {
            offsets.long2LongEntrySet().forEach { (recid, offset) ->
                if (offset == 0L) {
                    consumer(recid, null)
                    return@forEach
                }

                val (recid2, size) = readRecidSize(offset)
                assert(recid == recid2)

                val b = readRecord(offset + 12, size)
                consumer(recid, b)
            }
        }
    }

    override fun <K> getAndUpdate(recid: Long, serializer: Serializer<K>, newRecord: K): K {
        lock.lockWrite {
            return super.getAndUpdate(recid, serializer, newRecord)
        }
    }

    override fun <K> updateAndGet(recid: Long, serializer: Serializer<K>, m: (K) -> K): K {
        lock.lockWrite {
            return super.updateAndGet(recid, serializer, m)
        }
    }

    override fun preallocate(): Long {
        lock.lockWrite {
            return preallocate2()
        }
    }

    protected fun preallocate2(): Long {
        return maxRecid++
    }

    override fun <K> put(record: K, serializer: Serializer<K>): Long {
        val serialized = Serializers.serializeToByteArrayNullable(record, serializer) ?: throw DBException.DataNull()

        lock.lockWrite {
            val recid = preallocate2()
            append(recid, serialized)
            return recid
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K) {
        val serialized = Serializers.serializeToByteArrayNullable(newRecord, serializer) ?: throw DBException.DataNull()

        lock.lockWrite {
            if (!offsets.containsKey(recid))
                throw DBException.RecidNotFound()

            val offset = offsets[recid]
            tombStoneRecord(raf, offset)
            append(recid, serialized)
        }
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, m: (K) -> K) {
        lock.lockWrite {
            val oldRec = getNoLock(recid, serializer)
            val newRec = m(oldRec)
            val serialized =
                Serializers.serializeToByteArrayNullable(newRec, serializer) ?: throw DBException.DataNull()
            val offset = offsets[recid]
            tombStoneRecord(raf, offset)
            append(recid, serialized)
        }
    }

    override fun <K> compareAndUpdate(
        recid: Long,
        serializer: Serializer<K>,
        expectedOldRecord: K,
        newRecord: K
    ): Boolean {
        val expectedBytes =
            Serializers.serializeToByteArrayNullable(expectedOldRecord, serializer) ?: throw DBException.DataNull()
        val newRecordBytes =
            Serializers.serializeToByteArrayNullable(newRecord, serializer) ?: throw DBException.DataNull()

        return compareAnd(recid, expectedBytes) {
            val offset = offsets[recid]
            tombStoneRecord(raf, offset)
            append(recid, newRecordBytes)
        }
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K): Boolean {
        val expected =
            Serializers.serializeToByteArrayNullable(expectedOldRecord, serializer) ?: throw DBException.DataNull()

        return compareAnd(recid, expected) {
            val offset = offsets.remove(recid)
            tombStoneRecord(raf, offset)
            appendRecord(raf, recid, NULL_VALUE)
        }
    }

    private fun compareAnd(recid: Long, expected: ByteArray, block: () -> Unit): Boolean {
        lock.lockWrite {
            val offset = offsets.getOrDefault(recid, Long.MIN_VALUE)
            if (offset == Long.MIN_VALUE)
                throw DBException.RecidNotFound()

            val (recid2, size) = readRecidSize(offset)
            assert(recid == recid2)

            val old = readRecord(offset + 12, size)

            if (!expected.contentEquals(old))
                return false

            block()
            return true
        }
    }

    override fun <K> delete(recid: Long, serializer: Serializer<K>) {
        lock.lockWrite {
            if (!offsets.containsKey(recid))
                throw DBException.RecidNotFound()
            val offset = offsets.remove(recid)
            tombStoneRecord(raf, offset)
            appendRecord(raf, recid, NULL_VALUE)
        }
    }

    override fun <K> getAndDelete(recid: Long, serializer: Serializer<K>): K {
        lock.lockWrite {
            val ret = get(recid, serializer)
            delete(recid, serializer)
            return ret
        }
    }

    override fun close() {
        raf.close()
    }

    override fun verify() {
    }

    override fun commit() {
    }

    override fun compact() {
        if (raf.length() <= 8) {
            return
        }

        val newFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp", file.subpath(0, file.nameCount).toFile())
        val raf2 = RandomAccessFile(
            newFile,
            "rw"
        )

        raf2.writeLong(0)
        val newOffsets = Long2LongRBTreeMap()
        //Copy as much as possible while not holding the lock
        var pos = 8L
        pos = compactCopy(raf2, newOffsets, pos)

        //Try to copy anything new after getting the lock to synchronize the files.
        //This should be much faster than the first
        lock.lockWrite {
            compactCopy(raf2, newOffsets, pos)
            val success = newFile.renameTo(file.toFile())
            this.raf = raf2
            this.offsets = newOffsets
        }
    }

    private fun compactCopy(raf2: RandomAccessFile, newOffsets: MutableMap<Long, Long>, startPos: Long): Long {
        var pos = startPos
        while (pos < raf2.length()) {
            val offset = pos
            try {
                val (recid, size) = readRecidSize(offset)
                pos += 12 + size.coerceAtLeast(0)

                if (recid != TOMBSTONE_RECID) {
                    when {
                        size == 0 && newOffsets.containsKey(recid) -> {
                            //This case should only happen when the current file has a delete while compact is running,
                            //and compact has already added the value before the tombstoning
                            val newOffset = newOffsets[recid]!!
                            tombStoneRecord(raf2, newOffset)
                        }
                        //TODO should just be else
                        size > 0 -> {
                            //TODO assert that the recid exists?
                            val record = readRecord(12 + offset, size)
                            newOffsets[recid] = appendRecord(raf2, recid, record)
                        }
                    }
                }
            } catch (e: EOFException) {
                break
            }
        }
        return pos
    }

    override fun isEmpty(): Boolean {
        lock.lockWrite {
            return offsets.size == 0
        }
    }

    companion object {
        val NULL_VALUE = ByteArray(0)
        const val TOMBSTONE_RECID = 0L
    }
}
