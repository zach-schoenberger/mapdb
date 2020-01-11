package org.mapdb.store

import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap
import org.mapdb.io.DataOutput2ByteArray
import org.mapdb.ser.Serializer
import java.io.RandomAccessFile
import java.util.concurrent.atomic.AtomicLong

//TODO implement this to be mainly used to keep track of recid
class StoreFile(
    val fileName: String
) : MutableStore {
    private val file = RandomAccessFile(fileName, "rw")
    private val recid = AtomicLong(0)
    private val offsets = LongLongHashMap()

    override fun preallocate(): Long {
        return recid.getAndAdd(1)
    }

    override fun <K> put(record: K, serializer: Serializer<K>): Long {
        val out = DataOutput2ByteArray()
        when (record) {
            null -> {
            }
        }
        // serializer.serialize(out, record)
        return 0L
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, newRecord: K) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> update(recid: Long, serializer: Serializer<K>, m: (K) -> K) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> compareAndUpdate(
        recid: Long,
        serializer: Serializer<K>,
        expectedOldRecord: K,
        newRecord: K
    ): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> compareAndDelete(recid: Long, serializer: Serializer<K>, expectedOldRecord: K): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> delete(recid: Long, serializer: Serializer<K>) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun verify() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun commit() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override val isThreadSafe: Boolean
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun compact() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> getAndDelete(recid: Long, serializer: Serializer<K>): K {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun <K> get(recid: Long, serializer: Serializer<K>): K {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun getAll(consumer: (Long, ByteArray?) -> Unit) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun isEmpty(): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
