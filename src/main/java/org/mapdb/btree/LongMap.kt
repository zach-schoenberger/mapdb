package org.mapdb.btree

import it.unimi.dsi.fastutil.longs.Long2LongRBTreeMap
import it.unimi.dsi.fastutil.longs.LongComparators
import org.mapdb.ser.Serializer
import org.mapdb.store.MutableStore
import java.util.Comparator
import java.util.NavigableMap
import java.util.NavigableSet
import java.util.SortedMap

class LongMap<V>(
    // private val keyStore: MutableStore,
    private val valueStore: MutableStore,
    private val rootRecid: Long,
    private val valSer: Serializer<V>
) : NavigableMap<Long, V>, AbstractMutableMap<Long, V>() {

    //    private val recidMap = Long2LongRBTreeMap()
    private val recidMap = Long2LongRBTreeMap()

    override val size: Int
        get() = recidMap.size

    override fun containsKey(key: Long): Boolean {
        return recidMap.containsKey(key)
    }

    override fun containsValue(value: V): Boolean {
        throw NotImplementedError()
    }

    override fun get(key: Long): V? {
        val recid = recidMap[key]
        return when (recid) {
            0L -> null
            else -> valueStore.get(recid, valSer)
        }
    }

    override fun clear() {
        recidMap.forEach { (_, u) ->
            valueStore.delete(u, valSer)
        }
        recidMap.clear()
    }

    override fun remove(key: Long): V? {
        val ret = get(key) ?: return null
        val recid = recidMap.remove(key)
        valueStore.delete(recid, valSer)

        return ret
    }

    override val entries: MutableSet<MutableMap.MutableEntry<Long, V>>
        get() = object : java.util.AbstractSet<MutableMap.MutableEntry<Long, V>>() {
            val keys = recidMap.keys
            var lastKey: Long? = null
            var lastValue: V? = null

            override val size: Int
                get() = keys.size

            override fun iterator(): MutableIterator<MutableMap.MutableEntry<Long, V>> {
                return object : MutableIterator<MutableMap.MutableEntry<Long, V>> {
                    val iter = keys.iterator()
                    override fun hasNext(): Boolean {
                        return iter.hasNext()
                    }

                    override fun next(): MutableMap.MutableEntry<Long, V> {
                        val key = iter.nextLong()
                        val value = this@LongMap[key] ?: throw NoSuchElementException()
                        lastKey = key
                        lastValue = value
                        return MapEntry(key, value, this@LongMap)
                    }

                    override fun remove() {
                        when (lastKey) {
                            null -> throw NoSuchElementException()
                            else -> {
                                this@LongMap.recidMap.remove(lastKey!!)
                                this@LongMap.remove(lastKey!!)
                            }
                        }
                    }
                }
            }
        }

    override fun put(key: Long, value: V): V? {
        val recid = recidMap.get(key)
        return when (recid) {
            0L -> {
                recidMap[key] = valueStore.put(value, valSer)
                null
            }
            else -> {
                valueStore.getAndUpdate(recid, valSer, value)
            }
        }
    }

    private inner class MapEntry<K, V>(
        override val key: K,
        override var value: V,
        val backingMap: AbstractMutableMap<K, V>
    ) : MutableMap.MutableEntry<K, V> {
        override fun setValue(newValue: V): V {
            val oldValue = value
            value = newValue
            backingMap[key] = newValue
            return oldValue
        }
    }

    override fun floorKey(key: Long): Long? {
        return when {
            recidMap.containsKey(key) -> return key
            else -> {
                val headMap = recidMap.headMap(key)
                if (headMap.size > 0) {
                    headMap.lastLongKey()
                } else {
                    null
                }
            }
        }
    }

    override fun floorEntry(key: Long): MutableMap.MutableEntry<Long, V>? {
        val floorKey = floorKey(key) ?: return null
        val value = this[floorKey] ?: return null
        return MapEntry(floorKey, value, this)
    }

    override fun lastKey(): Long = recidMap.lastLongKey()

    override fun higherEntry(key: Long): MutableMap.MutableEntry<Long, V>? {
        return higherKey(key)?.let {
            val v = get(key) ?: return null
            MapEntry(it, v, this)
        }
    }

    override fun descendingKeySet(): NavigableSet<Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun navigableKeySet(): NavigableSet<Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun subMap(
        fromKey: Long?,
        fromInclusive: Boolean,
        toKey: Long?,
        toInclusive: Boolean
    ): NavigableMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun subMap(fromKey: Long?, toKey: Long?): SortedMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun tailMap(fromKey: Long?, inclusive: Boolean): NavigableMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun tailMap(fromKey: Long?): SortedMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pollLastEntry(): MutableMap.MutableEntry<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun headMap(toKey: Long?, inclusive: Boolean): NavigableMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun headMap(toKey: Long?): SortedMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pollFirstEntry(): MutableMap.MutableEntry<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun descendingMap(): NavigableMap<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lastEntry(): MutableMap.MutableEntry<Long, V>? {
        return when {
            recidMap.isEmpty() -> null
            else -> {
                val key = recidMap.lastLongKey()
                val value = valueStore.get(recidMap.get(key), valSer) ?: throw NoSuchElementException()
                MapEntry(key, value, this)
            }
        }
    }

    override fun comparator(): Comparator<in Long> = LongComparators.NATURAL_COMPARATOR

    override fun lowerKey(key: Long): Long? {
        val iter = recidMap.keys.iterator(key)
        return when {
            iter.hasPrevious() -> iter.previousLong()
            else -> null
        }
    }

    override fun ceilingEntry(key: Long?): MutableMap.MutableEntry<Long, V> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun firstEntry(): MutableMap.MutableEntry<Long, V>? {
        return when {
            recidMap.isEmpty() -> null
            else -> {
                val key = recidMap.firstLongKey()
                val value = valueStore.get(recidMap.get(key), valSer) ?: throw NoSuchElementException()
                MapEntry(key, value, this)
            }
        }
    }

    override fun lowerEntry(key: Long): MutableMap.MutableEntry<Long, V>? {
        return lowerKey(key)?.let {
            val v = get(key) ?: return null
            MapEntry(it, v, this)
        }
    }

    override fun ceilingKey(key: Long?): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun firstKey(): Long = recidMap.firstLongKey()

    override fun higherKey(key: Long): Long? {
        val iter = recidMap.keys.iterator(key)
        return when {
            iter.hasNext() -> iter.nextLong()
            else -> null
        }
    }
}
