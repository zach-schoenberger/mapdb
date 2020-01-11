package org.mapdb.btree

import org.junit.Assert
import org.junit.Test
import org.junit.jupiter.api.BeforeEach
import org.mapdb.ser.Serializers
import org.mapdb.store.StoreAppend
import java.io.File
import java.util.UUID
import kotlin.random.Random

class LongMapTest {

    val store = StoreAppend(File.createTempFile(UUID.randomUUID().toString(), null).toPath())
    val map = LongMap<String>(store, 0, Serializers.STRING)

    @BeforeEach
    fun setup() {

    }

    @Test
    fun `insert and get`() {
        val entries = (1..1000).map {
            val key = Random.nextLong()
            val value = UUID.randomUUID().toString()
            map[key] = value
            key to value
        }

        Assert.assertTrue(entries.size == 1000)

        for (entry in entries) {
            Assert.assertEquals(entry.second, map[entry.first])
        }
    }

    @Test
    fun `insert, delete, and get`() {
        val entries = (1..1000).map {
            val key = Random.nextLong()
            val value = UUID.randomUUID().toString()
            map[key] = value
            key to value
        }

        Assert.assertTrue(entries.size == 1000)

        for (i in 0..10) {
            map.remove(entries[i].first)
        }

        for (i in entries.indices) {
            when (i) {
                in 0..10 -> Assert.assertEquals(null, map[entries[i].first])
                else -> Assert.assertEquals(entries[i].second, map[entries[i].first])
            }
        }
    }

    @Test
    fun `insert, delete, re-insert, and get`() {
        val entries = (1..1000).map {
            val key = Random.nextLong()
            val value = UUID.randomUUID().toString()
            map[key] = value
            key to value
        }.toMutableList()

        Assert.assertTrue(entries.size == 1000)

        for (i in 0..10) {
            map.remove(entries[i].first)
        }

        for (i in 0..10) {
            val value = UUID.randomUUID().toString()
            map[entries[i].first] = value
            entries[i] = entries[i].first to value
        }

        for (i in entries.indices) {
            Assert.assertEquals(entries[i].second, map[entries[i].first])
        }
    }

    @Test
    fun `insert, delete, compact, and get`() {
        val entries = (1..1000).map {
            val key = Random.nextLong()
            val value = UUID.randomUUID().toString()
            map[key] = value
            key to value
        }

        Assert.assertTrue(entries.size == 1000)

        for (i in 0..10) {
            map.remove(entries[i].first)
        }

        store.compact()

        for (i in entries.indices) {
            when (i) {
                in 0..10 -> Assert.assertEquals(null, map[entries[i].first])
                else -> Assert.assertEquals(entries[i].second, map[entries[i].first])
            }
        }
    }
}
