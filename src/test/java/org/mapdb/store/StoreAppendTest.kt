package org.mapdb.store

import org.junit.Assert
import org.junit.Test
import org.mapdb.ser.Serializers
import java.io.File
import java.util.UUID
import kotlin.random.Random

// class StoreAppendTest : StoreTest() {
//
//     override fun openStore(): MutableStore {
//         val f = File.createTempFile("mapdb","adasdsa")
//         f.delete()
//
//         return StoreAppend(file=f.toPath())
//     }
//
// }

class StoreAppendTest {

    @Test
    fun `RA test channel`() {
        val f = File.createTempFile("mapdb","adasdsa")
        val store = StoreAppendChannel(f.toPath(), true)
        loadTestStore(store)
    }

    @Test
    fun `RA test file`() {
        val f = File.createTempFile("mapdb","adasdsa")
        val store = StoreAppendFile(f.toPath(), true)
        loadTestStore(store)
    }

    fun loadTestStore(store: MutableStore) {
        val numRecords = 50_000_000

        val insertStartTime = System.currentTimeMillis()
        var minRecid = Long.MAX_VALUE
        var maxRecid = Long.MIN_VALUE
        (1..numRecords).forEach {
            val value = UUID.randomUUID().toString()
            val key = store.put(value, Serializers.STRING)
            minRecid = Math.min(key, minRecid)
            maxRecid = Math.max(key, maxRecid)
        }
        val insertStopTime = System.currentTimeMillis()

        println("inserting $numRecords took ${insertStopTime - insertStartTime}")

        val raStartTime = System.currentTimeMillis()
        val numSelects = 10_000_000
        for ( i in 1..numSelects) {
            val index = Random.nextLong(minRecid, maxRecid)
            val x = store.get(index, Serializers.STRING)
        }
        val raEndTime = System.currentTimeMillis()

        println("selecting $numSelects took ${raEndTime - raStartTime}")
    }
}
