package org.mapdb.btree

import org.junit.Assert
import org.junit.Test

class RBTreeTest {
    @Test
    fun recidTest() {
       Assert.assertEquals(0, temp())
    }

    @Test
    fun rbTest() {
        val tree = Long2LongRBTreeMap("./temp.db")
        (1..100).map { Math.random() * 100 }.map { it.toLong() }.forEach {
            tree[it] = it*2
        }
        tree.print()

        tree.forEach {
            print("$it ")
        }

        // tree.add(3)
        // tree.print()
        // tree.add(1)
        // tree.print()
        // tree.add(5)
        // tree.print()
        // tree.add(6)
        // tree.print()
        // tree.add(4)
        // tree.print()
    }

    var t = 0L
    fun temp(): Long {
        return t++
    }
}
