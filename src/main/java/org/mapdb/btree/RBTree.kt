package org.mapdb.btree

import java.io.DataInput
import java.io.DataOutput
import java.io.RandomAccessFile
import java.util.Comparator
import java.util.NavigableMap
import java.util.NavigableSet
import java.util.SortedMap

class Long2LongRBTreeMap(val fileName: String) : AbstractMutableMap<Long, Long>(), NavigableMap<Long, Long> {
    val raf = RandomAccessFile(fileName, "rw")
    var currentIndex = 0L
    var cnt = 0L
    var currentRootIndex: Long = 0L

    override val size: Int
        get() {
            return cnt.toInt()
            // raf.seek(RECORD_CNT_OFFSET)
            // return raf.readLong().toInt()
        }

    private fun getNextIndex() = currentIndex++

    override fun put(key: Long, value: Long): Long? {
        val node = insertNode(Node(key, value))
        return when (node) {
            null -> {
                cnt++
                null
            }
            else -> {
                node.value
            }
        }
    }

    private fun insertNode(node: Node): Node? {
        var root = getRootNode()

        if (root == null) {
            node.isBlack = true
            node.index = getNextIndex()
            node.writeNode()
            currentRootIndex = node.index
            return null
        }

        while (true) {
            when (root!!.compareTo(node)) {
                -1 -> {
                    if (root.rightNodeIndex == NULL_INDEX) {
                        node.parentNodeIndex = root.index
                        node.index = getNextIndex()
                        node.writeNode()
                        root.rightNodeIndex = node.index
                        root.writeNode()
                        ft(node.index)
                        return null
                    }
                    root = getNode(root.rightNodeIndex)!!
                }
                1 -> {
                    if (root.leftNodeIndex == NULL_INDEX) {
                        node.parentNodeIndex = root.index
                        node.index = getNextIndex()
                        node.writeNode()
                        root.leftNodeIndex = node.index
                        root.writeNode()
                        ft(node.index)
                        return null
                    }
                    root = getNode(root.leftNodeIndex)!!
                }
                0 -> {
                    root = root.copy(value = node.value)
                    root.writeNode()
                    return root
                }
            }
        }
    }

    private fun ft(nodeIndex: Long) {
        val currentNode = getNode(nodeIndex)!!

        if (currentNode.parentNodeIndex == NULL_INDEX) {
            if (!currentNode.isBlack) {
                currentNode.isBlack = true
                currentNode.writeNode()
            }
            return
        }

        val parentNode = getNode(currentNode.parentNodeIndex)!!
        if (parentNode.isBlack) {
            return
        }

        val gpNode = getNode(parentNode.parentNodeIndex)
        if (gpNode != null) {
            val uncleNode = getUncle(parentNode, gpNode)
            if (uncleNode != null && !uncleNode.isBlack()) {
                swapColors(gpNode, parentNode, uncleNode)
                ft(gpNode.index)
            } else {
                val cNode = innerRotation(currentNode.index, parentNode.index, gpNode.index) ?: return
                outerRotation(cNode)
            }
        }
    }

    fun innerRotation(currentNodeIndex: Long, parentNodeIndex: Long, gpNodeIndex: Long): Long {
        val currentNode = getNode(currentNodeIndex)!!
        val parentNode = getNode(parentNodeIndex)!!
        val gpNode = getNode(gpNodeIndex)!!
        return when {
            currentNode.index == parentNode.rightNodeIndex && parentNode.index == gpNode.leftNodeIndex -> {
                rl(parentNode.index)
                getNode(getNode(currentNode.index)!!.leftNodeIndex)!!.index
            }
            currentNode.index == parentNode.leftNodeIndex && parentNode.index == gpNode.rightNodeIndex -> {
                rr(parentNode.index)
                getNode(getNode(currentNode.index)!!.rightNodeIndex)!!.index
            }
            else -> currentNode.index
        }
    }

    fun outerRotation(currentNodeIndex: Long) {
        outerRotationA(currentNodeIndex)
    }

    fun outerRotationA(currentNodeIndex: Long) {
        val currentNode = getNode(currentNodeIndex)!!
        val parentNode = getNode(currentNode.parentNodeIndex)!!
        val gpNode = getNode(parentNode.parentNodeIndex)!!
        val parentIndex = parentNode.index
        val gpIndex = gpNode.index

        val n = when (currentNode.index) {
            parentNode.leftNodeIndex -> {
                rr(gpNode.index)
                getNode(gpNode.index)!!.leftNodeIndex
            }
            parentNode.rightNodeIndex -> {
                rl(gpNode.index)
                getNode(gpNode.index)!!.rightNodeIndex
            }
            else -> currentNodeIndex
        }

        outerRotationB(parentIndex, gpIndex)
    }

    fun outerRotationB(parentNodeIndex: Long, gpIndex: Long) {
        val parentNode = getNode(parentNodeIndex)!!
        val gpNode = getNode(gpIndex)!!

        parentNode.isBlack = true
        gpNode.isBlack = false

        parentNode.writeNode()
        gpNode.writeNode()
    }

    fun swapColors(gpNode: Node, parentNode: Node, uncleNode: Node) {
        gpNode.isBlack = false
        parentNode.isBlack = true
        uncleNode.isBlack = true

        gpNode.writeNode()
        parentNode.writeNode()
        uncleNode.writeNode()
    }

    fun getUncle(parentNode: Node, gpNode: Node): Node? {
        return when (parentNode.index) {
            gpNode.leftNodeIndex -> {
                getNode(gpNode.rightNodeIndex)
            }
            gpNode.rightNodeIndex -> {
                getNode(gpNode.leftNodeIndex)
            }
            else -> null
        }
    }

    fun rr(nodeIndex: Long) {
        val node = getNode(nodeIndex)!!
        val leftNode = getNode(node.leftNodeIndex)!!
        val leftRightNode = getNode(leftNode.rightNodeIndex)
        val parentNode = getNode(node.parentNodeIndex)

        node.leftNodeIndex = leftNode.rightNodeIndex
        leftNode.rightNodeIndex = node.index
        leftNode.parentNodeIndex = node.parentNodeIndex
        node.parentNodeIndex = leftNode.index

        leftRightNode?.parentNodeIndex = node.index

        if (parentNode != null) {
            when (node.index) {
                parentNode.leftNodeIndex -> {
                    parentNode.leftNodeIndex = leftNode.index
                }
                parentNode.rightNodeIndex -> {
                    parentNode.rightNodeIndex = leftNode.index
                }
            }
        } else {
            currentRootIndex = leftNode.index
        }

        node.writeNode()
        leftNode.writeNode()
        leftRightNode?.writeNode()
        parentNode?.writeNode()
    }

    fun rl(nodeIndex: Long) {
        val node = getNode(nodeIndex)!!
        val rightNode = getNode(node.rightNodeIndex)!!
        val rightLeftNode = getNode(rightNode.leftNodeIndex)
        val parentNode = getNode(node.parentNodeIndex)

        node.rightNodeIndex = rightNode.leftNodeIndex
        rightNode.leftNodeIndex = node.index
        rightNode.parentNodeIndex = node.parentNodeIndex
        node.parentNodeIndex = rightNode.index

        rightLeftNode?.parentNodeIndex = node.index

        if (parentNode != null) {
            when (node.index) {
                parentNode.leftNodeIndex -> {
                    parentNode.leftNodeIndex = rightNode.index
                }
                parentNode.rightNodeIndex -> {
                    parentNode.rightNodeIndex = rightNode.index
                }
            }
        } else {
            currentRootIndex = rightNode.index
        }

        node.writeNode()
        rightNode.writeNode()
        rightLeftNode?.writeNode()
        parentNode?.writeNode()
    }

    // override fun iterator(): MutableIterator<Long> {
    //     return object : MutableIterator<Long> {
    //         val nodeQueue = LinkedList<Node>()
    //
    //         init {
    //             var currentNode = getRootNode()
    //             while(currentNode != null) {
    //                 nodeQueue.push(currentNode)
    //                 currentNode = getNode(currentNode.leftNodeIndex)
    //             }
    //         }
    //
    //         override fun hasNext(): Boolean {
    //             return nodeQueue.size > 0
    //         }
    //
    //         override fun next(): Long {
    //             val ret = nodeQueue.pop()
    //             var currentNode = getNode(ret.rightNodeIndex)
    //             while (currentNode != null) {
    //                 nodeQueue.push(currentNode)
    //                 currentNode = getNode(currentNode!!.leftNodeIndex)
    //             }
    //
    //             return ret.value
    //         }
    //
    //         override fun remove() {
    //             TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    //         }
    //     }
    // }

    fun print() {
        printInOrder(getRootNode()!!, 0)
        println()
    }

    fun printInOrder(node: Node, spaceCnt: Int) {
        getNode(node.rightNodeIndex)?.let { printInOrder(it, spaceCnt + 3) }
        repeat(spaceCnt) { print(" ") }
        println("${"%2d".format(node.value)}${if (node.isBlack) "B" else "R"}")
        getNode(node.leftNodeIndex)?.let { printInOrder(it, spaceCnt + 3) }
    }

    private fun Node.writeNode() {
        raf.seek(getOffset(this.index))
        this.write(raf)
    }

    private fun getRootNode(): Node? {
        if (cnt == 0L)
            return null
        // return getNode(0)
        return getNode(currentRootIndex)
    }

    private fun getNode(index: Long?): Node? {
        if (index == null || index == NULL_INDEX) {
            return null
        }
        raf.seek(getOffset(index))
        return Node.read(raf)
    }

    private fun getOffset(index: Long) = index * Node.nodeSize()

    companion object {
        const val RECORD_CNT_OFFSET = 0L
        const val ROOT_NODE_OFFSET = Long.SIZE_BYTES.toLong()
        const val NULL_INDEX = Long.MAX_VALUE
    }

    override val entries: MutableSet<MutableMap.MutableEntry<Long, Long>>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.

    override fun floorKey(key: Long?): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun floorEntry(key: Long?): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lastKey(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun higherEntry(key: Long?): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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
    ): NavigableMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun subMap(fromKey: Long?, toKey: Long?): SortedMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun tailMap(fromKey: Long?, inclusive: Boolean): NavigableMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun tailMap(fromKey: Long?): SortedMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pollLastEntry(): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun headMap(toKey: Long?, inclusive: Boolean): NavigableMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun headMap(toKey: Long?): SortedMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun pollFirstEntry(): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun descendingMap(): NavigableMap<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lastEntry(): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun comparator(): Comparator<in Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lowerKey(key: Long?): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ceilingEntry(key: Long?): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun firstEntry(): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun lowerEntry(key: Long?): MutableMap.MutableEntry<Long, Long> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ceilingKey(key: Long?): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun firstKey(): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun higherKey(key: Long?): Long {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

data class Node(
    val key: Long,
    val value: Long,
    var index: Long = Long2LongRBTreeMap.NULL_INDEX,
    var isBlack: Boolean = false,
    var parentNodeIndex: Long = Long2LongRBTreeMap.NULL_INDEX,
    var leftNodeIndex: Long = Long2LongRBTreeMap.NULL_INDEX,
    var rightNodeIndex: Long = Long2LongRBTreeMap.NULL_INDEX
) : Comparable<Node> {

    fun write(dataOutput: DataOutput) {
        dataOutput.writeLong(index)
        dataOutput.writeLong(key)
        dataOutput.writeLong(value)
        dataOutput.writeLong(parentNodeIndex)
        dataOutput.writeLong(leftNodeIndex)
        dataOutput.writeLong(rightNodeIndex)
        dataOutput.writeChar(if (isBlack) 1 else 0)
    }

    companion object {
        fun read(dataInput: DataInput): Node? {
            val index = dataInput.readLong()
            if (index == Long2LongRBTreeMap.NULL_INDEX) {
                return null
            }
            val key = dataInput.readLong()
            val value = dataInput.readLong()
            val parentNode = dataInput.readLong()
            val leftNode = dataInput.readLong()
            val rightNode = dataInput.readLong()
            val isBlack = dataInput.readChar() == 1.toChar()
            return Node(
                key = key,
                value = value,
                index = index,
                isBlack = isBlack,
                leftNodeIndex = leftNode,
                parentNodeIndex = parentNode,
                rightNodeIndex = rightNode
            )
        }

        fun nodeSize(): Long {
            return Char.SIZE_BYTES.toLong() + (6 * Long.SIZE_BYTES.toLong())
        }
    }

    override fun compareTo(other: Node): Int {
        return key.compareTo(other.key)
    }
}

fun Node?.isBlack(): Boolean {
    return this?.isBlack != false
}
