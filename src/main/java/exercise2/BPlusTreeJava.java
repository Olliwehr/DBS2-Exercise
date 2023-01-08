package exercise2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Stack;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.exercise2.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This is the B+-Tree implementation you will work on.
 * Your task is to implement the insert-operation.
 *
 */
@ChosenImplementation(true)
public class BPlusTreeJava extends AbstractBPlusTree {
    public BPlusTreeJava(int order) {
        super(order);
    }

    public BPlusTreeJava(BPlusTreeNode<?> rootNode) {
        super(rootNode);
    }

    private void optimisticallyInsertInLeaf(LeafNode leafToInsert, Integer key, ValueReference value) {
        for (int pos = 0; pos < leafToInsert.keys.length; pos++) {
            if (leafToInsert.keys[pos] == null) {
                // We found a free slot in the Node.
                leafToInsert.keys[pos] = key;
                leafToInsert.references[pos] = value;
                return;
            }

            if (leafToInsert.keys[pos] > key) {
                // Key needs to be inserted between other keys in the node --> shift
                Entry entryToShift = new Entry(leafToInsert.keys[pos], leafToInsert.references[pos]);

                for (int shiftPos = pos + 1; shiftPos < leafToInsert.keys.length; shiftPos++) {
                    if (leafToInsert.keys[shiftPos] == null) {
                        leafToInsert.keys[shiftPos] = entryToShift.getKey();
                        leafToInsert.references[shiftPos] = entryToShift.getValue();

                        // Insert on now free pos
                        leafToInsert.keys[pos] = key;
                        leafToInsert.references[pos] = value;
                        return;
                    } else {
                        Entry nextEntryToShift = new Entry(leafToInsert.keys[shiftPos], leafToInsert.references[shiftPos]);
                        leafToInsert.keys[shiftPos] = entryToShift.getKey();
                        leafToInsert.references[shiftPos] = entryToShift.getValue();
                        entryToShift = nextEntryToShift;
                    }
                }
            }
        }
    }

    private void optimisticallyInsertInInner(InnerNode innerToInsert, Integer key, BPlusTreeNode<?> nodeReferenceRight) {
        for (int pos = 0; pos < innerToInsert.keys.length; pos++) {
            if (innerToInsert.keys[pos] == null) {
                // We found a free slot in the node.
                innerToInsert.keys[pos] = key;
                innerToInsert.references[pos + 1] = nodeReferenceRight;
                return;
            }

            if (innerToInsert.keys[pos] > key) {
                // Key needs to be inserted between other keys in the node --> shift
                Integer keyToShift = innerToInsert.keys[pos];
                BPlusTreeNode<?> nodeReferenceToShift = innerToInsert.references[pos + 1];

                for (int shiftPos = pos + 1; shiftPos < innerToInsert.keys.length; shiftPos++) {
                    if (innerToInsert.keys[shiftPos] == null) {
                        innerToInsert.keys[shiftPos] = keyToShift;
                        innerToInsert.references[shiftPos + 1] = nodeReferenceToShift;

                        // Insert on now free pos
                        innerToInsert.keys[pos] = key;
                        innerToInsert.references[pos + 1] = nodeReferenceRight;
                        return;
                    } else {
                        Integer nextKeyToShift = innerToInsert.keys[shiftPos];
                        BPlusTreeNode<?> nextNodeReferenceToShift = innerToInsert.references[shiftPos + 1];
                        innerToInsert.keys[shiftPos] = keyToShift;
                        innerToInsert.references[shiftPos + 1] = nodeReferenceToShift;
                        keyToShift = nextKeyToShift;
                        nodeReferenceToShift = nextNodeReferenceToShift;
                    }
                }
            }
        }
    }

    @Nullable
    @Override
    public ValueReference insert(@NotNull Integer key, @NotNull ValueReference value) {
        // Find LeafNode in which the key has to be inserted.
        // It is a good idea to track the "path" to the LeafNode in a Stack or something alike.
        Stack<BPlusTreeNode<?>> nodePathToLeaf = new Stack<>();
        BPlusTreeNode<?> rootNode = getRootNode();

        LeafNode leafNode = null;
        if (!(rootNode instanceof InnerNode)) {
            leafNode = (LeafNode) rootNode;
        } else {
            BPlusTreeNode<?> currentNode = rootNode;
            while (!(currentNode instanceof LeafNode)) {
                nodePathToLeaf.push(currentNode);
                BPlusTreeNode<?> childOfCurrentNode = ((InnerNode) currentNode).selectChild(key);
                if (childOfCurrentNode instanceof LeafNode) {
                    leafNode = (LeafNode) childOfCurrentNode;
                    break;
                } else {
                    currentNode = childOfCurrentNode;
                }
            }
        }

        // Does the key already exist? Overwrite! But remember to return the old value!
        for (int pos = 0; pos < leafNode.keys.length; pos++) {
            if (leafNode.keys[pos] == key) {
                ValueReference oldValue = leafNode.references[pos];
                leafNode.references[pos] = value;
                return oldValue;
            }
        }

        if (!leafNode.isFull()) {
            optimisticallyInsertInLeaf(leafNode, key, value);
            return null;
        } else {
            // Split the LeafNode in two!
            LeafNode leftLeaf;
            LeafNode rightLeaf = new LeafNode(order);
            if (leafNode instanceof InitialRootNode) {
                // If the leafNode is the InitialRootNode, we cant re-use it as the new left leaf.
                // So create a new one and first, copy all keys and values to this new node.
                leftLeaf = new LeafNode(order);
                for (int i = 0; i < leafNode.keys.length; i++) {
                    leftLeaf.keys[i] = leafNode.keys[i];
                    leftLeaf.references[i] = leafNode.references[i];
                }
            } else {
                // Otherwise, we can re-use it as the new left leaf.
                // This is quite handy, because we then don't need to change the reference to this node
                // in the parent node!
                leftLeaf = leafNode;
            }

            // Set references between (new) leafs
            rightLeaf.nextSibling = leafNode.nextSibling;
            leftLeaf.nextSibling = rightLeaf;

            LeafNode leafToInsert = rightLeaf;
            int splitPos = (int) Math.ceil(order / 2.0);
            // Because we did not insert the value which would cause the overflow in the leafNode yet,
            // splitting at the expected n+1/2 pos will not work everytime.
            if (leftLeaf.keys[splitPos - 1] > key) {
                // We need to insert the key/value pair in the left leaf!
                // So we need to copy more to the right leaf
                splitPos -= 1;
                leafToInsert = leftLeaf;
            }

            // Copy part of the keys and values to right leaf to distribute
            for (int copyToRightPos = splitPos; copyToRightPos < leftLeaf.keys.length; copyToRightPos++) {
                int posInRight = copyToRightPos - splitPos;
                rightLeaf.keys[posInRight] = leftLeaf.keys[copyToRightPos];
                rightLeaf.references[posInRight] = leftLeaf.references[copyToRightPos];
                leftLeaf.keys[copyToRightPos] = null;
                leftLeaf.references[copyToRightPos] = null;
            }

            optimisticallyInsertInLeaf(leafToInsert, key, value);

            // Is parent node root?
            if (leafNode.equals(rootNode)) {
                InnerNode newRoot = new InnerNode(order);
                newRoot.keys[0] = rightLeaf.getSmallestKey();
                newRoot.references[0] = leftLeaf;
                newRoot.references[1] = rightLeaf;
                this.rootNode = newRoot;
                return null;
            }

            // If we arrive here, we know a leaf was split.
            Integer keyToInsertInParent = rightLeaf.getSmallestKey();
            BPlusTreeNode<?> nodeReferenceToInsertInParentRight = rightLeaf;

            while (!nodePathToLeaf.isEmpty()) {
                // Leafs don't get pushed to the stack, and at this point the root node already is am InnerNode.
                InnerNode currentNode = (InnerNode) nodePathToLeaf.pop();
                if (!currentNode.isFull()) {
                    optimisticallyInsertInInner(currentNode, keyToInsertInParent, nodeReferenceToInsertInParentRight);
                    return null;
                } else {
                    // Parent innerNode needs to be split in two
                    // We again want to re-use the currentNode as the new left inner node.
                    InnerNode leftInner = currentNode;

                    // Create right inner node and distribute values
                    InnerNode rightInner = new InnerNode(order);

                    InnerNode innerNodeToInsert = rightInner;
                    int innerNodeSplitPos = (int) Math.ceil(order / 2.0);
                    // Because we did not insert the key/reference which would cause the overflow in the innerNode yet,
                    // splitting at the expected n+1/2 pos will not work everytime.
                    if (leftInner.keys[innerNodeSplitPos - 1] > keyToInsertInParent) {
                        // We need to insert the key/reference pair in the left inner!
                        // So we need to copy more to the right inner node
                        innerNodeSplitPos -= 1;
                        innerNodeToInsert = leftInner;
                    }

                    // Copy part of the keys and references to right to distribute
                    for (int copyToRightPos = innerNodeSplitPos; copyToRightPos < leftInner.references.length; copyToRightPos++) {
                        int posInRight = copyToRightPos - innerNodeSplitPos;
                        if (copyToRightPos < leftInner.keys.length) {
                            rightInner.keys[posInRight] = leftInner.keys[copyToRightPos];
                            leftInner.keys[copyToRightPos] = null;
                        }
                        rightInner.references[posInRight] = leftInner.references[copyToRightPos];
                        leftInner.references[copyToRightPos] = null;
                    }

                    optimisticallyInsertInInner(innerNodeToInsert, keyToInsertInParent, nodeReferenceToInsertInParentRight);

                    // Move the biggest of the left inner node key to parent (part 1: delete the key from the left node)
                    // Part 2 (insert in the parent node) is part of next iteration.
                    keyToInsertInParent = Arrays.stream(leftInner.keys).filter(leftInnerKey -> leftInnerKey != null).max(Comparator.comparing(myInt -> myInt)).get();
                    for (int i = 0; i < leftInner.keys.length; i++) {
                        if (leftInner.keys[i] == keyToInsertInParent) {
                            leftInner.keys[i] = null;
                        }
                    }

                    // Special case: Part 2 is handled here if the node is the current root
                    if (currentNode.equals(rootNode)) {
                        InnerNode newRoot = new InnerNode(order);
                        newRoot.keys[0] = keyToInsertInParent;
                        newRoot.references[0] = leftInner;
                        newRoot.references[1] = rightInner;
                        this.rootNode = newRoot;
                        return null;
                    }

                    // Prepare next iteration (update the parent node of the now split node)
                    nodeReferenceToInsertInParentRight = rightInner;
                }
            }
        }

        return null;
    }
}
