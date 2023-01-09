package exercise3;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.exercise3.InnerJoinOperation;
import de.hpi.dbs2.exercise3.JoinAttributePair;

import org.jetbrains.annotations.NotNull;

@ChosenImplementation(true)
public class HashEquiInnerJoinJava extends InnerJoinOperation {

	public HashEquiInnerJoinJava(@NotNull BlockManager blockManager, int leftColumnIndex, int rightColumnIndex) {
		super(blockManager, new JoinAttributePair.EquiJoinAttributePair(leftColumnIndex, rightColumnIndex));
	}

	@Override
	public int estimatedIOCost(@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation) {
        return 3 * (leftInputRelation.estimatedBlockCount() + rightInputRelation.estimatedBlockCount());
	}

    private List<List<Block>> partitionRelation(Relation relation, int bucketCount, boolean isLeftRelation) {
        List<List<Block>> relationBuckets = new ArrayList<>(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            relationBuckets.add(new ArrayList<>());
        }

        BlockManager blockManager = getBlockManager();
        JoinAttributePair joinAttributePair = getJoinAttributePair();

        List<Block> bucketRepresentatives = new ArrayList<>(bucketCount);
        for (int i = 0; i < bucketCount; i++) {
            Block bucketBlock = blockManager.allocate(true);
            bucketRepresentatives.add(bucketBlock);
        }

        int columnIndexToHash = isLeftRelation ? joinAttributePair.getLeftColumnIndex() : joinAttributePair.getRightColumnIndex();

        for (Block blockReference : relation) {
            // Read the next block of the relation into main memory
            Block loadedBlock = blockManager.load(blockReference);

            for (Tuple tuple : loadedBlock) {
                // Use modulo blockCount as hash function
                int hash = Math.abs(tuple.get(columnIndexToHash).hashCode()) % bucketCount;
                Block currentBucketRepresentative = bucketRepresentatives.get(hash);

                if (!currentBucketRepresentative.isFull()) {
                    currentBucketRepresentative.append(tuple);
                } else {
                    // Write full block of bucket to disk and save reference to it for later use
                    relationBuckets.get(hash).add(blockManager.release(currentBucketRepresentative, true));
                    // Allocate new bucket representative block and append to it
                    Block newBucketRepresentative = blockManager.allocate(true);
                    newBucketRepresentative.append(tuple);
                    // Update the bucket representative in our temp structure
                    bucketRepresentatives.set(hash, newBucketRepresentative);
                }
            }

            // Make space for next block of relation to read in
            blockManager.release(loadedBlock, false);
        }

        // Write rest of buckets to disk
        for (int i = 0; i < bucketCount; i++) {
            Block bucketRepresentative = bucketRepresentatives.get(i);
            if (!bucketRepresentative.isEmpty()) {
                relationBuckets.get(i).add(blockManager.release(bucketRepresentative, true));
            }
        }

        return relationBuckets;
    }

	@Override
	public void join(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation,
		@NotNull Relation outputRelation
	) {
        BlockManager blockManager = getBlockManager();

        // 1 block for reading in the relations
        int bucketCount = blockManager.getFreeBlocks() - 1;

        boolean isLeftRelationSmaller = leftInputRelation.estimatedBlockCount() <= rightInputRelation.estimatedBlockCount();
        Relation smallerRelation = isLeftRelationSmaller ? leftInputRelation : rightInputRelation;
        Relation largerRelation = isLeftRelationSmaller ? rightInputRelation : leftInputRelation;

        // Check for valid relation size using the smaller relation
        // Note that another 1 is subtracted because we need an extra output buffer block when joining later on
        if (smallerRelation.estimatedBlockCount() > (bucketCount - 1) * (bucketCount - 1)) {
            throw new RelationSizeExceedsCapacityException();
        }

        List<List<Block>> smallerRelationBuckets = partitionRelation(smallerRelation, bucketCount, isLeftRelationSmaller);
        List<List<Block>> largerRelationBuckets = partitionRelation(largerRelation, bucketCount, !isLeftRelationSmaller);

        TupleAppender tupleAppender = new TupleAppender(outputRelation.getBlockOutput());
        for (int i = 0; i < bucketCount; i++) {
            List<Block> smallerRelationBucket = smallerRelationBuckets.get(i);
            List<Block> largerRelationBucket = largerRelationBuckets.get(i);


            List<Block> loadedSmallerRelationBucket = new ArrayList<>();
            for (Block smallerRelationBucketBlockRef : smallerRelationBucket) {
                // Load all blocks of the bucket of the smaller relation in main memory
                loadedSmallerRelationBucket.add(blockManager.load(smallerRelationBucketBlockRef));
            }


            for (Block largerRelationBucketBlockRef: largerRelationBucket) {
                // Load the next block of the bucket of the larger relation in main memory
                Block loadedLargerRelationBlock = blockManager.load(largerRelationBucketBlockRef);


                // Do the actual join
                for (Block loadedSmallerRelationBlock : loadedSmallerRelationBucket) {
                    joinBlocks(loadedSmallerRelationBlock, loadedLargerRelationBlock, outputRelation.getColumns(), tupleAppender);
                }

                // Release the loaded block of the bucket of the larger relation
                blockManager.release(loadedLargerRelationBlock, false);
            }

            for (Block loadedSmallerRelationBlock : loadedSmallerRelationBucket) {
                // Release all loaded blocks of the bucket of the smaller relation
                blockManager.release(loadedSmallerRelationBlock, false);
            }
        }
        tupleAppender.close();
	}

    class TupleAppender implements AutoCloseable, Consumer<Tuple> {

        BlockOutput blockOutput;

        TupleAppender(BlockOutput blockOutput) {
            this.blockOutput = blockOutput;
        }

        Block outputBlock = getBlockManager().allocate(true);

        @Override
        public void accept(Tuple tuple) {
            if (outputBlock.isFull()) {
                blockOutput.move(outputBlock);
                outputBlock = getBlockManager().allocate(true);
            }
            outputBlock.append(tuple);
        }

        @Override
        public void close() {
            if (!outputBlock.isEmpty()) {
                blockOutput.move(outputBlock);
            } else {
                getBlockManager().release(outputBlock, false);
            }
        }
    }
}
