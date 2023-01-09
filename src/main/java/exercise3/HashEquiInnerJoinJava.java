package exercise3;

import java.util.ArrayList;
import java.util.List;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.exercise3.InnerJoinOperation;
import de.hpi.dbs2.exercise3.JoinAttributePair;

import org.jetbrains.annotations.NotNull;

@ChosenImplementation(true)
public class HashEquiInnerJoinJava extends InnerJoinOperation {

	public HashEquiInnerJoinJava(
		@NotNull BlockManager blockManager, int leftColumnIndex, int rightColumnIndex
	) {
		super(blockManager, new JoinAttributePair.EquiJoinAttributePair(leftColumnIndex, rightColumnIndex));
	}

	@Override
	public int estimatedIOCost(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation
	) {
		throw new UnsupportedOperationException();
	}

    private List<List<Block>> partitionRelation(int bucketCount, boolean swapped, Relation relation) {
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

        int columnIndexToHash = swapped ? joinAttributePair.getRightColumnIndex() : joinAttributePair.getLeftColumnIndex();

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

        boolean swapped = leftInputRelation.estimatedBlockCount() > rightInputRelation.estimatedBlockCount();
        Relation smallerRelation = swapped ? rightInputRelation : leftInputRelation;
        Relation largerRelation = swapped ? leftInputRelation : rightInputRelation;

        // Check for valid relation size using the smaller relation
        // Note that another 1 is subtracted because we need an extra output buffer block when joining later on
        if (smallerRelation.estimatedBlockCount() > (bucketCount - 1) * (bucketCount - 1)) {
            throw new RelationSizeExceedsCapacityException();
        }

        List<List<Block>> smallerRelationBuckets = partitionRelation(bucketCount, swapped, smallerRelation);
        List<List<Block>> largerRelationBuckets = partitionRelation(bucketCount, swapped, largerRelation);

        // TODO:
		//  - join hashed blocks
	}
}
