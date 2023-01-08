package exercise1;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.Block;
import de.hpi.dbs2.dbms.BlockManager;
import de.hpi.dbs2.dbms.BlockOutput;
import de.hpi.dbs2.dbms.Relation;
import de.hpi.dbs2.dbms.Tuple;
import de.hpi.dbs2.dbms.utils.BlockSorter;
import de.hpi.dbs2.exercise1.SortOperation;
import org.jetbrains.annotations.NotNull;

@ChosenImplementation(true)
public class TPMMSJava extends SortOperation {
    public TPMMSJava(@NotNull BlockManager manager, int sortColumnIndex) {
        super(manager, sortColumnIndex);
    }

    @Override
    public int estimatedIOCost(@NotNull Relation relation) {
        return relation.getEstimatedSize() * 4;
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput outputRelation) {
        BlockManager blockManager = getBlockManager();

        // Check for valid relation size
        if (relation.getEstimatedSize() > blockManager.getFreeBlocks() * (blockManager.getFreeBlocks() - 1)) {
            throw new RelationSizeExceedsCapacityException();
        }

        Comparator<Tuple> tupleComparator = relation.getColumns().getColumnComparator(getSortColumnIndex());

        List<Block> blocksInMemory = new LinkedList<>();
        // We know it's not that optimal to use a 2D-List, but we cant be certain whether the block is saved back to the same place on disk
        List<List<Block>> phaseOneLists = new LinkedList<>();

        // Phase 1
        for (Iterator<Block> blockIterator = relation.iterator(); blockIterator.hasNext();) {
            // Assumption: getFreeBlocks() > 0 at the start

            Block blockOnDisk = blockIterator.next(); // reference on disk
            Block blockInMemory = blockManager.load(blockOnDisk); // load into main memory
            blocksInMemory.add(blockInMemory); // temporarily save

            if (blockManager.getFreeBlocks() == 0 || !blockIterator.hasNext()) {
                BlockSorter.INSTANCE.sort(relation, blocksInMemory, tupleComparator); // in-place


                List<Block> savedSortedBlocks = blocksInMemory
                        .stream()
                        .map(sortedBlockInMemory -> blockManager.release(sortedBlockInMemory, true))
                        .collect(Collectors.toList());
                phaseOneLists.add(savedSortedBlocks);

                // Clear list of currently saved blocks in memory for next sort-iteration inside phase 1
                blocksInMemory.clear();
            }
        }

        // Phase 2
        Block outputBlock = blockManager.allocate(true);

        PriorityQueue<Tuple> tuplePriorityQueue = new PriorityQueue<>(tupleComparator);
        Map<Block, List<Block>> blockToPhaseOneList = new HashMap<>();
        // Unfortunately, there is no `remove` on the Block interface :(
        // So we need to track the current size of the block by ourselves
        // in order to correctly detect that a block is "half full" for example.
        Map<Block, Integer> blockToCurrentSize = new HashMap<>();
        Map<Tuple, Block> tupleToBlock = new HashMap<>();

        Consumer<List<Block>> loadFirstBlockOfList = phaseOneList -> {
            if (phaseOneList.isEmpty()) return;

            Block loadedFirstBlock = blockManager.load(phaseOneList.remove(0));

            if (loadedFirstBlock.isEmpty()) {
                // should not happen though
                blockManager.release(loadedFirstBlock, false);
                return;
            }

            blockToPhaseOneList.put(loadedFirstBlock, phaseOneList);
            blockToCurrentSize.put(loadedFirstBlock, loadedFirstBlock.getSize());

            for (Tuple tuple : loadedFirstBlock) {
                tuplePriorityQueue.add(tuple);
                tupleToBlock.put(tuple, loadedFirstBlock);
            }
        };

        // Initial loading with first blocks for each phase one list
        phaseOneLists.forEach(loadFirstBlockOfList);

        while (!tuplePriorityQueue.isEmpty()) {
            Tuple headTuple = tuplePriorityQueue.remove();
            outputBlock.append(headTuple);

            if (outputBlock.isFull()) outputRelation.output(outputBlock);

            Block sourceBlock = tupleToBlock.remove(headTuple);
            blockToCurrentSize.compute(sourceBlock, (Block block, Integer prevBlockSize) -> prevBlockSize - 1);

            if (blockToCurrentSize.get(sourceBlock) == 0) {
                List<Block> phaseOneList = blockToPhaseOneList.remove(sourceBlock);

                blockToCurrentSize.remove(sourceBlock);
                blockManager.release(sourceBlock, false);

                // Load next block of used phase one list into memory
                loadFirstBlockOfList.accept(phaseOneList);
            }
        }

        if (!outputBlock.isEmpty()) outputRelation.output(outputBlock);
        blockManager.release(outputBlock, false);
        phaseOneLists.clear();
    }
}

