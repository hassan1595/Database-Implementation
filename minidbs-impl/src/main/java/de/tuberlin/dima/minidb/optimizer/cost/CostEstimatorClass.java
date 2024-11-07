package de.tuberlin.dima.minidb.optimizer.cost;

import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.core.DataTuple;
import de.tuberlin.dima.minidb.core.DataType;
import de.tuberlin.dima.minidb.io.cache.PageSize;
import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.qexec.heap.QueryHeap;
import de.tuberlin.dima.minidb.semantics.Column;
import de.tuberlin.dima.minidb.semantics.predicate.LocalPredicate;

public class CostEstimatorClass implements  CostEstimator{

    private long readCost;
    private long writeCost;
    private long randomReadOverhead;
    private long randomWriteOverhead;


    public CostEstimatorClass(long readCost, long writeCost, long randomReadOverhead, long randomWriteOverhead){
        this.readCost = readCost;
        this.writeCost = writeCost;
        this.randomReadOverhead = randomReadOverhead;
        this.randomWriteOverhead = randomWriteOverhead;
    }
    @Override
    public long computeTableScanCosts(TableDescriptor table) {
        return this.randomReadOverhead + (table.getResourceManager().getPageSize().getNumberOfBytes()/PageSize.getDefaultPageSize().getNumberOfBytes()) * this.readCost * table.getStatistics().getNumberOfPages();
    }

    @Override
    public long computeIndexLookupCosts(IndexDescriptor index, TableDescriptor baseTable, long cardinality) {

        int depth = index.getStatistics().getTreeDepth();
        long traversalCost;
        long readIndexCost = (index.getResourceManager().getPageSize().getNumberOfBytes()/PageSize.getDefaultPageSize().getNumberOfBytes()) * this.readCost;
        if(depth <= 2){
            traversalCost = 0;
        }
        else{
            traversalCost = (long)((depth - 2) * (readIndexCost + this.randomReadOverhead)) + this.randomReadOverhead;
        }
        int numberOfReadLeaves = (int) Math.ceil(((float)cardinality/(float)baseTable.getStatistics().getCardinality()) * index.getStatistics().getNumberOfLeaves());
        return traversalCost +  this.randomReadOverhead + this.readCost* numberOfReadLeaves ;
    }

    @Override
    public long computeSortCosts(Column[] columnsInTuple, long numTuples) {

        DataType[] columnsDataType = new DataType[columnsInTuple.length];

        for(int i = 0; i < columnsDataType.length; i++){
            columnsDataType[i] = columnsInTuple[i].getDataType();
        }
        long tuplesPerBlock = (QueryHeap.getPageSize().getNumberOfBytes() - TablePage.TABLE_DATA_PAGE_HEADER_BYTES)/QueryHeap.getTupleBytes(columnsDataType);
        long numberOfBlocks = numTuples/tuplesPerBlock;
        numberOfBlocks = (numberOfBlocks == 0)?1:numberOfBlocks;
        long writingBlocksCost = numberOfBlocks * (QueryHeap.getPageSize().getNumberOfBytes()/ PageSize.getDefaultPageSize().getNumberOfBytes()) * this.writeCost + this.randomWriteOverhead;
        long readingBlocksCost = numberOfBlocks * ((QueryHeap.getPageSize().getNumberOfBytes()/ PageSize.getDefaultPageSize().getNumberOfBytes()) * this.readCost + this.randomReadOverhead/16);

        return readingBlocksCost + writingBlocksCost;
    }

    @Override
    public long computeFetchCosts(TableDescriptor fetchedTable, long cardinality, boolean sequential) {

        long numberReadPagesFetch = getNumberReadPagesFetch(fetchedTable, cardinality);
        double randomRatio = 1 - ( ((double) numberReadPagesFetch/(double)fetchedTable.getStatistics().getNumberOfPages()) * ((double) 7)/(double) 8);
        if(sequential){

        return numberReadPagesFetch * (this.readCost + (long)(this.randomReadOverhead * randomRatio));

        }
        double randomRatioHit  = Math.log((double) cardinality)/(((double)1/0.66) * Math.log((double) 10 *  fetchedTable.getStatistics().getNumberOfPages()));
        return (long) (cardinality * (1 - randomRatioHit) * (this.readCost + this.randomReadOverhead));
    }

    private  long getNumberReadPagesFetch(TableDescriptor fetchedTable, long cardinality) {
        double k =  (double)(fetchedTable.getStatistics().getNumberOfPages() - 1)/(double) fetchedTable.getStatistics().getNumberOfPages();

        double term1 = ((double)1/k) - ((double)1/(k- Math.pow(k,2))) ;
        double term2 = (double)1/(1-k);
        double term3 = Math.pow(k, cardinality);
        long numberReadPagesFetch = (long)((term1 * term3) + term2);
        numberReadPagesFetch = (numberReadPagesFetch < 1)?1:numberReadPagesFetch;
        numberReadPagesFetch = Math.min(numberReadPagesFetch, fetchedTable.getStatistics().getNumberOfPages());
        return numberReadPagesFetch;
    }

    @Override
    public long computeFilterCost(LocalPredicate pred, long cardinality) {
        return 0;
    }

    @Override
    public long computeMergeJoinCost() {
        return 0;
    }

    @Override
    public long computeNestedLoopJoinCost(long outerCardinality, OptimizerPlanOperator innerOp) {
        return innerOp.getCumulativeCosts() * outerCardinality;
    }
}
