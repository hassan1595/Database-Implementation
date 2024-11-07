package de.tuberlin.dima.minidb.optimizer.generator;

import de.tuberlin.dima.minidb.catalogue.Catalogue;
import de.tuberlin.dima.minidb.catalogue.IndexDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableDescriptor;
import de.tuberlin.dima.minidb.catalogue.TableSchema;
import de.tuberlin.dima.minidb.optimizer.*;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.cost.CostEstimator;
import de.tuberlin.dima.minidb.optimizer.generator.util.PhysicalPlanGeneratorUtils;
import de.tuberlin.dima.minidb.parser.OutputColumn;
import de.tuberlin.dima.minidb.semantics.*;
import de.tuberlin.dima.minidb.semantics.predicate.*;
import org.codehaus.jackson.map.MapperConfig;
import org.hsqldb.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class PhysicalPlanGeneratorClass implements PhysicalPlanGenerator{

    private Catalogue catalogue;
    private CardinalityEstimator cardEstimator;
    private CostEstimator costEstimator;
    public PhysicalPlanGeneratorClass(Catalogue catalogue, CardinalityEstimator cardEstimator, CostEstimator costEstimator) {

        this.catalogue = catalogue;
        this.cardEstimator = cardEstimator;
        this.costEstimator = costEstimator;
    }
    @Override
    public OptimizerPlanOperator generatePhysicalPlan(AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan) throws OptimizerException {

        for(Relation r: joinPlan.getInvolvedRelations()){

            System.out.println(r);
            System.out.println(r.getPredicate());
            System.out.println(r.getOutputCardinality());
            this.cardEstimator.estimateTableAccessCardinality(((BaseTableAccess) r));
            System.out.println(r.getOutputCardinality());
        }

        OptimizerPlanOperator[] candidates = buildBestOrderByPlans(query, joinPlan);;

        if(candidates.length >= 1){
            return candidates[0];
        }

        return  null;

    }

    @Override
    public OptimizerPlanOperator[] buildBestOrderByPlans(AnalyzedSelectQuery query, OptimizerPlanOperator joinPlan) throws OptimizerException {


        ProducedColumn[] prc = query.getOutputColumns();
        ArrayList<RequestedOrder> ordColArr = new ArrayList<>();
        ArrayList<Integer> ordColRankArr = new ArrayList<>();
        for(int i =0; i < prc.length; i++){
            if(prc[i].getOrder() != Order.NONE) {
                ordColArr.add(new RequestedOrder(prc[i], prc[i].getOrder()));
                ordColRankArr.add(prc[i].getOrderRank() -1);
            }
        }
        RequestedOrder[] ordCol = null;
        int[] ordColRank = null;

        if(!ordColArr.isEmpty()){
            ordCol= ordColArr.toArray(new RequestedOrder[0]);
            ordColRank= ordColRankArr.stream().mapToInt(Integer::intValue).toArray();
        }

        OptimizerPlanOperator[] candidates = buildBestGroupByPlans(prc, query.isGrouping(), query.getPredicate(), joinPlan,
                ordCol,ordColRank );
        return candidates;
    }

    @Override
    public OptimizerPlanOperator[] buildBestGroupByPlans(ProducedColumn[] outCols, boolean grouping, LocalPredicate havingPred, OptimizerPlanOperator joinPlan, RequestedOrder[] order, int[] orderColIndices) throws OptimizerException {



        RequestedOrder[] interestingReqOrder = null;
        InterestingOrder[] intOrd= new InterestingOrder[1];
        ArrayList<Integer> intIdx;
        ArrayList<Column> intColumn;

        int deltaCostOrder = 0;
        ArrayList<Column> ordColArr = null;
        ArrayList<Integer> ordColIdx = null;
        if(order != null){
            ordColArr = new ArrayList<>();
            ordColIdx = new ArrayList<>();
            for (RequestedOrder requestedOrder : order) {
                for(int i = 0 ; i < outCols.length;i++){
                    if(outCols[i] == requestedOrder.getColumn()){
                        ordColIdx.add(i);
                        ordColArr.add(requestedOrder.getColumn());
                        deltaCostOrder = (int) (2 * joinPlan.getOutputCardinality());

                    }

                }

            }

        }
        ArrayList<Column> groupByColArr = null;
        ArrayList<RequestedOrder> groupByColReqArr = null;
        ArrayList<Integer> groupColIdxArr = null;
        ArrayList<Integer> aggColIdxArr = null;

        int deltaCostGroup = 0;
        if(grouping){

            groupByColArr = new ArrayList<>();
            groupByColReqArr = new ArrayList<>();
            groupColIdxArr = new ArrayList<>();
            aggColIdxArr = new ArrayList<>();

            int realIdx = 0;
            for(int i = 0 ; i < outCols.length; i++){

                if(outCols[i].getAggregationFunction() == OutputColumn.AggregationType.NONE){
                    groupByColArr.add(outCols[i]);
                    groupColIdxArr.add(i);
                    deltaCostGroup = (int) (2 * joinPlan.getOutputCardinality());
                    groupByColReqArr.add(new RequestedOrder(outCols[i], Order.ASCENDING));
                }
                else{
                    aggColIdxArr.add(i);
                }
            }

            if(ordColArr != null && groupByColArr.containsAll(ordColArr)){
                interestingReqOrder = order;
                intIdx = ordColIdx;
                intColumn = ordColArr;
                intOrd[0] = new InterestingOrder(interestingReqOrder, deltaCostOrder);
            }
            else{
                interestingReqOrder = groupByColReqArr.toArray(new RequestedOrder[0]);
                intIdx= groupColIdxArr;
                intColumn = groupByColArr;
                intOrd[0] = new InterestingOrder(interestingReqOrder, deltaCostGroup);
            }


        }
        else if (order != null){
            interestingReqOrder = order;
            intIdx = ordColIdx;
            intColumn = ordColArr;
            intOrd[0] = new InterestingOrder(interestingReqOrder, deltaCostOrder);
        }
        else{
            intColumn = null;
            intIdx = null;
            intOrd = null;
        }

        OptimizerPlanOperator[] candidates = buildBestSubPlans(outCols, joinPlan, intOrd);

        if(grouping)
        {
            boolean[] interestingBoolean = new boolean[interestingReqOrder.length];
            for(int i =0 ; i <interestingReqOrder.length; i++){
                interestingBoolean[i] = interestingReqOrder[i].getOrder() == Order.ASCENDING;
            }

            OptimizerPlanOperator[] candidatesGrouped = new OptimizerPlanOperator[candidates.length];
            for(int i = 0 ; i < candidates.length; i++){
                OptimizerPlanOperator candidate =  candidates[i];
                OptimizerPlanOperator sortPlanOperator =PhysicalPlanGeneratorUtils.addSortIfNecessary(candidate, interestingReqOrder,
                        intIdx.stream().mapToInt(Integer::intValue).toArray(),
                        interestingBoolean);
                if(candidate != sortPlanOperator){
                    sortPlanOperator.setOperatorCosts(this.costEstimator.computeSortCosts(groupByColArr.toArray(new Column[0]), candidate.getOutputCardinality() ));
                    sortPlanOperator.setCumulativeCosts(candidate.getCumulativeCosts() + sortPlanOperator.getOperatorCosts());
                    candidatesGrouped[i] = new GroupByPlanOperator(sortPlanOperator, outCols, groupColIdxArr.stream().mapToInt(Integer::intValue).toArray(),
                            aggColIdxArr.stream().mapToInt(Integer::intValue).toArray(),
                            (int)candidate.getOutputCardinality());
                }
                else{
                    candidatesGrouped[i] = new GroupByPlanOperator(candidate, outCols, groupColIdxArr.stream().mapToInt(Integer::intValue).toArray(),
                            aggColIdxArr.stream().mapToInt(Integer::intValue).toArray(),
                            (int)candidate.getOutputCardinality());;
                }
                return candidatesGrouped;

            }
        }
        else{
            return candidates;
        }

        return null;
    }

    @Override
    public OptimizerPlanOperator[] buildBestSubPlans(Column[] neededCols, OptimizerPlanOperator abstractJoinPlan, InterestingOrder[] intOrders) throws OptimizerException {



        OptimizerPlanOperator[] candidates;
       if (abstractJoinPlan instanceof AbstractJoinPlanOperator){

           candidates = buildBestConcreteJoinPlans(neededCols, (AbstractJoinPlanOperator) abstractJoinPlan, intOrders);

       }
       else if (abstractJoinPlan instanceof Relation) {
           candidates = buildBestRelationAccessPlans(neededCols, (Relation) abstractJoinPlan, intOrders);
       }
       else{
           throw new OptimizerException("Plan is neither a join nor a table access");
       }
        return candidates;
    }

    @Override
    public OptimizerPlanOperator[] buildBestConcreteJoinPlans(Column[] neededCols, AbstractJoinPlanOperator join, InterestingOrder[] intOrders) throws OptimizerException {




        ArrayList<Column> colLeft = new ArrayList<>();
        ArrayList<Integer> colLeftIdx = new ArrayList<>();
        int[] colLeftIdxArr;
        ArrayList<InterestingOrder> colIntOrderLeft = new ArrayList<>();
        ArrayList<Column> colRight = new ArrayList<>();
        ArrayList<Integer> colRightIdx = new ArrayList<>();
        int[] colRightIdxArr;
        ArrayList<InterestingOrder> colIntOrderRight = new ArrayList<>();

        this.cardEstimator.estimateJoinCardinality(join);


        for(int i =0; i < neededCols.length; i++){

            Column c = neededCols[i];

            if(join.getLeftChild().getInvolvedRelations().contains(c.getRelation())){
                colLeft.add(c);
                colLeftIdx.add(i);
                colRightIdx.add(-1);

            }
            else if(join.getRightChild().getInvolvedRelations().contains(c.getRelation())){
                colRight.add(c);
                colRightIdx.add(i);
                colLeftIdx.add(-1);
            }
            else{
                throw new OptimizerException("Columns must be contained in the relations");
            }
        }

        if(intOrders != null){
            for(InterestingOrder intOrder: intOrders){

                boolean leftContained = true;
                boolean rightContained = true;
                for(RequestedOrder reqOrder :intOrder.getOrderColumns()){
                    if(!join.getLeftChild().getInvolvedRelations().contains(reqOrder.getColumn().getRelation())){
                        leftContained = false;
                    }
                    if(!join.getRightChild().getInvolvedRelations().contains(reqOrder.getColumn().getRelation())){
                        rightContained = false;
                    }
                    if(leftContained){
                        colIntOrderLeft.add(intOrder);
                    }
                    if(rightContained){
                        colIntOrderRight.add(intOrder);
                    }

                }
            }
        }

        JoinPredicate jpa;
        ArrayList<RequestedOrder> columnsLeftReq = new ArrayList<>();
        ArrayList<Integer> columnsLeftIdx = new ArrayList<>();
        ArrayList<RequestedOrder> columnsRightReq =  new ArrayList<>();
        ArrayList<Integer> columnsRightIdx =  new ArrayList<>();
        if(join.getJoinPredicate() instanceof JoinPredicateAtom){

            if((join.getLeftChild().getInvolvedRelations().contains( ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn().getRelation() ))){

                for(int i = 0 ; i < colLeft.size();i++){
                    Column c = colLeft.get(i);
                    if(c == ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn()){
                        columnsLeftIdx.add(i);
                    }
                }
                columnsLeftReq.add(new RequestedOrder( ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn(), Order.ASCENDING));
            }
            else if((join.getRightChild().getInvolvedRelations().contains( ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn().getRelation() ))){

                for(int i = 0 ; i < colRight.size();i++){
                    Column c = colRight.get(i);
                    if(c == ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn()){
                        columnsRightIdx.add(i);
                    }
                }
                columnsRightReq.add(new RequestedOrder( ((JoinPredicateAtom)join.getJoinPredicate()).getLeftHandColumn(), Order.ASCENDING));
            }
            else{
                throw new OptimizerException("Column not found in two join children");
            }

            if((join.getLeftChild().getInvolvedRelations().contains( ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn().getRelation() ))){

                for(int i = 0 ; i < colLeft.size();i++){
                    Column c = colLeft.get(i);
                    if(c == ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn()){
                        columnsLeftIdx.add(i);
                    }
                }
                columnsLeftReq.add( new RequestedOrder( ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn(), Order.ASCENDING));
            }
            else if((join.getRightChild().getInvolvedRelations().contains( ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn().getRelation() ))){

                for(int i = 0 ; i < colRight.size();i++){
                    Column c = colRight.get(i);
                    if(c == ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn()){
                        columnsRightIdx.add(i);
                    }
                }
                columnsRightReq.add(new RequestedOrder( ((JoinPredicateAtom)join.getJoinPredicate()).getRightHandColumn(), Order.ASCENDING));
            }
            else{
                throw new OptimizerException("Column not found in two join children");
            }

            colIntOrderLeft.add(new InterestingOrder(columnsLeftReq.toArray(new RequestedOrder[0]), 2 * join.getLeftChild().getOutputCardinality()));
            colIntOrderRight.add(new InterestingOrder(columnsRightReq.toArray(new RequestedOrder[0]), 2 * join.getLeftChild().getOutputCardinality()));

            colLeftIdx.add(-1);
            colRightIdx.add(-1);

            colLeftIdxArr = colLeftIdx.stream().mapToInt(Integer::intValue).toArray();
            colRightIdxArr = colRightIdx.stream().mapToInt(Integer::intValue).toArray();

            jpa = PhysicalPlanGeneratorUtils.addKeyColumnsAndAdaptPredicate((JoinPredicateAtom) join.getJoinPredicate(),colLeft,
                    colRight,join.getLeftChild().getInvolvedRelations(),join.getRightChild().getInvolvedRelations(),colLeftIdxArr,colRightIdxArr,colRightIdxArr.length -1);

            if(colLeftIdxArr[colLeftIdxArr.length -1] == -1 && colRightIdxArr[colRightIdxArr.length -1] == -1){
                colLeftIdx.remove(colLeftIdx.size() -1);
                colRightIdx.remove(colRightIdx.size() -1);
                colLeftIdxArr = colLeftIdx.stream().mapToInt(Integer::intValue).toArray();
                colRightIdxArr = colRightIdx.stream().mapToInt(Integer::intValue).toArray();

            }


        }
        else if (join.getJoinPredicate() instanceof  JoinPredicateConjunct){
            colLeftIdxArr = colLeftIdx.stream().mapToInt(Integer::intValue).toArray();
            colRightIdxArr = colRightIdx.stream().mapToInt(Integer::intValue).toArray();

            jpa = join.getJoinPredicate();
        }
        else{
            throw new OptimizerException("Join type not defined");
        }


        ArrayList<OptimizerPlanOperator> candidatesList = new ArrayList<>();
        OptimizerPlanOperator[] leftCandidates = buildBestSubPlans(colLeft.toArray(new Column[0]),join.getLeftChild(),
                intOrders != null?colIntOrderLeft.toArray(new InterestingOrder[0]):null);
        OptimizerPlanOperator[] rightCandidates = buildBestSubPlans(colRight.toArray(new Column[0]),join.getRightChild(),
                intOrders != null?colIntOrderRight.toArray(new InterestingOrder[0]):null);


        for(OptimizerPlanOperator leftCand : leftCandidates){
            for(OptimizerPlanOperator rightCand : rightCandidates){


                if(leftCand.getOutputCardinality() < rightCand.getOutputCardinality()){
                    NestedLoopJoinPlanOperator nestedOperator = new NestedLoopJoinPlanOperator(leftCand,rightCand,
                            jpa, colLeftIdxArr,
                            colRightIdxArr, join.getOutputCardinality());

                    this.cardEstimator.estimateJoinCardinality(nestedOperator);
                    nestedOperator.setOperatorCosts(this.costEstimator.computeNestedLoopJoinCost(leftCand.getOutputCardinality(),rightCand));
                    nestedOperator.setCumulativeCosts(leftCand.getCumulativeCosts() + rightCand.getCumulativeCosts() + nestedOperator.getOperatorCosts());
                    candidatesList.add(nestedOperator);


                }
                else{
                    NestedLoopJoinPlanOperator nestedOperator = new NestedLoopJoinPlanOperator(rightCand,leftCand,
                            jpa, colRightIdxArr,
                            colLeftIdxArr, join.getOutputCardinality());

                    this.cardEstimator.estimateJoinCardinality(nestedOperator);
                    nestedOperator.setOperatorCosts(this.costEstimator.computeNestedLoopJoinCost(rightCand.getOutputCardinality(),leftCand));
                    nestedOperator.setCumulativeCosts(leftCand.getCumulativeCosts() + rightCand.getCumulativeCosts() + nestedOperator.getOperatorCosts());
                    candidatesList.add(nestedOperator);

                }

                if(PhysicalPlanGeneratorUtils.isMergeJoinPossible(join.getJoinPredicate())){
                    boolean[] boolArrayLeft = new boolean[columnsLeftIdx.size()];
                    boolean[] boolArrayRight = new boolean[columnsRightIdx.size()];
                    Arrays.fill(boolArrayLeft, true);
                    Arrays.fill(boolArrayRight, true);
                    OptimizerPlanOperator sortPlanOperatorLeft = PhysicalPlanGeneratorUtils.addSortIfNecessary(leftCand,
                            colIntOrderLeft.get(colIntOrderLeft.size() -1).getOrderColumns(),
                            columnsLeftIdx.stream().mapToInt(Integer::intValue).toArray(),
                            boolArrayLeft);

                    if(sortPlanOperatorLeft != leftCand){
                        sortPlanOperatorLeft.setOperatorCosts(this.costEstimator.computeSortCosts(colLeft.toArray(new Column[0]),leftCand.getOutputCardinality()));
                        sortPlanOperatorLeft.setCumulativeCosts(sortPlanOperatorLeft.getOperatorCosts() + leftCand.getCumulativeCosts());

                    }
                    OptimizerPlanOperator sortPlanOperatorRight = PhysicalPlanGeneratorUtils.addSortIfNecessary(rightCand,
                            colIntOrderRight.get(colIntOrderLeft.size() -1).getOrderColumns(),
                            columnsRightIdx.stream().mapToInt(Integer::intValue).toArray(),
                            boolArrayRight);

                    if(sortPlanOperatorRight != rightCand){
                        sortPlanOperatorRight.setOperatorCosts(this.costEstimator.computeSortCosts(colRight.toArray(new Column[0]),rightCand.getOutputCardinality()));
                        sortPlanOperatorRight.setCumulativeCosts(sortPlanOperatorRight.getOperatorCosts() + rightCand.getCumulativeCosts());
                    }

                    if(leftCand.getOutputCardinality() < rightCand.getOutputCardinality()){
                        System.out.println("a7a1");

                        System.out.println(colLeft);
                        System.out.println(colRight);
                        System.out.println(jpa);
                        System.out.println(join.getJoinPredicate());
                        System.out.println(Arrays.toString(sortPlanOperatorLeft.getReturnedColumns()));
                        System.out.println(Arrays.toString(sortPlanOperatorRight.getReturnedColumns()));
                        MergeJoinPlanOperator mJoin = new MergeJoinPlanOperator(sortPlanOperatorLeft, sortPlanOperatorRight,jpa,
                                columnsLeftIdx.stream().mapToInt(Integer::intValue).toArray(), columnsRightIdx.stream().mapToInt(Integer::intValue).toArray(),
                                colLeftIdxArr,
                                colRightIdxArr,
                                join.getOutputCardinality());
                        this.cardEstimator.estimateJoinCardinality(mJoin);
                        mJoin.setOperatorCosts(this.costEstimator.computeMergeJoinCost());
                        mJoin.setCumulativeCosts(mJoin.getOperatorCosts() + sortPlanOperatorLeft.getCumulativeCosts() + sortPlanOperatorRight.getCumulativeCosts());
                        candidatesList.add(mJoin);

                    }
                    else{
                        System.out.println("a7a2");

                        System.out.println(jpa);
                        System.out.println(join.getJoinPredicate());
                        MergeJoinPlanOperator mJoin =new MergeJoinPlanOperator(sortPlanOperatorRight, sortPlanOperatorLeft,jpa,
                                columnsRightIdx.stream().mapToInt(Integer::intValue).toArray(), columnsLeftIdx.stream().mapToInt(Integer::intValue).toArray(),
                                colRightIdxArr,
                                colLeftIdxArr,
                                join.getOutputCardinality());
                        this.cardEstimator.estimateJoinCardinality(mJoin);
                        mJoin.setOperatorCosts(this.costEstimator.computeMergeJoinCost());
                        mJoin.setCumulativeCosts(mJoin.getOperatorCosts() + sortPlanOperatorLeft.getCumulativeCosts() + sortPlanOperatorRight.getCumulativeCosts());
                        candidatesList.add(mJoin);

                    }


                }

            }
        }




        return PhysicalPlanGeneratorUtils.prunePlans(candidatesList.toArray(new OptimizerPlanOperator[0]), intOrders);
    }

    @Override
    public OptimizerPlanOperator[] buildBestRelationAccessPlans(Column[] neededCols, Relation toAccess, InterestingOrder[] intOrders) throws OptimizerException {


        if(!(toAccess  instanceof BaseTableAccess)){
            throw new OptimizerException("Relation must be an instance of BaseTableAccess");
        }
        TableDescriptor td = ((BaseTableAccess)toAccess).getTable();
        System.out.println(toAccess);
        System.out.println(toAccess.getOutputCardinality());
        this.cardEstimator.estimateTableAccessCardinality(((BaseTableAccess)toAccess));
        System.out.println(toAccess.getOutputCardinality());
        ArrayList<OptimizerPlanOperator> candidatesList = new ArrayList<>();
        TableScanPlanOperator candidate_1 = new TableScanPlanOperator(((BaseTableAccess)toAccess),neededCols);
        candidate_1.setOperatorCosts(this.costEstimator.computeTableScanCosts(td));
        candidate_1.setCumulativeCosts(candidate_1.getOperatorCosts());
        candidatesList.add(candidate_1);

        int columnIndex = -1;
        if (toAccess.getPredicate() != null && toAccess.getPredicate() instanceof LocalPredicateAtom){
            columnIndex = ((LocalPredicateAtom) toAccess.getPredicate()).getColumn().getColumnIndex();

        } else if (toAccess.getPredicate() != null && toAccess.getPredicate() instanceof LocalPredicateBetween) {
            columnIndex = ((LocalPredicateBetween) toAccess.getPredicate()).getColumn().getColumnIndex();
        }

        if(columnIndex != -1){
            IndexLookupPlanOperator candidate_ix = PhysicalPlanGeneratorUtils.createIndexLookup( (BaseTableAccess) toAccess,
                    toAccess.getOutputCardinality(),toAccess.getPredicate(),
                    columnIndex,
                    this.catalogue.getAllIndexesForTable(td.getTableName()));

            if(candidate_ix != null){
                candidate_ix.setOperatorCosts(this.costEstimator.computeIndexLookupCosts(candidate_ix.getIndex(), td, toAccess.getOutputCardinality() ));
                candidate_ix.setCumulativeCosts(candidate_ix.getOperatorCosts());

                OptimizerPlanOperator candidate_fetch = new FetchPlanOperator(candidate_ix, ((BaseTableAccess)toAccess), neededCols);
                candidate_fetch.setOperatorCosts(this.costEstimator.computeFetchCosts(td, toAccess.getOutputCardinality(), false));
                candidate_fetch.setCumulativeCosts(candidate_ix.getCumulativeCosts() + candidate_fetch.getOperatorCosts());
                candidatesList.add(candidate_fetch);

            }

        }


        return PhysicalPlanGeneratorUtils.prunePlans(candidatesList.toArray(new OptimizerPlanOperator[0]), intOrders);
    }
}
