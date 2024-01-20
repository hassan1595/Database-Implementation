package de.tuberlin.dima.minidb.optimizer.joins;

import de.tuberlin.dima.minidb.optimizer.AbstractJoinPlanOperator;
import de.tuberlin.dima.minidb.optimizer.OptimizerPlanOperator;
import de.tuberlin.dima.minidb.optimizer.cardinality.CardinalityEstimator;
import de.tuberlin.dima.minidb.optimizer.joins.util.JoinOrderOptimizerUtils;
import de.tuberlin.dima.minidb.semantics.JoinGraphEdge;
import de.tuberlin.dima.minidb.semantics.Relation;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicate;
import de.tuberlin.dima.minidb.semantics.predicate.JoinPredicateConjunct;
import java.util.ArrayList;

public class JoinOrderOptimizerClass implements  JoinOrderOptimizer{

    private final CardinalityEstimator estimator;

    private final ArrayList<OptimizerPlanOperator> optimizerPlanOperatorsList;
    private final ArrayList<Long> bitMapList;
    private final ArrayList<Long> outputCardinalityList;

    private final ArrayList<Long> operatorCostList;

    private final ArrayList<Long> cumulativeCostList;

    public JoinOrderOptimizerClass(CardinalityEstimator estimator){

        this.estimator = estimator;
        this.optimizerPlanOperatorsList = new ArrayList<>();
        this.bitMapList = new ArrayList<>();
        this.outputCardinalityList = new ArrayList<>();
        this.operatorCostList = new ArrayList<>();
        this.cumulativeCostList = new ArrayList<>();
    }

    private JoinPredicate findJoin(ArrayList<Long> leftRelationBitMapList,
                                              ArrayList<Long> rightRelationBitMapList ,
                                              ArrayList<JoinPredicate> joinPredicateList,
                                              Long bitMapLeft,
                                              Long bitMapRight){

        if((bitMapLeft & bitMapRight) != 0){
            return  null;
        }

        JoinPredicateConjunct jpc = new JoinPredicateConjunct();
        boolean joinable = false;

        for(int i = 0 ; i < leftRelationBitMapList.size(); i++) {

            if ((bitMapLeft & leftRelationBitMapList.get(i)) != 0) {
                if ((bitMapRight & rightRelationBitMapList.get(i)) != 0) {
                    jpc.addJoinPredicate(joinPredicateList.get(i));
                    joinable = true;
                }
            }
            else if ((bitMapLeft & rightRelationBitMapList.get(i)) != 0){
                if ((bitMapRight & leftRelationBitMapList.get(i)) != 0) {
                    jpc.addJoinPredicate(joinPredicateList.get(i).createSideSwitchedCopy());
                    joinable = true;
                }
            }
        }

        if(joinable){
            return JoinOrderOptimizerUtils.filterTwinPredicates(jpc);
        }
        else{
            return null;
        }


    }
    private int getMinComulativeCostIdx(ArrayList<Long> cumulativeCostList, ArrayList<Long> bitMapList,
                                        int numRelations){

        Long minCost = null;
        int minIdx = -1;
        for(int i = 0 ; i < bitMapList.size(); i ++){
            if(bitMapList.get(i) == Math.pow(2,numRelations) -1){
                if(minCost == null || minCost > cumulativeCostList.get(i)){
                    minCost = cumulativeCostList.get(i);
                    minIdx = i;
                }
            }
        }
        return minIdx;

    }
    @Override
    public OptimizerPlanOperator findBestJoinOrder(Relation[] relations, JoinGraphEdge[] joins) {

        long tempBitMap = 1;
        long lastIndex = 0;
        boolean finished = false;
        ArrayList<Long> leftRelationBitMapList = new ArrayList<>();;
        ArrayList<Long> rightRelationBitMapList = new ArrayList<>();;
        ArrayList<JoinPredicate> joinPredicateList = new ArrayList<>();

        for(int i = 0 ; i < relations.length ; i++){
            this.optimizerPlanOperatorsList.add(relations[i]);
            this.bitMapList.add(tempBitMap << i);
            this.outputCardinalityList.add(relations[i].getOutputCardinality());
            this.operatorCostList.add(0L);
            this.cumulativeCostList.add(0L);

            for(JoinGraphEdge edge: joins){
                if(relations[i] == edge.getLeftNode()){
                    for(int j = 0 ; j < relations.length ; j++){

                        if(relations[j] == edge.getRightNode()){
                            leftRelationBitMapList.add(tempBitMap << i);
                            rightRelationBitMapList.add(tempBitMap << j);
                            joinPredicateList.add(edge.getJoinPredicate());
                        }
                    }
                }
            }
        }
        while(!finished){
            int tableDPSizeBefore = this.optimizerPlanOperatorsList.size();
            for(int i =  tableDPSizeBefore- 1 ; i > 0 ; i--){
                if(i < lastIndex){
                    break;
                }
                for(int j = 0 ; j < i ; j ++){

                    JoinPredicate joinablePredicate = this.findJoin(leftRelationBitMapList,
                            rightRelationBitMapList, joinPredicateList,
                            this.bitMapList.get(i), this.bitMapList.get(j));

                    if(joinablePredicate != null){
                        AbstractJoinPlanOperator newJoinOrder = new AbstractJoinPlanOperator(this.optimizerPlanOperatorsList.get(i),
                                this.optimizerPlanOperatorsList.get(j), joinablePredicate);
                        this.optimizerPlanOperatorsList.add(newJoinOrder);
                        this.bitMapList.add(this.bitMapList.get(i) | this.bitMapList.get(j));
                        this.estimator.estimateJoinCardinality(newJoinOrder);
                        this.outputCardinalityList.add(newJoinOrder.getOutputCardinality());
                        long operatorCost = this.optimizerPlanOperatorsList.get(i).getOutputCardinality() +
                                this.optimizerPlanOperatorsList.get(j).getOutputCardinality();
                        this.operatorCostList.add(operatorCost);
                        this.cumulativeCostList.add(operatorCost +
                                this.cumulativeCostList.get(i) + this.cumulativeCostList.get(j));


                    }

                }
            }
            lastIndex = tableDPSizeBefore;
            if(this.optimizerPlanOperatorsList.size() == tableDPSizeBefore){
                finished = true;
            }
        }
        int minIdx = this.getMinComulativeCostIdx(this.cumulativeCostList, this.bitMapList, relations.length);
        return this.optimizerPlanOperatorsList.get(minIdx);
    }
}
