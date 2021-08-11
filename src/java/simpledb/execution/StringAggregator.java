package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldId;
    private Type groupByFieldType;
    private int aggregateFieldId;
    private Op what;

    private Map<Field, Integer> aggregateMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldId = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldId = afield;
        this.what = what;
        this.aggregateMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = Aggregator.NO_GROUPING == this.groupByFieldId?null:tup.getField(this.groupByFieldId);
        if (null != groupByField && !groupByField.getType().equals(this.groupByFieldType)) {
            throw new IllegalArgumentException("wrong type");
        }
        switch (this.what) {
            case COUNT:
                aggregateMap.put(groupByField, aggregateMap.getOrDefault(groupByField, 0) + 1);
                break;
            default:
                throw new IllegalArgumentException("Aggregator not support.");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for lab2");
        return new StringAggIterator();
    }

    private class StringAggIterator implements OpIterator {

        private Iterator<Map.Entry<Field, Integer>> aggregateIterator;
        private TupleDesc tupleDesc;

        public StringAggIterator() {
            this.tupleDesc = null == groupByFieldType?
                    new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"groupValue"}):
                    new TupleDesc(new Type[]{groupByFieldType, Type.INT_TYPE}, new String[]{"groupValue", "aggregateValue"});
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            aggregateIterator = aggregateMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return aggregateIterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(this.tupleDesc);
            switch (what) {
                case COUNT:
                    Map.Entry<Field, Integer> groupEntry = aggregateIterator.next();
                    Field groupField = groupEntry.getKey();
                    Integer aggregateValue = groupEntry.getValue();
                    if (null == groupField) {
                        tuple.setField(0, new IntField(aggregateValue));
                    }else {
                        tuple.setField(0, groupField);
                        tuple.setField(1, new IntField(aggregateValue));
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Aggregator not support.");
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            aggregateIterator = null;
        }
    }

}
