package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFieldId;
    private Type groupByFieldType;
    private int aggregateFieldId;
    private Op what;

    private Map<Field, List<Integer>> aggregateMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByFieldId = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateFieldId = afield;
        this.what = what;
        this.aggregateMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField aggregateField = (IntField) tup.getField(aggregateFieldId);
        Field groupByField = Aggregator.NO_GROUPING == this.groupByFieldId?null:tup.getField(this.groupByFieldId);
        if (null != groupByField && !groupByField.getType().equals(this.groupByFieldType)) {
            throw new IllegalArgumentException("wrong type");
        }
        switch (this.what) {
            case COUNT:
            case SUM:
            case AVG:
            case MIN:
            case MAX:
                List<Integer> aggregateList = aggregateMap.containsKey(groupByField)?aggregateMap.get(groupByField):new ArrayList<>();
                aggregateList.add(aggregateField.getValue());
                aggregateMap.put(groupByField, aggregateList);
                break;
            default:
                throw new IllegalArgumentException("Aggregator not support.");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new
//        UnsupportedOperationException("please implement me for lab2");
        return new IntegerAggIterator();
    }

    private class IntegerAggIterator implements OpIterator {

        private Iterator<Map.Entry<Field, List<Integer>>> aggregateIterator;
        private TupleDesc tupleDesc;

        public IntegerAggIterator() {
            this.tupleDesc = null == groupByFieldType?
                    new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateValue"}):
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
            Map.Entry<Field, List<Integer>> groupEntry = aggregateIterator.next();
            Field groupField = groupEntry.getKey();
            Integer aggregateValue;
            List<Integer> aggregateList = groupEntry.getValue();
            switch (what) {
                case COUNT:
                    aggregateValue = aggregateList.size();
                    break;
                case SUM:
                    aggregateValue = aggregateList.stream().reduce(Integer::sum).orElse(0);
                    break;
                case AVG:
                    aggregateValue = aggregateList.stream().reduce(Integer::sum).orElse(0)/aggregateList.size();
                    break;
                case MIN:
                    aggregateValue = aggregateList.stream().reduce(Integer::min).orElseThrow(NoSuchElementException::new);
                    break;
                case MAX:
                    aggregateValue = aggregateList.stream().reduce(Integer::max).orElseThrow(NoSuchElementException::new);
                    break;
                default:
                    throw new IllegalArgumentException("Aggregator not support.");
            }
            if (null == groupField) {
                tuple.setField(0, new IntField(aggregateValue));
            }else {
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(aggregateValue));
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
