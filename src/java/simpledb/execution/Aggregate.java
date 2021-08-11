package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aggregateFieldId;
    private int groupByFieldId;
    private Aggregator.Op aop;
    private TupleDesc tupleDesc;
    private Aggregator aggregator;
    private OpIterator aggIterator;
    private boolean childIterated;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aggregateFieldId = afield;
        this.groupByFieldId = gfield;
        this.aop = aop;
        TupleDesc childTupleDesc = this.child.getTupleDesc();
        Type groupByType = Aggregator.NO_GROUPING == this.groupByFieldId?null:childTupleDesc.getFieldType(this.groupByFieldId);
        Type aggregateType = childTupleDesc.getFieldType(this.aggregateFieldId);
        switch (aggregateType) {
            case INT_TYPE:
                this.aggregator = new IntegerAggregator(this.groupByFieldId, groupByType, aggregateFieldId, this.aop);
                break;
            case STRING_TYPE:
                this.aggregator = new StringAggregator(this.groupByFieldId, groupByType, aggregateFieldId, this.aop);
                break;
            default:
                break;
        }
        this.aggIterator = this.aggregator.iterator();
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        if (null != groupByType) {
            types.add(groupByType);
            names.add(childTupleDesc.getFieldName(groupByFieldId));
        }
        types.add(aggregateType);
        names.add(childTupleDesc.getFieldName(aggregateFieldId));
        this.tupleDesc = new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return groupByFieldId;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return Aggregator.NO_GROUPING == groupByFieldId?null:tupleDesc.getFieldName(0);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aggregateFieldId;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return Aggregator.NO_GROUPING == groupByFieldId? tupleDesc.getFieldName(0):tupleDesc.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        child.open();
        while (!childIterated && child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        childIterated = true;
        aggIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return aggIterator.hasNext()?aggIterator.next():null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        aggIterator.close();
        child.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
