package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc tupleDesc;
    private Integer insertCount;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
            throw new DbException("tuple desc mismatch");
        }
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"inserts"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.open();
        this.insertCount = 0;
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.called = false;
        this.insertCount = null;
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (called) {
            return null;
        }
        called = true;
        while (child.hasNext()) {
            Tuple tuple = child.next();
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, tuple);
                this.insertCount++;
            }catch (IOException e) {
                break;
            }
        }
        Tuple next = new Tuple(this.tupleDesc);
        next.setField(0, new IntField(this.insertCount));
        return next;
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
