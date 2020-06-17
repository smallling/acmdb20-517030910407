package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tid;
    private TupleDesc td;
    private boolean hasInsert;

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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tid = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Insert Count"});
        this.hasInsert = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasInsert = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
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
        if(hasInsert)return null;
        int cnt = 0;
        while(child.hasNext()) {
            Tuple cur = child.next();
            try {
                Database.getBufferPool().insertTuple(t, tid, cur);
            } catch (IOException e) {
                throw new DbException("");
            }
            cnt++;
        }
        Tuple tmp = new Tuple(td);
        tmp.setField(0, new IntField(cnt));
        hasInsert = true;
        return tmp;
    }

    @Override
    public DbIterator[] getChildren() {
       return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
}
