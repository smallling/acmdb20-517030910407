package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t;
    private DbIterator child;
    private int tid;
    private TupleDesc td;
    private boolean hasDelete;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"Delete Count"});
        this.hasDelete = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        hasDelete = false;
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(hasDelete)return null;
        int cnt = 0;
        while(child.hasNext()) {
            Tuple cur = child.next();
            try {
                Database.getBufferPool().deleteTuple(t, cur);
            } catch (Exception e) {
            }
            cnt++;
        }
        Tuple tmp = new Tuple(td);
        tmp.setField(0, new IntField(cnt));
        hasDelete = true;
        return tmp;
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator children[] = new DbIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }

}
