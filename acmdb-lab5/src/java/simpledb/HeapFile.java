package simpledb;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        RandomAccessFile fin = null;
         try {
            fin = new RandomAccessFile(f, "r");
            byte readByte[] = new byte[BufferPool.getPageSize()];
            if(fin.skipBytes(pid.pageNumber() * BufferPool.getPageSize()) != pid.pageNumber() * BufferPool.getPageSize()) {
                throw new IllegalArgumentException();
            }
            int tmp = fin.read(readByte, 0, BufferPool.getPageSize());
            if(tmp == -1 || tmp < BufferPool.getPageSize()) {
                throw new IllegalArgumentException();
            }
            Page nowPage = new HeapPage((HeapPageId) pid, readByte);
            return nowPage;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
             try {
                 if(fin != null) {
                     fin.close();
                 }
             } catch (IOException e) {

             }
         }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile file = new RandomAccessFile(this.getFile(), "rw");
        file.seek(BufferPool.getPageSize() * page.getId().pageNumber());
        file.write(page.getPageData(), 0, BufferPool.getPageSize());
        file.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)f.length() / (BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        HeapPage page = null;
        for(int i = 0; i < numPages(); i++) {
            PageId pid = new HeapPageId(getId(), i);
            HeapPage curpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(curpage.getNumEmptySlots() > 0) {
                page = curpage;
                break;
            }
        }
        if(page == null) {
            HeapPageId hpid = new HeapPageId(getId(), numPages());
            page = new HeapPage(hpid, HeapPage.createEmptyPageData());
            page.insertTuple(t);
            writePage(page);
        }
        else {
            page.insertTuple(t);
        }
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> it;
        int curCnt;
        TransactionId tid;
        HeapFile hf;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.hf = hf;
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            curCnt = 0;
            HeapPageId curPageId = new HeapPageId(hf.getId(), curCnt);
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
            it = curPage.iterator();
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            if(it == null) {
                return null;
            }
            while(!it.hasNext() && ++curCnt < hf.numPages()) {
                HeapPageId curPageId = new HeapPageId(hf.getId(), curCnt);
                HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
                it = curPage.iterator();
            }
            if(!it.hasNext()) {
                return null;
            }
            return it.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            curCnt = 0;
            HeapPageId curPageId = new HeapPageId(hf.getId(), curCnt);
            HeapPage curPage = (HeapPage) Database.getBufferPool().getPage(tid, curPageId, Permissions.READ_ONLY);
            it = curPage.iterator();
        }

        public void close() {
            super.close();
            it = null;
            curCnt = 0;
        }
    }
}

