package simpledb;

import com.sun.corba.se.impl.interceptors.PICurrent;
import org.omg.CORBA.TIMEOUT;

import javax.xml.crypto.Data;
import java.io.*;
import java.lang.reflect.Array;
import java.security.Permission;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    private int maxPage;

    private ConcurrentHashMap<PageId, Page> bufferMap;
    private MyLock myLock;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        maxPage = numPages;
        bufferMap = new ConcurrentHashMap<PageId, Page>();
        myLock = new MyLock();
    }

    public static int getPageSize() {
      return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        boolean flag = myLock.acquireLock(tid, pid, perm);
        while(!flag) {
            Thread.yield();
            flag = myLock.acquireLock(tid, pid, perm);
        }
        if(bufferMap.containsKey(pid)) {
            return bufferMap.get(pid);
        }
        else {
            while(bufferMap.size() >= maxPage) {
                evictPage();
            }
            Page tmp = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            bufferMap.put(pid, tmp);
            tmp.setBeforeImage();
            return tmp;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        myLock.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return myLock.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        HashSet<PageId> lockedPage = myLock.getExclusivePage(tid);
        if(lockedPage != null) {
            for (PageId pid : lockedPage) {
                Page curPage = bufferMap.get(pid);
                if (curPage == null) {
                    continue;
                }
                if (commit) {
                    if (curPage.isDirty() != null) {
                        flushPage(pid);
                        curPage.setBeforeImage();
                    }
                } else {
                    bufferMap.put(pid, curPage.getBeforeImage());
                }
            }
        }
        myLock.releaseAllLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList = file.insertTuple(tid, t);
        for(Page page : pageList) {
            PageId pid = page.getId();
            while(!bufferMap.containsKey(pid) && bufferMap.size() >= maxPage) {
                evictPage();
            }
            page.markDirty(true, tid);
            bufferMap.put(pid, page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pageList = file.deleteTuple(tid, t);
        for(Page page : pageList) {
            PageId pid = page.getId();
            while(!bufferMap.containsKey(pid) && bufferMap.size() >= maxPage) {
                evictPage();
            }
            page.markDirty(true, tid);
            bufferMap.put(pid, page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid: bufferMap.keySet()) {
            flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        bufferMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page p = bufferMap.get(pid);
        if(p == null) {
            throw new IOException("");
        }
        if(p.isDirty() == null) {
            return;
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        p.markDirty(false, null);
        file.writePage(p);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        for (PageId pid: bufferMap.keySet()) {
            Page p = bufferMap.get(pid);
            if(p.isDirty() != null) {
                continue;
            }
            discardPage(pid);
            return;
        }
        throw new DbException("");
    }

    private class MyLock {
        private ConcurrentHashMap<PageId, HashSet<TransactionId>> shared;
        private ConcurrentHashMap<PageId, TransactionId> exclusive;
        private ConcurrentHashMap<TransactionId, HashSet<PageId>> sharedPage;
        private ConcurrentHashMap<TransactionId, HashSet<PageId>> exclusivePage;
        private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitGraph;

        public MyLock() {
            shared = new ConcurrentHashMap<>();
            exclusive = new ConcurrentHashMap<>();
            sharedPage = new ConcurrentHashMap<>();
            exclusivePage = new ConcurrentHashMap<>();
            waitGraph = new ConcurrentHashMap<>();
        }

        private synchronized boolean deadLock(TransactionId st) {
            HashSet<TransactionId> vis = new HashSet<>();
            Queue<TransactionId> q = new LinkedList<>();
            q.add(st);
            vis.add(st);
            while(!q.isEmpty()) {
                TransactionId x = q.poll();
                HashSet<TransactionId> edge = waitGraph.get(x);
                if(edge == null)continue;
                for(TransactionId y : edge) {
                    if(y.equals(st)) {
                        return true;
                    }
                    if(!vis.contains(y)) {
                        q.add(y);
                        vis.add(y);
                    }
                }
            }
            return false;
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
            boolean flag = true;
            if(perm.equals(Permissions.READ_ONLY)) {
                flag = acquireSharedLock(tid, pid);
            }
            else {
                flag = acquireExclusiveLock(tid, pid);
            }
            if(!flag) {
                Thread.yield();
                waitGraph.putIfAbsent(tid, new HashSet<>());
                waitGraph.get(tid).clear();
                {
                    HashSet<TransactionId> curShared = shared.get(pid);
                    if(curShared != null) {
                        for(TransactionId t : curShared) {
                            waitGraph.get(tid).add(t);
                        }
                    }
                    TransactionId curExclusive = exclusive.get(pid);
                    if(curExclusive != null) {
                        waitGraph.get(tid).add(curExclusive);
                    }
                }
                if(deadLock(tid)) {
                    throw new TransactionAbortedException();
                }
            }
            else {
                waitGraph.remove(tid);
            }
            return flag;
        }

        private void addSharedLock(TransactionId tid, PageId pid) {
            shared.putIfAbsent(pid, new HashSet<>());
            shared.get(pid).add(tid);
            sharedPage.putIfAbsent(tid, new HashSet<>());
            sharedPage.get(tid).add(pid);
        }

        private boolean acquireSharedLock(TransactionId tid, PageId pid) {
            TransactionId curExclusive = exclusive.get(pid);
            if(curExclusive != null) {
                return curExclusive.equals(tid);
            }
            addSharedLock(tid, pid);
            return true;
        }

        private void removeSharedLock(TransactionId tid, PageId pid) {
            shared.putIfAbsent(pid, new HashSet<>());
            shared.get(pid).remove(tid);
            sharedPage.putIfAbsent(tid, new HashSet<>());
            sharedPage.get(tid).remove(pid);
        }

        private void addExclusiveLock(TransactionId tid, PageId pid) {
            exclusive.put(pid, tid);
            exclusivePage.putIfAbsent(tid, new HashSet<>());
            exclusivePage.get(tid).add(pid);
        }

        private boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
            HashSet<TransactionId> curShared = shared.get(pid);
            TransactionId curExclusive = exclusive.get(pid);
            if(curExclusive != null) {
                return curExclusive.equals(tid);
            }
            if(curShared != null && (curShared.size() > 1 || (curShared.size() == 1 && !curShared.contains(tid)))) {
                return false;
            }
            if(curShared != null && curShared.size() > 0) {
                removeSharedLock(tid, pid);
            }
            addExclusiveLock(tid, pid);
            return true;
        }

        private void removeExclusiveLock(TransactionId tid, PageId pid) {
            exclusive.remove(pid);
            exclusivePage.putIfAbsent(tid, new HashSet<>());
            exclusivePage.get(tid).remove(pid);
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            TransactionId curExclusive = exclusive.get(pid);
            if(curExclusive != null && curExclusive.equals(tid)) {
                return true;
            }
            HashSet<TransactionId> curShared = shared.get(pid);
            if(curShared != null && curShared.contains(tid)) {
                return true;
            }
            return false;
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            HashSet<PageId> curSharedPage = sharedPage.get(tid);
            if(curSharedPage != null && curSharedPage.contains(pid)) {
                removeSharedLock(tid, pid);
            }
            HashSet<PageId> curExclusivePage = exclusivePage.get(tid);
            if(curExclusivePage != null && curExclusivePage.contains(pid)) {
                removeExclusiveLock(tid, pid);
            }
        }

        public synchronized void releaseAllLock(TransactionId tid) {
            if(sharedPage.get(tid) != null) {
                for(PageId pid : sharedPage.get(tid)) {
                    shared.putIfAbsent(pid, new HashSet<>());
                    shared.get(pid).remove(tid);
                }
                sharedPage.remove(tid);
            }
            if(exclusivePage.get(tid) != null) {
                for(PageId pid : exclusivePage.get(tid)) {
                    exclusive.remove(pid);
                }
                exclusivePage.remove(tid);
            }
        }

        public synchronized HashSet<PageId> getExclusivePage(TransactionId tid) {
            return exclusivePage.get(tid);
        }
    }
}
