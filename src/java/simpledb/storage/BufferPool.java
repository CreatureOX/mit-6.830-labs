package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

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
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int pageIndex;
    private Page[] pages;

    private static class Lock {
        TransactionId tid;
        int lockType;

        static final int SHARED = 0;
        static final int EXCLUSIVE = 1;

        public Lock(TransactionId tid, int lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }

        public static int getLockType(Permissions perm) {
            if (Permissions.READ_ONLY.equals(perm)) {
                return SHARED;
            }
            if (Permissions.READ_WRITE.equals(perm)) {
                return EXCLUSIVE;
            }
            throw new IllegalArgumentException("illegal lock type");
        }

    }

    private static class PageLockManager {
        private Map<PageId, Vector<Lock>> pageLockMap;

        public PageLockManager() {
            this.pageLockMap = new ConcurrentHashMap<>();
        }

        public synchronized boolean acquireLock(TransactionId tid, PageId pid, int lockType) {
            Vector<Lock> locks = pageLockMap.get(pid);
            if (null == locks) {
                Lock lock = new Lock(tid, lockType);
                Vector<Lock> newLockList = new Vector<>();
                newLockList.add(lock);
                pageLockMap.put(pid, newLockList);
                return true;
            }else {
                // check tid held locks
                for (Lock lock: locks) {
                    if (!lock.tid.equals(tid)) {
                        continue;
                    }
                    if (lock.lockType == lockType) {
                        return true;
                    }
                    if (Lock.EXCLUSIVE == lock.lockType) {
                        return true;
                    }
                    if (locks.size() == 1) {
                        lock.lockType = Lock.EXCLUSIVE;
                        return true;
                    }else {
                        return false;
                    }
                }
                boolean heldExclusiveLock = locks.stream().anyMatch(lock -> Lock.EXCLUSIVE == lock.lockType);
                if (heldExclusiveLock) {
                    return false;
                }
                if (Lock.SHARED == lockType) {
                    Lock lock = new Lock(tid, Lock.SHARED);
                    locks.add(lock);
                    pageLockMap.put(pid, locks);
                    return true;
                }
                return false;
            }
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            Vector<Lock> locks = pageLockMap.get(pid);
            assert !locks.isEmpty(): "page not locked";
            for (Lock lock: locks) {
                if (!lock.tid.equals(tid)) {
                    continue;
                }
                locks.remove(lock);
                if (locks.isEmpty()) {
                    pageLockMap.remove(pid);
                }
                return ;
            }
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            if (!pageLockMap.containsKey(pid)) {
                return false;
            }
            List<Lock> locks = pageLockMap.get(pid);
            for (Lock lock: locks) {
                if (lock.tid.equals(tid)) {
                    return true;
                }
            }
            return false;
        }

    }

    private PageLockManager pageLockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pages = new Page[numPages];
        this.pageIndex = 0;
        this.pageLockManager = new PageLockManager();
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
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        while (!pageLockManager.acquireLock(tid, pid, Lock.getLockType(perm))) {
        }
        Page cachedPage = Arrays.stream(pages)
                .filter(page -> null != page && page.getId().equals(pid))
                .findAny().orElse(null);
        if (null != cachedPage){
            return cachedPage;
        }
        if (pageIndex == pages.length){
            evictPage();
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        cachedPage = file.readPage(pid);
        pages[pageIndex++] = cachedPage;
        return cachedPage;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        pageLockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return pageLockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
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
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifyPages = dbFile.insertTuple(tid, t);
        for (Page modifyPage: modifyPages) {
            if (this.pageIndex >= this.pages.length) {
                evictPage();
            }
            this.pages[this.pageIndex++] = modifyPage;
            modifyPage.markDirty(true, tid);
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
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> modifyPages = dbFile.deleteTuple(tid, t);
        for (Page modifyPage: modifyPages) {
            for (int i=0;i<pages.length;i++) {
                if (pages[i].getId().equals(modifyPage.getId())) {
                    modifyPage.markDirty(true, tid);
                    pages[i] = modifyPage;
                    break;
                }
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (int i=0;i<pages.length;i++) {
            if (null != pages[i] && null != pages[i].isDirty()) {
                flushPage(pages[i].getId());
            }
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
        // some code goes here
        // not necessary for lab1
        for (int i=0;i<pages.length;i++) {
            if (!pages[i].getId().equals(pid)) {
                continue;
            }
            pages[i] = null;
            if (pages.length - (i + 1) >= 0) {
                System.arraycopy(
                        pages, i + 1,
                        pages, i + 1 - 1,
                        pages.length - (i + 1));
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page matchedPage = Arrays.stream(pages)
                .filter(page -> page.getId().equals(pid))
                .findAny().orElseThrow(NoSuchElementException::new);
        TransactionId tid = matchedPage.isDirty();
        if (tid != null) {
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(matchedPage);
            matchedPage.markDirty(false, null);
        }
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
        // some code goes here
        // not necessary for lab1
        this.pageIndex--;
        PageId pid = pages[pageIndex].getId();
        try {
            flushPage(pid);
        }catch (IOException e) {
        }
        discardPage(pid);
    }

}
