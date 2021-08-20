package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");) {
            long pos = (long) pid.getPageNumber() * BufferPool.getPageSize();
            if (pos > randomAccessFile.length()) {
                throw new IllegalArgumentException("invalid page number");
            }
            randomAccessFile.seek(pos);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            randomAccessFile.read(bytes, 0, bytes.length);
            return new HeapPage((HeapPageId) pid, bytes);
        }catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        if (pageNumber > numPages()) {
            throw new IllegalArgumentException();
        }
        try(RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            randomAccessFile.seek((long) pageNumber * BufferPool.getPageSize());
            byte[] data = page.getPageData();
            randomAccessFile.write(data);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> modifyPages = new ArrayList<>();
        for (int i=0;i<numPages();i++) {
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                modifyPages.add(page);
                return modifyPages;
            }else {
                Database.getBufferPool().unsafeReleasePage(tid, heapPageId);
            }
        }
        try(BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file, true))) {
            byte[] emptyPageData = HeapPage.createEmptyPageData();
            bufferedOutputStream.write(emptyPageData);
        }
        HeapPageId heapPageId = new HeapPageId(this.getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        modifyPages.add(page);
        return modifyPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modifyPages = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        modifyPages.add(page);
        return modifyPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    private static class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;
        private final TransactionId transactionId;
        private Iterator<Tuple> tupleIterator;
        private int pageNumber;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.transactionId = tid;
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws DbException {
            PageId pageId = new HeapPageId(heapFile.getId(), pageNumber);
            try {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(transactionId, pageId, Permissions.READ_ONLY);
                return page.iterator();
            }catch (TransactionAbortedException | DbException e) {
                e.printStackTrace();
            }
            throw new DbException("tuple iterator error");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNumber = 0;
            this.tupleIterator = getTupleIterator(pageNumber);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (null == tupleIterator) {
                return false;
            }
            while (!tupleIterator.hasNext()) {
                if (pageNumber >= heapFile.numPages() - 1) {
                    return false;
                }
                tupleIterator = getTupleIterator(++pageNumber);
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (null == tupleIterator || !tupleIterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            tupleIterator = null;
        }
    }

}

