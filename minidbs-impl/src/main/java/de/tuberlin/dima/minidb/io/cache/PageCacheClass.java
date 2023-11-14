package de.tuberlin.dima.minidb.io.cache;

import de.tuberlin.dima.minidb.io.tables.TablePage;
import de.tuberlin.dima.minidb.io.tables.TablePageClass;
import de.tuberlin.dima.minidb.util.Pair;

import java.util.ArrayList;

public class PageCacheClass implements PageCache {

    private final PageSize pageSize;
    private final int numPages;
    private final byte[][] binaryPageArray;
    private final ArrayList<CacheData> listT1;
    private final ArrayList<CacheData> listT2;
    private final ArrayList<Pair<Integer, Integer>> listB1;
    private final ArrayList<Pair<Integer, Integer>> listB2;
    private float adaptation;
    private int freeIdx;

    public PageCacheClass(PageSize pageSize, int numPages) {
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.binaryPageArray = new byte[numPages][pageSize.getNumberOfBytes()];
        this.listT1 = new ArrayList<CacheData>();
        this.listT2 = new ArrayList<CacheData>();
        this.listB1 = new ArrayList<Pair<Integer, Integer>>();
        this.listB2 = new ArrayList<Pair<Integer, Integer>>();
        this.adaptation = 0;
        this.freeIdx = 0;
    }

    private CacheableData getPageAndPinOpt(int resourceId, int pageNumber, boolean pin) {
        for (int i = 0; i < this.listT1.size(); i++) {
            CacheData cacheData = this.listT1.get(i);

            if (cacheData.getResourceId() == resourceId && cacheData.getPage().getPageNumber() == pageNumber && !cacheData.getEviction()) {

                if (pin) {
                    cacheData.setPinNumber(cacheData.getPinNumber() + 1);
                }

                // handling prefetch
                if (cacheData.getPrefetched()) {
                    this.listT1.remove(i);
                    this.listT1.add(0, cacheData);
                    cacheData.setPrefetched(false);
                } else {
                    this.listT1.remove(i);
                    this.listT2.add(0, cacheData);
                }

                return cacheData.getPage();
            }
        }


        for (int i = 0; i < this.listT2.size(); i++) {
            CacheData cacheData = this.listT2.get(i);

            if (cacheData.getResourceId() == resourceId && cacheData.getPage().getPageNumber() == pageNumber && !cacheData.getEviction()) {
                if (pin) {
                    cacheData.setPinNumber(cacheData.getPinNumber() + 1);
                }

                this.listT2.remove(i);
                this.listT2.add(0, cacheData);

                return cacheData.getPage();
            }
        }

        return null;
    }
    @Override
    public CacheableData getPage(int resourceId, int pageNumber) {
        return this.getPageAndPinOpt(resourceId, pageNumber, false);
    }
    @Override
    public CacheableData getPageAndPin(int resourceId, int pageNumber) {
        return this.getPageAndPinOpt(resourceId, pageNumber, true);
    }

    private void replacePage(byte[] bufsrc, byte[] bufdest) {
        System.arraycopy(bufsrc, 0, bufdest, 0, bufdest.length);
    }

    private boolean checkBlist(ArrayList<Pair<Integer, Integer>> Blist, int resourceId, int pageNum) {
        for (Pair<Integer, Integer> resourceIdPageNum : Blist) {
            if (resourceIdPageNum.getFirst() == resourceId && resourceIdPageNum.getSecond() == pageNum) {
                return true;
            }
        }

        return false;
    }

    private boolean checkTList(ArrayList<CacheData> Tlist, int resourceId, int pageNum) {
        for (CacheData cd : Tlist) {
            if (cd.getResourceId() == resourceId && cd.getPage().getPageNumber() == pageNum) {
                return true;
            }
        }

        return false;
    }

    private int findNearestUnpinned(ArrayList<CacheData> listT) {
        for (int i = listT.size() - 1; i >= 0; i--) {
            if (listT.get(i).getPinNumber() == 0) {
                return i;
            }
        }

        return -1;
    }

    private void evictBlists(int ResourceId, int pageNum) {
        if (checkBlist(this.listB1, ResourceId, pageNum) || checkBlist(this.listB2, ResourceId, pageNum)) return;

        if (this.listT1.size() + this.listB1.size() == this.numPages && this.listT1.size() < this.numPages) {
            this.listB1.remove(this.listB1.size() - 1);
        }

        if (this.listT1.size() == numPages && this.listB1.isEmpty()) {
            this.listT1.remove(this.listT1.size() - 1);
        }

        if (this.listT1.size() + this.listB1.size() < this.numPages && this.listT1.size() + this.listB1.size() + this.listT2.size() + this.listB2.size() == 2 * this.numPages) {
            this.listB2.remove(this.listB2.size() - 1);
        }
    }

    private int findEvicted(ArrayList<CacheData> listT) {
        for (int i = 0; i < listT.size(); i++) {
            CacheData cd = listT.get(i);
            if (cd.getEviction()) {
                return i;
            }
        }

        return -1;
    }

    private void addToTAndChangeAdapt(CacheData cd) {
        if (checkBlist(this.listB1, cd.getResourceId(), cd.getPage().getPageNumber()) || checkBlist(this.listB2, cd.getResourceId(), cd.getPage().getPageNumber())) {
            this.listT2.add(0, cd);
        } else {
            this.listT1.add(0, cd);
        }

        // change adaptation
        if (checkBlist(this.listB1, cd.getResourceId(), cd.getPage().getPageNumber())) {
            this.adaptation = (float) Math.min(this.numPages, this.adaptation + (this.listB1.size() > this.listB2.size() ? 1.0 : ((float) this.listB2.size() / this.listB1.size())));
        }

        if (checkBlist(this.listB2, cd.getResourceId(), cd.getPage().getPageNumber())) {
            this.adaptation = (float) Math.max(0.0, this.adaptation - (this.listB2.size() > this.listB1.size() ? 1.0 : ((float) this.listB1.size() / this.listB2.size())));
        }
    }

    // replaces the best cache entry and returns it
    private EvictedCacheEntry getAndAdapt(CacheableData newPage, int resourceId, int freeEvictIndex, boolean T1prio, boolean pin) throws CachePinnedException {
        // cache is cold
        if (freeEvictIndex != -1) {
            byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
            this.replacePage(newPage.getBuffer(), this.binaryPageArray[freeEvictIndex]);
            ((TablePageClass) newPage).setBuffer(this.binaryPageArray[freeEvictIndex]);
            CacheData cdNew = new CacheData(resourceId, newPage, freeEvictIndex);
            this.addToTAndChangeAdapt(cdNew);

            return new EvictedCacheEntry(evictedByteArray);
        }

        int toEvictTIndex = -1;
        // search expelled paged and replace
        toEvictTIndex = this.findEvicted(this.listT1);
        if (toEvictTIndex != -1) {
            CacheData cdOld = this.listT1.remove(toEvictTIndex);
            Pair<Integer, Integer> b1Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
            byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
            this.replacePage(evictedByteArray, this.binaryPageArray[cdOld.getPagIdx()]);
            this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
            this.listB1.add(0, b1Pair);
            ((TablePageClass) newPage).setBuffer(this.binaryPageArray[cdOld.getPagIdx()]);
            CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());
            this.addToTAndChangeAdapt(cdNew);
            ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);
            return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
        } else {
            toEvictTIndex = this.findEvicted(this.listT2);
            if (toEvictTIndex != -1) {
                CacheData cdOld = this.listT2.remove(toEvictTIndex);
                Pair<Integer, Integer> b2Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
                byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
                this.replacePage(this.binaryPageArray[cdOld.getPagIdx()], evictedByteArray);
                this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
                this.listB2.add(0, b2Pair);
                ((TablePageClass) newPage).setBuffer(this.binaryPageArray[cdOld.getPagIdx()]);
                CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());
                this.addToTAndChangeAdapt(cdNew);
                ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);
                return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
            }
        }

        // Cache is full and searching for elem to evict
        if (T1prio) {
            toEvictTIndex = this.findNearestUnpinned(this.listT1);
            if (toEvictTIndex != -1) {
                CacheData cdOld = this.listT1.remove(toEvictTIndex);
                Pair<Integer, Integer> b1Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
                byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
                this.replacePage(this.binaryPageArray[cdOld.getPagIdx()], evictedByteArray);
                this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
                this.listB1.add(0, b1Pair);
                ((TablePageClass) newPage).setBuffer(this.binaryPageArray[cdOld.getPagIdx()]);
                CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());

                if (pin) {
                    cdNew.setPinNumber(cdNew.getPinNumber() + 1);
                }

                this.addToTAndChangeAdapt(cdNew);
                ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);
                return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
            } else {
                toEvictTIndex = this.findNearestUnpinned(this.listT2);
                if (toEvictTIndex != -1) {
                    CacheData cdOld = this.listT2.remove(toEvictTIndex);
                    Pair<Integer, Integer> b2Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
                    byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
                    this.replacePage(this.binaryPageArray[cdOld.getPagIdx()], evictedByteArray);
                    this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
                    this.listB2.add(0, b2Pair);
                    ((TablePageClass) newPage).setBuffer(this.binaryPageArray[cdOld.getPagIdx()]);
                    CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());

                    if (pin) {
                        cdNew.setPinNumber(cdNew.getPinNumber() + 1);
                    }

                    this.addToTAndChangeAdapt(cdNew);
                    ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);

                    return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
                } else {
                    throw new CachePinnedException();
                }
            }
        } else {
            // DO NOT WRITE PAGE FIRST
            toEvictTIndex = this.findNearestUnpinned(this.listT2);
            if (toEvictTIndex != -1) {
                CacheData cdOld = this.listT2.remove(toEvictTIndex);
                Pair<Integer, Integer> b2Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
                byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
                this.replacePage(this.binaryPageArray[cdOld.getPagIdx()], evictedByteArray);
                this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
                this.listB2.add(0, b2Pair);
                CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());

                if (pin) {
                    cdNew.setPinNumber(cdNew.getPinNumber() + 1);
                }

                this.addToTAndChangeAdapt(cdNew);
                ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);

                return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
            } else {
                toEvictTIndex = this.findNearestUnpinned(this.listT1);
                if (toEvictTIndex != -1) {
                    CacheData cdOld = this.listT1.remove(toEvictTIndex);
                    Pair<Integer, Integer> b1Pair = new Pair<>(cdOld.getResourceId(), cdOld.getPage().getPageNumber());
                    byte[] evictedByteArray = new byte[this.pageSize.getNumberOfBytes()];
                    this.replacePage(this.binaryPageArray[cdOld.getPagIdx()], evictedByteArray);
                    this.replacePage(newPage.getBuffer(), this.binaryPageArray[cdOld.getPagIdx()]);
                    this.listB1.add(0, b1Pair);
                    CacheData cdNew = new CacheData(resourceId, newPage, cdOld.getPagIdx());

                    if (pin) {
                        cdNew.setPinNumber(cdNew.getPinNumber() + 1);
                    }

                    this.addToTAndChangeAdapt(cdNew);
                    ((TablePageClass) cdOld.getPage()).setBuffer(evictedByteArray);

                    return new EvictedCacheEntry(evictedByteArray, cdOld.getPage(), cdOld.getResourceId());
                } else {
                    throw new CachePinnedException();
                }
            }
        }
    }

    public EvictedCacheEntry addPagepinOpt(CacheableData newPage, int resourceId, boolean pin) throws DuplicateCacheEntryException, CachePinnedException {

        if (checkTList(this.listT1, resourceId, newPage.getPageNumber()) || checkTList(this.listT2, resourceId, newPage.getPageNumber())) {
            throw new DuplicateCacheEntryException(resourceId, newPage.getPageNumber());
        }

        // cold cache
        if (this.freeIdx < this.numPages) {
            this.evictBlists(resourceId, newPage.getPageNumber());
            EvictedCacheEntry ec = this.getAndAdapt(newPage, resourceId, this.freeIdx, false, pin);
            this.freeIdx++;

            return ec;
        }

        // case a
        if (!this.listT1.isEmpty() && (this.listT1.size() > this.adaptation || (this.listT1.size() == this.adaptation && checkBlist(this.listB2, resourceId, newPage.getPageNumber())))) {
            this.evictBlists(resourceId, newPage.getPageNumber());
            return this.getAndAdapt(newPage, resourceId, -1, true, pin);
        } else {
            // case b
            this.evictBlists(resourceId, newPage.getPageNumber());
            return this.getAndAdapt(newPage, resourceId, -1, false, pin);
        }
    }
    @Override
    public EvictedCacheEntry addPage(CacheableData newPage, int resourceId) throws DuplicateCacheEntryException, CachePinnedException {
        return addPagepinOpt(newPage, resourceId, false);
    }
    @Override
    public EvictedCacheEntry addPageAndPin(CacheableData newPage, int resourceId) throws DuplicateCacheEntryException, CachePinnedException {
        return addPagepinOpt(newPage, resourceId, true);
    }
    @Override
    public void unpinPage(int resourceId, int pageNumber) {
        for (CacheData cacheData : this.listT1) {
            if (cacheData.getResourceId() == resourceId && cacheData.getPage().getPageNumber() == pageNumber) {
                cacheData.setPinNumber(cacheData.getPinNumber() - 1);
            }
        }

        for (CacheData cacheData : this.listT2) {
            if (cacheData.getResourceId() == resourceId && cacheData.getPage().getPageNumber() == pageNumber) {
                cacheData.setPinNumber(cacheData.getPinNumber() - 1);
            }
        }
    }
    @Override
    public void unpinAllPages() {
        for (CacheData cacheData : this.listT1) {
            cacheData.setPinNumber(0);
        }

        for (CacheData cacheData : this.listT2) {
            cacheData.setPinNumber(0);
        }
    }
    @Override
    public int getCapacity() {
        return this.numPages;
    }
    @Override
    public CacheableData[] getAllPagesForResource(int resourceId) {
        ArrayList<CacheableData> cads = new ArrayList<>();
        for (CacheData cd : this.listT1) {
            if (cd.getResourceId() == resourceId) {
                cads.add(cd.getPage());
            }
        }

        for (CacheData cd : this.listT2) {
            if (cd.getResourceId() == resourceId) {
                cads.add(cd.getPage());
            }
        }


        CacheableData[] cads_arr = new CacheableData[cads.size()];

        return cads.toArray(cads_arr);
    }
    @Override
    public void expelAllPagesForResource(int resourceId) {


        for (CacheData cd : this.listT1) {
            if (cd.getResourceId() == resourceId) {
                cd.toEvict();
            }
        }

        for (CacheData cd : this.listT2) {
            if (cd.getResourceId() == resourceId) {
                cd.toEvict();
            }
        }
    }
}
