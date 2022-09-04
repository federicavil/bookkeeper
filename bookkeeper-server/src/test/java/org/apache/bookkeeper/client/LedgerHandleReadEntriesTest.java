package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.*;


@RunWith(Parameterized.class)
public class LedgerHandleReadEntriesTest extends LedgerHandleTest {

    private long firstEntry;
    private long lastEntry;
    private boolean isExceptionExpected;

    private List<Long> entryIds;
    private List<byte[]> entryDatas;
    private static final int N = 4;
    private static final int dataSize = 10;

    public LedgerHandleReadEntriesTest(long firstEntry, long lastEntry, boolean isExceptionExpected){
        super();
        configure(firstEntry,lastEntry,isExceptionExpected);
    }

    private void configure(long firstEntry, long lastEntry, boolean isExceptionExpected){
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.isExceptionExpected = isExceptionExpected;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {-1L,   -2L,    true},
                {-1L,   -1L,    true},
                {-1L,    0L,    true},
                {-1L,    4L,    true},
                { 0L,   -1L,    true},
                { 0L,    0L,    false},
                { 0L,    1L,    false},
                { 0L,    4L,    true},
                { 3L,    2L,    true},
                { 3L,    3L,    false},
                { 3L,    4L,    true},
                { 4L,    3L,    true},
                { 4L,    4L,    true},
                { 4L,    5L,    true},


        });
    }

    @Before
    public void addEntries(){
        entryIds = new ArrayList<>();
        entryDatas = new ArrayList<>();
        for (int i = 0; i < N; i++) {
            byte[] data = new byte[dataSize];
            new Random().nextBytes(data);
            entryDatas.add(data);
            try {
                entryIds.add(ledgerHandle.addEntry(data));
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BKException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void ReadEntriesWithClosedLedgerTest(){
        closeLedgerHandle();
        readEntriesTest();
    }

    @Test
    public void readEntriesTest(){
        Exception e = null;
        try {
            Enumeration<LedgerEntry> result = ledgerHandle.readEntries(this.firstEntry,this.lastEntry);
            checkResult(result);
        } catch (InterruptedException | BKException ex) {
            e = ex;
        }

        if(isExceptionExpected)
            Assert.assertNotNull(e);
        else
            Assert.assertNull(e);
    }

    public void checkResult(Enumeration<LedgerEntry> result){
        long nEntries = this.lastEntry - this.firstEntry +1;
        long currentId = this.firstEntry;
        try{
            while(true){
                LedgerEntry entry = result.nextElement();
                Assert.assertEquals(currentId,entry.getEntryId());
                int idx = entryIds.indexOf(currentId);
                String expected = new String(entryDatas.get(idx));
                String actual = new String(entry.getEntry());
                Assert.assertEquals(expected,actual);
                nEntries--;
                currentId++;
            }

        }catch(NoSuchElementException e){
            if(nEntries != 0){
                Assert.fail();
            }
        }

    }


}
