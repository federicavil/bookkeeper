package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;


@RunWith(value = Parameterized.class)
public class EntryLoggerReadEntryTest {

    private static final int HEADER_SIZE = 2*(Long.BYTES) + (Integer.BYTES); //ledgerIdSize, EntryIdSize, Length

    private long ledgerId;
    private long entryId;
    private long location;
    private static long ledgerIdMatching;
    private static long entryIdMatching;
    private static long locationMatching;
    private static long locationNotMatching;
    private boolean isExceptionExpected;
    private static byte[] expectedValue;

    private static EntryLogger entryLogger;
    private static List<File> dirs;
    //Seconda iterazione Jacoco
    private static int maxSizeEntry;

    public EntryLoggerReadEntryTest(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected) {
        configure(ledgerId, entryId, location, isExceptionExpected);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters(){
        return Arrays.asList(new Object[][]{
                //LedgerId              EntryId,                Location,                isExceptionExpected
                {IdType.INVALID,        IdType.INVALID,         IdType.INVALID,          true},
                {IdType.INVALID,        IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.INVALID,        IdType.MATCHING,        IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.NOT_MATCHING,   IdType.MATCHING,        IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.INVALID,         IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.NOT_MATCHING,    IdType.MATCHING,         true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.INVALID,          true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.NOT_MATCHING,     true},
                {IdType.MATCHING,       IdType.MATCHING,        IdType.MATCHING,         false},
                //Seconda iterazione jacoco
                {IdType.MATCHING,       IdType.OVERSIZE,        IdType.MATCHING,         false},



        });
    }

    private void configure(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected) {
        switch(entryId){
            case INVALID:
                this.entryId = -1L;
                break;
            case MATCHING:
                this.entryId = entryIdMatching;
                break;
            case NOT_MATCHING:
                this.entryId = entryIdMatching +1;
                break;
            //iterazione jacoco
            case OVERSIZE:
                ledgerIdMatching = ledgerIdMatching+2;
                entryIdMatching = entryIdMatching +2;
                locationMatching = createNewEntry(ledgerIdMatching, entryIdMatching, maxSizeEntry);
                this.entryId = entryIdMatching;
                break;
        }

        switch(ledgerId){
            case INVALID:
                this.ledgerId = -1L;
                break;
            case MATCHING:
                this.ledgerId = ledgerIdMatching;
                break;
            case NOT_MATCHING:
                this.ledgerId = ledgerIdMatching +1;
                break;
        }

        switch(location){
            case INVALID:
                this.location = 1023L;
                break;
            case MATCHING:
                this.location = locationMatching;
                break;
            case NOT_MATCHING:
                this.location = locationNotMatching;
                break;
        }

        this.isExceptionExpected = isExceptionExpected;
    }

    @BeforeClass
    public static void setUp(){
        try {

            File rootDir = IOUtils.createTempDir("readEntryTest",".dir");
            File curDir = Bookie.getCurrentDirectory(rootDir);
            Bookie.checkDirectoryStructure(curDir);

            dirs = new ArrayList<>();
            dirs.add(rootDir);
            dirs.add(curDir);

            ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
            maxSizeEntry = conf.getNettyMaxFrameSizeBytes();

            entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, new File[]{rootDir},new DiskChecker(
                    conf.getDiskUsageThreshold(),
                    conf.getDiskUsageWarnThreshold())));

            ledgerIdMatching = 0L;
            entryIdMatching = 0L;
            locationNotMatching = createNewEntry(10L,10L);
            locationMatching = createNewEntry(0L,0L);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private static long createNewEntry(long ledgerId, long entryId){
        return createNewEntry(ledgerId, entryId, 4);
    }

    private static long createNewEntry(long ledgerId, long entryId, int dataSize){
        long location = -1L;
        try {
            // test data
            byte[] data = new byte[dataSize];
            new Random().nextBytes(data);
            expectedValue = data;

            int entrySize = HEADER_SIZE + dataSize;

            ByteBuf entry = Unpooled.buffer(entrySize);
            entry.writeLong(ledgerId);
            entry.writeLong(entryId);
            entry.writeInt(entrySize);
            entry.writeBytes(expectedValue);

            location = entryLogger.addEntry(ledgerId,entry,true);

        }catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        return location;
    }

    @Test
    public void test_internalReadEntry(){
        Exception exception = null;
        try {
            entryLogger.internalReadEntry(this.ledgerId,this.entryId,this.location,false);
        } catch (Exception e) {
            exception = e;
        }
        Assert.assertNull(exception);
    }


    @Test
    public void test_readEntry(){
        Exception exception = null;
        ByteBuf result = null;
        try{
            result = entryLogger.readEntry(this.ledgerId,this.entryId,this.location);
        }catch(IOException | IllegalArgumentException e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else{
            Assert.assertNull(exception);
            checkResult(result);
        }
    }

    private void checkResult(ByteBuf result){
        byte[] all_data = new byte[HEADER_SIZE + expectedValue.length];
        result.readBytes(all_data);

        byte[] final_data = new byte[expectedValue.length];
        for(int i = HEADER_SIZE; i < HEADER_SIZE + expectedValue.length; i++){
            final_data[i-HEADER_SIZE] = all_data[i];
        }
        String expected = new String(expectedValue);
        String actual = new String(final_data, Charset.defaultCharset());
        Assert.assertEquals(expected,actual);
    }

    @AfterClass
    public static void tearDown(){
        entryLogger.shutdown();

    }

    private enum IdType{
        INVALID,
        MATCHING,
        NOT_MATCHING,
        //Seconda iterazione Jacoco
        OVERSIZE
    }

}


