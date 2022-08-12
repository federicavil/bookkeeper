package org.apache.bookkeeper;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


@RunWith(value = Parameterized.class)
public class ReadEntryTest {

    private final int HEADER_SIZE = 2*(Long.BYTES) + (Integer.BYTES); //ledgerIdSize, EntryIdSize, Length

    private long ledgerId;
    private long entryId;
    private long location;
    private boolean isExceptionExpected;
    private byte[] expectedValue;

    private long ledgerIdMatching;
    private long entryIdMatching;
    private long locationMatching;
    private static EntryLogger entryLogger;
    private static List<File> dirs;
    //Seconda iterazione Jacoco
    private static int maxSizeEntry;
    private static long numEntries;

    static LedgerHandle ledgerHandle;


    public ReadEntryTest(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected) {
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
                //{IdType.MATCHING,       IdType.OVERSIZE,        IdType.MATCHING,         false},
                //Terza iterazione Ba-Dua


        });
    }

    private void configure(IdType ledgerId, IdType entryId, IdType location, boolean isExceptionExpected) {
        //createNewEntry();
        switch(entryId){
            case INVALID:
                this.entryId = -1L;
                createNewEntry();
                break;
            case MATCHING:
                createNewEntry();
                this.entryId = this.entryIdMatching;
                break;
            case NOT_MATCHING:
                createNewEntry();
                this.entryId = ThreadLocalRandom.current().nextLong(123456789L);
                break;
            //Seconda iterazione jacoco
            case OVERSIZE:
                createNewEntry(maxSizeEntry);
                this.entryId = this.entryIdMatching;
                break;
        }

        switch(ledgerId){
            case INVALID:
                this.ledgerId = -1L;
                break;
            case MATCHING:
                this.ledgerId = this.ledgerIdMatching;
                break;
            case NOT_MATCHING:
                this.ledgerId = ledgerHandle.getId();
                break;
        }

        switch(location){
            case INVALID:
                this.location = 1023L;
                break;
            case MATCHING:
                this.location = this.locationMatching;
                break;
            case NOT_MATCHING:
                this.location = this.locationMatching+1;
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

            ledgerHandle = Mockito.mock(LedgerHandle.class);
            Mockito.when(ledgerHandle.getId()).thenAnswer(new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock invocation) throws Throwable {
                    return ThreadLocalRandom.current().nextLong(123456789L);
                }
            });
            numEntries = 0L;
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    private void createNewEntry(){
        createNewEntry(4);
    }

    private void createNewEntry(int dataSize){
        try {
            this.ledgerIdMatching = ledgerHandle.getId();
            // test data
            byte[] data = new byte[dataSize];
            new Random().nextBytes(data);
            this.expectedValue = data;

            int entrySize = HEADER_SIZE + dataSize;
            this.entryIdMatching = numEntries;
            numEntries++;

            ByteBuf entry = Unpooled.buffer(entrySize);
            entry.writeLong(this.ledgerIdMatching);
            entry.writeLong(this.entryIdMatching);
            entry.writeInt(entrySize);
            entry.writeBytes(this.expectedValue);

            this.locationMatching = entryLogger.addEntry(this.ledgerIdMatching,entry,true);

        }catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
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
            byte[] all_data = new byte[HEADER_SIZE + this.expectedValue.length];
            result.readBytes(all_data);

            byte[] final_data = new byte[this.expectedValue.length];
            for(int i = HEADER_SIZE; i < HEADER_SIZE + this.expectedValue.length; i++){
                final_data[i-HEADER_SIZE] = all_data[i];
            }
            String expected = new String(this.expectedValue);
            String actual = new String(final_data, Charset.defaultCharset());
            Assert.assertEquals(expected,actual);
        }
    }
    /*
    @AfterClass
    public static void tearDown(){
        try {
            //entryLogger.shutdown();
            for(File dir: dirs){
                FileUtils.deleteDirectory(dir);
            }
        } catch (IOException  e) {
            e.printStackTrace();
            //Assert.fail();
        }finally {
            dirs.clear();
        }
    }
*/
    private enum IdType{
        INVALID,
        MATCHING,
        NOT_MATCHING,
        //Seconda iterazione Jacoco
        OVERSIZE
    }

}


