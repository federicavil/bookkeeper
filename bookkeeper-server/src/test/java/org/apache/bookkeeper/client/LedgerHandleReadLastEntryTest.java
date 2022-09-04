package org.apache.bookkeeper.client;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class LedgerHandleReadLastEntryTest extends LedgerHandleTest{

    private Object[][] entries;

    public LedgerHandleReadLastEntryTest(Object[][] entries){
        super();
        configure(entries);
    }

    private void configure(Object[][] entries) {
        this.entries = entries;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {new Object[][]{}},
                {new Object[][]{
                        {new byte[0],0,0}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3}
                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4}

                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  0}

                }},
                {new Object[][]{
                        {new byte[0],                              0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  3},
                        {"test".getBytes(StandardCharsets.UTF_8),  0,  4},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  0},
                        {"test".getBytes(StandardCharsets.UTF_8),  3,  1}

                }},
        });
    }

    @Before
    public void ledgerConfiguration(){
        if(entries.length > 0){
            try {
                for(Object[] toAdd : entries)
                    ledgerHandle.addEntry((byte[]) toAdd[0], (Integer) toAdd[1],(Integer) toAdd[2]);
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void readLastEntryTest(){
        try {
            LedgerEntry result = ledgerHandle.readLastEntry();
            Object[] lastEntryExpected = entries[entries.length-1];
            byte[] expectedData = new byte[(Integer)lastEntryExpected[2]];
            for (int i = 0; i < expectedData.length; i++){
                expectedData[i] = ((byte[])lastEntryExpected[0])[(int)lastEntryExpected[1]+i];
            }
            String expected = new String(expectedData);
            String actual = new String(result.getEntry());
            Assert.assertEquals(expected,actual);
            Assert.assertEquals((long)entries.length-1,result.getEntryId());
        } catch (InterruptedException | BKException e) {
            Assert.assertEquals(0,entries.length);
        }
    }

}
