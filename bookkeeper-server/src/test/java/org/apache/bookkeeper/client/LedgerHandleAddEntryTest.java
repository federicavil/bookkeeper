package org.apache.bookkeeper.client;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.*;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.tls.SecurityException;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.versioning.Versioned;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.*;

import java.awt.print.Book;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class LedgerHandleAddEntryTest extends LedgerHandleTest{

    private byte[] data;
    private int offset;
    private int length;
    private boolean isExceptionExpected;

   // @Spy
//    private BookKeeper bk;
//    @Mock
//    private BookieWatcher bookieWatcher;
//    @Spy
//    private LedgerIdGenerator ledgerIdGenerator;
//    @Spy
//    private LedgerManager ledgerManager;
//    @Mock
//    private CompletableFuture<Versioned<LedgerMetadata>> whenComplete;
//    @Mock
//    private Versioned<LedgerMetadata> written;
//    @Mock
//    private LedgerMetadata metadata;
//
//    @Spy
//    @InjectMocks
//    private BookieClientImpl bookieClient;
//
//    @Spy
//    private ClientConfiguration clientConfiguration;
//    @Mock
//    private EventLoopGroup eventLoopGroup;
//    @Mock
//    private ByteBufAllocator allocator;
//    @Mock
//    private OrderedExecutor executor;
//    @Mock
//    private ScheduledExecutorService scheduler;
//    @Spy
//    private StatsLogger statsLogger;
//    @Mock
//    private BookieAddressResolver bookieAddressResolver;


    // Iterazione Jacoco
    //private boolean isLedgerHandleClosed;

    public LedgerHandleAddEntryTest(byte[] data, int offset, int length, boolean isLedgerHandleClosed, boolean isExceptionExpected) {
        super(isLedgerHandleClosed);
        configure(data,offset,length,isLedgerHandleClosed,isExceptionExpected);
    }

    private void configure(byte[] data, int offset, int length,boolean isLedgerHandleClosed, boolean isExceptionExpected){
        this.data = data;
        this.offset = offset;
        this.length = length;
        //this.isLedgerHandleClosed = isLedgerHandleClosed;
        this.isExceptionExpected = isExceptionExpected;

    }

//    @Before
//    public void setUp(){
//        //Iterazione Jacoco
//        if(isLedgerHandleClosed){
//            try {
//                ledgerHandle.close();
//            } catch (InterruptedException | BKException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {      new byte[0],                             -1, -1, false, true},
                {      new byte[0],                             -1,  0, false, true},
                {      new byte[0],                             -1,  1, false, true},
                {      new byte[0],                              0,  0, false, false},
                {      new byte[0],                              0, -1, false, true},
                {      new byte[0],                              0,  1, false, true},
                {      new byte[0],                              1, -1, false, true},
                {      new byte[0],                              1,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8), -1,  5, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  0, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  3, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  4, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  0,  5, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3, -1, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  0, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  1, false, false},
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  2, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4, -1, false, true},
                //{      "test".getBytes(StandardCharsets.UTF_8),  4,  0, false, true},
                {      "test".getBytes(StandardCharsets.UTF_8),  4,  1, false, true},
                //Iterazione Jacoco
                {      "test".getBytes(StandardCharsets.UTF_8),  3,  1, true, true},


        });
    }

//    @BeforeClass
//    public static void createLedgerHandle(){
//        boolean connected = false;
//        localBookkeeper = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    ServerConfiguration conf = new ServerConfiguration();
//                    conf.setAllowLoopback(true);
//                    LocalBookKeeper.startLocalBookies("127.0.0.1",2181,1,true,5000,conf);
//                } catch (Exception e) {
//                    System.out.println("bookkeeper fail");
//                }
//            }
//        });
//        localBookkeeper.start();
//        while(!connected){
//            try{
//                Thread.sleep(40000);
//                BookKeeper bk = new BookKeeper("127.0.0.1:2181");
//                ledgerHandle = bk.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
//                connected = true;
//
//            } catch (Exception ignored) {
//
//            }
//        }
//        BookKeeper bk = BookkeeperInstance.getBookkeeperInstance();
//        try {
//            ledgerHandle = bk.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
//        } catch (InterruptedException | BKException e) {
//            e.printStackTrace();
//        }
//
//    }

//    @Before
//    public void configureLedgerHandle(){
//        try {
//            MockitoAnnotations.initMocks(this);
//            doAnswer(invocation -> {
//                ((BookkeeperInternalCallbacks.GenericCallback<Long>) invocation.getArguments()[0]).operationComplete(BKException.Code.OK, 0L);
//                return  null;
//            }).when(ledgerIdGenerator).generateLedgerId(any(BookkeeperInternalCallbacks.GenericCallback.class));
//
//            doReturn(whenComplete).when(ledgerManager).createLedgerMetadata(anyLong(), any(LedgerMetadata.class));
//            when(bookieWatcher.newEnsemble(anyInt(), anyInt(), anyInt(), any())).thenReturn(Collections.singletonList(BookieId.parse("test")));
//            when(written.getValue()).thenReturn(metadata);
//            when(metadata.getEnsembleSize()).thenReturn(1);
//            when(metadata.getWriteQuorumSize()).thenReturn(1);
//            when(metadata.getAckQuorumSize()).thenReturn(1);
//            doAnswer( invocation -> {
//                ((BiConsumer<Versioned<LedgerMetadata>, Throwable>) invocation.getArguments()[0]).accept(written, null);
//                return null;
//            }).when(whenComplete).whenComplete(any(BiConsumer.class));
//
//            Map<Long, List<BookieId>> ensembles = new HashMap<>();
//            ensembles.put(0L, Collections.singletonList(BookieId.parse("test")));
//
//            NavigableMap<Long, List<BookieId>> ensembleMap = Collections.unmodifiableNavigableMap(
//                    ensembles.entrySet().stream().collect(TreeMap::new,
//                            (m, e) -> m.put(e.getKey(),
//                                    ImmutableList.copyOf(e.getValue())),
//                            TreeMap::putAll));
//            doReturn(ensembleMap).when(metadata).getAllEnsembles();
//            doReturn(ledgerIdGenerator).when(bk).getLedgerIdGenerator();
//            doReturn(bookieWatcher).when(bk).getBookieWatcher();
//            doReturn(ledgerManager).when(bk).getLedgerManager();
//            doReturn(bookieClient).when(bk).getBookieClient();
//            doReturn(statsLogger).when(statsLogger).scope(any());
//            doReturn(new BookieSocketAddress("127.0.0.1",2181)).when(bookieAddressResolver).resolve(any());
//            //doReturn(perChannelBookieClient).when(bookieClient).create(any(),any(),any(),anyBoolean());
//
//            OrderedExecutor executor = OrderedExecutor.newBuilder().build();
//            doReturn(executor).when(bk).getMainWorkerPool();
//            ledgerHandle = bk.createLedger(1,1,1,BookKeeper.DigestType.MAC,"test".getBytes(StandardCharsets.UTF_8));
//        } catch (BKException | InterruptedException  e) {
//            e.printStackTrace();
//        }
//    }

    @Test
    public void addEntryTest(){
        Exception exception = null;
        try{
            ledgerHandle.addEntry(this.data, this.offset, this.length);
        }catch(InterruptedException | BKException e){
            exception = e;
        }catch(Exception e){
            exception = e;
        }
        if(isExceptionExpected)
            Assert.assertNotNull(exception);
        else {
            Assert.assertNull(exception);
        }
    }



}
