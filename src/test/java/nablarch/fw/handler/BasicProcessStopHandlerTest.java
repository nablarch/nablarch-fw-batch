package nablarch.fw.handler;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.ThreadContext;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.handler.ProcessStopHandler.ProcessStop;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link ProcessStopHandler}のテストクラス。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class BasicProcessStopHandlerTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    /** テストに必要となる情報をセットアップする。 */
    @BeforeClass
    public static void beforeClass() {
        VariousDbTestHelper.createTable(HandlerBatchRequest.class);
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止とならないケースのため正常に処理が終わること。
     */
    @Test
    public void testHandler1() {
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000005", "リクエスト０５", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000001");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.initialize();

        // 処理を停止しない場合
        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        handlerList.add(new DummyHandler());
        context.setHandlerQueue(handlerList);

        for (int i = 0; i < 10; i++) {
            Object result = new ExecutionContext(context).handleNext(
                    String.valueOf(i));
            assertThat(result.toString(), is(String.valueOf(i)));
        }
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止となるパターン。チェック間隔は1なので、最初のデータで処理停止となること。
     */
    @Test
    public void testHandler2() {

        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000005", "リクエスト０５", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000005");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setExitCode(123);
        handler.setCheckInterval(1);
        handler.initialize();

        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        handlerList.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                if ("5".equals(o)) {
                    VariousDbTestHelper.setUpTable(
                            new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                            new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                            new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                            new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                            new HandlerBatchRequest("RW000005", "リクエスト０５", "1", "1", "1", 0L),
                            new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));
                }
                return o;
            }
        });
        context.setHandlerQueue(handlerList);

        List<Object> list = new ArrayList<Object>();
        for (int i = 1; i < 50; i++) {
            try {
                list.add(new ExecutionContext(context).handleNext(String.valueOf(
                        i)));
            } catch (ProcessStop e) {
                assertThat("ステータスコードは設定した値であること", e.getStatusCode(), is(123));
            }
        }
        assertThat("5件目で終了フラグがオンになるので、処理できた件数は5件であること", list.size(), is(5));
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止となるパターン。チェック間隔は-1なので、最初のデータで処理停止となること。
     */
    @Test
    public void testHandler3() {

        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000005", "リクエスト０５", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000005");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setCheckInterval(-1);
        handler.setExitCode(10);
        handler.initialize();

        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        handlerList.add(new DummyHandler());
        context.setHandlerQueue(handlerList);

        List<String> result = new ArrayList<String>();
        try {
            for (int i = 0; i < 10; i++) {
                result.add(String.valueOf(new ExecutionContext(context)
                        .handleNext(
                                String.valueOf(i))));
            }
            fail("dose not run.");
        } catch (ProcessStop e) {
            assertThat("ステータスコードは、設定した値であること", e.getStatusCode(), is(10));

        }

        assertThat("1件目で処理停止となるため、結果オブジェクトのサイズは0であること。", result.size(), is(0));
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止となるパターン。チェック間隔は10なので、10件目でエラーとなること。
     */
    @Test
    public void testHandler4() {

        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000005", "リクエスト０５", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000005");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setCheckInterval(10);
        handler.initialize();

        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        handlerList.add(new Handler<Object, Object>() {
            public Object handle(Object o, ExecutionContext context) {
                VariousDbTestHelper.setUpTable(
                        new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                        new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                        new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                        new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                        new HandlerBatchRequest("RW000005", "リクエスト０５", "1", "1", "1", 0L),
                        new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));
                return o;
            }
        });
        context.setHandlerQueue(handlerList);

        List<String> result = new ArrayList<String>();
        try {
            for (int i = 0; i < 30; i++) {
                result.add(String.valueOf(new ExecutionContext(context)
                        .handleNext(
                                String.valueOf(i))));
            }
            fail("dose not run.");
        } catch (ProcessStop e) {
            e.printStackTrace();
        }

        assertThat("10件目で処理停止となるため、結果オブジェクトのサイズは10であること。", result.size(), is(10));
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止となるパターン。
     * チェック間隔は10で15件目で処理停止フラグがonになるため、
     * 20件目でエラーとなる。
     */
    @Test
    public void testHandler5() {

        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000002");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setCheckInterval(10);
        handler.initialize();

        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        // 15件目で処理停止フラグをオンにする。
        handlerList.add(new Handler<String, Object>() {
            public Object handle(String o, ExecutionContext context) {
                if ("15".equals(o)) {
                    VariousDbTestHelper.setUpTable(
                            new HandlerBatchRequest("RW000001", "リクエスト０１", "1", "1", "1", 0L),
                            new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                            new HandlerBatchRequest("RW000003", "リクエスト０３", "1", "1", "1", 0L));
                }
                return o;
            }
        });
        context.setHandlerQueue(handlerList);

        List<String> result = new ArrayList<String>();
        try {
            for (int i = 0; i < 30; i++) {
                result.add(String.valueOf(new ExecutionContext(context)
                        .handleNext(
                                String.valueOf(i))));
            }
            fail("dose not run.");
        } catch (ProcessStop e) {
            e.printStackTrace();
        }

        assertThat("20件目で処理停止となるため、結果オブジェクトのサイズは20であること。", result.size(), is(
                20));
    }

    /**
     * {@link ProcessStopHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理停止となるパターン。チェック間隔に100を設定しているが、
     * 初回起動時にプロセス停止フラグが'1'となっているため処理が即停止すること。
     */
    @Test
    public void testHandler6() {

        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000004", "リクエスト０４", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000005", "リクエスト０５", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000006", "リクエスト０６", "1", "1", "1", 0L));

        // リクエストIDの設定
        ThreadContext.setRequestId("RW000005");

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setCheckInterval(100);
        handler.setExitCode(10);
        handler.initialize();

        ExecutionContext context = new ExecutionContext();
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(handler);
        handlerList.add(new DummyHandler());
        context.setHandlerQueue(handlerList);

        List<String> result = new ArrayList<String>();
        try {
            for (int i = 0; i < 10; i++) {
                result.add(String.valueOf(new ExecutionContext(context)
                        .handleNext(
                                String.valueOf(i))));
            }
            fail("dose not run.");
        } catch (ProcessStop e) {
            assertThat("ステータスコードは、設定した値であること", e.getStatusCode(), is(10));

        }

        assertThat("1件目で処理停止となるため、結果オブジェクトのサイズは0であること。", result.size(), is(0));
    }


    /** {@link ProcessStopHandler#setExitCode(int)}のテスト。 */
    @Test
    public void testSetExitCode() {
        // テーブルデータの準備
        VariousDbTestHelper.setUpTable(
                new HandlerBatchRequest("RW000001", "リクエスト０１", "1", "1", "1", 0L),
                new HandlerBatchRequest("RW000002", "リクエスト０２", "0", "1", "1", 0L),
                new HandlerBatchRequest("RW000003", "リクエスト０３", "1", "1", "1", 0L));

        BasicProcessStopHandler handler = new BasicProcessStopHandler();
        SimpleDbTransactionManager transactionManager =
                SystemRepository.get("tran");
        handler.setDbTransactionManager(transactionManager);
        handler.setTableName("handler_batch_request");
        handler.setRequestIdColumnName("request_id");
        handler.setProcessHaltColumnName("process_halt_flg");
        handler.setCheckInterval(1);
        handler.initialize();

        ThreadContext.setRequestId("RW000001");

        // 正常系
        for (int i = 1; i <= 255; i++) {
            handler.setExitCode(i);
            try {
                handler.handle(null, new ExecutionContext());
                fail();
            } catch (ProcessStop e) {
                assertThat("設定した終了コードが取得出来ること。", e.getStatusCode(), is(i));
            }
        }

        // 異常系
        try {
            handler.setExitCode(0);
            fail("does not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString(
                    "exit code was invalid range."));
        }

        try {
            handler.setExitCode(256);
            fail("does not run");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString(
                    "exit code was invalid range."));
        }

    }

    private static class DummyHandler implements Handler<Object, Object> {

        public Object handle(Object o, ExecutionContext context) {
            return o;
        }
    }
}

