package nablarch.fw.handler;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.ThreadContext;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.handler.DuplicateProcessCheckHandler.DuplicateProcess;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;

/**
 * {@link DuplicateProcessCheckHandler}のテストクラス。
 *
 * @author hisaaki sioiri
 */
public class DuplicateProcessCheckHandlerTest {

    /** テスト対象 */
    DuplicateProcessCheckHandler sut = new DuplicateProcessCheckHandler();

    /** 多重起動チェックのモックオブジェクト */
    @Mocked
    DuplicateProcessChecker mockChecker;

    @Before
    public void setUp() throws Exception {
        OnMemoryLogWriter.clear();
        sut.setDuplicateProcessChecker(mockChecker);
    }

    @After
    public void tearDown() throws Exception {
        ThreadContext.clear();
    }

    /**
     * {@link DuplicateProcessCheckHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * ２重起動とならない場合、正常に終了すること。
     */
    @Test
    public void testHandle() throws Exception {

        // テスト用のリクエストID設定
        ThreadContext.setRequestId("RW000003");

        sut.handle(null, new ExecutionContext().addHandler(new Handler<Object, Object>() {
            @Override
            public Object handle(Object o, ExecutionContext context) {
                return null;
            }
        }));

        new Verifications() {{
            mockChecker.checkAndActive("RW000003");
            mockChecker.inactive("RW000003");
        }};
    }

    /**
     * {@link DuplicateProcessCheckHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * ２重起動の場合、{@link nablarch.fw.launcher.ProcessAbnormalEnd}が送出されること。
     */
    @Test
    public void testAlreadyRunning() throws Exception {

        ThreadContext.setRequestId("RW000002");

        new Expectations() {{
            mockChecker.checkAndActive("RW000002");
            result = new AlreadyProcessRunningException();
        }};

        try {
            sut.handle(null, new ExecutionContext());
            fail("does not run.");
        } catch (DuplicateProcess e) {
            assertThat(e.getMessage(), is(
                    "specified request_id is already used by another process. "
                            + "you can not start two or more processes with same request_id."));
            assertThat(e.getStatusCode(), is(500));
        }
    }

    /**
     * {@link DuplicateProcessCheckHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 終了コードを明示的に設定した場合の動作確認
     */
    @Test
    public void testAlreadyRunningExitCode() throws Exception {

        new Expectations() {{
            mockChecker.checkAndActive("RW000004");
            result = new AlreadyProcessRunningException();
        }};


        // テストの準備
        sut.setExitCode(1);
        ThreadContext.setRequestId("RW000004");

        // テスト実行用の実行コンテキストの構築
        List<Handler<?, ?>> handlerList = new ArrayList<Handler<?, ?>>();
        handlerList.add(new StatusCodeConvertHandler());
        handlerList.add(new GlobalErrorHandler());
        handlerList.add(sut);

        final ExecutionContext context = new ExecutionContext();
        context.setHandlerQueue(handlerList);
        final int exitCode = context.<Object, Integer>handleNext(null);

        assertThat("終了コードは設定した1であること。", exitCode, is(1));
    }

    /**
     * {@link DuplicateProcessCheckHandler#handle(Object, nablarch.fw.ExecutionContext)}のテスト。
     * <p/>
     * 処理中にエラーが発生して処理を終了する場合でもアクティブフラグが'0'に更新されること。
     */
    @Test
    public void testInactiveOnError() throws Exception {

        // テスト用のリクエストID設定
        ThreadContext.setRequestId("RW000003");

        final ExecutionContext context = new ExecutionContext();
        context.addHandler(sut);
        context.addHandler(new Handler<Object, Object>() {
            @Override
            public Object handle(Object o, ExecutionContext context) {
                throw new IllegalArgumentException("error!");
            }
        });


        try {
            context.handleNext(null);
            fail("does not run");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("error!"));
        }

        new Verifications() {{
            mockChecker.checkAndActive("RW000003");
            // 例外が発生しても非アクティブ化が呼び出されること。
            mockChecker.inactive("RW000003");
        }};

    }


    /**
     * {@link DuplicateProcessCheckHandler#setExitCode(int)} のテスト。
     *
     * @throws Exception
     */
    @Test
    public void testSetExitCode() throws Exception {
        new Expectations() {{
            mockChecker.checkAndActive("RW000002");
            result = new AlreadyProcessRunningException();
        }};

        ThreadContext.setRequestId("RW000002");

        // 1～255はOK
        for (int i = 1; i <= 255; i++) {
            sut.setExitCode(i);
            try {
                sut.handle(null, new ExecutionContext());
                fail("does not run.");
            } catch (DuplicateProcess e) {
                assertThat(e.getStatusCode(), is(i));
            }
        }

        // 0以下はNG
        try {
            sut.setExitCode(0);
            fail("does not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString(
                    "exit code"));
        }
        // 256以上はNG
        try {
            sut.setExitCode(256);
            fail("does not run.");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString(
                    "exit code"));
        }
    }

    /**
     * finallyでのアクティブフラグOFF処理で例外が発生した場合その例外が送出されること。
     * <p/>
     * また、WARNレベルのログが出力されること。
     */
    @Test
    public void testNormalBodyAndFailedFinally() throws Exception {

        new Expectations() {{
            mockChecker.inactive("RW000002");
            result = new IllegalStateException("IllegalStateException:error");
        }};

        final ExecutionContext context = new ExecutionContext();
        context.addHandler(sut);
        context.addHandler(new Handler<Object, Object>() {
            @Override
            public Object handle(Object o, ExecutionContext context) {
                return null;
            }
        });
        ThreadContext.setRequestId("RW000002");
        try {
            context.handleNext(null);
            fail("");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("IllegalStateException:error"));
        }

        List<String> warnLogs = findWarnLogs();
        assertThat(warnLogs.size(), is(1));
        assertThat(warnLogs.get(0), containsString("IllegalStateException:error"));
    }

    /**
     * try句で例外（非チェック例外）が発生していて、finallyで再度例外が発生した場合。
     * <p/>
     * finallyで発生した例外がWARNレベルのログで出力され、元例外がthrowされること。
     */
    @Test
    public void testExceptionBodyAndFailedFinally() throws Exception {

        new Expectations() {{
            mockChecker.inactive("RW000002");
            result = new OutOfMemoryError("");
        }};

        ExecutionContext context = new ExecutionContext();
        context.addHandler(sut);
        context.addHandler(new Handler<Object, Object>() {
            @Override
            public Object handle(Object o, ExecutionContext context) {
                throw new IllegalArgumentException("IllegalArgumentException:error");
            }
        });

        ThreadContext.setRequestId("RW000002");
        try {
            context.handleNext(null);
            fail("");
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), containsString("IllegalArgumentException:error"));
        }

        List<String> warnLogs = findWarnLogs();
        assertThat(warnLogs.size(), is(1));
        assertThat(warnLogs.get(0), containsString("failed to disable process."));
    }

    /**
     * try句で例外（エラー）が発生していて、finallyで再度例外が発生した場合。
     * <p/>
     * finallyで発生した例外がWARNレベルのログで出力され、元例外がthrowされること。
     */
    @Test
    public void testErrorBodyAndFailedFinally() throws Exception {

        new Expectations() {{
            mockChecker.inactive("RW000002");
            result = new NullPointerException("null error.");
        }};

        final ExecutionContext context = new ExecutionContext();
        context.addHandler(sut);
        context.addHandler(new Handler<Object, Object>() {
            @Override
            public Object handle(Object o, ExecutionContext context) {
                throw new ClassFormatError("class format error.");
            }
        });

        ThreadContext.setRequestId("RW000002");
        try {
            context.handleNext(null);
            fail("");
        } catch (ClassFormatError e) {
            assertThat(e.getMessage(), containsString("class format error."));
        }

        List<String> warnLogs = findWarnLogs();
        assertThat(warnLogs.size(), is(1));
        assertThat(warnLogs.get(0), containsString("failed to disable process."));
    }

    /**
     * ログ出力結果からWARNレベルのログを取得する。
     *
     * @return
     */
    private static List<String> findWarnLogs() {
        List<String> warnMessages = new ArrayList<String>();
        List<String> messages = OnMemoryLogWriter.getMessages("writer.memory");
        for (String message : messages) {
            if (message.contains("WARN")) {
                warnMessages.add(message);
            }
        }
        return warnMessages;
    }

    /** アクティブフラグをアサートする。 */
    private void assertActiveFlg(final String requestId,
            final String expected) {
        HandlerBatchRequest hbr = VariousDbTestHelper.findById(HandlerBatchRequest.class, requestId);

        // 比較を実施
        assertThat(hbr, is(notNullValue()));
        assertThat("アクティブフラグは" + expected + "であること", hbr.processActiveFlg, is(expected));
    }
}
