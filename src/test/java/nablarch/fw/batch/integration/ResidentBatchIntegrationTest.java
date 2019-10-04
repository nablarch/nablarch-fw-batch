package nablarch.fw.batch.integration;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import nablarch.common.io.FileRecordWriterHolder;
import nablarch.core.dataformat.FileRecordWriter;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.log.LogUtil;
import nablarch.core.message.MessageNotFoundException;
import nablarch.core.message.StringResource;
import nablarch.core.message.StringResourceHolder;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

/**
 * 常駐バッチ処理の機能結合テストクラス。
 * <p/>
 * 本クラスでは常駐バッチの標準ハンドラ構成を構築し、
 * 機能結合テストを実施する。
 *
 * @author hisaaki sioiri
 */
@RunWith(DatabaseTestRunner.class)
public class ResidentBatchIntegrationTest {
    /** テストメソッド名取得ルール */
    @Rule
    public TestName testName = new TestName();

    /** システムプロパティ復元用ルール */
    @Rule
    public SystemPropertyRule systemPropertyRule = new SystemPropertyRule();

    /** テスト用の一時ディレクトリ */
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    /** 常駐バッチ実行用スレッド */
    private static final ExecutorService service = Executors.newSingleThreadExecutor();

    /**
     * テストクラス全体のセットアップ処理。
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(TestBatchRequest3.class);
        VariousDbTestHelper.createTable(BatchIntegrationInput.class);
        VariousDbTestHelper.createTable(BatchIntegrationOutput.class);
    }

    /**
     * テストクラス全体の破棄処理。
     */
    @AfterClass
    public static void tearDownClass() throws Exception {
        service.shutdownNow();
    }

    /**
     * テストケースのセットアップ処理。
     *
     * テストテーブルのデータセットアップや不要なリソースの初期化処理を行う。
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        System.out.println("nablarch.fw.batch.integration.ResidentBatchIntegrationTest#" + testName.getMethodName() + " start");
        System.clearProperty("nablarch.appLog.filePath");
        LogUtil.removeAllObjectsBoundToContextClassLoader();
        OnMemoryLogWriter.clear();
        setUpTestData();
        systemPropertyRule.setSystemProperty("dir", folder.getRoot().toURI().toString());
    }

    /**
     * テストデータのセットアップ処理を行う。
     */
    private static void setUpTestData() {
        VariousDbTestHelper.delete(BatchIntegrationInput.class);
        VariousDbTestHelper.delete(BatchIntegrationOutput.class);
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest3("01", "0","0","1")
        );
    }


    /**
     * シングルスレッド構成を想定した常駐バッチアプリケーションのテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>入力データを取得し処理ができていること</li>
     * <li>正常終了時のコールバックが実行されていること（ステータスが処理済みになっていることで確認）</li>
     * <li>入力データのキー情報がログに出力されていること</li>
     * <li>バッチの実行中フラグのオン・オフの制御が行われていること</li>
     * <li>処理停止をオンにした場合プロセスが終了すること</li>
     * </ul>
     */
    @Test
    public void testSingleThreadBatch() throws Exception {

        ThreadControlHandler.countDownLatch = new CountDownLatch(5);

        // setup
        setupInputData(1, 5);
        systemPropertyRule.setSystemProperty("threadCount", "1");

        Future<Integer> future = executeBatch();
        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        SqlRow activeBatchRequest = findBatchRequest("01");
        assertThat("バッチプロセス実行中なので、アクティブフラグがONになっている", activeBatchRequest.getString("act"), is("1"));

        // stop batch process
        stopBatchProcess("01");

        assertThat("バッチ処理は正常に終了すること", future.get(), is(0));
        assertThat("全てのレコードが処理済みになっていること", findSuccessRecord().size(), is(5));
        assertThat("アウトプットテーブルにレコードが作成されていること", findOutputTable().size(), is(5));

        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        List<String> inputInfo = filterInputRecordLog(logMessages);
        assertThat("入力情報ログが5レコード分出力されていること", inputInfo.size(), is(5));
        int count = 1;
        for (String log : inputInfo) {
            assertThat(log, containsString("key info: {ID=" + count + "}"));
            count++;
        }

        // 処理停止フラグと実行中フラグの確認
        SqlRow batchRequest = findBatchRequest("01");
        assertThat("バッチプロセスが停止したのでアクティブフラグがOFFになっていること", batchRequest.getString("act"), is("0"));
    }

    /**
     * マルチスレッド構成を想定したバッチアプリケーションのテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>マルチスレッド環境下でも入力データが処理されていること</li>
     * <li>マルチスレッド環境下でも正常終了時のコールバック処理が行われていること（処理ステータスの更新で確認）</li>
     * <li>マルチスレッド環境下でも入力レコード情報がログ出力されていること</li>
     * <li>マルチスレッド環境下でも処理停止をオンにすることでプロセスが終了すること</li>
     * </ul>
     */
    @Test
    public void testMultiThreadBatch() throws Exception {
        ThreadControlHandler.countDownLatch = new CountDownLatch(10);

        // setup
        setupInputData(1, 10);
        systemPropertyRule.setSystemProperty("threadCount", "3");

        Future<Integer> future = executeBatch();

        // バッチ処理が全レコード処理するまで待機
        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);
        stopBatchProcess("01");

        assertThat("バッチ処理は正常に終了すること", future.get(), is(0));
        assertThat("全てのレコードが処理済みになっていること", findSuccessRecord().size(), is(10));
        assertThat("アウトプットテーブルにレコードが作成されていること", findOutputTable().size(), is(10));
        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        List<String> inputInfo = filterInputRecordLog(logMessages);
        assertThat("入力情報ログが10レコード分出力されていること", inputInfo.size(), is(10));
        int count = 1;
        for (String log : inputInfo) {
            assertThat(log, containsString("key info: {ID=" + count + "}"));
            count++;
        }
    }

    /**
     * バッチ処理中に追加されたレコードも随時処理されることを確認するテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>未処理のデータが存在しなくてもバッチプロセスは終了しないこと</li>
     * <li>未処理のレコードがない状態でレコードを追加した場合、そのレコードが処理されること</li>
     * </ul>
     */
    @Test
    public void testProceedAddedRecord() throws Exception {

        systemPropertyRule.setSystemProperty("threadCount", "5");

        Future<Integer> future = executeBatch();
        assertThat("未処理レコードはなし", findUntreatedRecord().size(), is(0));
        assertThat("処理済みレコードもなし", findSuccessRecord().size(), is(0));

        // 処理対象レコードを追加
        ThreadControlHandler.countDownLatch = new CountDownLatch(5);
        setupInputData(10, 5);

        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        assertThat("未処理レコードはなし", findUntreatedRecord().size(), is(0));
        assertThat("追加されたレコードが全て処理される", findSuccessRecord().size(), is(5));

        ThreadControlHandler.countDownLatch = new CountDownLatch(10);
        setupInputData(100, 10);

        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);
        assertThat("未処理レコードはなし", findUntreatedRecord().size(), is(0));
        assertThat("追加されたレコードが全て処理される", findSuccessRecord().size(), is(15));

        stopBatchProcess("01");

        assertThat("バッチプロセスが正常終了すること", future.get(), is(0));
    }

    /**
     * バッチアクションハンドラで異常終了するレコードが存在している場合のテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>処理異常のコールバックが呼び出されること(処理ステータスが異常なっていることで確認)</li>
     * <li>処理異常のレコード以外は処理されること</li>
     * <li>リトライハンドラによりリトライがされること</li>
     * </ul>
     */
    @Test
    public void testAbnormalEndRecord() throws Exception {

        systemPropertyRule.setSystemProperty("threadCount", "2");

        ThreadControlHandler.countDownLatch = new CountDownLatch(11);

        VariousDbTestHelper.setUpTable(
                new BatchIntegrationInput(1, "異常終了するレコード","0")
        );
        setupInputData(10, 10);

        Future<Integer> future = executeBatch();

        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        stopBatchProcess("01");

        assertThat("バッチプロセスが正常終了すること", future.get(), is(0));

        assertThat("正常に処理可能なレコードは、ステータスが正常終了になっていること", findSuccessRecord().size(), is(10));
        assertThat("正常に処理できたレコードはアウトプットの書き込まれていること", findOutputTable().size(), is(10));
        assertThat("異常終了のレコードはステータスが異常終了となっていること", findAbnormalEndRecord().size(), is(1));

        // assert log
        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        System.out.println("nablarch.fw.batch.integration.ResidentBatchIntegrationTest#testAbnormalEndRecord  writer.appLog");
        sysOutLogMessages(logMessages);
        List<String> fatalLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("FATAL")) {
                fatalLog.add(message);
            }
        }
        assertThat("障害レコードが1レコードなので障害通知ログが1回出力されていること", fatalLog.size(), is(1));
        assertThat(fatalLog.get(0), containsString("invalid data. data=異常終了するレコード"));
        assertThat(fatalLog.get(0), containsString("java.lang.IllegalArgumentException"));

        List<String> retryLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("start retry")) {
                retryLog.add(message);
            }
        }
        assertThat("リトライを示すログが1件出力されていること", retryLog.size(), is(1));
        assertThat("リトライカウントは1であること", retryLog.get(0), containsString("retryCount[1]"));
        assertThat("リトライログはWARNレベルであること", retryLog.get(0), containsString("WARN"));
    }

    /**
     * 複数の入力データで異常終了する場合のテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>リトライリミットに到達していないのでリトライが成功すること</li>
     * <li>障害となったレコード以外は正常に処理されること</li>
     * </ul>
     */
    @Test
    public void testMultiAbnormalEndRecord() throws Exception {
        systemPropertyRule.setSystemProperty("threadCount", "2");

        ThreadControlHandler.countDownLatch = new CountDownLatch(12);

        VariousDbTestHelper.insert(
                new BatchIntegrationInput(1, "異常終了するレコードその１","0"),
                new BatchIntegrationInput(100, "異常終了するレコードその２","0")
        );

        setupInputData(10, 10);

        Future<Integer> future = executeBatch();

        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        stopBatchProcess("01");

        assertThat("バッチプロセスが正常終了すること", future.get(), is(0));

        assertThat("正常に処理可能なレコードは、ステータスが正常終了になっていること", findSuccessRecord().size(), is(10));
        assertThat("正常に処理できたレコードはアウトプットの書き込まれていること", findOutputTable().size(), is(10));
        assertThat("異常終了のレコードはステータスが異常終了となっていること", findAbnormalEndRecord().size(), is(2));

        // assert log
        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        System.out.println("nablarch.fw.batch.integration.ResidentBatchIntegrationTest#testMultiAbnormalEndRecord writer.appLog");
        sysOutLogMessages(logMessages);
        List<String> fatalLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("FATAL")) {
                fatalLog.add(message);
            }
        }
        assertThat("障害レコードが2レコードなので障害通知ログが2回出力されていること", fatalLog.size(), is(2));
        assertThat(fatalLog.get(0), containsString("invalid data. data=異常終了するレコードその１"));
        assertThat(fatalLog.get(1), containsString("invalid data. data=異常終了するレコードその２"));

        List<String> retryLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("start retry")) {
                retryLog.add(message);
            }
        }
        assertThat("リトライを示すログが1件出力されていること", retryLog.size(), is(2));
        assertThat("リトライカウントは1であること", retryLog.get(0), containsString("retryCount[1]"));
        assertThat("リトライカウントは2であること", retryLog.get(1), containsString("retryCount[2]"));
    }

    /**
     * リトライ回数をオーバするテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>リトライ可能回数まではリトライされること</li>
     * <li>リトライリミットを超えた場合プロセスが終了されること</li>
     * </ul>
     */
    @Test
    public void testRetryLimitOver() throws Exception {
        systemPropertyRule.setSystemProperty("threadCount", "1");

        for (int i = 0; i < 3; i++) {
            VariousDbTestHelper.insert(
                    new BatchIntegrationInput(new Integer(i+1), "異常終了するレコード" + i,"0")
            );
        }

        Future<Integer> future = executeBatch();


        assertThat("リトライ上限にたっするので、プロセスは異常終了すること", future.get(), is(180));

        assertThat("正常終了レコードはなし", findSuccessRecord().size(), is(0));
        assertThat("異常終了のレコードはステータスが異常終了となっていること", findAbnormalEndRecord().size(), is(3));

        // assert log
        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        List<String> fatalLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("FATAL")) {
                fatalLog.add(message);
            }
        }
        assertThat("障害となるレコードとプロセス異常終了の障害通知ログが出力されること", fatalLog.size(), is(4));
        assertThat(fatalLog.get(0), containsString("invalid data. data=異常終了するレコード0"));
        assertThat(fatalLog.get(1), containsString("invalid data. data=異常終了するレコード1"));
        assertThat(fatalLog.get(2), containsString("invalid data. data=異常終了するレコード2"));
        assertThat(fatalLog.get(3), containsString("[180 ProcessAbnormalEnd] リトライ上限に達しました。"));

        List<String> retryLog = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("start retry")) {
                retryLog.add(message);
            }
        }
        assertThat("リトライを示すログが1件出力されていること", retryLog.size(), is(2));
        assertThat("リトライカウントは1であること", retryLog.get(0), containsString("retryCount[1]"));
        assertThat("リトライカウントは2であること", retryLog.get(1), containsString("retryCount[2]"));
    }

    /**
     * 処理遅延が発生しているスレッドがある場合のテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>処理遅延が発生していても他のスレッドで随時データを処理できること</li>
     * <li>処理遅延が発生しているスレッドが処理中のレコードを他のスレッドが処理することはないこと(2重取り込みされないこと)</li>
     * </ul>
     */
    @Test
    public void testSlowThread() throws Exception {

        systemPropertyRule.setSystemProperty("threadCount", "3");

        // setup slow data
        VariousDbTestHelper.setUpTable(
                new BatchIntegrationInput(Integer.MAX_VALUE, "処理遅延データ data_" + Integer.MAX_VALUE,"0")
        );

        // setup data
        setupInputData(1, 3);

        ThreadControlHandler.countDownLatch = new CountDownLatch(3);
        ThreadControlHandler.threadWait = new CountDownLatch(1);

        Future<Integer> future = executeBatch();

        // 処理遅延のレコード以外が処理されること
        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);
        assertThat("処理遅延データの1レコードは未処理として残っていること", findUntreatedRecord().get(0).getBigDecimal("id").intValue(), is(Integer.MAX_VALUE));
        assertThat("処理遅延データ以外レコード(3レコード)は処理が完了していること", findSuccessRecord().size(), is(3));
        assertThat("アウトプットテーブルにデータが作成されていること", findOutputTable().size(), is(3));

        // 処理対象レコードを追加
        ThreadControlHandler.countDownLatch = new CountDownLatch(5);
        setupInputData(5, 5);
        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);
        assertThat("処理遅延データの1レコードは未処理として残っていること", findUntreatedRecord().get(0).getBigDecimal("id").intValue(), is(Integer.MAX_VALUE));
        assertThat("追加されたレコードが処理済みになっていること", findSuccessRecord().size(), is(8));
        assertThat("アウトプットテーブルにデータが作成されていること", findOutputTable().size(), is(8));

        ThreadControlHandler.countDownLatch = new CountDownLatch(1);
        ThreadControlHandler.threadWait.countDown();
        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        stopBatchProcess("01");

        assertThat("プロセスが正常終了していること", future.get(), is(0));
        assertThat("処理が遅いデータも処理済みになっていること", findUntreatedRecord().size(), is(0));
        assertThat("全レコード正常に処理済みになっている", findSuccessRecord().size(), is(9));
        assertThat("全て出力テーブルに書き込まれていること", findOutputTable().size(), is(9));


    }

    /**
     * アクションでファイル出力処理がある場合のテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>アクションで開いた書き込みファイルが閉じられていること</li>
     * </ul>
     *
     * ファイルが閉じられたことを厳密にチェックすることは出来ないため、
     * 本テストでは{@link nablarch.core.dataformat.FileRecordWriter}のスタブを使用してテストを実施する。
     */
    @Test
    public void testCloseWriteFile() throws Exception {

        final File formatFile = new File(folder.getRoot(), "test");
        final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(formatFile), "utf-8"));
        try {
            writer.write("file-type:        \"Fixed\" # 固定長\n");
            writer.write("text-encoding:    \"MS932\" # 文字列型フィールドの文字エンコーディング\n");
            writer.write("record-length:    5     # 各レコードの長さ\n");
            writer.write("record-separator: \"\\r\\n\"  # 改行コード(crlf)\n");
            writer.write("[Classifier] # レコードタイプ識別フィールド定義\n");
            writer.write("[data] # データレコード\n");
            writer.write("1   id                     X(5)\n");
        } finally {
            writer.close();
        }
        FileRecordWriterStub.closeFiles.clear();
        systemPropertyRule.setSystemProperty("threadCount", "1");

        setupInputData(1, 3);

        ThreadControlHandler.countDownLatch = new CountDownLatch(3);

        Future<Integer> future = service.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                CommandLine commandLine = new CommandLine(
                        "-diConfig", "nablarch/fw/batch/integration/batch-integration.xml",
                        "-requestPath", "FileWriterBatchAction/01",
                        "-userId", "batchUser1"
                );
                return Main.execute(commandLine);
            }
        });

        ThreadControlHandler.countDownLatch.await(5, TimeUnit.MINUTES);

        stopBatchProcess("01");

        assertThat("プロセスが正常に終了していること", future.get(), is(0));
        assertThat("入力レコード数分のファイルが閉じられていること", FileRecordWriterStub.closeFiles.size(), is(3));
        assertThat("閉じられたファイルのファイル名を確認", FileRecordWriterStub.closeFiles,
                hasItems("file_1", "file_2", "file_3"));

    }

    /**
     * 多重起動の場合のテスト。
     * <p/>
     * 本テストケースでは、主に以下の点の確認を行う。
     * <ul>
     * <li>バッチがアクティブな状態の場合(アクティブフラグがオンの場合)は、プロセス２重起動チェックでエラーとなること</li>
     * </ul>
     */
    @Test
    public void testDuplicateProcess() throws Exception {

        systemPropertyRule.setSystemProperty("threadCount", "1");

        setupInputData(1, 1);

        // 多重起動フラグをオンに
        TestBatchRequest3 testBatchRequest3 = VariousDbTestHelper.findById(TestBatchRequest3.class, "01");
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest3("01","1",testBatchRequest3.stop,testBatchRequest3.service)
        );

        Future<Integer> future = executeBatch();
        assertThat("プロセスは２重起動エラーで異常終了する", future.get(), is(50));

        assertThat("レコードは未処理のまま残っていること", findUntreatedRecord().size(), is(1));

        List<String> logMessages = OnMemoryLogWriter.getMessages("writer.appLog");
        String fatalLog = null;
        for (String message : logMessages) {
            if (message.contains("FATAL")) {
                fatalLog = message;
            }
        }
        assertThat(fatalLog, containsString("DuplicateProcess"));
    }

    /**
     * バッチアプリケーションを実行する。
     *
     * @return 非同期処理の結果(バッチアプリケーションの実行結果)
     */
    private static Future<Integer> executeBatch() {
        return service.submit(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                CommandLine commandLine = new CommandLine(
                        "-diConfig", "nablarch/fw/batch/integration/batch-integration.xml",
                        "-requestPath", "BatchActionHandler/01",
                        "-userId", "batchUser1"
                );
                return Main.execute(commandLine);
            }
        });
    }

    /**
     * バッチの入力テーブルにデータをセットアップする。
     *
     * @param from IDカラムの開始番号
     * @param count 作成するレコード数
     */
    private static void setupInputData(int from, int count) {
        for (int i = from; i < (from + count); i++) {
            VariousDbTestHelper.insert(
                    new BatchIntegrationInput(i, "data_" + i,"0")
            );
        }
    }

    /**
     * 指定したIDのバッチプロセスを停止する。
     *
     * @param id ID
     */
    private static void stopBatchProcess(String id) {
        TestBatchRequest3 testBatchRequest3 = VariousDbTestHelper.findById(TestBatchRequest3.class, id);
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest3(id,testBatchRequest3.act,"1",testBatchRequest3.service)
        );

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 開閉局状態を変更する。
     */
    private static void changeServiceStatus(String id, String status) {
        TestBatchRequest3 testBatchRequest3 = VariousDbTestHelper.findById(TestBatchRequest3.class, id);
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest3(id,testBatchRequest3.act,testBatchRequest3.stop,status)
        );
    }

    /**
     * 未処理のレコードを取得する。
     *
     * @return 取得結果
     */
    private static List<SqlRow> findUntreatedRecord() {
        int type = java.sql.Types.VARCHAR;
        List<BatchIntegrationInput> batchInputList = VariousDbTestHelper.findAll(BatchIntegrationInput.class, "status");
        List<SqlRow> sqlResultSet = new ArrayList<SqlRow>();
        for (BatchIntegrationInput batchInput : batchInputList) {
            if (batchInput.status.equals("0")) {
                Map<String, Object> data = new HashMap<String, Object>();
                Map<String, Integer> colType = new HashMap<String, Integer>();
                data.put("id", batchInput.id);
                data.put("data", batchInput.data);
                data.put("status", batchInput.status);
                colType.put("id", type);
                colType.put("data", type);
                colType.put("status", type);
                SqlRow sqlRow = new SqlRow(data, colType);
                sqlResultSet.add(sqlRow);
            }
        }
        return sqlResultSet;
    }

    /**
     * 処理ステータスが正常終了のレコードを取得する。
     *
     * @return 取得結果
     */
    private static List<SqlRow> findSuccessRecord() {
        int type = java.sql.Types.VARCHAR;
        List<BatchIntegrationInput> batchInputList= VariousDbTestHelper.findAll(BatchIntegrationInput.class, "status");
        List<SqlRow> sqlResultSet = new ArrayList<SqlRow>();
        for(BatchIntegrationInput batchInput:batchInputList){
            if (batchInput.status.equals("1")){
                Map<String, Object> data = new HashMap<String, Object>();
                Map<String, Integer> colType = new HashMap<String, Integer>();
                data.put("id", batchInput.id);
                data.put("data", batchInput.data);
                data.put("status", batchInput.status);
                colType.put("id", type);
                colType.put("data", type);
                colType.put("status", type);
                SqlRow sqlRow = new SqlRow(data,colType);
                sqlResultSet.add(sqlRow);
            }
        };
//        SqlPStatement statement = connection.prepareStatement("SELECT * FROM BATCH_INPUT WHERE STATUS = '1'");
        return sqlResultSet;
    }

    /**
     * 処理ステータスが異常終了のレコードを取得する。
     *
     * @return 取得結果
     */
    private static List<SqlRow> findAbnormalEndRecord() {
        int type = java.sql.Types.VARCHAR;
        List<BatchIntegrationInput> batchInputList= VariousDbTestHelper.findAll(BatchIntegrationInput.class, "status");
        List<SqlRow> sqlResultSet = new ArrayList<SqlRow>();
        for(BatchIntegrationInput batchInput:batchInputList){
            if (batchInput.status.equals("9")){
                Map<String, Object> data = new HashMap<String, Object>();
                Map<String, Integer> colType = new HashMap<String, Integer>();
                data.put("id", batchInput.id);
                data.put("data", batchInput.data);
                data.put("status", batchInput.status);
                colType.put("id", type);
                colType.put("data", type);
                colType.put("status", type);
                SqlRow sqlRow = new SqlRow(data,colType);
                sqlResultSet.add(sqlRow);
            }
        };
        return sqlResultSet;
    }

    /**
     * バッチアウトプットテーブルの情報を取得する。
     *
     * @return 取得結果
     */
    private static List<SqlRow> findOutputTable() {
        int type = java.sql.Types.VARCHAR;
        List<BatchIntegrationOutput> batchOutputList= VariousDbTestHelper.findAll(BatchIntegrationOutput.class, "id");
        List<SqlRow> sqlResultSet = new ArrayList<SqlRow>();
        for(BatchIntegrationOutput batchOutput:batchOutputList){
            Map<String, Object> data = new HashMap<String, Object>();
            Map<String, Integer> colType = new HashMap<String, Integer>();
            data.put("id", batchOutput.id);
            data.put("data", batchOutput.data);
            colType.put("id", type);
            colType.put("data", type);
            SqlRow sqlRow = new SqlRow(data,colType);
            sqlResultSet.add(sqlRow);
        };
        return sqlResultSet;
    }

    /**
     * バッチリクエスト取得
     *
     * @param id ID
     * @return 取得結果
     */
    private static SqlRow findBatchRequest(String id) {
        TestBatchRequest3 testBatchRequest3 = VariousDbTestHelper.findById(TestBatchRequest3.class, id);
        int type = java.sql.Types.VARCHAR;
        Map<String, Object> data = new HashMap<String, Object>();
        Map<String, Integer> colType = new HashMap<String, Integer>();
        data.put("id", testBatchRequest3.id);
        data.put("act", testBatchRequest3.act);
        data.put("service", testBatchRequest3.service);
        data.put("stop", testBatchRequest3.stop);
        colType.put("id", type);
        colType.put("act", type);
        colType.put("service",type);
        colType.put("stop", type);
        SqlRow sqlRow = new SqlRow(data,colType);
        if (VariousDbTestHelper.findById(TestBatchRequest3.class, id) == null) {
            throw new IllegalStateException("not found. id = " + id);
        }
        return sqlRow;
    }

    private static List<String> filterInputRecordLog(List<String> logMessages) {
        List<String> inputInfo = new ArrayList<String>();
        for (String message : logMessages) {
            if (message.contains("read database record.")) {
                inputInfo.add(message);
            }
        }
        Collections.sort(inputInfo, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                int len1 = o1.length();
                int len2 = o2.length();

                if (len1 < len2) {
                    return -1;
                } else if (len1 > len2) {
                    return 1;
                }
                return o1.compareTo(o2);
            }
        });
        return inputInfo;
    }

    /**
     * {@link OnMemoryLogWriter} から取得したメッセージを引数として受け取り、標準出力に出力する。
     * @param logMessages {@link OnMemoryLogWriter} から取得したメッセージ
     */
    private void sysOutLogMessages(List<String> logMessages) {
        StringBuilder writerAppLogBuffer = new StringBuilder();
        for (int i = 0; i < logMessages.size(); i++) {
            writerAppLogBuffer.append("[");
            writerAppLogBuffer.append(i);
            writerAppLogBuffer.append("]");
            writerAppLogBuffer.append(logMessages.get(i));
            writerAppLogBuffer.append(System.getProperty("line.separator"));
        }
        System.out.println(writerAppLogBuffer.toString());
    }

    public static class ThreadControlHandler implements Handler<SqlRow, Object> {

        private static CountDownLatch countDownLatch = null;

        private static CountDownLatch threadWait = null;

        @Override
        public Object handle(SqlRow input, ExecutionContext context) {
            try {
                int id = input.getBigDecimal("id").intValue();
                if (id == Integer.MAX_VALUE) {
                    // intの最大値のレコードはThreadを待機させる
                    if (threadWait != null) {
                        try {
                            threadWait.await(5, TimeUnit.MINUTES);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }

                return context.handleNext(input);
            } finally {
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        }
    }

    /**
     * テスト使用するメッセージリソースホルダ
     */
    public static class StringResourceHolderStub extends StringResourceHolder {

        @Override
        public StringResource get(final String messageId) throws MessageNotFoundException {
            if ("RETRY_ERROR".equals(messageId)) {
                return new StringResource() {
                    @Override
                    public String getId() {
                        return messageId;
                    }

                    @Override
                    public String getValue(Locale locale) {
                        return "リトライ上限に達しました。";
                    }
                };
            }
            return super.get(messageId);
        }
    }

    /**
     * テストで使用するファイルライター実装。
     * <p/>
     * closeメソッドが呼び出されたことをハンドリングするために使用する。
     */
    private static class FileRecordWriterStub extends FileRecordWriter {

        public static List<String> closeFiles = new ArrayList<String>();

        private final File dataFile;

        public FileRecordWriterStub(File dataFile, File layoutFile) {
            super(dataFile, layoutFile);
            this.dataFile = dataFile;
        }

        @Override
        public void close() {
            closeFiles.add(dataFile.getName());
            super.close();
        }
    }

    /**
     * テストで使用するFileWriterHolder
     */
    public static class FileRecordWriterHolderStub extends FileRecordWriterHolder {

        @Override
        protected FileRecordWriter createFileRecordWriter(String dataFileBasePathName, String dataFileName,
                                                          String layoutFileBasePathName, String layoutFileName, int bufferSize) {

            FilePathSetting filePathSetting = SystemRepository.get("filePathSetting");
            File dataFile = filePathSetting.getFile(dataFileBasePathName, dataFileName);
            // レイアウトファイルオブジェクトの生成
            File layoutFile = filePathSetting.getFileWithoutCreate(layoutFileBasePathName, layoutFileName);
            return new FileRecordWriterStub(dataFile, layoutFile);
        }
    }
}

