package nablarch.fw.action;

import nablarch.core.message.ApplicationException;
import nablarch.core.repository.SystemRepository;
import nablarch.fw.Result;
import nablarch.fw.TestSupport;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.handler.CatchingHandler;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("NonAsciiCharacters")
@RunWith(DatabaseTestRunner.class)
public class DataBindBatchActionTest {


    private static final File tempDir = new File(System.getProperty("java.io.tmpdir"));

    private final File dataFile = new File(tempDir, "test.dat");

    @BeforeClass
    public static void beforeClass() throws Exception {
        VariousDbTestHelper.createTable(TestBatchRequest.class);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        VariousDbTestHelper.dropTable(TestBatchRequest.class);
    }

    @Before
    public void setUp() {
        CatchingHandler.clear();
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest("req00001", 0)
        );
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @After
    public void tearDown() {
        SystemRepository.clear();
        CatchingHandler.clear();
        if (dataFile.exists()) {
            dataFile.delete();
        }
    }


    @Test
    public void バッチの実行が正常終了すること() throws Exception {

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);

        assertEquals(0, exitCode);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(3, results.size());
        assertEquals("山田太郎", results.get(0).getMessage());
        assertEquals("鈴木次郎", results.get(1).getMessage());
        assertEquals("佐藤花子", results.get(2).getMessage());

        List<String> execIds = CatchingHandler.getExecutionIds();
        assertEquals(3, execIds.size());
        Set<String> ids = new HashSet<>();
        for (String execId : execIds) {
            assertTrue(execId.matches("\\d{21}"));
            ids.add(execId);
        }
        assertEquals(3, ids.size());
    }

    @Test
    public void ディスパッチ先が存在しない場合は異常終了すること() throws Exception {

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子"
        );

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "NoneDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);
        assertEquals(13, exitCode);
    }

    @Test
    public void handle内のバリデーションに失敗した場合は異常終了すること() throws Exception {

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "300,鈴木次郎",
                "40,佐藤花子"
        );

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);
        assertEquals(20, exitCode);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(2, results.size());
        assertEquals("山田太郎", results.get(0).getMessage());

        Exception exception = (Exception) results.get(1);
        assertTrue(exception instanceof ApplicationException);
        assertEquals("年齢が不正です。\n", exception.getMessage()); // ApplicationException.getMessage()でLFを付与している
    }

    @Test
    public void 事前の全件バリデーションに失敗した場合は異常終了すること() throws Exception {

        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "300,鈴木次郎",
                "40,佐藤花子"
        );

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "TinyDataBindBatchActionWithPreCheck/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);
        assertEquals(20, exitCode);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(1, results.size());
        Exception exception = (Exception) results.get(0);
        assertTrue(exception instanceof ApplicationException);
        assertEquals("年齢が不正です。\n", exception.getMessage()); // ApplicationException.getMessage()でLFを付与している
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    @TargetDb
    public void バリデーションに失敗してもレジュームポイントから処理を続行できること() throws Exception {

        // エラーを発生させるファイルを作成する。
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "400,佐藤花子",
                "50,高橋葉子"
        );

        // 1回目の実行
        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);
        assertEquals(20, exitCode);

        // 1回目の実行では、3番目のデータでエラーが発生する。
        List<Result> results = CatchingHandler.getResults();
        assertEquals(3, results.size());
        assertEquals("山田太郎", results.get(0).getMessage());
        assertEquals("鈴木次郎", results.get(1).getMessage());
        Exception exception = (Exception) results.get(2);
        assertTrue(exception instanceof RuntimeException);
        assertEquals("年齢が不正です。\n", exception.getMessage());


        CatchingHandler.clear();

        // エラー箇所を修正したファイルを作成する。
        if(dataFile.exists()) {
            dataFile.delete();
        }
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子",
                "50,高橋葉子"
        );

        // 2回目の実行
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTest.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        exitCode = Main.execute(commandline);
        assertEquals(0, exitCode);

        // 2回目の実行では、3番目のデータから処理が実行される。
        results = CatchingHandler.getResults();
        assertEquals(2, results.size());
        assertEquals("佐藤花子", results.get(0).getMessage());
        assertEquals("高橋葉子", results.get(1).getMessage());



    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    @TargetDb
    public void コミット間隔を3に設定した場合_バリデーションに失敗してもレジュームポイントから処理を続行できること() throws Exception {

        // エラーを発生させるファイルを作成する。2番目と4番目のデータが不正
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "300,鈴木次郎",
                "40,佐藤花子",
                "500,高橋葉子",
                "60,藤原芳雄"
        );


        // 1回目の実行
        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTestLoop.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        int exitCode = Main.execute(commandline);
        assertEquals(20, exitCode);

        // 1回目の実行では、2番目のデータでエラーが発生し、処理がロールバックする。
        List<Result> results = CatchingHandler.getResults();
        assertEquals(2, results.size());
        assertEquals("山田太郎", results.get(0).getMessage());
        Exception exception = (Exception) results.get(1);
        assertTrue(exception instanceof RuntimeException);
        assertEquals("年齢が不正です。\n", exception.getMessage());

        CatchingHandler.clear();

        // エラー箇所を修正したファイルを作成する。4番目のデータにはまだ不正が残っている。
        if(dataFile.exists()) {
            dataFile.delete();
        }
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子",
                "500,高橋葉子",
                "60,藤原芳雄"
        );

        // 2回目の実行
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTestLoop.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        exitCode = Main.execute(commandline);
        assertEquals(20, exitCode);

        // 1回目の実行がロールバックされているので、最初から処理され、4番目のデータでエラーが発生する。
        results = CatchingHandler.getResults();
        assertEquals(4, results.size());
        assertEquals("山田太郎", results.get(0).getMessage());
        assertEquals("鈴木次郎", results.get(1).getMessage());
        assertEquals("佐藤花子", results.get(2).getMessage());
        exception = (Exception) results.get(3);
        assertTrue(exception instanceof RuntimeException);
        assertEquals("年齢が不正です。\n", exception.getMessage());

        CatchingHandler.clear();

        // エラー箇所を修正したファイルを作成する。
        if(dataFile.exists()) {
            dataFile.delete();
        }
        TestSupport.createFile(dataFile, "\r\n", StandardCharsets.UTF_8,
                "年齢,氏名",
                "20,山田太郎",
                "30,鈴木次郎",
                "40,佐藤花子",
                "50,高橋葉子",
                "60,藤原芳雄"
        );

        // 3回目の実行
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/DataBindBatchActionTestLoop.xml"
                , "-requestPath", "TinyDataBindBatchAction/req00001"
                , "-userId", "NabuTaro"
        );

        exitCode = Main.execute(commandline);
        assertEquals(0, exitCode);

        // 3回目の実行では、4番目のデータから処理が実行される。
        results = CatchingHandler.getResults();
        assertEquals(2, results.size());
        assertEquals("高橋葉子", results.get(0).getMessage());
        assertEquals("藤原芳雄", results.get(1).getMessage());

    }
}