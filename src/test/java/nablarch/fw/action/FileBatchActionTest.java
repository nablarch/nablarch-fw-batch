package nablarch.fw.action;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nablarch.core.dataformat.InvalidDataFormatException;
import nablarch.core.repository.SystemRepository;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.Result;
import nablarch.fw.TestSupport;
import nablarch.fw.launcher.CommandLine;
import nablarch.fw.launcher.Main;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.handler.CatchingHandler;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DatabaseTestRunner.class)
public class FileBatchActionTest {

    private static final File tempDir = new File(System.getProperty("java.io.tmpdir"));

    @BeforeClass
    public static void beforeClass() throws Exception {

        FilePathSetting.getInstance()
                       .getBasePathSettings()
                       .clear();
        FilePathSetting.getInstance()
                       .getFileExtensions()
                       .clear();

        VariousDbTestHelper.createTable(TestBatchRequest.class);

        // データフォーマット定義ファイル
        File formatFile = new File(tempDir, "test.fmt");
        TestSupport.createFile(formatFile, Charset.forName("utf-8"),
                "file-type: \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 10",
                "[classifier]",
                "1 type X(1)",
                "[header]",
                "type = \"H\"",
                "1  type   X(1) \"H\"",
                "2 ?filler X(9)",
                "[data]",
                "type = \"D\"",
                "1    type   X(1)  \"D\"",
                "2    amount Z(9)",
                "[trailer]",
                "type = \"T\"",
                "1    type        X(1)  \"T\"",
                "2    records     Z(3)   # データレコード件数",
                "5    totalAmount Z(6)   # 合算値"
        );
        VariousDbTestHelper.createTable(TestBatchRequest.class);
    }

    @Before
    public void setUp() {
        VariousDbTestHelper.setUpTable(
                new TestBatchRequest("req00001", 0)
        );

    }

    @After
    public void tearDown() {
        SystemRepository.clear();
    }

    /**
     * 正常系
     */
    @Test
    public void testBasicUsage() throws Exception {
        withFlowlessDataFile();
        CatchingHandler.clear();

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                , "-requestPath", "TinyFileBatchAction/req00001"
                , "-userId", "wetherreport"
        );

        int exitCode = Main.execute(commandline);

        assertEquals(0, exitCode);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(7, results.size());
        assertEquals("H", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());
        assertEquals("D", results.get(3)
                                 .getMessage());
        assertEquals("D", results.get(4)
                                 .getMessage());
        assertEquals("D", results.get(5)
                                 .getMessage());
        assertEquals("T", results.get(6)
                                 .getMessage());

        List<String> execIds = CatchingHandler.getExecutionIds();
        assertEquals(7, execIds.size());
        Set<String> ids = new HashSet<String>();
        for (String execId : execIds) {
            assertTrue(execId.matches("\\d{21}"));
            ids.add(execId);
        }
        assertEquals(7, ids.size());
    }

    /**
     * ディスパッチ先のメソッドが定義されていない場合は404
     */
    @Test
    public void testErrorInDispatcher() throws Exception {

        withFlowlessDataFile();

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                // ディスパッチ先メソッドが定義されていない。
                , "-requestPath", "NoHandlerMethodAction/req00001"
                , "-userId", "wetherreport"
        );
        try {
            int exitCode = Main.execute(commandline);

            assertEquals(13, exitCode); // NotFound
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    /**
     * 入力ファイルの形式不正の為、本処理の途中で落ちる。
     */
    @Test
    public void testErrorInDataFileFormat() throws Exception {
        withBrokenDataFile();
        CatchingHandler.clear();

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                , "-requestPath", "TinyFileBatchAction/req00001"
                , "-userId", "wetherreport"
        );

        int exitCode = Main.execute(commandline);

        assertEquals(20, exitCode); // Internal Error (未補足実行時例外)

        List<Result> results = CatchingHandler.getResults();
        assertEquals(4, results.size());
        assertEquals("H", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());

        // 4件めでデータ不正によりエラー終了

        Exception e = (Exception) results.get(3);
        assertTrue(e instanceof InvalidDataFormatException);
        assertTrue(e.getMessage()
                    .contains(
                            "an applicable layout definition was not found in the record.")
        );
    }

    /**
     * 事前検証処理を使用し、入力データの形式不正があったとしても、
     * 本処理実行前にエラーで落とす。
     */
    @Test
    public void testPreCheckFileFormatValiditiy() throws Exception {
        withBrokenDataFile();
        CatchingHandler.clear();

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                , "-requestPath", "FileBatchActionWithPreCheck/req00001"
                , "-userId", "wetherreport"
        );

        int exitCode = Main.execute(commandline);

        assertEquals(20, exitCode); // Internal Error (未補足実行時例外)

        List<Result> results = CatchingHandler.getResults();
        assertEquals(1, results.size()); //1件めの読み込み時点でエラーになる。
    }

    @Test
    public void withBrokenDataFile() throws Exception {
        TestSupport.createFile(new File(tempDir, "test.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "000000100",
                "D000001000",
                "D000010000",
                "T005011111");
    }


    @Test
    public void withFlowlessDataFile() throws Exception {
        TestSupport.createFile(new File(tempDir, "test.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
        );
    }

    /**
     * FileBatchActionでレジュームを行う。
     * <p/>
     * また、４レコード目の読み込み時に障害を発生させ、正常にロールバックおよび処理の再開が行われることを確認する。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.DB2) // DB2では本ケース実行後にDB2が停止してしまうため暫定対処。
    public void testResume() throws Exception {
        CatchingHandler.clear();

        TestSupport.createFile(new File(tempDir, "test.dat"), "", Charset.forName("windows-31j"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
        );

        TestSupport.createFile(new File(tempDir, "test.fmt"), Charset.forName("utf-8"),
                "file-type: \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 10",
                "",
                "[classifier]",
                "1 type X(1)",
                "",
                "[header]",
                "type = \"H\"",
                "1  type   X(1) \"H\"",
                "2 ?filler X(9)",
                "[data]",
                "type = \"D\"",
                "1    type   X(1)  \"D\"",
                "2    amount Z(9)",
                "[trailer]",
                "type = \"T\"",
                "1    type        X(1)  \"T\"",
                "2    records     Z(3)   # データレコード件数",
                "5    totalAmount Z(6)   # 合算値"
        );

        /*
         * １回目の実行（４レコード目の読み込み時に障害が発生する）
         */

        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                , "-requestPath", "TinyFileBatchAction2/req00001"
                , "-userId", "wetherreport"
        );

        // 例外を発生させるamountの値を設定する
        TinyFileBatchAction2.errorValue = 100;

        int exitCode = Main.execute(commandline);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(4, results.size());
        assertEquals("H", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());
        assertEquals(20, exitCode);

        CatchingHandler.clear();


        /*
         * ２回目の実行（４レコード目から処理が再開される）
         */
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTest.xml"
                , "-requestPath", "TinyFileBatchAction/req00001"
                , "-userId", "wetherreport"
        );

        exitCode = Main.execute(commandline);

        assertEquals(0, exitCode);

        results = CatchingHandler.getResults();
        assertEquals(4, results.size());
        assertEquals("D", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());
        assertEquals("T", results.get(3)
                                 .getMessage());

    }

    /**
     * FileBatchActionでレジュームを行う（複数件コミットのパターン）。
     * <p/>
     * コミット間隔「３」で、１回目の実行では３レコード目、２回目の実行では５レコード目まで読み込んだ後の業務処理で障害を発生させ、正常にロールバックおよび処理の再開が行われることを確認する。
     * <p/>
     * 具体的には以下のような動作となる。<br/>
     * ・１回目の実行では、すべての処理がロールバックされる。
     * ・２回目の実行では、最初のレコードから読み込みが再開される。そして、３レコード目までの処理がコミットされ、５レコード目までの処理はロールバックされる。
     * ・３回目の実行では、４レコード目から読み込みが再開される。そして、すべての処理がコミットされる。
     */
    @Test
    @TargetDb(exclude = TargetDb.Db.DB2) // DB2では本ケース実行後にDB2が停止してしまうため暫定対処。
    public void testResumeMultiDataCommit() throws Exception {

        TestBatchRequest testBatchRequest = VariousDbTestHelper.findById(TestBatchRequest.class, "req00001");
        CatchingHandler.clear();
        TestSupport.createFile(new File(tempDir, "test.dat"), "", Charset.forName("Shift_JIS"),
                "H         ",
                "D000000001",
                "D000000010",
                "D000000100",
                "D000001000",
                "D000010000",
                "T005011111"
        );

        // データフォーマット定義ファイル
        TestSupport.createFile(new File(tempDir, "test.fmt"), Charset.forName("UTF-8"),
                "file-type: \"Fixed\"",
                "text-encoding: \"sjis\"",
                "record-length: 10",
                "[classifier]",
                "1 type X(1)",
                "[header]",
                "type = \"H\"",
                "1  type   X(1) \"H\"",
                "2 ?filler X(9)",
                "",
                "[data]",
                "type = \"D\"",
                "1    type   X(1)  \"D\"",
                "2    amount Z(9)",
                "",
                "[trailer]",
                "type = \"T\"",
                "1    type        X(1)  \"T\"",
                "2    records     Z(3)   # データレコード件数",
                "5    totalAmount Z(6)   # 合算値"
        );
        /*
         * １回目の実行。３レコード目まで読み込んだ後の業務処理で障害が発生する。すべての処理がロールバックされる。
         */
        CommandLine commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTestLoop.xml"
                , "-requestPath", "TinyFileBatchAction2/req00001"
                , "-userId", "wetherreport"
        );

        // 例外を発生させるamountの値を設定する
        TinyFileBatchAction2.errorValue = 10;

        int exitCode = Main.execute(commandline);

        List<Result> results = CatchingHandler.getResults();
        assertEquals(3, results.size());
        assertEquals("H", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        RuntimeException result = (RuntimeException) results.get(2);
        assertEquals("error", result.getMessage());
        assertEquals(20, exitCode);

        // 正常に処理できたポイントがインクリメントされていないことを確認
        testBatchRequest = VariousDbTestHelper.findById(TestBatchRequest.class, "req00001");
        /*
        SELECT * FROM BATCH_REQUEST WHERE REQUEST_ID = 'req00001' ORDER BY REQUEST_ID */
        assertThat(testBatchRequest.resumePoint.toString(), is("0"));


        CatchingHandler.clear();

        /*
         * ２回目の実行。最初のレコードから読み込みが再開され、５レコード目まで読み込んだ後の業務処理で障害が発生する。３レコード目までの処理はコミットされる。
         */
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTestLoop.xml"
                , "-requestPath", "TinyFileBatchAction2/req00001"
                , "-userId", "wetherreport"
        );

        // 例外を発生させるamountの値を設定する
        TinyFileBatchAction2.errorValue = 1000;

        exitCode = Main.execute(commandline);

        results = CatchingHandler.getResults();
        assertEquals(5, results.size());
        assertEquals("H", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());
        assertEquals("D", results.get(3)
                                 .getMessage());
        result = (RuntimeException) results.get(4);
        assertEquals("error", result.getMessage());
        assertEquals(20, exitCode);

        // 正常に処理できたポイントが"３回"（コミット間隔分）インクリメントされることを確認
        testBatchRequest = VariousDbTestHelper.findById(TestBatchRequest.class, "req00001");
        assertThat(testBatchRequest.resumePoint.toString(), is("3"));

        CatchingHandler.clear();


        /*
         * ３回目の実行。４レコード目から読み込みが再開され、最終レコードまでの処理が完了する。
         */
        commandline = new CommandLine(
                "-diConfig", "nablarch/fw/action/FileBatchActionTestLoop.xml"
                , "-requestPath", "TinyFileBatchAction/req00001"
                , "-userId", "wetherreport"
        );

        exitCode = Main.execute(commandline);

        assertEquals(0, exitCode);

        results = CatchingHandler.getResults();
        assertEquals(4, results.size());
        assertEquals("D", results.get(0)
                                 .getMessage());
        assertEquals("D", results.get(1)
                                 .getMessage());
        assertEquals("D", results.get(2)
                                 .getMessage());
        assertEquals("T", results.get(3)
                                 .getMessage());

        // 正常に処理できたポイントが"４回"インクリメントされることを確認
        testBatchRequest = VariousDbTestHelper.findById(TestBatchRequest.class, "req00001");
        assertThat(testBatchRequest.resumePoint.toString(), is("7"));

    }
}
