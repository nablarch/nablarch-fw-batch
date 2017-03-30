package nablarch.fw.reader;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.Charset;

import nablarch.core.ThreadContext;
import nablarch.core.dataformat.DataRecord;
import nablarch.core.dataformat.FileRecordReader;
import nablarch.core.dataformat.FormatterFactory;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Handler;
import nablarch.fw.Result;
import nablarch.fw.TestSupport;
import nablarch.fw.reader.ValidatableFileDataReader.FileValidatorAction;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link ResumeDataReader}のレジューム機能のテスト。
 */
@RunWith(DatabaseTestRunner.class)
public class ResumeDataReaderTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/fw/reader/ResumePointManager.xml");

    private final String LS = System.getProperty("line.separator");

    private final File tempFile = new File(System.getProperty("java.io.tmpdir"));

    /** connection */
    private static SimpleDbTransactionManager transactionManager;

    private FileRecordReader reader;

    private File formatFile;

    @BeforeClass
    public static void setUpClass() {
        // キャッシュをオフにして、強制的にこのクラスで使用するフォーマットファイルが読み込まれるようにする。
        FormatterFactory.getInstance()
                        .setCacheLayoutFileDefinition(false);

        VariousDbTestHelper.createTable(ResumeBatchRequest.class);
    }

    @Before
    public void setUp() throws Exception {

        // トランザクションマネージャの取得
        transactionManager = repositoryResource.getComponent("tran");

        if (transactionManager != null) {
            transactionManager.beginTransaction();
        }

        // レイアウト定義ファイル
        formatFile = new File(tempFile, "format.fmt");
        TestSupport.createFile(formatFile, Charset.forName("UTF-8"),
                "file-type:    \"Variable\"",
                "text-encoding:     \"ms932\"",
                "record-separator:  \"\\n\"",
                "field-separator:   \",\"",
                "quoting-delimiter: \"\\\"\"",
                "ignore-blank-lines: true",
                "",
                "[DataRecord]",
                "1   userId           X",
                "2   userCode         X",
                "3   price            X    number"
        );
        VariousDbTestHelper.delete(ResumeBatchRequest.class);
        ThreadContext.clear();
    }

    @After
    public void tearDown() {
        ThreadContext.clear();
        if (transactionManager != null) {
            transactionManager.endTransaction();
        }
        if (reader != null) {
            reader.close();
        }
    }

    /**
     * それぞれのリクエストIDに紐付く処理が、正常に処理できたポイントから実行できることのテスト。
     * 正常に処理できたポイントがレコード数を上回る場合は、例外がスローされることも確認する。
     */
    @Test
    public void testReadEachRequst() throws Exception {
        File file = new File(tempFile, "record.dat");

        // テーブル
        VariousDbTestHelper.setUpTable(
                new ResumeBatchRequest("RW000000", 0L),
                new ResumeBatchRequest("RW000001", 1L),
                new ResumeBatchRequest("RW000002", 2L),
                new ResumeBatchRequest("RW000003", 3L),
                new ResumeBatchRequest("RW000004", 4L));

        String requestId = "RW000000";

        ThreadContext.setRequestId(requestId);

        TestSupport.createFile(file, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30"
        );

        ExecutionContext ctx = new ExecutionContext();

        FileDataReader fileReader = new FileDataReader().setDataFile("record")
                                                        .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 1レコード目からロードされる
        DataRecord record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(1L));

        record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("20"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));
        reader.close(ctx);

        // 実行後の正常に処理できたポイントを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        requestId = "RW000001";

        ThreadContext.setRequestId(requestId);
        TestSupport.createFile(file, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30"
        );

        // 2レコード目からロードされる
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("20"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));
        reader.close(ctx);

        // 実行後の正常に処理できたポイントを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        /**
         * リクエストID「RW000002」、正常に処理できたポイント「2」の場合に、3レコード目から読み込まれることの確認。
         */
        requestId = "RW000002";

        ThreadContext.setRequestId(requestId);

        // データファイル
        TestSupport.createFile(file, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30");

        // 3レコード目からロードされる
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        record = reader.read(ctx);
        assertEquals("0001", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));
        reader.close(ctx);


        /**
         * リクエストID「RW000003」、正常に処理できたポイント「3」の場合に、例外がスローされることの確認。
         */
        requestId = "RW000003";

        ThreadContext.setRequestId(requestId);

        // データファイル
        TestSupport.createFile(file, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30");

        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");

        try {
            reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);
            reader.read(ctx);
            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), startsWith(
                    "invalid resume point was specified. The total number of reads input data was [3], but resume point was [3]. request id=[RW000003]. Perhaps this request has been completed."));
        }

        reader.close(ctx);

        requestId = "RW000004";

        ThreadContext.setRequestId(requestId);

        // データファイル
        TestSupport.createFile(file, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30");

        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");

        try {
            reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);
            reader.read(ctx);
            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), startsWith(
                    "invalid resume point was specified. The total number of reads input data was [3], but resume point was [4]. request id=[RW000004]."));
        }

        reader.close(ctx);
    }


    /**
     * 処理の中断、再開を繰り返すテスト。
     */
    @Test
    public void testRead() throws Exception {

        String requestId = "RW000001";
        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // データファイル
        TestSupport.createFile(filePath, "\n", Charset.forName("windows-31j"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30",
                "0002,A02,40",
                "0003,A03,50"
        );

        FileDataReader fileReader = new FileDataReader().setDataFile("record")
                                                        .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext();

        /**
         * 1レコード目だけ読み込んで、処理を終了する。
         */
        // 1レコード目のロード
        DataRecord record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));


        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(1L));

        reader.close(ctx);

        /**
         * 2レコード目から処理を再開し、3レコード目まで読み込んで、処理を終了する。
         */
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 2レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("20"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        // 3レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        // 4レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0002", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("40"), record.get("price"));
        assertEquals(4, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(4L));

        reader.close(ctx);

        /**
         * 5レコード目から処理を再開し、5レコード目のみ読み込んで、処理を終了する。
         */
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 5レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0003", record.get("userId"));
        assertEquals("A03", record.get("userCode"));
        assertEquals(new BigDecimal("50"), record.get("price"));
        assertEquals(5, record.getRecordNumber());

        // もうレコードがないことの確認
        assertFalse(reader.hasNext(ctx));
        assertNull(reader.read(ctx));


        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(5L));

        reader.close(ctx);

        /**
         * 6レコード目から処理を再開しようとして、例外がスローされる。
         */
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 6レコード目のロードはできないので、例外がスローされる
        try {
            record = reader.read(ctx);
            fail();
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(),
                    is("invalid resume point was specified. The total number of reads input data was [5], but resume point was [5]. request id=[RW000001]. Perhaps this request has been completed."));
        }
    }


    /**
     * コンポーネント設定ファイルにResumePointManagerの定義が存在しない場合に、レジュームが行われないことのテスト。
     */
    @Test
    public void testIsResumableFalse() throws Exception {

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // ResumePointManagerの定義を削除する
        repositoryResource.addComponent("resumePointManager", null);

        String requestId = "RW000001";

        doTestDisableResume(requestId);
    }

    /**
     * コンポーネント設定ファイルでisResumableがfalseに設定されている場合に、レジュームが行われないことのテスト。
     */
    @Test
    public void testNotExistResumePointManagerConfiguration() throws Exception {

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // レジューム機能を無効化する
        repositoryResource.getComponentByType(ResumePointManager.class)
                          .setResumable(false);

        String requestId = "RW000001";

        doTestDisableResume(requestId);
    }

    /**
     * コンポーネント設定ファイルのexcludingRequestListに該当するリクエストIDの場合に、レジュームが行われないことのテスト。
     */
    @Test
    public void testExcludingRequestList() throws Exception {

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("EX_R0001", 0L));

        String requestId = "EX_R0001";

        doTestDisableResume(requestId);
    }

    /** レジューム機能が無効の場合のテストを行う */
    private void doTestDisableResume(String requestId) throws Exception {

        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        TestSupport.createFile(filePath, "\n", Charset.forName("windows-31j"),
                "0001,A01,10",
                "0001,A01,20",
                "0001,A02,30",
                "0002,A02,40",
                "0003,A03,50"
        );

        FileDataReader fileReader = new FileDataReader().setDataFile("record")
                                                        .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext();

        /**
         * 1レコード目だけ読み込んで、処理を終了する。
         */
        // 1レコード目のロード
        DataRecord record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントが0から変化しないことを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(0L));

        reader.close(ctx);

        /**
         * 処理を再開した場合に、また1レコード目から読み込まれることを確認する。
         */
        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 1レコード目がロードされる
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされないことを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(0L));
    }

    /**
     * １レコード目、３レコード目に空行があっても、ただしくレジュームが行われることの確認。
     */
    @Test
    public void testReadBlankLine() throws Exception {

        String requestId = "RW000001";
        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // データファイル
        TestSupport.createFile(filePath, "\n", Charset.forName("ms932"),
                "",
                "0001,A01,10",
                "",
                "0001,A02,30",
                "0002,A02,40",
                ""
        );
        FileDataReader fileReader = new FileDataReader().setDataFile("record")
                                                        .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext();

        // 2レコード目のロード（1レコード目は空行なので読み飛ばし）
        DataRecord record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));


        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(1L));

        reader.close(ctx);

        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 4レコード目のロード（3レコード目は空行なので読み飛ばし）
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(4, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        reader.close(ctx);

        fileReader = new FileDataReader().setDataFile("record")
                                         .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 5レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0002", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("40"), record.get("price"));
        assertEquals(5, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        reader.close(ctx);
    }

    /**
     * LoopHandlerの一括コミットとの連動機能のテスト
     */
    @Test
    public void testThatWorksProperlyWithBulkCommitFeatureOfLoopHandler() throws Exception {

        String requestId = "RW000001";
        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // データファイル
        TestSupport.createFile(filePath, "\n", Charset.forName("utf-8"),
                "0001,A01,10",
                "0002,A02,30",
                "0003,A02,30",
                "0004,A02,30",
                "0005,A02,30",
                "0006,A02,30",
                "0007,A02,30",
                "0008,A02,30",
                "0009,A02,30",
                "0010,A02,30"
                );

        FileDataReader fileReader = new ValidatableFileDataReader().setValidatorAction(new StubValidatorAction())
                                                                   .setDataFile("record")
                                                                   .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext()
                .setDataReader(reader)
                .setRequestScopedVar("nablarch_LoopHandler_is_about_to_commit", false);
        reader.read(ctx);
        reader.read(ctx);
        reader.read(ctx);
        reader.read(ctx);
        transactionManager.commitTransaction();

        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat("LoopHandlerが一括コミットを行うまではRESUME_POINTを更新しないこと。", result.resumePoint, is(0L));

        ctx.setRequestScopedVar("nablarch_LoopHandler_is_about_to_commit", true);
        reader.read(ctx);
        transactionManager.commitTransaction();

        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat("LoopHandlerが一括コミットを実施した時点でRESUME_POINTを更新すること。", result.resumePoint, is(5L));

        reader.close(ctx);
    }


    /**
     * バリデーション機能（キャッシュ無効）を使用する場合に、正しくレジュームが行われることの確認テスト。
     */
    @Test
    public void testReadValidatableResumeDisableCache() throws Exception {

        String requestId = "RW000001";
        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // データファイル
        TestSupport.createFile(filePath, "\n", Charset.forName("UTF-8"),
                "0001,A01,10",
                "0001,A01,20",
                "0002,A02,30");

        FileDataReader fileReader = new ValidatableFileDataReader().setValidatorAction(new StubValidatorAction())
                                                                   .setDataFile("record")
                                                                   .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext();

        // 1レコード目のロード
        DataRecord record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 2レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("20"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));


        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        reader.close(ctx);


        /**
         * 3レコード目（空行）から処理を再開する
         */
        fileReader = new ValidatableFileDataReader().setValidatorAction(new StubValidatorAction())
                                                    .setDataFile("record")
                                                    .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 3レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0002", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        reader.close(ctx);
    }


    /**
     * バリデーション機能（キャッシュ有効）を使用する場合に、正しくレジュームが行われることの確認テスト。
     */
    @Test
    public void testReadValidatableResumeEnableCache() throws Exception {

        String requestId = "RW000001";
        File filePath = new File(tempFile, "record.dat");

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 0L));

        // データファイル
        TestSupport.createFile(filePath, "\n", Charset.forName("ms932"),
                "0001,A01,10",
                "0001,A01,20",
                "0002,A02,30"
        );
        FileDataReader fileReader = new ValidatableFileDataReader().setValidatorAction(new StubValidatorAction())
                                                                   .setUseCache(true)
                                                                   .setDataFile("record")
                                                                   .setLayoutFile("format");
        ResumeDataReader<DataRecord> reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        ExecutionContext ctx = new ExecutionContext();

        // 1レコード目のロード
        DataRecord record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("10"), record.get("price"));
        assertEquals(1, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));

        // 2レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0001", record.get("userId"));
        assertEquals("A01", record.get("userCode"));
        assertEquals(new BigDecimal("20"), record.get("price"));
        assertEquals(2, record.getRecordNumber());
        assertTrue(reader.hasNext(ctx));


        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        ResumeBatchRequest result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(2L));

        reader.close(ctx);


        /**
         * 3レコード目（空行）から処理を再開する
         */
        fileReader = new ValidatableFileDataReader().setValidatorAction(new StubValidatorAction())
                                                    .setUseCache(true)
                                                    .setDataFile("record")
                                                    .setLayoutFile("format");
        reader = new ResumeDataReader<DataRecord>().setSourceReader(fileReader);

        // 3レコード目のロード
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("0002", record.get("userId"));
        assertEquals("A02", record.get("userCode"));
        assertEquals(new BigDecimal("30"), record.get("price"));
        assertEquals(3, record.getRecordNumber());
        assertFalse(reader.hasNext(ctx));

        // 正常に処理できたポイントがインクリメントされることを確認
        transactionManager.commitTransaction();
        result = VariousDbTestHelper.findById(ResumeBatchRequest.class, requestId);
        assertThat(result, is(notNullValue()));
        assertThat(result.resumePoint, is(3L));

        reader.close(ctx);
    }

    /**
     * スタブValidatorAction。（実際に精査は行わない）
     *
     * @author Masato Inoue
     */
    private class StubValidatorAction implements Handler<DataRecord, Result>, FileValidatorAction {

        /** 何もしない */
        @Override
        public Result handle(DataRecord data, ExecutionContext context) {
            // nop
            return null;
        }

        /** 何もしない */
        @Override
        public void onFileEnd(ExecutionContext ctx) {
            // nop
        }
    }
}
