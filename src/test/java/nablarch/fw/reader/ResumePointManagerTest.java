package nablarch.fw.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.charset.Charset;
import java.sql.SQLException;

import nablarch.core.ThreadContext;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.util.FilePathSetting;
import nablarch.fw.TestSupport;
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
 * {@link ResumePointManager}のテスト。
 * 正常系のテストは、{@link ResumeDataReaderTest}で行っているので、
 * ここでは異常系のテストのみ行う。
 *
 * @author Masato Inoue
 */
@RunWith(DatabaseTestRunner.class)
public class ResumePointManagerTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource(
            "nablarch/fw/reader/ResumePointManager.xml");

    /** connection */
    private static SimpleDbTransactionManager transactionManager;
    
    private static File tempDir = new File(System.getProperty("java.io.tmpdir"));

    private File formatFile;

    /**
     * テストクラスの事前処理。
     *
     * @throws SQLException
     */
    @BeforeClass
    public static void beforeClass() throws SQLException {
        ThreadContext.clear();
        VariousDbTestHelper.createTable(ResumeBatchRequest.class);
    }

    @Before
    public void setUp() throws Exception {
        transactionManager = repositoryResource.getComponent("tran");

        if (transactionManager != null) {
            transactionManager.beginTransaction();
        }

        // レイアウト定義ファイル
        formatFile = new File(tempDir, "./format.fmt ");
        TestSupport.createFile(formatFile, Charset.forName("utf-8"),
                "file-type:    \"Variable\"",
                "text-encoding:     \"ms932\"",
                "record-separator:  \"\\n\"",
                "field-separator:   \",\"",
                "quoting-delimiter: \"\\\"\"",
                "[DataRecord]",
                "1   userId           X",
                "2   userCode         X",
                "3   price            X    number"
        );
        ThreadContext.setConcurrentNumber(1);
    }

    @After
    public void tearDown() {
        ThreadContext.clear();
        if (transactionManager != null) {
            transactionManager.endTransaction();
        }
    }

    /**
     * 正常に処理できたポイントのロードに失敗するテスト。
     */
    @Test
    public void testCannotLoadResumePoint() throws Exception {

        // テーブル
        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 1L));

        /**
         * リクエストIDがテーブルに存在しない場合。
         */
        ResumePointManager target = new ResumePointManager();
        target.setRequestIdColumnName("REQUEST_ID");
        target.setResumable(true);
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setResumePointColumnName("RESUME_POINT");
        target.initialize();
        target.setDbTransactionName("resumeTransaction");

        try {
            target.loadResumePoint("error");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(
                    "Couldn't load resume point from the table. sql=[SELECT RESUME_POINT FROM RESUME_BATCH_REQUEST WHERE "
                            +
                            "REQUEST_ID = ?], request id=[error].",
                    e.getMessage());
        }

        transactionManager.rollbackTransaction();
    }


    /**
     * 正常に処理できたポイントの保存に失敗するテスト。
     */
    @Test
    public void testCannotSaveResumePoint() throws Exception {

        // テーブル（空のテーブルを作成する）
        VariousDbTestHelper.delete(ResumeBatchRequest.class);

        String requestId = "RW000001";
        ThreadContext.setRequestId(requestId);

        ResumePointManager target = new ResumePointManager();
        target.setRequestIdColumnName("REQUEST_ID");
        target.setResumable(true);
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setResumePointColumnName("RESUME_POINT");
        target.initialize();
        target.setDbTransactionName("resumeTransaction");

        try {
            target.saveResumePoint(requestId, 10);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(
                    "Couldn't save resume point. sql=[UPDATE RESUME_BATCH_REQUEST SET RESUME_POINT = ? WHERE REQUEST_ID = ?], request id=[RW000001].",
                    e.getMessage());
        }

    }


    /**
     * 正常に処理できたポイントに-1未満の値が設定されている場合のテスト。
     */
    @Test
    public void testInvalidResumePoint() throws Exception {

        String requestId = "RW000001";

        ThreadContext.setRequestId(requestId);

        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", -1L));

        /**
         * リクエストIDがテーブルに存在しない場合。
         */
        ResumePointManager target = new ResumePointManager();
        target.setRequestIdColumnName("REQUEST_ID");
        target.setResumable(true);
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setResumePointColumnName("RESUME_POINT");
        target.initialize();
        target.setDbTransactionName("resumeTransaction");

        try {
            target.loadResumePoint(requestId);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(
                    "invalid resume point was stored on the table. " +
                            "resume point must be more than 0. " +
                            "resume point=[-1], " +
                            "sql=[SELECT RESUME_POINT FROM RESUME_BATCH_REQUEST WHERE REQUEST_ID = ?], " +
                            "request id=[RW000001].",
                    e.getMessage());
        }
    }

    /**
     * コンポーネント設定ファイルのパラメータが空の場合のテスト。
     */
    @Test
    public void testNotSetProperty() {

        ResumePointManager target = new ResumePointManager();
        target.setRequestIdColumnName("REQUEST_ID");
        target.setResumePointColumnName("RESUME_POINT");

        // テーブル名が存在しないパターン
        try {
            target.initialize();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("[tableName] property must be set. class=[nablarch.fw.reader.ResumePointManager].",
                    e.getMessage());
        }


        target = new ResumePointManager();
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setResumePointColumnName("RESUME_POINT");

        // リクエストIDのカラム名が存在しないパターン
        try {
            target.initialize();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("[requestIdColumnName] property must be set. class=[nablarch.fw.reader.ResumePointManager].",
                    e.getMessage());
        }


        target = new ResumePointManager();
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setRequestIdColumnName("REQUEST_ID");

        // 正常に処理できたポイントのカラム名が存在しないパターン
        try {
            target.initialize();
            fail();
        } catch (IllegalStateException e) {
            assertEquals("[resumePointColumnName] property must be set. class=[nablarch.fw.reader.ResumePointManager].",
                    e.getMessage());
        }
    }


    /**
     * マルチスレッドで実行した場合に、例外がスローされることを確認するテスト。
     */
    @Test
    public void testMultiThreadExecution() throws Exception {

        // テーブル
        VariousDbTestHelper.setUpTable(new ResumeBatchRequest("RW000001", 1L));
        
        /*
         * スレッド数2で実行。
         */
        ThreadContext.setConcurrentNumber(2);

        ResumePointManager target = new ResumePointManager();
        target.setRequestIdColumnName("REQUEST_ID");
        target.setResumable(true);
        target.setTableName("RESUME_BATCH_REQUEST");
        target.setResumePointColumnName("RESUME_POINT");
        target.initialize();
        target.setDbTransactionName("resumeTransaction");

        try {
            target.loadResumePoint("RW000001");
            fail();
        } catch (IllegalStateException e) {
            assertEquals(
                    "Cannot use resume function in multi thread. resume function is operated only in single thread. concurrent number=[2], request id=[RW000001].",
                    e.getMessage());
        }

        transactionManager.rollbackTransaction();

        ThreadContext.setConcurrentNumber(1);
        
        /*
         * スレッド数1で実行。（例外が発生しない）
         */
        int loadResumePoint = target.loadResumePoint("RW000001");

        assertEquals(1, loadResumePoint);
    }


}
