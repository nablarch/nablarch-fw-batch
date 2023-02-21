package nablarch.fw.handler;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link BasicDuplicateProcessChecker}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class BasicDuplicateProcessCheckerTest {

    /** テスト対象 */
    BasicDuplicateProcessChecker sut = new BasicDuplicateProcessChecker();

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(BatchProcess.class);
    }

    @Before
    public void setUp() throws Exception {
        VariousDbTestHelper.setUpTable(
                new BatchProcess("batch1", "0"),
                new BatchProcess("batch2", "1"),
                new BatchProcess("batch3", "0"),
                new BatchProcess("batch4", "1"));

        sut.setDbTransactionManager(SystemRepository.<SimpleDbTransactionManager>get("tran"));
        sut.setTableName("batch_process");
        sut.setProcessIdentifierColumnName("process_identifier");
        sut.setProcessActiveFlgColumnName("status");
        sut.initialize();
    }

    /**
     * プロセスが実行中でない場合、アクティブ化に成功すること。
     */
    @Test
    public void testCheckAndActive() throws Exception {
        sut.checkAndActive("batch3");

        final BatchProcess batchProcess = VariousDbTestHelper.findById(BatchProcess.class, "batch3");
        assertThat("ステータスが実行中に変更されていること", batchProcess.status, is("1"));
    }

    /**
     * プロセスが実行中の場合、非活性化に成功すること。
     */
    @Test
    public void testInactive() throws Exception {
        sut.inactive("batch4");

        final BatchProcess batchProcess = VariousDbTestHelper.findById(BatchProcess.class, "batch4");
        assertThat("ステータスが未実行に変更されていること", batchProcess.status, is("0"));
    }

    /**
     * 多重起動を起こった場合{@link AlreadyProcessRunningException}が送出されること
     */
    @Test(expected = AlreadyProcessRunningException.class)
    public void testDuplicatedRunning() throws Exception {
        sut.checkAndActive("batch4");
    }

    /**
     * ２重起動が許可されたプロセスの場合、状態管理が行われないこと。
     */
    @Test
    public void permitProcess() throws Exception {
        sut.setPermitProcessIdentifier(new String[] {"batch1", "batch4", "batch5"});
        sut.checkAndActive("batch1");

        assertThat("状態が変更されないこと",
                VariousDbTestHelper.findById(BatchProcess.class, "batch1").status, is("0"));

        sut.inactive("batch4");
        assertThat("状態が変更されないこと",
                VariousDbTestHelper.findById(BatchProcess.class, "batch4").status, is("1"));
    }

    @Entity
    @Table(name = "batch_process")
    public static class BatchProcess {

        @Id
        @Column(name = "process_identifier", length = 30)
        public String processIdentifier;

        @Column(name = "status", length = 1)
        public String status;

        public BatchProcess() {
        }

        public BatchProcess(String processIdentifier, String status) {
            this.processIdentifier = processIdentifier;
            this.status = status;
        }
    }
}

