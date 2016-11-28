package nablarch.fw.reader;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.SystemRepository;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionFactory;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;
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
 * {@link DatabaseRecordReader}テスト。
 *
 * @author Masato Inoue
 */
@RunWith(DatabaseTestRunner.class)
public class DatabaseRecordReaderTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    private static TransactionManagerConnection tmConn;

    /**
     * テーブル初期化処理。
     *
     * @throws Exception すべての例外
     */
    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(ReaderBook.class);
    }

    /**
     * テーブル初期化処理。
     *
     * @throws Exception すべての例外
     */
    @Before
    public void setUp() throws Exception {
        ConnectionFactory factory = SystemRepository.get("connectionFactory");
        tmConn = factory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(tmConn);
        VariousDbTestHelper.delete(ReaderBook.class);
    }

    @After
    public void tearDown() throws Exception {
        tmConn.terminate();
        DbConnectionContext.removeConnection();
    }

    /**
     * データベースを参照するテスト。
     * SQL IDを設定するパターン。
     */
    @Test
    public void testReadBySqlId() {
        ExecutionContext ctx = new ExecutionContext();

        // 0件のパターン
        VariousDbTestHelper.delete(ReaderBook.class);
        SqlPStatement statement = DbConnectionContext.getConnection()
                                                     .prepareStatementBySqlId(
                                                             DatabaseRecordReaderTest.class.getName() + "#"
                                                                     + "SQL_001");
        DataReader<SqlRow> reader = new DatabaseRecordReader().setStatement(statement);

        assertNull(reader.read(ctx));
        assertNull(reader.read(ctx));
        assertFalse(reader.hasNext(ctx));


        // 3件のパターン
        VariousDbTestHelper.setUpTable(
                new ReaderBook("Learning the vi and vim Editors", "OReilly", "Robbins Hanneah and Lamb"),
                new ReaderBook("Patterns of Enterprise Application Architecture", "Addison-Wesley", "Martin Fowler"),
                new ReaderBook("Programming with POSIX Threads", "Addison-Wesley", "David R. Butenhof"));
        reader = new DatabaseRecordReader().setStatement(statement);

        assertTrue(reader.hasNext(ctx));
        SqlRow record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Learning the vi and vim Editors", record.get("title"));
        assertEquals("OReilly", record.get("publisher"));
        assertEquals("Robbins Hanneah and Lamb", record.get("authors"));

        assertTrue(reader.hasNext(ctx));
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Patterns of Enterprise Application Architecture", record.get("title"));
        assertEquals("Addison-Wesley", record.get("publisher"));
        assertEquals("Martin Fowler", record.get("authors"));

        assertTrue(reader.hasNext(ctx));
        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Programming with POSIX Threads", record.get("title"));
        assertEquals("Addison-Wesley", record.get("publisher"));
        assertEquals("David R. Butenhof", record.get("authors"));

        assertNull(reader.read(ctx));
        assertFalse(reader.hasNext(ctx));


        // 検索条件ありのパターン
        statement = DbConnectionContext.getConnection()
                                       .prepareStatementBySqlId(
                                               DatabaseRecordReaderTest.class.getName() + "#" + "SQL_002");
        reader = new DatabaseRecordReader().setStatement(statement);

        assertTrue(reader.hasNext(ctx));
        record = reader.read(ctx);
        assertEquals(2, record.size());
        assertEquals("Learning the vi and vim Editors", record.get("title"));
        assertEquals("OReilly", record.get("publisher"));

        assertNull(reader.read(ctx));
        assertFalse(reader.hasNext(ctx));

    }


    /**
     * データベースを参照するテスト。
     * SQL文を直接設定するパターン。
     */
    @Test
    public void testReadBySql() {
        ExecutionContext ctx = new ExecutionContext();

        // 条件なしパターン
        VariousDbTestHelper.setUpTable(
                new ReaderBook("Learning the vi and vim Editors", "OReilly", "Robbins Hanneah and Lamb"),
                new ReaderBook("Programming with POSIX Threads", "Addison-Wesley", "David R. Butenhof"),
                new ReaderBook("Patterns of Enterprise Application Architecture", "Addison-Wesley", "Martin Fowler"));

        SqlPStatement statement = DbConnectionContext.getConnection()
                                                     .prepareStatement("SELECT * FROM READER_BOOK ORDER BY title");
        DataReader<SqlRow> reader = new DatabaseRecordReader().setStatement(statement);

        SqlRow record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Learning the vi and vim Editors", record.get("title"));
        assertEquals("OReilly", record.get("publisher"));
        assertEquals("Robbins Hanneah and Lamb", record.get("authors"));

        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Patterns of Enterprise Application Architecture", record.get("title"));
        assertEquals("Addison-Wesley", record.get("publisher"));
        assertEquals("Martin Fowler", record.get("authors"));

        record = reader.read(ctx);
        assertEquals(3, record.size());
        assertEquals("Programming with POSIX Threads", record.get("title"));
        assertEquals("Addison-Wesley", record.get("publisher"));
        assertEquals("David R. Butenhof", record.get("authors"));

        assertFalse(reader.hasNext(ctx));


        // 条件ありパターン
        statement = DbConnectionContext.getConnection()
                                       .prepareStatement("SELECT title, publisher FROM READER_BOOK WHERE publisher = 'OReilly'");
        

        reader = new DatabaseRecordReader().setStatement(statement);

        record = reader.read(ctx);
        assertEquals(2, record.size());
        assertEquals("Learning the vi and vim Editors", record.get("title"));
        assertEquals("OReilly", record.get("publisher"));

        assertFalse(reader.hasNext(ctx));
        reader.close(ctx);
    }

    /**
     * データベースを参照するテスト。
     * SQL文を直接設定するパターン。
     */
    @Test
    public void testParameterizedSql() {
        ExecutionContext ctx = new ExecutionContext();

        // 条件なしパターン
        VariousDbTestHelper.setUpTable(
                new ReaderBook("Learning the vi and vim Editors", "OReilly", "Robbins Hanneah and Lamb"),
                new ReaderBook("Programming with POSIX Threads", "Addison-Wesley", "David R. Butenhof"),
                new ReaderBook("Patterns of Enterprise Application Architecture", "Addison-Wesley", "Martin Fowler"));

        ParameterizedSqlPStatement statement = DbConnectionContext.getConnection()
                                                                  .prepareParameterizedSqlStatement(
                                                                          "SELECT * FROM READER_Book");

        // object
        DataReader<SqlRow> reader = new DatabaseRecordReader().setStatement(statement, new Object());
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(false));
        reader.close(ctx);

        // map
        reader = new DatabaseRecordReader().setStatement(statement, new HashMap<String, Object>());
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(true));
        reader.read(ctx);
        assertThat(reader.hasNext(ctx), is(false));
        reader.close(ctx);
    }

    /**
     * データベースを参照するテスト。
     * SQLステートメントが設定されないパターン。
     */
    @Test
    public void testReadSqlNotSet() {
        ExecutionContext ctx = new ExecutionContext();
        VariousDbTestHelper.setUpTable(
                new ReaderBook("Learning the vi and vim Editors", "OReilly", "Robbins Hanneah and Lamb"),
                new ReaderBook("Programming with POSIX Threads", "Addison-Wesley", "David R. Butenhof"),
                new ReaderBook("Patterns of Enterprise Application Architecture", "Addison-Wesley", "Martin Fowler"));

        DataReader<SqlRow> reader = new DatabaseRecordReader();

        try {
            reader.read(ctx);
            fail();
        } catch (IllegalStateException e) {
        }
    }


    /**
     * statementがnullの場合にcloseするテスト。（何も起こらない）
     */
    @Test
    public void testClose() {
        DataReader<SqlRow> reader = new DatabaseRecordReader();
        reader.close(new ExecutionContext());
        assertTrue(true);
    }

    /**
     * 設定したリスナが正しく実行されることを確認。
     */
    @Test
    public void testListener() {
        VariousDbTestHelper.setUpTable(
                new ReaderBook("title1", "publisher1", "authors1"),
                new ReaderBook("title2", "publisher2", "authors2"),
                new ReaderBook("title3", "publisher3", "authors3"));


        // テスト用のトランザクションマネージャをリポジトリに登録
        ConnectionFactory connectionFactory = repositoryResource.getComponent("connectionFactory");
        TransactionFactory transactionFactory = repositoryResource.getComponent("jdbcTransactionFactory");
        SimpleDbTransactionManager manager = new SimpleDbTransactionManager();
        manager.setDbTransactionName("testTransaction");
        manager.setConnectionFactory(connectionFactory);
        manager.setTransactionFactory(transactionFactory);
        repositoryResource.addComponent("testTransaction", manager);

        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(DbConnectionContext.getConnection().prepareStatement("SELECT * FROM READER_BOOK"));

        // PUBLISHERカラムの値を全て"change"に変更するSQLを発行するリスナを追加
        reader.setListener(new DatabaseRecordListener() {
            @Override
            public void beforeReadRecords() {
                SimpleDbTransactionManager manager = SystemRepository.get("testTransaction");
                new SimpleDbTransactionExecutor<Void>(manager) {
                    @Override
                    public Void execute(AppDbConnection appDbConnection) {
                        appDbConnection.prepareStatement("UPDATE READER_BOOK SET PUBLISHER = 'change'").executeUpdate();
                        return null;
                    }
                }.doTransaction();
            }
        });

        reader.read(null);

        List<ReaderBook> readerBooks = VariousDbTestHelper.findAll(ReaderBook.class);
        assertThat(readerBooks.get(0).publisher, is("change"));
        assertThat(readerBooks.get(1).publisher, is("change"));
        assertThat(readerBooks.get(2).publisher, is("change"));
    }
}
