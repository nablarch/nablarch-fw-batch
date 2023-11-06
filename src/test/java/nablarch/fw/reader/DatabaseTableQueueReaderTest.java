package nablarch.fw.reader;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.transaction.TransactionContext;
import nablarch.fw.SynchronizedDataReaderWrapper;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link DatabaseTableQueueReader}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class DatabaseTableQueueReaderTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    private TransactionManagerConnection connection;

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(BatchRequestTable.class);
        VariousDbTestHelper.createTable(MultiPrimaryKey.class);
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory factory = repositoryResource.getComponent("connectionFactory");
        connection = factory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        VariousDbTestHelper.delete(BatchRequestTable.class);
        VariousDbTestHelper.delete(MultiPrimaryKey.class);
        OnMemoryLogWriter.clear();
    }

    @After
    public void tearDown() throws Exception {
        try {
            connection.terminate();
        } catch (Exception e) {
            // NOP
        }
    }

    /**
     * テーブルからデータが読み取れること。
     * <p/>
     * データを全件読み込み終わった場合、{@code hasNext}は{@code true}を返すが、
     * {@code read}結果は{@code null}を返すこと。
     */
    @Test
    public void dataFound() throws Exception {

        createTestData(5);

        DatabaseTableQueueReader sut = createReader();

        // 対象データが全て読み込まれること
        for (int i = 1; i <= 5; i++) {
            assertThat(sut.hasNext(null), is(true));
            SqlRow row = sut.read(null);
            assertThat(row.getBigDecimal("id")
                          .intValue(), is(i));
            assertThat(row.getString("data"), is("data_" + i));
            assertThat(row.getString("status"), is("0"));
        }

        updateStatus();

        assertThat("対象データが無くなってもhasNextはtrueを返すこと", sut.hasNext(null), is(true));
        assertThat("対象データが無いため、read結果はnullとなること", sut.read(null), is(nullValue()));
    }

    /**
     * テーブルにレコードが存在しない場合。
     * <p/>
     * データが存在しないので、{@code hasNext}は{@code true}を返すが、
     * {@code read}結果は{@code null}を返すこと。
     */
    @Test
    public void dataNotFound() throws Exception {

        DatabaseTableQueueReader sut = createReader();

        assertThat("対象データがなくなってもhasNextはtrueを返すこと", sut.hasNext(null), is(true));
        assertThat("対象データがないため、readはnullを返すこと", sut.read(null), is(nullValue()));
    }

    /**
     * 一度データが無くなった場合でも、追加されたデータが再度取得できること
     */
    @Test
    public void repeatDataRead() throws Exception {

        createTestData(1);

        DatabaseTableQueueReader sut = createReader();

        assertThat("次のデータが存在する。", sut.hasNext(null), is(true));
        assertThat("データが読み取れること", sut.read(null), is(notNullValue()));

        updateStatus();

        assertThat("データは存在しないが、hasNextはtrue", sut.hasNext(null), is(true));
        assertThat("データが存在しないのでreadはnull", sut.read(null), is(nullValue()));

        createTestData(10, 1);

        assertThat(sut.hasNext(null), is(true));
        SqlRow row = sut.read(null);
        assertThat(row.getBigDecimal("id")
                      .intValue(), is(10));
    }

    /**
     * 他のスレッドで処理中のレコードは未処理状態でもリードされないこと
     */
    @Test
    public void executingDataUnread() throws Exception {
        createTestData(1);

        final DatabaseTableQueueReader sut = createReader();

        // 別スレッドでデータをリード
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<SqlRow> future = executor.submit(new DataReadTask<>(sut, new CountDownLatch(1), null));
        executor.shutdown();
        SqlRow row = future.get();
        assertThat("子スレッド側でデータが読み込めていること", row.getBigDecimal("id")
                                              .intValue(), is(1));

        SqlRow nullObj = sut.read(null);
        assertThat("子スレッドでデータが読み込まれているので、親スレッド側でデータは読み取れないこと",
                nullObj, is(nullValue()));
    }


    /**
     * 同一スレッドの場合は、同じレコードが未処理で残っていた場合、再度読み取れること。
     */
    @Test
    public void sameThreadRepeatRead() throws Exception {
        createTestData(1);

        final DatabaseTableQueueReader sut = createReader();

        // 別スレッドでデータをリード
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<SqlRow> future = executor.submit(new DataReadTask<>(sut, new CountDownLatch(1), null));
        assertThat("子スレッド側でデータが読み込めること", future.get()
                                               .getBigDecimal("id")
                                               .intValue(), is(1));

        future = executor.submit(new DataReadTask<>(sut, new CountDownLatch(1), null));
        assertThat("子スレッドで再度リードした場合同じレコードが読み込めること", future.get()
                                                          .getBigDecimal("id")
                                                          .intValue(), is(1));

        updateStatus();
        future = executor.submit(new DataReadTask<>(sut, new CountDownLatch(1), null));
        assertThat("処理済みになった場合は読み取れない", future.get(), is(nullValue()));
        executor.shutdown();
    }

    /**
     * 複数スレッドでリードをするテスト。
     */
    @Test
    public void multiThreadRead() throws Exception {
        createTestData(5);

        // synchronizedでラップしたデータリーダの作成
        // 複数スレッドでDataReader#read()の呼出タイミングを重複させて問題なく動作することを確認したいが、
        // DatabaseRecordReaderでは、DataReader#read()の呼出タイミングを重複させることができない。
        // そのため、DataReadTaskのラッチ機構で各スレッドのDataReader#read()呼出タイミングを同期させ、擬似的にread()の実行を重複させている。
        final DatabaseTableQueueReader sut = createReader();
        SynchronizedDataReaderWrapper<SqlRow> testReader = new SynchronizedDataReaderWrapper<>(sut);

        // 並列実行するタスクを作成
        List<DataReadTask<SqlRow>> tasks = new ArrayList<>(5);
        DataReadTask<SqlRow> task = new DataReadTask<>(testReader, new CountDownLatch(5), null);
        for (int i = 0; i < 5; i++) {
            tasks.add(task);
        }

        // 並列実行し、結果を取得
        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Future<SqlRow>> result = executor.invokeAll(tasks);

        List<Integer> actualList = new ArrayList<>();
        for (Future<SqlRow> future : result) {
            actualList.add(future.get()
                                 .getBigDecimal("id")
                                 .intValue());
        }
        assertThat(actualList, hasItems(1, 2, 3, 4, 5));

        assertThat("子スレッド側で全レコード読み込んでいるので次のレコードが存在しない", sut.read(null), is(nullValue()));
    }

    /**
     * 主キーカラムが複数の場合のテスト。
     *
     * @throws Exception Exception
     */
    @Test
    public void multiplePrimaryKeys() throws Exception {
        SqlPStatement statement = connection.prepareStatement("INSERT INTO MULTI_PRIMARY_KEY VALUES (?, ?, ?)");
        // null, null
        statement.setInt(1, 1);
        statement.setNull(2, Types.INTEGER);
        statement.setNull(3, Types.INTEGER);
        statement.addBatch();
        // 2, null
        statement.setInt(1, 2);
        statement.setInt(2, 2);
        statement.setNull(3, Types.INTEGER);
        statement.addBatch();
        // null, 3
        statement.setInt(1, 3);
        statement.setNull(2, Types.INTEGER);
        statement.setInt(3, 3);
        statement.addBatch();
        // 4, 4
        statement.setInt(1, 4);
        statement.setInt(2, 4);
        statement.setInt(3, 4);
        statement.addBatch();
        statement.executeBatch();

        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(connection.prepareStatement("SELECT * FROM MULTI_PRIMARY_KEY ORDER BY ID"));
        final DatabaseTableQueueReader sut = new DatabaseTableQueueReader(reader, 1000, "id1", "id2");
        final SynchronizedDataReaderWrapper<SqlRow> testReader = new SynchronizedDataReaderWrapper<>(sut);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        Callable<SqlRow> task = new DataReadTask<>(testReader, new CountDownLatch(0), null);
        List<Future<SqlRow>> result = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            result.add(executor.submit(task));
        }

        List<Integer> actual = new ArrayList<Integer>();
        for (Future<SqlRow> future : result) {
            actual.add(future.get()
                             .getBigDecimal("id")
                             .intValue());
        }
        assertThat("処理対象レコードが全て読み込まれていること", actual, hasItems(1, 2, 3, 4));
        assertThat("次のレコードは存在しない", sut.read(null), is(nullValue()));
    }

    /**
     * INFOログ読み込んだレコードの主キー情報が出力されていることを確認する。
     *
     * @throws Exception Exception
     */
    @Test
    public void writeInfoLog() throws Exception {
        createTestData(3);

        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID"));
        DatabaseTableQueueReader sut = new DatabaseTableQueueReader(reader, 1000, "ID", "STATUS");

        assertThat(sut.read(null)
                      .getBigDecimal("id")
                      .intValue(), is(1));
        assertThat(sut.read(null)
                      .getBigDecimal("id")
                      .intValue(), is(2));
        assertThat(sut.read(null)
                      .getBigDecimal("id")
                      .intValue(), is(3));
        updateStatus();
        assertThat(sut.read(null), is(nullValue()));

        List<String> log = OnMemoryLogWriter.getMessages("writer.queue");
        assertThat("読み込んだレコード数分の情報がログに出力されていること", log.size(), is(3));

        assertLog(log.get(0), "key info: {ID=1, STATUS=0}");
        assertLog(log.get(1), "key info: {ID=2, STATUS=0}");
        assertLog(log.get(2), "key info: {ID=3, STATUS=0}");
    }

    @Test
    public void testOverrideWriteLog() throws Exception {

        createTestData(1);

        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID"));
        DatabaseTableQueueReader sut = new DatabaseTableQueueReader(reader, 1000, "ID", "STATUS") {
            @Override
            protected void writeLog(final InputDataIdentifier inputDataIdentifier) {
                assertThat(inputDataIdentifier.get("ID")
                                              .toString(), is("1"));
                assertThat(inputDataIdentifier.get("STATUS")
                                              .toString(), is("0"));
            }
        };

        assertThat(sut.read(null)
                      .getBigDecimal("id")
                      .intValue(), is(1));
        updateStatus();
        assertThat(sut.read(null), is(nullValue()));
    }

    /**
     * 待機時間分待機後にデータが取得できること
     *
     * @throws Exception Exception
     */
    @Test
    public void waitTime() throws Exception {
        createTestData(1);

        DatabaseTableQueueReader sut = createReader();
        long start = System.currentTimeMillis();
        SqlRow row = sut.read(null);
        long end = System.currentTimeMillis();
        assertThat("データが取得できること", row, is(notNullValue()));
        assertThat("データが存在するため待機時間未満でデータが取得できること", (end - start) < 1000, is(true));

        updateStatus();
        start = System.currentTimeMillis();
        row = sut.read(null);
        end = System.currentTimeMillis();
        assertThat("データは存在しない", row, is(nullValue()));
        assertThat("データが存在しないので一定時間待機後にデータが取得される", (end - start) >= 1000, is(true));
    }

    /**
     * ログメッセージのアサートを行う。
     * <p>
     * 期待するメッセージがログに含まれていることを検証する。
     * また、ログ内に固定メッセージ(read database record.)が出力されていることも検証する。
     *
     * @param actual ログに出力された内容（1 write分)
     * @param expected 期待するメッセージ
     */
    private static void assertLog(String actual, String expected) {
        assertThat(actual, containsString("read database record."));
        assertThat(actual, containsString(expected));
    }

    /**
     * クローズ後にデータを読み取ろうとした場合例外が発生すること
     *
     * @throws Exception Exception
     */
    @Test
    public void close() throws Exception {
        createTestData(1);

        DatabaseTableQueueReader sut = createReader();

        sut.close(null);

        assertThat("クローズされたので、次のレコードは存在しない", sut.hasNext(null), is(false));
        try {
            sut.read(null);
        } catch (Exception e) {
            // SqlStatementExceptionが発生する。
            assertThat(e, is(instanceOf(SqlStatementException.class)));
        }
    }

    /**
     * 指定したプライマリーキーが要求に存在しない場合例外が発生すること。
     *
     * @throws Exception Exception
     */
    @Test
    public void pkWasNotFound() throws Exception {
        createTestData(1);
        DatabaseRecordReader reader = new DatabaseRecordReader();
        SqlPStatement statement = connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID");
        reader.setStatement(statement);

        DatabaseTableQueueReader sut = new DatabaseTableQueueReader(reader, 1000, "not found pk");
        try {
            sut.read(null);
            fail("ここはとおらない");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("primary key was not found in request. primary key name = [not found pk]"));
        }
    }

    /**
     * プライマリーキーにnullを指定した場合例外が発生すること
     *
     * @throws Exception Exception
     */
    @Test
    public void specifiedPrimaryKeyNull() throws Exception {
        DatabaseRecordReader reader = new DatabaseRecordReader();
        SqlPStatement statement = connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID");
        reader.setStatement(statement);

        try {
            new DatabaseTableQueueReader(reader, 1000, (String[]) null);
            fail("ここはとおらない");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("primary keys must be set."));
        }
    }

    /**
     * 主キーを指定しない場合例外が発生すること
     */
    @Test
    public void notSpecifiedPrimaryKeys() throws Exception {
        createTestData(1);

        DatabaseRecordReader reader = new DatabaseRecordReader();
        SqlPStatement statement = connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID");
        reader.setStatement(statement);

        try {
            new DatabaseTableQueueReader(reader, 1000);
            fail("ここはとおらない");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("primary keys must be set."));
        }
    }

    /**
     * 主キーのカラム名が重複している場合、例外が発生すること
     */
    @Test
    public void duplicatedPrimaryKey() throws Exception {
        createTestData(1);

        DatabaseRecordReader reader = new DatabaseRecordReader();
        SqlPStatement statement = connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID");
        reader.setStatement(statement);

        try {
            new DatabaseTableQueueReader(reader, 1000, "id", "id");
            fail("ここはとおらない");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(
                    "duplicated primary key. must be unique column name. primary keys = [id, id]"));
        }
    }

    /**
     * テスト対象の{@link DatabaseTableQueueReader}を生成する。
     *
     * @return 生成したリーダ
     */
    private DatabaseTableQueueReader createReader() {
        DatabaseRecordReader reader = new DatabaseRecordReader();
        reader.setStatement(connection.prepareStatement(
                "SELECT * FROM BATCH_REQUEST_TABLE WHERE STATUS = '0' ORDER BY ID"));
        return new DatabaseTableQueueReader(reader, 1000, "ID");
    }

    /**
     * テストデータを作成する。
     *
     * @param dataCount 作成レコード数
     */
    private void createTestData(int dataCount) {
        createTestData(1, dataCount);
    }

    /**
     * テストデータを作成する。
     *
     * @param dataCount 作成レコード数
     */
    private void createTestData(int startCount, int dataCount) {
        SqlPStatement statement = connection.prepareStatement("INSERT INTO BATCH_REQUEST_TABLE VALUES (?, ?, ?)");
        for (int i = startCount; i <= (startCount + dataCount) - 1; i++) {
            statement.setInt(1, i);
            statement.setString(2, "data_" + i);
            statement.setString(3, "0");
            statement.addBatch();
        }
        statement.executeBatch();
        connection.commit();
    }


    /**
     * 要求データのステータスを全て処理済みに更新する。
     */
    private void updateStatus() {
        SqlPStatement statement = connection.prepareStatement("UPDATE BATCH_REQUEST_TABLE SET STATUS = '1'");
        statement.executeUpdate();
        connection.commit();
    }
}

