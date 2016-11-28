package nablarch.fw.reader;

import java.util.Iterator;
import java.util.Map;

import nablarch.core.db.statement.ParameterizedSqlPStatement;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlRow;
import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReader;
import nablarch.fw.ExecutionContext;

/**
 * データベースの参照結果を1レコードづつ読み込むデータリーダ。
 *
 * @author Iwauo Tajima
 */
public class DatabaseRecordReader implements DataReader<SqlRow> {

    /** 参照結果レコードのイテレータ */
    private Iterator<SqlRow> records = null;

    /** テーブル参照用SQLステートメント */
    private SqlPStatement statement;

    /** テーブル参照用SQLステートメント(オブジェクトを条件に指定する場合) */
    private ParameterizedSqlPStatement parameterizedSqlPStatement;

    /** 条件 */
    private Object condition;

    /**
     * データベースからのレコード取得前に実行するリスナ
     */
    private DatabaseRecordListener listener;

    /**
     * {@code DatabaseRecordReader}オブジェクトを生成する。
     */
    @Published
    public DatabaseRecordReader() {
        super();
    }

    /**
     * 参照結果のレコードを1行づつ返却する。
     * <p/>
     * 初回読み込み時にデータベースからレコードを取得し、キャッシュする。<br/>
     * レコードはそのキャッシュから返却する。<br/>
     * 参照結果に次のレコードが存在しない場合、{@code null}を返す。
     *
     * @param ctx 実行コンテキスト
     * @return レコードデータをキャッシュするオブジェクト
     */
    public synchronized SqlRow read(ExecutionContext ctx) {
        if (records == null) {
            readRecords();
        }
        return records.hasNext() ? records.next() : null;
    }

    /**
     * 参照結果から次のレコードが存在するかどうかを返却する。
     * <p/>
     * 初回読み込み時にデータベースからレコードを取得し、キャッシュする。<br/>
     * 結果はそのキャッシュから返却する。
     *
     * @param ctx 実行コンテキスト
     * @return 次に読み込むレコードが存在する場合 {@code true}
     */
    public synchronized boolean hasNext(ExecutionContext ctx) {
        if (records == null) {
            readRecords();
        }
        return records.hasNext();
    }
    
    /**
     *  内部的にキャッシュしている各種リソースを解放する。
     *  <p/>
     *  この実装では、レコードの読み込みに使用したステートメントオブジェクトが
     *  {@code null}でない場合、解放する。
     *
     *  @param ctx 実行コンテキスト
     */
    public synchronized void close(ExecutionContext ctx) {
        if (statement != null) {
            statement.close();
        }
    }

    /**
     * ステートメントを再実行し、最新の情報を取得し直す。
     * <p/>
     * 取得した参照結果をキャッシュする。
     *
     * @param ctx 実行コンテキスト
     */
    public synchronized void reopen(ExecutionContext ctx) {
        readRecords();
    }

    /**
     * 参照結果のイテレータをキャッシュする。
     *
     * @throws IllegalStateException SQLステートメントが{@code null}の場合
     */
    @SuppressWarnings("unchecked")
    private void readRecords() {
        if (listener != null) {
            listener.beforeReadRecords();
        }

        if (statement != null) {
            records = statement.executeQuery().iterator();
        } else if (parameterizedSqlPStatement != null) {
            if (condition instanceof Map<?, ?>) {
                records = parameterizedSqlPStatement.executeQueryByMap(
                        (Map<String, ?>) condition).iterator();
            } else {
                records = parameterizedSqlPStatement.executeQueryByObject(condition).iterator();
            }
        } else {
            throw new IllegalStateException("Statement was not set.");
        }

        if (listener != null) {
            listener.afterReadRecords();
        }
    }

    /**
     * テーブルを参照するSQLステートメントを設定する。
     *
     * @param statement SQLステートメント
     * @return このオブジェクト自体
     */
    @Published
    public synchronized DatabaseRecordReader setStatement(SqlPStatement statement) {
        this.statement = statement;
        return this;
    }

    /**
     * テーブルを参照するSQLステートメント及び条件を設定する。
     *
     * @param parameterizedSqlPStatement SQLステートメント
     * @param condition ステートメントのINパラメータに設定する値を持つオブジェクト
     * @return このオブジェクト自体
     */
    @Published
    public DatabaseRecordReader setStatement(
            ParameterizedSqlPStatement parameterizedSqlPStatement, Object condition) {
        this.parameterizedSqlPStatement = parameterizedSqlPStatement;
        this.condition = condition;
        return this;
    }

    /**
     * データベースレコードリスナを設定する。
     * <p/>
     * リスナに定義されたコールバック処理は、それぞれ以下のタイミングで実行される。<br/>
     * ・処理対象レコードをキャッシュするためのデータベースアクセス前<br/>
     * ・データベースから取得した処理対象レコードをキャッシュした後
     * <p/>
     * 本リーダにリスナを設定することで、
     * 処理対象レコードを取得する前に別トランザクションでレコードを更新するといったことが実現できる。
     *
     * @param listener データベースレコードリスナ
     * @return このオブジェクト自体
     */
    @Published
    public DatabaseRecordReader setListener(DatabaseRecordListener listener) {
        this.listener = listener;
        return this;
    }
}
