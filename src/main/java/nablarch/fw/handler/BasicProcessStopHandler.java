package nablarch.fw.handler;

import nablarch.core.ThreadContext;
import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.initialization.Initializable;
import nablarch.fw.ExecutionContext;
import nablarch.fw.Result;

/**
 * 処理中のプロセスを停止するためのハンドラ。
 * <p/>
 * 本ハンドラは、{@link LoopHandler}や{@link ProcessResidentHandler}の後続ハンドラに設定することにより、
 * 処理中に安全にプロセスを停止することが可能となる。
 * <p/>
 * なお、プロセスを停止するために{@link ProcessStop}を送出するため、障害通知ログが出力されプロセスは異常終了する。
 * 異常終了する際に終了コードは、{@link #setExitCode(int)}によって設定することが出来る。
 * 終了コードの設定を省略した場合のデフォルト動作として終了コードは1となる。
 * <b>また、未コミットのトランザクションは全てロールバックされることに注意すること。</b>
 * <p/>
 * ※処理を異常終了するかどうかは、前段に設定されたハンドラによって決定される。
 * <p/>
 * 処理を停止するか否かのチェックは、リクエストテーブルにて行う。
 * 本ハンドラが使用するリクエストテーブルの定義情報を下記に示す。
 * <p/>
 * <pre>
 * -----------------------------+----------------------------------------------------------
 * カラム名                     | 説明
 * -----------------------------+----------------------------------------------------------
 * リクエストID                 | プロセスを特定するためのリクエストID
 * 処理停止フラグ               | 処理を停止するか否かの情報
 *                              | 本フラグの値が'1'の場合に処理を停止する。
 *                              |
 *                              | <b>本フラグの値は、自動的に'0'には変更されないため再実行する際には、
 *                              | 手動で'0'に変更する必要がある。</b>
 * -----------------------------+----------------------------------------------------------
 * </pre>
 *
 * @author hisaaki sioiri
 */
public class BasicProcessStopHandler implements ProcessStopHandler, Initializable {

    /** プロセス停止を示す値 */
    private static final String PROCESS_STOP = "1";

    /** プロセスを停止するか否かをチェックする間隔 */
    private int checkInterval = 1;

    /** データベーストランザクションマネージャ */
    private SimpleDbTransactionManager dbTransactionManager;

    /** チェック対象のテーブル名 */
    private String tableName;

    /** リクエストIDを示すカラム名 */
    private String requestIdColumnName;

    /** 処理停止フラグを示すカラム名 */
    private String processHaltColumnName;

    /** プロセス停止可否をチェックするためのSQL文 */
    private String query;

    /** 終了コード */
    private int exitCode = 1;

    /** 現在の処理件数 */
    private final ThreadLocal<Integer> count = new ThreadLocal<Integer>() {
        @Override
        protected Integer initialValue() {
            return 0;
        }
    };

    /**
     * {@inheritDoc}
     * <p/>
     * 処理停止チェックを行う。
     */
    public Object handle(Object o, ExecutionContext context) {
        int nowCount = count.get();
        if (nowCount++ % checkInterval == 0) {
            if (isProcessStop(ThreadContext.getRequestId())) {
                throw new ProcessStop(exitCode);
            }
            nowCount = 1;
        }
        count.set(nowCount);
        return context.handleNext(o);
    }

    /**
     * プロセス停止可否を判定する。
     *
     * @param requestId リクエストID
     * @return プロセスを停止する必要がある場合はtrue
     */
    @Override
    public boolean isProcessStop(final String requestId) {
        return new SimpleDbTransactionExecutor<Boolean>(
                dbTransactionManager) {
            @Override
            public Boolean execute(AppDbConnection connection) {
                SqlPStatement statement = connection.prepareStatement(query);
                statement.setString(1, requestId);
                statement.setString(2, PROCESS_STOP);
                SqlResultSet retrieve = statement.retrieve();
                // データが存在する場合は、プロセスを停止することを意味する。
                return !retrieve.isEmpty();
            }
        }
                .doTransaction();
    }

    /**
     * チェック間隔（{@link #handle(Object, ExecutionContext)}が
     * 何回呼び出されるごとに停止フラグを確認するか？）を設定する。
     * <p/>
     *
     * @param checkInterval チェック間隔(0以下の値が設定された場合は1)
     */
    @Override
    public void setCheckInterval(int checkInterval) {
        this.checkInterval = checkInterval <= 0 ? 1 : checkInterval;
    }

    /**
     * トランザクションマネージャ({@link SimpleDbTransactionManager})を設定する。
     * <p/>
     * 本ハンドラは、ここで設定されたトランザクションマネージャを使用してデータベースアクセスを行う。
     *
     * @param dbTransactionManager トランザクションマネージャ
     */
    public void setDbTransactionManager(
            SimpleDbTransactionManager dbTransactionManager) {
        this.dbTransactionManager = dbTransactionManager;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * プロセス停止可否をチェックするためのSELECT文を構築する。
     */
    @Override
    public void initialize() {
        String query = "SELECT " + requestIdColumnName
                + " FROM " + tableName
                + " WHERE " + requestIdColumnName + " = ?"
                + " AND " + processHaltColumnName + " = ?";
        this.query = query;
    }

    /**
     * プロセス停止可否のチェックを行うテーブルの物理名を設定する。
     *
     * @param tableName テーブル物理名
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * プロセスを特定するためのリクエストIDが格納されるカラムの物理名を設定する。
     *
     * @param requestIdColumnName リクエストIDカラムの物理名
     */
    public void setRequestIdColumnName(String requestIdColumnName) {
        this.requestIdColumnName = requestIdColumnName;
    }

    /**
     * プロセス停止フラグが格納されるカラムの物理名を設定する。
     *
     * @param processHaltColumnName プロセス停止カラムフラグの物理名
     */
    public void setProcessHaltColumnName(String processHaltColumnName) {
        this.processHaltColumnName = processHaltColumnName;
    }

    /**
     * 終了コードを設定する。
     * <p/>
     * 終了コードの設定がない場合、デフォルトで{@link Result.InternalError#STATUS_CODE}が使用される。
     *
     * @param exitCode 終了コード
     */
    public void setExitCode(int exitCode) {
        if (exitCode <= 0 || exitCode >= 256) {
            throw new IllegalArgumentException("exit code was invalid range. "
                    + "please set it in the range of 255 from 1. "
                    + "specified value was:" + exitCode);
        }
        this.exitCode = exitCode;
    }

}

