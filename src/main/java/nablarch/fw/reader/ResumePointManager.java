package nablarch.fw.reader;

import java.util.ArrayList;
import java.util.List;

import nablarch.core.ThreadContext;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.statement.SqlResultSet;
import nablarch.core.repository.SystemRepository;
import nablarch.core.repository.initialization.Initializable;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.util.Builder;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;

/**
 * 実行管理テーブルに格納されている、正常に処理できたポイントの参照・更新を行うクラス。
 * <p>
 * 正常に処理できたポイントは「0」が初期値となる。
 * よって、レジューム機能を使用する場合は、先行ジョブで対象リクエストの正常に処理できたポイントをゼロクリアしておくこと。</br>
 * 正常に処理できたポイントに負数を設定することは許容しない。もし負数が設定された場合は、例外をスローして処理を終了する。
 * </p>
 * <p>
 * 本クラスは、リポジトリで管理されることを想定しており、
 * コンポーネント設定ファイルに設定された実行管理テーブルのスキーマ情報をもとにデータベースアクセスを行う。
 * また、本クラスはリポジトリの機能を用いて初期化することを想定しているので、コンポーネント設定ファイルに初期化の設定を行うこと。
 * </p>
 * <p>
 * レジューム機能は、デフォルトでは全リクエストに対して無効となっている。<br/>
 * 全リクエストに対するレジューム機能を有効にする場合は、resumableプロパティにtrueを設定すること。<br/>
 * また、全リクエストに対するレジューム機能が有効な場合に、特定のリクエストに対するレジューム機能を無効にする場合は、excludingRequestListプロパティに無効にするリクエストIDのリストを設定すること。<br/>
 * （※全リクエストに対してレジューム機能を無効にする場合、実行管理テーブルを用意する必要はない）
 * </p>
 * <p>
 * レジューム機能を使用する場合は、必ずシングルスレッドで本クラスを実行すること。
 * マルチスレッドでの実行を検知した場合は、例外をスローして処理を終了する。
 * </p>
 * @author Masato Inoue
 * @see nablarch.core.repository.initialization.Initializable
 */
@Published(tag = "architect")
public class ResumePointManager implements Initializable {

    /** 本クラスのコンポーネント設定ファイル上の名前 */
    private static final String REPOSITORY_KEY = "resumePointManager";

    /** 何もしないNULLオブジェクト。レジューム機能が有効でない場合に使用する。 */
    private static final ResumePointManager NOP_MANAGER = new ResumePointManager();

    /** データベースリソース名 */
    private String dbTransactionName = TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY;
    
    /** 実行管理テーブルの名前 */
    private String tableName;

    /** リクエストIDの物理カラム名 */
    private String requestIdColumnName;

    /** 正常に処理できたポイントの物理カラム名 */
    private String resumePointColumnName;

    /** 正常に処理できたポイントを読み込むSQL文 */
    private String loadResumePointSql;
    
    /** 正常に処理できたポイントを保存するSQL文 */
    private String saveResumePointSql;

    /** レジューム機能を（全リクエストに対して）有効にするかどうか（デフォルトは無効） */
    private boolean isResumable = false;
    
    /** レジューム機能を無効にするリクエストIDのリスト */
    private List<String> excludingRequestList = new ArrayList<String>();
    
    /**
     * 本クラスのインスタンスを取得する。
     * @return {@link ResumePointManager}のインスタンス
     */
    public static ResumePointManager getInstance() {
        ResumePointManager manager = SystemRepository.get(REPOSITORY_KEY);
        if (manager == null) {
            return NOP_MANAGER;
        }
        return manager;
    }

    /**
     * 初期化時に、コンポーネント設定ファイルで設定されたプロパティの内容をチェックし、
     * 問題がなければレジューム機能で使用するSQLを構築する。
     */
    public void initialize() {
        checkPropertySet("tableName", tableName);
        checkPropertySet("requestIdColumnName", requestIdColumnName);
        checkPropertySet("resumePointColumnName", resumePointColumnName);

        loadResumePointSql = buildLoadResumePointSql();
        saveResumePointSql = buildSaveResumePointSql();
    }
    
    /**
     * プロパティの値が設定されていることを確認する。
     * @param name  プロパティ名
     * @param value プロパティの値
     * @throws IllegalStateException プロパティが設定されていない場合
     */
    protected void checkPropertySet(String name, String value) throws IllegalStateException {
        if (StringUtil.isNullOrEmpty(value)) {
            throw new IllegalStateException(String.format(
                    "[%s] property must be set. class=[%s].", name, getClass().getName()));
        }
    }
    
    /**
     * 実行管理テーブルから正常に処理できたポイントを取得する。
     * <p>
     * レジューム機能が無効に設定されている場合は、固定で0を返却する。
     * </p>
     * <p>
     * 正常に処理できたポイントを取得できなかった場合は、例外をスローする。<br/>
     * また、取得した正常に処理できたポイントが負数の場合も、例外をスローする。
     * </p>
     * @param requestId リクエストID
     * @return 正常に処理できたポイント
     */
    public int loadResumePoint(String requestId) {
        if (!isResumable(requestId)) {
            return 0;
        }
        
        // マルチスレッド実行でないことをチェック
        checkSingleThreadExecution(requestId);

        SqlPStatement statement = DbConnectionContext.getConnection(dbTransactionName)
                .prepareStatement(loadResumePointSql);
        statement.setString(1, requestId);
        SqlResultSet retrieve = statement.retrieve();
        
        if (retrieve.size() != 1) {
            throw new IllegalStateException(String.format(
                    "Couldn't load resume point from the table. "
                            + "sql=[%s], request id=[%s].", 
                            loadResumePointSql, requestId));
        }
        
        int resumePoint = retrieve.get(0).getBigDecimal(resumePointColumnName)
                .intValue();
        
        if (resumePoint < 0) {
            throw new IllegalStateException(String.format(
                    "invalid resume point was stored on the table. resume point must be more than 0."
                            + " resume point=[%s], sql=[%s], request id=[%s].", 
                            resumePoint, loadResumePointSql, requestId));
        }

        return resumePoint;
    }

    /**
     * シングルスレッド実行であることを確認する。
     * マルチスレッドで、レジューム機能を使用しようとした場合、例外をスローする
     * @param requestId リクエストID
     * @throws IllegalStateException マルチスレッド実行の場合
     */
    protected void checkSingleThreadExecution(String requestId) throws IllegalStateException {
        int concurrentNumber = ThreadContext.getConcurrentNumber();
        boolean isMultiThread = concurrentNumber > 1;
        if (isMultiThread) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use resume function in multi thread. " 
                          + "resume function is operated only in single thread. concurrent number=[%s], request id=[%s].",
                            concurrentNumber, requestId));
        }
    }
    
    /**
     * 正常に処理できたポイントを保存する。
     * <p>
     * レジューム機能が無効に設定されている場合は、正常に処理できたポイントの保存処理は行わない。
     * また、正常に処理できたポイントの保存に失敗した場合は、例外をスローする。
     * </p>
     * @param requestId リクエストID
     * @param resumePoint 正常に処理できたポイント
     */
    public void saveResumePoint(String requestId, int resumePoint) {
        if (!isResumable(requestId)) {
            return;
        }
        
        SqlPStatement statement = DbConnectionContext.getConnection(dbTransactionName)
                .prepareStatement(saveResumePointSql);
        statement.setInt(1, resumePoint); // レコード番号を保存する
        statement.setString(2, requestId);
        int updateCount = statement.executeUpdate();

        // 更新件数が１件でない場合、例外をスローする
        if (updateCount != 1) {
            throw new IllegalStateException(String.format(
                    "Couldn't save resume point. "
                            + "sql=[%s], request id=[%s].", 
                            saveResumePointSql, requestId));
        }
    }
    
    /**
     * レジューム機能が有効かどうかを取得する。
     * <p/>
     * <p>
     * 具体的には、以下の条件をすべて満たす場合にレジューム機能が有効だと判断する。
     * <ul>
     * <li>{@link #isResumable}プロパティがtrue</li>
     * <li>引数のリクエストIDが、{@link #excludingRequestList}プロパティに設定されたリストに含まれない</li>
     * </ul>
     * </p>
     * @param requestId リクエストID
     * @return isResumable レジューム機能が有効な場合、true
     */
    protected boolean isResumable(String requestId) {
        return isResumable && !excludingRequestList.contains(requestId);
    }
    

    /**
     * 正常に処理できたポイントを取得するためのSQLを取得する。
     *
     * @return 正常に処理できたポイントを読み込むためのSQL
     */
    protected String buildLoadResumePointSql() {
        return Builder.concat(
                "SELECT ", resumePointColumnName,
                " FROM ", tableName,
                " WHERE ", requestIdColumnName, " = ?");
    }

    /**
     * 正常に処理できたポイントを保存するためのSQLを取得する。
     *
     * @return 正常に処理できたポイントを保存するためのSQL
     */
    protected String buildSaveResumePointSql() {
        return Builder.concat(
                "UPDATE ", tableName,
                " SET ",
                resumePointColumnName, " = ?",
                " WHERE ", requestIdColumnName, " = ?");
    }

    /**
     * 実行管理テーブルのテーブル名を設定する。
     * @param tableName tableName テーブル名
     * @return このオブジェクト自体
     */
    public ResumePointManager setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * リクエストIDの物理カラム名を設定する。
     * @param requestIdColumnName requestIdColumnName リクエストIDの物理カラム名
     * @return このオブジェクト自体
     */
    public ResumePointManager setRequestIdColumnName(String requestIdColumnName) {
        this.requestIdColumnName = requestIdColumnName;
        return this;
    }

    /**
     * 正常に処理できたポイントの物理カラム名を設定する。
     * @param resumePointColumnName resumePointColumnName 正常に処理できたポイントの物理カラム名
     * @return このオブジェクト自体
     */
    public ResumePointManager setResumePointColumnName(String resumePointColumnName) {
        this.resumePointColumnName = resumePointColumnName;
        return this;
    }
    
    /**
     * レジューム機能を有効にするかどうかを設定する。
     * @param isResumable レジューム機能を有効にする場合、trueを設定する
     * @return このオブジェクト自体
     */
    public ResumePointManager setResumable(boolean isResumable) {
        this.isResumable = isResumable;
        return this;
    }
    
    /**
     * レジューム機能を無効にするリクエストIDのリストを設定する。
     * @param excludingRequestList レジューム機能を無効にするリクエストIDのリスト
     * @return このオブジェクト自体
     */
    public ResumePointManager setExcludingRequestList(List<String> excludingRequestList) {
        this.excludingRequestList = excludingRequestList;
        return this;
    }

    /**
     * データベースリソース名を設定する。
     *
     * @param dbTransactionName データベースリソース名
     * @return このオブジェクト自体
     */
    public ResumePointManager setDbTransactionName(String dbTransactionName) {
        this.dbTransactionName = dbTransactionName;
        return this;
    }
}
