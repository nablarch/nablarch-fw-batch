package nablarch.fw.handler;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nablarch.core.db.connection.AppDbConnection;
import nablarch.core.db.statement.SqlPStatement;
import nablarch.core.db.transaction.SimpleDbTransactionExecutor;
import nablarch.core.db.transaction.SimpleDbTransactionManager;
import nablarch.core.repository.initialization.Initializable;

/**
 * データベースのテーブルを用いてプロセスの多重起動防止を行う{@link DuplicateProcessChecker}の実装クラス。
 * <p/>
 * ２重起動チェックは、データベースのテーブルを用いて行う。
 * データベースのテーブルには、予めプロセスを識別するための値を設定しておく必要がある。
 * プロセスを識別する値が設定さていない場合は、２重起動チェックが正しく行えないが、
 * ２重起動の可能性もあるため２重起動であることを示す例外を送出する。
 * <p/>
 * ２重起動チェック用テーブルレイアウト例を以下に示す。
 * <pre>
 * ----------------------------------   ------------------------------------------------------
 * カラム名                             説明
 * ----------------------------------   ------------------------------------------------------
 * プロセス識別子                       プロセスを一意に識別するための値を格納する。
 *
 *                                      例えばジョブIDなど
 *
 * プロセスアクティブフラグ             プロセスの現在の状態を格納する。
 *
 *                                      * 0:非アクティブ(実行されていない状態)
 *                                      * 1:アクティブ(実行されている状態)
 * ----------------------------------   ------------------------------------------------------
 * </pre>
 *
 * @author Hisaaki Shioiri
 */
public class BasicDuplicateProcessChecker implements DuplicateProcessChecker, Initializable {

    /** データベーストランザクションマネージャ */
    private SimpleDbTransactionManager dbTransactionManager;

    /** テーブル名 */
    private String tableName;

    /** リクエストIDの物理カラム名 */
    private String processIdentifierColumnName;

    /** アクティブフラグの物理カラム名 */
    private String processActiveFlgColumnName;

    /** チェック用SQL文 */
    private String activeSql;

    /** アクティブフラグを非アクティブに更新するSQL文 */
    private String inactiveSql;

    /** ２重起動を許可するリクエストID */
    private List<String> permitProcessIdentifier = Collections.emptyList();

    /**
     * ２重起動チェック用のSQL文を構築する。
     */
    @Override
    public void initialize() {
        activeSql = buildActiveSql();
        inactiveSql = buildInactiveSql();
    }

    /**
     * プロセスをアクティブ化するSQLを構築する。
     *
     * @return 構築したSQL文
     */
    private String buildActiveSql() {
        final Map<String, String> replace = new HashMap<String, String>();
        replace.put("$tableName$", tableName);
        replace.put("$processIdentifierColumnName$", processIdentifierColumnName);
        replace.put("$processActiveFlgColumnName$", processActiveFlgColumnName);

        // アクティブ化のSQL
        final String activeSqlTemplate = "update $tableName$ "
                + "set $processActiveFlgColumnName$ = '1' "
                + "where $processIdentifierColumnName$ = ? "
                + "and $processActiveFlgColumnName$ = '0'";

        final StringBuffer sql = new StringBuffer(256);
        final Matcher matcher = Pattern.compile("\\$[a-zA-Z]+\\$")
                .matcher(activeSqlTemplate);
        while (matcher.find()) {
            matcher.appendReplacement(sql, replace.get(matcher.group(0)));
        }
        matcher.appendTail(sql);
        return sql.toString();
    }

    /**
     * プロセスを非アクティブ化するSQL文を構築する。
     *
     * @return 構築したSQL文
     */
    private String buildInactiveSql() {

        final Map<String, String> replace = new HashMap<String, String>();
        replace.put("$tableName$", tableName);
        replace.put("$processIdentifierColumnName$", processIdentifierColumnName);
        replace.put("$processActiveFlgColumnName$", processActiveFlgColumnName);

        // 非アクティブ化のSQL
        final String inactiveSqlTemplate = "update $tableName$ "
                + "set $processActiveFlgColumnName$ = '0' "
                + "where $processIdentifierColumnName$ = ? "
                + "and $processActiveFlgColumnName$ = '1'";

        final StringBuffer sql = new StringBuffer(256);
        final Matcher matcher = Pattern.compile("\\$[a-zA-Z]+\\$")
                .matcher(inactiveSqlTemplate);
        while (matcher.find()) {
            matcher.appendReplacement(sql, replace.get(matcher.group(0)));
        }
        matcher.appendTail(sql);
        return sql.toString();
    }

    /**
     * データベーストランザクションマネージャを設定する。
     *
     * @param dbTransactionManager データベーストランザクションマネージャ
     */
    public void setDbTransactionManager(
            final SimpleDbTransactionManager dbTransactionManager) {
        this.dbTransactionManager = dbTransactionManager;
    }

    /**
     * テーブル名を設定する。
     *
     * @param tableName テーブル名
     */
    public void setTableName(final String tableName) {
        this.tableName = tableName;
    }

    /**
     * プロセスを特定するための識別子が格納されるカラムの物理名を設定する。
     *
     * @param processIdentifierColumnName プロセスを識別する値のカラム物理名
     */
    public void setProcessIdentifierColumnName(final String processIdentifierColumnName) {
        this.processIdentifierColumnName = processIdentifierColumnName;
    }

    /**
     * プロセスが起動中であることを示すフラグが格納されるカラムの物理名を設定する。
     *
     * @param processActiveFlgColumnName プロセス起動中フラグのカラム物理名
     */
    public void setProcessActiveFlgColumnName(final String processActiveFlgColumnName) {
        this.processActiveFlgColumnName = processActiveFlgColumnName;
    }

    /**
     * ２重起動を許可するプロセスの識別子リストを設定する。
     *
     * @param permitProcessIdentifier 許可リクエストIDのリスト
     */
    public void setPermitProcessIdentifier(String[] permitProcessIdentifier) {
        this.permitProcessIdentifier = Arrays.asList(permitProcessIdentifier);
    }

    @Override
    public void checkAndActive(final String processIdentifier) throws AlreadyProcessRunningException {
        if (isPermitProcess(processIdentifier)) {
            return;
        }
        if (isDuplicateProcess(processIdentifier)) {
            throw new AlreadyProcessRunningException(
                    "same process already running. process = [" + processIdentifier + ']');
        }
    }

    /**
     * ２重起動チェックと現在のプロセスのアクティブ化を行う。
     *
     * @param processIdentifier チェック対象のリクエストID
     * @return ２重起動の場合はtrue
     */
    private boolean isDuplicateProcess(final String processIdentifier) {
        return new SimpleDbTransactionExecutor<Boolean>(dbTransactionManager) {
            @Override
            public Boolean execute(final AppDbConnection connection) {
                final SqlPStatement statement = connection.prepareStatement(activeSql);
                statement.setString(1, processIdentifier);
                return statement.executeUpdate() != 1;
            }
        }
                .doTransaction();
    }

    @Override
    public void inactive(final String processIdentifier) {
        if (isPermitProcess(processIdentifier)) {
            return;
        }

        new SimpleDbTransactionExecutor<Void>(dbTransactionManager) {
            @Override
            public Void execute(final AppDbConnection connection) {
                final SqlPStatement statement = connection.prepareStatement(
                        inactiveSql);
                statement.setString(1, processIdentifier);
                statement.executeUpdate();
                return null;
            }
        }
        .doTransaction();
    }

    /**
     * ２重起動が許可されたプロセスか否か
     * @param processIdentifier プロセスを識別する値
     * @return ２重起動が許可されている場合は{@code true}
     */
    private boolean isPermitProcess(final String processIdentifier) {
        return permitProcessIdentifier.contains(processIdentifier);
    }
}

