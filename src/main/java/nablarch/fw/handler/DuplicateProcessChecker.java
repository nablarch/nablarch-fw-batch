package nablarch.fw.handler;

/**
 * プロセスの多重起動を防止するためのチェック処理を行うインタフェース。
 *
 * @author Hisaaki Shioiri
 */
public interface DuplicateProcessChecker {

    /**
     * プロセスの2重起動チェックとアクティブ化を行う。
     *
     * プロセスが既に実行中の場合には、{@link AlreadyProcessRunningException}を送出する。
     *
     * @param processIdentifier プロセスを識別する値
     * @throws AlreadyProcessRunningException プロセスの多重起動の場合
     */
    void checkAndActive(String processIdentifier) throws AlreadyProcessRunningException;

    /**
     * プロセスの非アクティブ化を行う。
     *
     * @param processIdentifier プロセスを識別する値
     */
    void inactive(String processIdentifier);

}
