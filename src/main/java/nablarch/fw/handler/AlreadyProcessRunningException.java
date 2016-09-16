package nablarch.fw.handler;

/**
 * 既にプロセスが実行中であることを示す例外クラス。
 *
 * @author Hisaaki Shioiri
 */
public class AlreadyProcessRunningException extends Exception {

    /**
     * 例外を生成する。
     */
    public AlreadyProcessRunningException() {
    }

    /**
     * 例外を生成する。
     *
     * @param message 例外メッセージ
     */
    public AlreadyProcessRunningException(String message) {
        super(message);
    }
}
