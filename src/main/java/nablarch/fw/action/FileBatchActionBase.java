package nablarch.fw.action;

import nablarch.core.util.annotation.Published;
import nablarch.fw.DataReaderFactory;
import nablarch.fw.reader.ValidatableFileDataReader;

/**
 * ファイル入力を行う業務処理が継承すべき抽象基底クラス。
 * 
 * @param <D> データリーダが読み込むデータの型
 * @author Masato Inoue
 */
@Published
public abstract class FileBatchActionBase<D> extends BatchActionBase<D> implements DataReaderFactory<D> {
    
    // ----------------------------------------------- required to implement
    /**
     * 入力ファイルのファイル名を返す。
     * 
     * @return 入力ファイルのファイル名
     * @see nablarch.core.util.FilePathSetting
     */
    public abstract String getDataFileName();
    
    /**
     * 入力ファイルを読み込む際に使用するフォーマット定義ファイルのファイル名を返す。
     * 
     * @return フォーマット定義ファイルのファイル名
     * @see nablarch.core.util.FilePathSetting
     */
    public abstract String getFormatFileName();
    
    
    // -------------------------------------------------- override if you need
    /**
     * 入力ファイル配置先の論理名を返す。
     * <p/>
     * デフォルト実装では"input"を返す。
     * デフォルトの入力ファイル配置先以外から入力ファイルを取得する場合は、
     * このメソッドをオーバーライドする。
     *
     * @return 入力ファイル配置先の論理名
     */
    public String getDataFileDirName() {
        return "input";
    }
    
    /**
     * フォーマット定義ファイル配置先の論理名を返す。
     * <p/>
     * デフォルト実装では"format"を返す。
     * デフォルトのフォーマット定義ファイル配置先以外からフォーマット定義ファイルを取得する場合は、
     * このメソッドをオーバーライドする。
     * 
     * @return フォーマット定義ファイル配置先の論理名
     */
    public String getFormatFileDirName() {
        return "format";
    }
    
    /**
     * 入力ファイルのバリデーションを実装したオブジェクトを返す。
     * <p/>
     * デフォルト実装ではバリデーションは行われない。
     * 入力ファイルのバリデーションが必要な場合にオーバーライドすること。
     * 
     * @return {@code null}または入力ファイルのバリデーションを実装したオブジェクト
     */
    public ValidatableFileDataReader.FileValidatorAction getValidatorAction() {
        return null;
    }

}
