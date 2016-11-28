package nablarch.fw.reader;

import nablarch.core.util.annotation.Published;

/**
 * {@link DatabaseRecordReader}で処理対象レコードをキャッシュするためのデータベースアクセス前後に
 * コールバックされるメソッドを定義するインタフェース。
 *
 * @author Naoki Yamamoto
 */
@Published
public interface DatabaseRecordListener {

    /**
     * 処理対象レコードをキャッシュするためのデータベースアクセス前に呼び出される。
     */
    void beforeReadRecords();

    /**
     * データベースから取得した処理対象レコードをキャッシュした後で呼び出される。
     */
    void afterReadRecords();
}
