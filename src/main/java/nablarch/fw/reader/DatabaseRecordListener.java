package nablarch.fw.reader;

import nablarch.core.util.annotation.Published;

/**
 * {@link DatabaseRecordReader}のデータベースからのレコード取得前に行う処理を定義するインタフェース。
 *
 * @author Naoki Yamamoto
 */
@Published
public interface DatabaseRecordListener {

    /**
     * データベースからレコードを取得する前に呼び出される。
     */
    void beforeReadRecords();
}
