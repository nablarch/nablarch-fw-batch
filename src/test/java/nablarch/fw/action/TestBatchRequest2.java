package nablarch.fw.action;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * テストバッチリクエスト
 *
 */
@Entity
@Table(name = "BATCH_REQUEST")
public class TestBatchRequest2 {

    public TestBatchRequest2() {
    };

    public TestBatchRequest2(String requestId, String processHaltFlg,String activeFlg) {
        this.requestId = requestId;
        this.processHaltFlg = processHaltFlg;
        this.activeFlg = activeFlg;
    }

    @Id
    @Column(name = "REQUEST_ID", length = 10)
    public String requestId;

    @Column(name = "PROCESS_HALT_FLG", length = 1)
    public String processHaltFlg;

    @Column(name = "ACTIVE_FLG", length = 1)
    public String activeFlg;
}
