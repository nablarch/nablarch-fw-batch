package nablarch.fw.action;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * テストバッチリクエスト
 *
 */
@Entity
@Table(name = "BATCH_REQUEST")
public class TestBatchRequest {

    public TestBatchRequest() {
    }

    public TestBatchRequest(String requestId, Integer resumePoint) {
        this.requestId = requestId;
        this.resumePoint = resumePoint;
    }

    @Id
    @Column(name = "REQUEST_ID", length = 8, nullable = false)
    public String requestId;

    @Column(name = "RESUME_POINT", length = 5, nullable = false)
    public Integer resumePoint;
}
