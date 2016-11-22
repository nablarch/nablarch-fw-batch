package nablarch.fw.handler;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * ハンドラテスト用バッチリクエスト
 */
@Entity
@Table(name = "HANDLER_BATCH_REQUEST")
public class HandlerBatchRequest {
    
    public HandlerBatchRequest() {
    }

    public HandlerBatchRequest(String requestId, String requestName, String processHaltFlg,
            String processActiveFlg, String serviceAvailable, Long resumePoint) {
        this.requestId = requestId;
        this.requestName = requestName;
        this.processHaltFlg = processHaltFlg;
        this.processActiveFlg = processActiveFlg;
        this.serviceAvailable = serviceAvailable;
        this.resumePoint = resumePoint;
    }

    @Id
    @Column(name = "REQUEST_ID", length = 10, nullable = false)
    public String requestId;

    @Column(name = "REQUEST_NAME", length = 100, nullable = false)
    public String requestName;

    @Column(name = "PROCESS_HALT_FLG", length = 1, nullable = false)
    public String processHaltFlg;

    @Column(name = "PROCESS_ACTIVE_FLG", length = 1, nullable = false)
    public String processActiveFlg;

    @Column(name = "SERVICE_AVAILABLE", length = 1, nullable = false)
    public String serviceAvailable;
    
    @Column(name = "RESUME_POINT", length = 5, nullable = false)
    public Long resumePoint;
}
