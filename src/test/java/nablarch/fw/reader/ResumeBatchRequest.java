package nablarch.fw.reader;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * RESUME_BATCH_REQUEST
 */
@Entity
@Table(name = "RESUME_BATCH_REQUEST")
public class ResumeBatchRequest {

    public ResumeBatchRequest() {
    }

    public ResumeBatchRequest(String requestId, Long resumePoint) {
        this.requestId = requestId;
        this.resumePoint = resumePoint;
    }

    @Id
    @Column(name = "REQUEST_ID", length = 8, nullable = false)
    public String requestId;

    @Column(name = "RESUME_POINT", length = 5)
    public Long resumePoint;
}