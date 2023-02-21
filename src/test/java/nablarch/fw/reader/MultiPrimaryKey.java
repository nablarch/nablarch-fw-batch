package nablarch.fw.reader;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * MULTI_PRIMARY_KEY
 */
@Entity
@Table(name = "MULTI_PRIMARY_KEY")
public class MultiPrimaryKey {

    public MultiPrimaryKey() {
    }

    public MultiPrimaryKey(Long id, Long id1, Long id2) {
        this.id = id;
        this.id1 = id1;
        this.id2 = id2;
    }

    @Id
    @Column(name = "ID", length = 10, nullable = false)
    public Long id;

    @Column(name = "ID1", length = 10)
    public Long id1;

    @Column(name = "ID2", length = 10)
    public Long id2;
}