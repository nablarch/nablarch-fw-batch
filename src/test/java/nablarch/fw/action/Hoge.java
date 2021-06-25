package nablarch.fw.action;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "HOGE")
public class Hoge {

    public Hoge() {
    };

    public Hoge(String id,String fuga) {
        this.id = id;
        this.fuga = fuga;
    }
    @Id
    @Column(name = "ID", length = 1)
    public String id;

    @Column(name = "FUGA", length = 1)
    public String fuga;
}
