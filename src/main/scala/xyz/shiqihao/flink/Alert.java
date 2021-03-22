package xyz.shiqihao.flink;

import java.util.Objects;

public class Alert {
    private long id;

    private String tnxDetail;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTnxDetail() {
        return tnxDetail;
    }

    public void setTnxDetail(String tnxDetail) {
        this.tnxDetail = tnxDetail;
    }

    public Alert() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return id == alert.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Alert{" +
                "id=" + id +
                ", tnxDetail='" + tnxDetail + '\'' +
                '}';
    }
}
