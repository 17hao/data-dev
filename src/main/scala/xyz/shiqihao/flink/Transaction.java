package xyz.shiqihao.flink;

import java.util.Objects;

public class Transaction {
    private long accountId;
    private long timestamp;
    private int amount;

    public long getAccountId() {
        return accountId;
    }

    public void setAccountId(long accountId) {
        this.accountId = accountId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public Transaction() {
    }

    public Transaction(long accountId, long timestamp, int amount) {
        this.accountId = accountId;
        this.timestamp = timestamp;
        this.amount = amount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return accountId == that.accountId && timestamp == that.timestamp && amount == that.amount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(accountId, timestamp, amount);
    }
}
