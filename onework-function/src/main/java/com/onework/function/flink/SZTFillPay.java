package com.onework.function.flink;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SZTFillPay extends ScalarFunction {
    private transient Jedis jedis;
    private transient Map<String, Object> state;
    private String redisHost;
    private Integer redisDb;
    private transient long redisSessionTs;

    @Override
    public void open(FunctionContext context) {
        redisHost = context.getJobParameter("stateRedisHost", "");
        String redisDbStr = context.getJobParameter("stateRedisDb", "");
        if (!redisHost.equals("") && !redisDbStr.equals("")) {
            jedis = new Jedis(redisHost, 6379);
            redisDb = Integer.parseInt(redisDbStr);
            jedis.select(redisDb);
            redisSessionTs = System.currentTimeMillis();
        } else {
            throw new JedisException("No redis configuration information");
        }

        state = new HashMap<>();
    }

    @Override
    public void close() {
        if (jedis != null) jedis.close();
    }

    private void redisConnect() {
        if (jedis != null) jedis.close();
        jedis = new Jedis(redisHost, 6379);
        jedis.select(redisDb);
    }

    public String eval(Integer txnType, Long txnDate, String cardNo, String busNo) {
        if (jedis == null || System.currentTimeMillis() - redisSessionTs > 7200000) {
            redisConnect();
            redisSessionTs = System.currentTimeMillis();
        }

        String payStateKey = "getOnPayState".concat(cardNo);
        if (txnType == 1) {
            PayState payState = new PayState(Integer.parseInt(new DateTime(txnDate).toString("yyyyMMdd")), busNo);
            state.put(payStateKey, payState);
            jedis.set(payStateKey.getBytes(), SerializeUtils.redisSerialize(payState));
        } else if (txnType == 3) {
            PayState payState = (PayState) state.get(payStateKey);
            if (payState == null && jedis.exists(payStateKey.getBytes())) {
                payState = (PayState) SerializeUtils.redisUnSerialize(jedis.get(payStateKey.getBytes()));
            }
            if (payState == null || payState.getTxnDate() < Integer.parseInt(new DateTime(txnDate).toString("yyyyMMdd")))
                return "";
            else return payState.getBusNo();
        }

        return busNo;
    }

    private static class PayState implements Serializable {
        private static final long serialVersionUID = 1;
        private int txnDate;
        private String busNo;

        public PayState(int txnDate, String busNo) {
            this.txnDate = txnDate;
            this.busNo = busNo;
        }

        public int getTxnDate() {
            return txnDate;
        }

        public void setTxnDate(int txnDate) {
            this.txnDate = txnDate;
        }

        public String getBusNo() {
            return busNo;
        }

        public void setBusNo(String busNo) {
            this.busNo = busNo;
        }
    }
}
