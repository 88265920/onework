package com.onework.function.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.shaded.org.joda.time.DateTime;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.onework.function.flink.SerializeUtils.redisSerialize;
import static com.onework.function.flink.SerializeUtils.redisUnSerialize;

@FunctionHint(output = @DataTypeHint("ROW<id VARCHAR(80), plan_start_time BIGINT, actual_start_time BIGINT, check_interval INT, abnormal_departure INT, prev_log VARCHAR(300)>"))
public class BusAbnormalDeparture extends TableFunction<Row> {
    private transient Jedis jedis;
    private transient Map<String, Object> state;
    private transient Map<String, List<DepartureState>> departureStatesMap;
    private String redisHost;
    private Integer redisDb;
    private transient long redisSessionTs;
    private transient long initialTs;

    private void shellSort4DepartureState(byte[] lineDepartureStateKey, Pipeline pipeline, List<DepartureState> departureStates) {
        // 控制间距: 间距逐渐减小,直到为1
        for (int gap = departureStates.size() / 2; gap > 0; gap /= 2) {
            // 扫描每个子数组
            for (int i = 0; i < gap; i++) {
                // 对每个字数组,扫描无序区: 注意增量
                // departureStates[i]是初始有序区
                for (int j = i + gap; j < departureStates.size(); j += gap) {
                    // 无序区首元素小于有序区尾元素,说明需要调整
                    if (departureStates.get(j).getActualStartTime() < departureStates.get(j - gap).getActualStartTime()) {
                        DepartureState departureState = departureStates.get(j);
                        int k = j - gap;
                        // 从有序区尾向前搜索查找适当的位置
                        while (k >= 0 && departureStates.get(k).getActualStartTime() > departureState.getActualStartTime()) {
                            DepartureState tempDepartureState = departureStates.get(k);
                            int idx = k + gap;
                            tempDepartureState.setIdx(idx);
                            departureStates.set(idx, tempDepartureState);
                            pipeline.hset(lineDepartureStateKey, String.valueOf(idx).getBytes(), redisSerialize(tempDepartureState));
                            k -= gap;
                        }
                        int idx = k + gap;
                        departureState.setIdx(idx);
                        departureStates.set(idx, departureState);
                        pipeline.hset(lineDepartureStateKey, String.valueOf(idx).getBytes(), redisSerialize(departureState));
                    }
                }
            }
        }
    }

    @Override
    public void open(FunctionContext context) {
        redisHost = context.getJobParameter("stateRedisHost", "");
        String redisDbParam = context.getJobParameter("stateRedisDb", "");
        if (!redisHost.equals("") && !redisDbParam.equals("")) {
            jedis = new Jedis(redisHost, 6379);
            redisDb = Integer.parseInt(redisDbParam);
            jedis.select(redisDb);
            redisSessionTs = System.currentTimeMillis();
        } else {
            throw new JedisException("No redis configuration information");
        }

        state = new HashMap<>();
        departureStatesMap = new ConcurrentHashMap<>();
        initialTs = System.currentTimeMillis() + 600000;
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

    public void eval(String id, String lineName, Integer direction, Long planStartTime, Long actualDeparture,
                     Integer checkInterval) {
        if (StringUtils.isEmpty(id) || actualDeparture == null || StringUtils.isEmpty(lineName)) return;

        if (jedis == null || System.currentTimeMillis() - redisSessionTs > 7200000) {
            redisConnect();
            redisSessionTs = System.currentTimeMillis();
        }

        String lineDepartureStateKey = lineName.concat(direction.toString());
        byte[] redisLineDepartureStateKey = "departure4line".concat(lineDepartureStateKey).getBytes();
        Long latestActualDeparture = actualDeparture;
        List<DepartureState> departureStates = new ArrayList<>(
                departureStatesMap.computeIfAbsent(lineDepartureStateKey, k -> {
                    if (jedis.exists(redisLineDepartureStateKey).equals(Boolean.FALSE)) return new ArrayList<>();
                    Map<byte[], byte[]> redisDepartureStates = jedis.hgetAll(redisLineDepartureStateKey);

                    List<DepartureState> resultDepartureStates = new ArrayList<>();
                    int i = 0;
                    for (byte[] bytes : redisDepartureStates.values()) {
                        DepartureState departureState = (DepartureState) redisUnSerialize(bytes);
                        departureState.setIdx(i);
                        resultDepartureStates.add(departureState);
                        i++;
                    }
                    Pipeline pipeline = jedis.pipelined();
                    shellSort4DepartureState(redisLineDepartureStateKey, pipeline, resultDepartureStates);
                    pipeline.sync();

                    return resultDepartureStates;
                }));

        String departureStateKey = "departure".concat(id);
        byte[] redisDepartureStateKey = departureStateKey.getBytes();

        if (!departureStates.isEmpty()) {
            latestActualDeparture = departureStates.get(departureStates.size() - 1).getActualStartTime();
            if (latestActualDeparture - actualDeparture > 86400000) return;

            String lineRunDateKey = "lineRunDate".concat(lineDepartureStateKey);
            Long lineRunDate = (Long) state.get(lineRunDateKey);
            if (lineRunDate == null) {
                state.put(lineRunDateKey, new DateTime(actualDeparture).withTimeAtStartOfDay().getMillis());
            } else if (actualDeparture - lineRunDate > 86400000) {
                state.put(lineRunDateKey, new DateTime(actualDeparture).withTimeAtStartOfDay().getMillis());
                departureStatesMap.remove(lineDepartureStateKey);
                jedis.del(redisLineDepartureStateKey);
                departureStates.clear();
                state.remove(departureStateKey);
                jedis.del(redisDepartureStateKey);
            }
        }

        DepartureState departureState = (DepartureState) state.get(departureStateKey);
        if (departureState == null && jedis.exists(redisDepartureStateKey)) {
            departureState = (DepartureState) redisUnSerialize(jedis.get(redisDepartureStateKey));
        }

        if (departureState == null || Math.abs(actualDeparture - departureState.getActualStartTime()) > 2000) {
            int departureStateIdx;
            if (departureState != null && (departureStateIdx = departureState.getIdx()) < departureStates.size()) {
                departureState = new DepartureState(id, planStartTime, actualDeparture, checkInterval);
                departureState.setIdx(departureStateIdx);
                departureStates.set(departureStateIdx, departureState);
            } else {
                departureState = new DepartureState(id, planStartTime, actualDeparture, checkInterval);
                departureStateIdx = departureStates.size();
                departureState.setIdx(departureStateIdx);
                departureStates.add(departureState);
            }

            Pipeline pipeline = jedis.pipelined();
            pipeline.hset(redisLineDepartureStateKey, String.valueOf(departureStateIdx).getBytes(), redisSerialize(departureState));
            if (actualDeparture < latestActualDeparture) {
                shellSort4DepartureState(redisLineDepartureStateKey, pipeline, departureStates);
            }
            pipeline.sync();
            if (departureState.getIdx() > 1) {
                DepartureState p = departureStates.get(departureState.getIdx() - 1);
                if ((actualDeparture - p.getActualStartTime()) / 60000 > (checkInterval / 60)) {
                    int abnormalDeparture = (int) ((actualDeparture - p.getActualStartTime()) / 1000);
                    if (actualDeparture > initialTs)
                        collect(Row.of(departureState.getId(), planStartTime, actualDeparture, checkInterval,
                                abnormalDeparture, p.toString()));
                }
            }
            if (departureState.getIdx() < departureStates.size() - 1) {
                DepartureState n = departureStates.get(departureState.getIdx() + 1);
                if ((n.getActualStartTime() - actualDeparture) / 60000 > (n.getCheckInterval() / 60)) {
                    int abnormalDeparture = (int) ((n.getActualStartTime() - actualDeparture) / 1000);
                    if (n.getActualStartTime() > initialTs)
                        collect(Row.of(n.getId(), n.getPlanStartTime(), n.getActualStartTime(), n.getCheckInterval(),
                                abnormalDeparture, departureState.toString()));
                }
            }
            departureStatesMap.put(lineDepartureStateKey, departureStates);
            state.put(departureStateKey, departureState);
            jedis.set(redisDepartureStateKey, redisSerialize(departureState));
        }
    }

    private static class DepartureState implements Serializable {
        private static final long serialVersionUID = 1;
        private int idx;
        private String id;
        private long planStartTime;
        private long actualStartTime;
        private int checkInterval;

        public DepartureState(String id, long planStartTime, long actualStartTime,
                              int checkInterval) {
            this.id = id;
            this.planStartTime = planStartTime;
            this.actualStartTime = actualStartTime;
            this.checkInterval = checkInterval;
        }

        @Override
        public String toString() {
            return "{id='" + id + '\'' +
                    ", actualStartTime=" + actualStartTime +
                    '}';
        }

        public int getIdx() {
            return idx;
        }

        public void setIdx(int idx) {
            this.idx = idx;
        }

        public String getId() {
            return id;
        }

        public long getPlanStartTime() {
            return planStartTime;
        }

        public long getActualStartTime() {
            return actualStartTime;
        }

        public int getCheckInterval() {
            return checkInterval;
        }
    }
}
