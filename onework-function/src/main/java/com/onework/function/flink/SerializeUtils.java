package com.onework.function.flink;

import java.io.*;

public class SerializeUtils {
    public static byte[] redisSerialize(Object object) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            return baos.toByteArray();
        } catch (IOException ignored) {
        }

        return new byte[0];
    }

    public static Object redisUnSerialize(byte[] bytes) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException ignored) {
        }
        return new Object();
    }
}
