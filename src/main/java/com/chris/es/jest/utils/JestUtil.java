package com.chris.es.jest.utils;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

import java.io.IOException;
import java.util.List;

/**
 * Created by Chris Chen
 * 2018/11/22
 * Explain:
 */

public class JestUtil {
    private static JestClient jestClient;

    public static <T> void save(T data, String index, String type) throws IOException {
        ((JestProcessor<T>) () -> jestClient).save(data, index, type);
    }

    public static <T> void saveAll(List<T> dataList, String index, String type) throws IOException {
        ((JestProcessor<T>) () -> jestClient).saveAll(dataList, index, type);
    }

    public static <T> void update(T data, String index, String type, String id) throws IOException {
        ((JestProcessor<T>) () -> jestClient).update(data, index, type, id);
    }

    public static <T> List<T> findAll(Class<T> clazz, String index, String type) throws IOException {
        return new JestProcessor<T>() {
            @Override
            public JestClient getJestClient() {
                return jestClient;
            }
        }.findAll(clazz, index, type);
    }

    public static JestClient getJestClient() {
        return jestClient;
    }

    public static void setJestClient(JestClient jestClient) {
        JestUtil.jestClient = jestClient;
    }

    public static void init(JestClient jestClient) {
        JestUtil.jestClient = jestClient;
    }

    public static JestClient createJestClient(String serverUri, int timeout) {
        JestClientFactory jestClientFactory = new JestClientFactory();
        HttpClientConfig config = new HttpClientConfig.Builder(serverUri)
                .connTimeout(1000 * timeout)
                .build();
        jestClientFactory.setHttpClientConfig(config);
        JestUtil.jestClient = jestClientFactory.getObject();
        return jestClient;
    }

}


