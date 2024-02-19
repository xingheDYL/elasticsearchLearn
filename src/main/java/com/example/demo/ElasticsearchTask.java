package com.example.demo;

import org.elasticsearch.client.RestHighLevelClient;

/**
 * 搜索任务
 */
public interface ElasticsearchTask {

    void doSomething(RestHighLevelClient client) throws Exception;
}
