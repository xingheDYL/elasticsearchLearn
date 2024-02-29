package com.example.demo;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

public class QueryDoc {

    public static void main(String[] args) {
        ConnectElasticsearch.connect(client -> {
            // 创建搜索请求对象
            SearchRequest request = new SearchRequest();
            request.indices("user");
            // 构建查询的请求体
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            // 查询所有数据
            setSearchAll(sourceBuilder);
            // 条件查询数据
//            setSearchByCondition(sourceBuilder);
            // 分页查询数据
//            setSearchByPage(sourceBuilder);
            // 数据排序
//            setSearchOrder(sourceBuilder);
            // 过滤字段
//            setSearchFilter(sourceBuilder);
            // 组合查询
//            setSearchCombination(sourceBuilder);
            // 范围查询
//            setSearchRange(sourceBuilder);
            // 模糊查询
//            setSearchFuzzy(sourceBuilder);
            // 高亮查询
//            setSearchHighlight(sourceBuilder);
            // 最大值查询
//            setSearchMax(sourceBuilder);
            // 分组查询
//            setSearchByGroup(sourceBuilder);
            request.source(sourceBuilder);
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 最大值查询和分组查询只打印这个
//            System.out.println(response);
            // 查询匹配
            SearchHits hits = response.getHits();
            System.out.println("took:" + response.getTook());
            System.out.println("timeout:" + response.isTimedOut());
            System.out.println("total:" + hits.getTotalHits());
            System.out.println("MaxScore:" + hits.getMaxScore());
            System.out.println("hits========>>");
            for (SearchHit hit : hits) {
                //输出每条查询的结果信息
                System.out.println(hit.getSourceAsString());
                // 只有高亮查询时加下面两行代码
                //打印高亮结果
//                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
//                System.out.println(highlightFields);
            }
            System.out.println("<<========");
        });
    }

    /**
     * 查询所有数据
     */
    public static void setSearchAll(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.query(QueryBuilders.matchAllQuery());
    }

    /**
     * 条件查询数据
     */
    public static void setSearchByCondition(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.query(QueryBuilders.termQuery("age", "30"));
    }

    /**
     * 分页查询数据
     */
    public static void setSearchByPage(SearchSourceBuilder sourceBuilder) {
        // 当前页其实索引(第一条数据的顺序号)， from
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        sourceBuilder.from(0);
        // 每页显示多少条 size
        sourceBuilder.size(2);
    }

    /**
     * 查询数据并排序
     */
    public static void setSearchOrder(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 排序
        sourceBuilder.sort("age", SortOrder.ASC);
    }

    /**
     * 查询数据并过滤字段
     */
    public static void setSearchFilter(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        // 过滤掉age字段
        String[] excludes = {"age"};
        String[] includes = {};
        sourceBuilder.fetchSource(includes, excludes);
    }

    /**
     * 组合查询
     */
    public static void setSearchCombination(SearchSourceBuilder sourceBuilder) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 必须包含
        boolQueryBuilder.must(QueryBuilders.matchQuery("age", "30"));
        // 一定不含
        boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
        // 可能包含
        boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));
        sourceBuilder.query(boolQueryBuilder);
    }

    /**
     * 范围查询
     */
    public static void setSearchRange(SearchSourceBuilder sourceBuilder) {
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        // 大于等于
        rangeQuery.gte(30);
        // 小于
        rangeQuery.lt(50);
        sourceBuilder.query(rangeQuery);
    }

    /**
     * 模糊查询
     */
    public static void setSearchFuzzy(SearchSourceBuilder sourceBuilder) {
        // Fuzziness.TWO 代表最多有两个字段模糊 例如可以查wangwu12,如果是Fuzziness.TWO 就查不到wangwu12
        sourceBuilder.query(QueryBuilders.fuzzyQuery("name", "wangwu").fuzziness(Fuzziness.TWO));
    }

    /**
     * 高亮查询
     */
    public static void setSearchHighlight(SearchSourceBuilder sourceBuilder) {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("name", "zhangsan");

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field("name");

        sourceBuilder.highlighter(highlightBuilder);
        sourceBuilder.query(termsQueryBuilder);
    }

    /**
     * 最大值查询
     */
    public static void setSearchMax(SearchSourceBuilder sourceBuilder) {
        AggregationBuilder aggregationBuilder = AggregationBuilders.max("maxAge").field("age");
        sourceBuilder.aggregation(aggregationBuilder);
    }

    /**
     * 分组查询
     */
    public static void setSearchByGroup(SearchSourceBuilder sourceBuilder) {
        AggregationBuilder aggregationBuilder = AggregationBuilders.terms("ageGroup").field("age");
        sourceBuilder.aggregation(aggregationBuilder);
    }
}