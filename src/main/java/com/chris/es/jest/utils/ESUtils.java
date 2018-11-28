package com.chris.es.jest.utils;

import com.chris.es.jest.model.EsSearchParams;
import com.chris.es.jest.model.PageData;
import com.chris.es.jest.model.Range;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MinAggregation;
import io.searchbox.core.search.sort.Sort;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.max.MaxBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by Chris Chen
 * 2018/10/11
 * Explain: ElasticSearch工具
 */

public class ESUtils {
    public static Integer PAGE_SIZE_MAX = 10000;

    public static Integer getPageSizeMax() {
        return PAGE_SIZE_MAX;
    }

    public static void setPageSizeMax(Integer maxPageSize) {
        if (maxPageSize != null && maxPageSize.intValue() > 0) {
            ESUtils.PAGE_SIZE_MAX = maxPageSize;
        }
    }

    /**
     * 把hit集合转换为对象集合
     *
     * @param hitList
     * @param <T>
     * @return
     */
    public static <T> List<T> converFromHitList(List<SearchResult.Hit<T, Void>> hitList) {
        if (hitList == null || hitList.size() == 0) {
            return null;
        }
        List<T> list = new ArrayList<>();
        hitList.stream().forEach(hit -> {
            T source = hit.source;
            setDefaultValue(source, "id", hit.id);
            list.add(source);
        });
        return list;
    }

    /**
     * 从hit集合中获取第一条数据
     *
     * @param hitList
     * @param <T>
     * @return
     */
    public static <T> T converFirstFromHitList(List<SearchResult.Hit<T, Void>> hitList) {
        if (hitList == null || hitList.size() == 0) {
            return null;
        }
        SearchResult.Hit<T, Void> hit = hitList.get(0);
        return hitToEntity(hit);
    }

    /**
     * hit转换为entity
     *
     * @param hit
     * @param <T>
     * @return
     */
    public static <T> T hitToEntity(SearchResult.Hit<T, Void> hit) {
        T source = hit.source;
        setDefaultValue(source, "id", hit.id);
        return source;
    }

    /**
     * 根据条件配到第一条数据
     * 可以自己取id和对象
     * 主要用作更新
     *
     * @param jestClient
     * @param params
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> SearchResult.Hit<T, ?> findOne(JestClient jestClient, EsSearchParams params, Class<T> clazz) {
        SearchResult searchResult = searchResult(jestClient, params);
        if (searchResult.isSucceeded()) {
            List<SearchResult.Hit<T, Void>> hits = searchResult.getHits(clazz);
            if (hits != null && hits.size() > 0) {
                SearchResult.Hit<T, Void> hit = hits.get(0);
                return hit;
            }
        }
        return null;
    }

    //搜索按照最大限制允许的数据集合
    public static <T> List<T> searchList(JestClient jestClient, EsSearchParams params, Class<T> clazz) {
        List<T> dataList = new ArrayList<>();
        SearchResult result = searchResult(jestClient, params);
        if (result == null || !result.isSucceeded()) {
            return dataList;
        }
        List<SearchResult.Hit<T, Void>> hits = result.getHits(clazz);
        dataList = ESUtils.converFromHitList(hits);
        return dataList;
    }

    /**
     * ElasticSearch搜索
     *
     * @return
     * @throws IOException
     */
    public static <T> PageData<T> searchPage(JestClient jestClient, EsSearchParams params, Class<T> clazz) {
        //long startTime = System.currentTimeMillis();//搜索开始时间
        SearchResult result = searchResult(jestClient, params);
        if (result == null || !result.isSucceeded()) {
            return PageData.buildNull();
        }
        List<SearchResult.Hit<T, Void>> hits = result.getHits(clazz);
        List<T> dataList = ESUtils.converFromHitList(hits);
        if (dataList == null) {
            return PageData.buildNull();
        }
        int page = params.getPage();
        int pageSize = params.getPageSize();
        long total = 0;
        try {
            total = result.getTotal();
        } catch (NoSuchMethodError e) {
            throw new RuntimeException("有问题");
        }

        //logger.d("ElasticSearch Data Search time： " + (System.currentTimeMillis() - startTime) + " ms");
        return PageData.get(clazz)
                .setPage(page)
                .setPageSize(pageSize)
                .setTotal(total)
                .setHasNext((page + 1) * pageSize < total)
                .setDataList(dataList);
    }

    public static SearchResult searchResult(JestClient jestClient, EsSearchParams params) {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        List<QueryBuilder> queryBuilderList = new ArrayList<>();//搜集查询条件
        //构建多条件查询
        ////1.一个字段对应一个关键字
        Map<String, Object> fieldMap = params.getFieldMap();
        TermQueryBuilder termQueryBuilder = null;
        if (fieldMap != null && fieldMap.size() > 0) {
            for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
                Object value = entry.getValue();
                if (checkIsEmpty(value)) {
                    continue;
                }
                termQueryBuilder = QueryBuilders.termQuery(entry.getKey(), value);
                bqb.must(termQueryBuilder);
                queryBuilderList.add(termQueryBuilder);
            }
        }

        ////2-1.单字段匹配模糊查询 should
        Map<String, String> shouldWildcardFieldMap = params.getShouldWildcardFieldMap();
        WildcardQueryBuilder shouldWildcardQueryBuilder = null;
        if (shouldWildcardFieldMap != null && shouldWildcardFieldMap.size() > 0) {
            for (Map.Entry<String, String> entry : shouldWildcardFieldMap.entrySet()) {
                String keyWords = entry.getValue();//模糊查询关键字 需要添加通配符
                if (checkIsEmpty(keyWords)) {
                    continue;
                }
                shouldWildcardQueryBuilder = QueryBuilders.wildcardQuery(entry.getKey(), keyWords);
                bqb.should(shouldWildcardQueryBuilder);
                queryBuilderList.add(shouldWildcardQueryBuilder);
            }
        }

        ////2-2.单字段匹配模糊查询 must
        Map<String, String> mustWildcardFieldMap = params.getMustWildcardFieldMap();
        WildcardQueryBuilder mustWildcardQueryBuilder = null;
        if (mustWildcardFieldMap != null && mustWildcardFieldMap.size() > 0) {
            for (Map.Entry<String, String> entry : mustWildcardFieldMap.entrySet()) {
                String keyWords = entry.getValue();//模糊查询关键字 需要添加通配符
                if (checkIsEmpty(keyWords)) {
                    continue;
                }
                mustWildcardQueryBuilder = QueryBuilders.wildcardQuery(entry.getKey(), keyWords);
                bqb.must(mustWildcardQueryBuilder);
                queryBuilderList.add(mustWildcardQueryBuilder);
            }
        }
        ////3.多个个字段对应一个关键字 精确匹配
        Map<String, String[]> multiFieldMap = params.getMultiFieldMap();
        MultiMatchQueryBuilder multiMatchQueryBuilder = null;
        if (multiFieldMap != null && multiFieldMap.size() > 0) {
            for (Map.Entry<String, String[]> entry : multiFieldMap.entrySet()) {
                String key = entry.getKey();
                String[] value = entry.getValue();
                if (StringUtils.isEmpty(key) || StringUtils.isEmpty(key.trim()) || value == null || value.length == 0) {
                    continue;
                }
                multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(key, value);
                bqb.must(multiMatchQueryBuilder);
                queryBuilderList.add(multiMatchQueryBuilder);
            }
        }

        ////3-2.多个个字段对应一个关键字 模糊匹配
        Map<String, String[]> multiWildcardFieldMap = params.getMultiWildcardFieldMap();
        if (multiWildcardFieldMap != null && multiWildcardFieldMap.size() > 0) {
            for (Map.Entry<String, String[]> entry : multiWildcardFieldMap.entrySet()) {
                ////一个元素一个bool查询
                BoolQueryBuilder mulBqb = QueryBuilders.boolQuery();
                ////取得共同的值
                String valWord = entry.getKey();
                ////取得所有的字段
                String[] fields = entry.getValue();
                ////遍历字段
                for (String field : fields) {
                    if (checkIsEmpty(valWord)) {
                        continue;
                    }
                    mustWildcardQueryBuilder = QueryBuilders.wildcardQuery(field, valWord);
                    mulBqb.should(mustWildcardQueryBuilder);
                }
                bqb.must(mulBqb);
                queryBuilderList.add(multiMatchQueryBuilder);
            }
        }

        ////4. 值区间查询
        Map<String, Range<?>> rangeFieldMap = params.getRangeFieldMap();
        RangeQueryBuilder rangeQueryBuilder = null;
        if (rangeFieldMap != null && rangeFieldMap.size() > 0) {
            for (Map.Entry<String, Range<?>> entry : rangeFieldMap.entrySet()) {
                String key = entry.getKey();
                Range<?> range = entry.getValue();
                if (range == null) {
                    continue;
                }
                rangeQueryBuilder = QueryBuilders.rangeQuery(key)
                        .gte(range.getMin())
                        .lte(range.getMax());
                bqb.must(rangeQueryBuilder);
                queryBuilderList.add(rangeQueryBuilder);
            }
        }
        ////5. 极值聚合查询
        Map<String, Boolean> extremeFieldMap = params.getExtremeFieldMap();
        if (extremeFieldMap != null && extremeFieldMap.size() > 0) {
            for (Map.Entry<String, Boolean> entry : extremeFieldMap.entrySet()) {
                String fieldName = entry.getKey();
                Boolean ismax = entry.getValue();

                QueryBuilder[] queryBuilders = queryBuilderListToArrays(queryBuilderList);
                if (ismax) {
                    //最大值
                    QueryBuilder maxQueryBuilder = createMaxQueryBuilder(jestClient, params.getIndex(), params.getType(), fieldName, queryBuilders);
                    if (maxQueryBuilder != null) {
                        bqb.must(maxQueryBuilder);
                    }

                } else {
                    //最小值
                    QueryBuilder minQueryBuilder = createMinQueryBuilder(jestClient, params.getIndex(), params.getType(), fieldName, queryBuilders);
                    if (minQueryBuilder != null) {
                        bqb.must(minQueryBuilder);
                    }
                }
            }
        }

        int page = params.getPage();
        int pageSize = params.getPageSize();
        ssb.query(bqb).from(page * pageSize).size(pageSize);//分页搜索

        String query = ssb.toString();
        Search.Builder builder = new Search.Builder(query)
                .addIndex(params.getIndex())
                .addType(params.getType());
        String sortFieldName = params.getSortFieldName();
        if (!checkIsEmpty(sortFieldName)) {
            builder.addSort(new Sort(sortFieldName, params.getSortMode()));
        }
        Search search = builder.build();
        try {
            return jestClient.execute(search);
        } catch (IOException e) {
            //logger.d("ES读取异常");
            //e.printStackTrace();
        }
        return null;
    }

    //判断参数值是否为空、空字符串或者全空格
    public static boolean checkIsEmpty(Object value) {
        //未传参数
        if (null == value) {
            return true;
        }
        if (value instanceof String) {
            //参数是空字符串
            if ("".equals(value)) {
                return true;
            }
            //参数全部都是空格
            if ("".equals(((String) value).trim())) {
                return true;
            }
        }
        return false;
    }

    //list转数组
    private static QueryBuilder[] queryBuilderListToArrays(List<QueryBuilder> queryBuilderList) {
        if (queryBuilderList == null || queryBuilderList.size() == 0) {
            return null;
        }
        int size = queryBuilderList.size();
        QueryBuilder[] queryBuilders = new QueryBuilder[size];
        for (int i = 0; i < size; i++) {
            queryBuilders[i] = queryBuilderList.get(i);
        }
        return queryBuilders;
    }

    // 字符串参数集合转为数组
    public static String listToArray(List<String> batteryNameList) {
        if (batteryNameList == null || batteryNameList.size() == 0) {
            return null;
        }
        int size = batteryNameList.size();
        String[] batteryNames = new String[size];
        for (int i = 0; i < size; i++) {
            batteryNames[i] = batteryNameList.get(i);
        }
        return null;
    }

    /**
     * 获取一个max极值查询 目前只支持long类型
     *
     * @param jestClient
     * @param index
     * @param type
     * @param fieldName
     * @param queryBuilders
     * @return
     */
    public static QueryBuilder createMaxQueryBuilder(JestClient jestClient, String index, String type, String fieldName, QueryBuilder... queryBuilders) {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        String maxName = UUID.randomUUID().toString();
        MaxBuilder maxBuilder = AggregationBuilders.max(maxName).field(fieldName);
        if (queryBuilders != null && queryBuilders.length > 0) {
            BoolQueryBuilder bqb = QueryBuilders.boolQuery();
            for (QueryBuilder tqb : queryBuilders) {
                bqb.must(tqb);
            }
            ssb.query(bqb);
        }
        ssb.aggregation(maxBuilder).size(1);
        String query = ssb.toString();
        //logger.prnln(query);
        Search search = new Search.Builder(query)
                .addIndex(index)
                .addType(type)
                .build();
        SearchResult result = null;
        try {
            result = jestClient.execute(search);
            MaxAggregation newVal = result.getAggregations().getMaxAggregation(maxName);
            Double newValMax = newVal.getMax();
            if (newValMax != null) {
                BigDecimal bigDecimal = new BigDecimal(newValMax);
                Long maxValue = Long.valueOf(bigDecimal.toPlainString());
                return QueryBuilders.termQuery(fieldName, maxValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取一个max极值查询 目前只支持long类型
     *
     * @param jestClient
     * @param index
     * @param type
     * @param fieldName
     * @param queryBuilders
     * @return
     */
    public static QueryBuilder createMinQueryBuilder(JestClient jestClient, String index, String type, String fieldName, QueryBuilder... queryBuilders) {
        SearchSourceBuilder ssb = new SearchSourceBuilder();
        String minName = UUID.randomUUID().toString();
        MinBuilder minBuilder = AggregationBuilders.min(minName).field(fieldName);
        if (queryBuilders != null && queryBuilders.length > 0) {
            BoolQueryBuilder bqb = QueryBuilders.boolQuery();
            for (QueryBuilder tqb : queryBuilders) {
                bqb.must(tqb);
            }
            ssb.query(bqb);
        }
        ssb.aggregation(minBuilder).size(1);
        String query = ssb.toString();
        Search search = new Search.Builder(query)
                .addIndex(index)
                .addType(type)
                .build();
        SearchResult result = null;
        try {
            result = jestClient.execute(search);
            MinAggregation newVal = result.getAggregations().getMinAggregation(minName);
            Double newValMin = newVal.getMin();
            if (newValMin != null) {
                BigDecimal bigDecimal = new BigDecimal(newValMin);
                Long minValue = Long.valueOf(bigDecimal.toPlainString());
                return QueryBuilders.termQuery(fieldName, minValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查找特定字段，设置默认值
     *
     * @param object
     * @param value
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    private static void setDefaultValue(Object object, String fieldName, Object value) {
        Field field = null;
        try {
            field = object.getClass().getDeclaredField(fieldName);
            if (field != null) {
                field.setAccessible(true);
                field.set(object, value);//设置默认值
                field.setAccessible(false);
            }
        } catch (NoSuchFieldException e) {
//            e.printStackTrace();
        } catch (IllegalAccessException e) {
//            e.printStackTrace();
        }

    }

    public <T> void save(JestClient jestClient, T entity, String index, String type) throws IOException {
        Index _index = new Index.Builder(entity).index(index).type(type).build();

        jestClient.execute(_index);
    }

    /**
     * 更新一条记录
     *
     * @param jestClient
     * @param entity
     * @param index
     * @param type
     * @param id
     * @param <T>
     * @throws IOException
     */
    public static <T> void update(JestClient jestClient, T entity, String index, String type, String id) {
        Index _index = new Index.Builder(entity).index(index).type(type).id(id).build();

        try {
            jestClient.execute(_index);
        } catch (IOException e) {
            e.printStackTrace();
            //logger.d("更新失败");
        }
    }
}
