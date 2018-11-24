package com.chris.es.jest.utils;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author wei.li
 * <p>
 * es base service index:type
 */
public interface JestProcessor<T> {
    JestClient getJestClient();

    /***
     * 保存文档
     * @param entity
     * @param index
     * @param type
     */
    default void save(T entity, String index, String type) throws IOException {
        Index _index = new Index.Builder(entity).index(index).type(type).build();
        getJestClient().execute(_index);
    }

    /**
     * 更新数据
     *
     * @param entity
     * @param index
     * @param type
     * @throws IOException
     */
    default void update(T entity, String index, String type, String id) throws IOException {
        Index _index = new Index.Builder(entity).index(index).type(type).id(id).build();
        getJestClient().execute(_index);
    }

    /**
     * 批量保存文档
     *
     * @param entitys
     * @param index
     * @param type
     */
    default void saveAll(List<T> entitys, String index, String type) throws IOException {
        Bulk.Builder _bulk = new Bulk.Builder();

        if (Optional.ofNullable(entitys).isPresent()) {
            entitys.stream().forEach(value -> {
                Index _index = new Index.Builder(value).index(index).type(type).build();
                _bulk.addAction(_index);
            });
        }

        getJestClient().execute(_bulk.build());
    }

    /**
     * 查询所有文档
     *
     * @return
     */
    default List<T> findAll(Class<T> clazz, String index, String type) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery()); //match_all
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(index).addType(type).build();
        SearchResult result = getJestClient().execute(search);

        if (result.isSucceeded()) {
            List<SearchResult.Hit<T, Void>> hits = result.getHits(clazz);
            if (hits != null) {
                return hits.stream().map(hit -> hit.source).collect(Collectors.toList());
            }
        }
        return null;
    }
}
