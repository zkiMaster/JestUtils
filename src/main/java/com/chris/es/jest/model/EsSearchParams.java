package com.chris.es.jest.model;

import io.searchbox.core.search.sort.Sort;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Chris Chen
 * 2018/10/15
 * Explain: 对ElasticSearch进行多条件分页搜索的参数类
 */

public class EsSearchParams extends PageParams {
    private Map<String, Object> fieldMap;//单字段精确匹配映射
    private Map<String, String> mustWildcardFieldMap;//单字段模糊匹配映射 must
    private Map<String, String> shouldWildcardFieldMap;//单字段模糊匹配映射 should
    private Map<String, String[]> multiFieldMap;//多字段同值精确匹配映射
    private Map<String, String[]> multiWildcardFieldMap;//多字段同值模糊匹配映射
    private Map<String, Range<?>> rangeFieldMap;//值区间搜索参数
    private Map<String, Boolean> extremeFieldMap;//极值搜索参数
    private String index;//es index 数据库
    private String type;//es type 数据表
    private String sortFieldName;//排序字段
    private Sort.Sorting sortMode = Sort.Sorting.ASC;//排序方式

    public EsSearchParams() {
    }

    public static EsSearchParams get() {
        return new EsSearchParams();
    }

    public Map<String, Object> getFieldMap() {
        return fieldMap;
    }

    public EsSearchParams setFieldMap(Map<String, Object> fieldMap) {
        this.fieldMap = fieldMap;
        return this;
    }

    public Map<String, String> getMustWildcardFieldMap() {
        return mustWildcardFieldMap;
    }

    public void setMustWildcardFieldMap(Map<String, String> mustWildcardFieldMap) {
        this.mustWildcardFieldMap = mustWildcardFieldMap;
    }

    public EsSearchParams addMustWildcardField(String key, String keyWords) {
        if (this.mustWildcardFieldMap == null) {
            this.mustWildcardFieldMap = new HashMap<>();
        }
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(keyWords)) {
            return this;
        }
        this.mustWildcardFieldMap.put(key, keyWords);
        return this;
    }

    public Map<String, String> getShouldWildcardFieldMap() {
        return shouldWildcardFieldMap;
    }

    public EsSearchParams setShouldWildcardFieldMap(Map<String, String> shouldWildcardFieldMap) {
        this.shouldWildcardFieldMap = shouldWildcardFieldMap;
        return this;
    }

    public EsSearchParams addShouldWildcardField(String key, String keyWords) {
        if (this.shouldWildcardFieldMap == null) {
            this.shouldWildcardFieldMap = new HashMap<>();
        }
        if (StringUtils.isEmpty(key) || StringUtils.isEmpty(keyWords)) {
            return this;
        }
        this.shouldWildcardFieldMap.put(key, keyWords);
        return this;
    }

    public Map<String, String[]> getMultiFieldMap() {
        return multiFieldMap;
    }

    public EsSearchParams setMultiFieldMap(Map<String, String[]> multiFieldMap) {
        this.multiFieldMap = multiFieldMap;
        return this;
    }

    public Map<String, String[]> getMultiWildcardFieldMap() {
        return multiWildcardFieldMap;
    }

    public EsSearchParams setMultiWildcardFieldMap(Map<String, String[]> multiWildcardFieldMap) {
        this.multiWildcardFieldMap = multiWildcardFieldMap;
        return this;
    }

    public EsSearchParams addMultiWildcardFiel(String val, String... fields) {
        if (this.multiWildcardFieldMap == null) {
            this.multiWildcardFieldMap = new HashMap<>();
        }
        this.multiWildcardFieldMap.put(val, fields);
        return this;
    }

    public String getIndex() {
        return index;
    }

    public EsSearchParams setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public EsSearchParams setType(String type) {
        this.type = type;
        return this;
    }

    public EsSearchParams setIndexAndType(String index, String type) {
        this.index = index;
        this.type = type;
        return this;
    }

    public EsSearchParams addFieldKV(String fieldName, Object valWord) {
        if (this.fieldMap == null) {
            this.fieldMap = new HashMap<>();
        }
        if (valWord instanceof String && (StringUtils.isEmpty(fieldName) || StringUtils.isEmpty(valWord))) {
            return this;
        }
        if (StringUtils.isEmpty(fieldName.trim()) || (valWord instanceof String && (StringUtils.isEmpty(String.valueOf(valWord).trim())))) {
            return this;
        }
        this.fieldMap.put(fieldName, valWord);
        return this;
    }

    public EsSearchParams addMultiFieldKV(String valWord, String... multiFields) {
        if (this.multiFieldMap == null) {
            this.multiFieldMap = new HashMap<>();
        }
        if (valWord == null) {
            return this;
        }
        this.multiFieldMap.put(valWord, multiFields);
        return this;
    }

    public EsSearchParams addMultiFieldKV(String valWord, List<String> multiFieldList) {
        if (this.multiFieldMap == null) {
            this.multiFieldMap = new HashMap<>();
        }
        if (multiFieldList == null || multiFieldList.size() == 0) {
            return this;
        }
        this.multiFieldMap.put(valWord, (String[]) multiFieldList.toArray());
        return this;
    }

    public EsSearchParams setPageParams(PageParams pageParams) {
        setPage(pageParams.getPage());
        setPageSize(pageParams.getPageSize());
        return this;
    }

    public EsSearchParams setPageParams(int page, int pageSize) {
        setPage(page);
        setPageSize(pageSize);
        return this;
    }

    public Map<String, Range<?>> getRangeFieldMap() {
        return rangeFieldMap;
    }

    public EsSearchParams setRangeFieldMap(Map<String, Range<?>> rangeFieldMap) {
        this.rangeFieldMap = rangeFieldMap;
        return this;
    }

    public EsSearchParams addRangeField(String fieldname, Range<?> range) {
        if (this.rangeFieldMap == null) {
            this.rangeFieldMap = new HashMap<>();
        }
        if (range == null) {
            return this;
        }
        this.rangeFieldMap.put(fieldname, range);
        return this;
    }

    public EsSearchParams addRangeField(String fieldname, Object min, Object max) {
        if (this.rangeFieldMap == null) {
            this.rangeFieldMap = new HashMap<>();
        }
        this.rangeFieldMap.put(fieldname, new Range<>(min, max));
        return this;
    }

    public Map<String, Boolean> getExtremeFieldMap() {
        return extremeFieldMap;
    }

    public EsSearchParams setExtremeFieldMap(Map<String, Boolean> extremeFieldMap) {
        this.extremeFieldMap = extremeFieldMap;
        return this;
    }

    public EsSearchParams addExtremeField(String fieldName, Boolean isMax) {
        if (this.extremeFieldMap == null) {
            this.extremeFieldMap = new HashMap<>();
        }
        this.extremeFieldMap.put(fieldName, isMax);
        return this;
    }

    public String getSortFieldName() {
        return sortFieldName;
    }

    public EsSearchParams setSortFieldName(String sortFieldName) {
        this.sortFieldName = sortFieldName;
        return this;
    }

    public Sort.Sorting getSortMode() {
        return sortMode;
    }

    public EsSearchParams setSortMode(Sort.Sorting sortMode) {
        this.sortMode = sortMode;
        return this;
    }

    public EsSearchParams setSort(String sortFieldName, boolean isAsc) {
        this.sortFieldName = sortFieldName;
        if (isAsc) {
            this.sortMode = Sort.Sorting.ASC;
        } else {
            this.sortMode = Sort.Sorting.DESC;
        }
        return this;
    }
}
