package com.example.es;

import com.example.bean.Employee;
import com.google.gson.Gson;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class ESTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ESTest.class);
    private static  final String INDEX_NAME="ericsson";
    private static  final String HOST_NAME="stack";
    private static  final int HOST_PORT=9300;
    private TransportClient client;

    @Before
    public void setup() throws UnknownHostException {
        LOGGER.info("开始设置客户端连接");
        // 1 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-cluster").build();

        // 2 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(HOST_NAME),
                HOST_PORT));

        // 3 打印集群名称
        LOGGER.info("集群名称:{}",client.toString());

    }

    /**
     * 测试创建索引。
     * 运行后访问web ui后加索引名路径，可以看到json格式的数据。
     * 即如：http://host:9200/gao
     * host为es集群的client（无client的用主机名），gao为索引名。
     */
    @Test
    public void testCreateIndex(){
        CreateIndexResponse response =
                client.admin().indices().prepareCreate(INDEX_NAME).get();

        LOGGER.info("创建索引响应:{}",response);
    }

    /**
     * 测试用json格式创建文档，若指定的索引不存在，会自己创建索引。
     */
    @Test
    public void testCreateDocFromJson() throws ExecutionException, InterruptedException {
        //创建数据
        Employee employee = new Employee();
        employee.setAge(26);
        employee.setEmployeeName("Big dragon");
        employee.setEmployeeId("594377");

        //建文档。
        IndexResponse response = client.prepareIndex(INDEX_NAME, "aa", "1")//返回一个builder
                .setSource(new Gson().toJson(employee)).execute()//返回一个future
                .get();//返回真正的响应。

        //打印响应数据
        String id = response.getId();
        String index = response.getIndex();
        DocWriteResponse.Result result = response.getResult();
        String type = response.getType();
        long version = response.getVersion();
        LOGGER.info("id:{}",id);
        LOGGER.info("index:{}",index);
        LOGGER.info("result:{}",result);
        LOGGER.info("type:{}",type);
        LOGGER.info("version:{}",version);
    }
    /**
     * 测试用map格式创建文档，若指定的索引不存在，会自己创建索引。
     * 此接口未过时。
     */
    @Test
    public void testCreateDocFromMap() throws ExecutionException, InterruptedException {
        //创建数据
        Map<String,Object> map = new HashMap<>();
        map.put("employeeId","094377");
        map.put("employeeName","Big dragon");
        map.put("age",26);

        //建文档。
        IndexResponse response = client.prepareIndex(INDEX_NAME, "tal", "2")//返回一个builder
                .setSource(map).execute()//返回一个future
                .get();//返回真正的响应。

        //打印响应数据
        String id = response.getId();
        String index = response.getIndex();
        DocWriteResponse.Result result = response.getResult();
        String type = response.getType();
        long version = response.getVersion();
        LOGGER.info("id:{}",id);
        LOGGER.info("index:{}",index);
        LOGGER.info("result:{}",result);
        LOGGER.info("type:{}",type);
        LOGGER.info("version:{}",version);
    }

    /**
     * 创建文档可以使用XContentType  也可以使用XContentBuilder直接创建。
     */
    @Test
    public void testCreateByBuilder() throws IOException, ExecutionException, InterruptedException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id").value("3")
                .field("body").value("文档数据体").endObject();

        IndexResponse response = client.prepareIndex(INDEX_NAME, "consultant","3")
                .setSource(builder).execute().get();
        //打印响应数据
        String id = response.getId();
        String index = response.getIndex();
        DocWriteResponse.Result result = response.getResult();
        String type = response.getType();
        long version = response.getVersion();
        LOGGER.info("id:{}",id);
        LOGGER.info("index:{}",index);
        LOGGER.info("result:{}",result);
        LOGGER.info("type:{}",type);
        LOGGER.info("version:{}",version);

    }


    /**
     * 测试查询单个索引。
     */
    @Test
    public void testGetSingle(){
        GetResponse response = client.prepareGet(INDEX_NAME, "tal", "2").get();
        LOGGER.info("response:{}",response);
    }

    /**
     * 测试查询多个索引。
     */
    @Test
    public void testGetMulti(){
        MultiGetResponse response = client.prepareMultiGet().add(INDEX_NAME, "tal", "2")
                .add(INDEX_NAME, "aa", "1").get();
        response.forEach((resp)->{
                if(resp.getResponse().isExists()){
                    LOGGER.info("response:{}",resp.getResponse());
                }
            }
        );
    }
    @Test
    public void testDeleteIndex(){
        LOGGER.info("开始删除索引:{}",INDEX_NAME);
        client.admin().indices().prepareDelete(INDEX_NAME);
    }


    /**
     * 测试更改文档。
     */
    @Test
    public void testUpdateDoc() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest request = new UpdateRequest();
        request.index(INDEX_NAME).type("consultant").id("3");

        //存在的字段更新，不存在的新建。
        request.doc(XContentFactory.jsonBuilder().startObject()
                .field("title","教育是立国之本，国之根本，知识之基，富强之源")
                .field("content","我们的使命是用教育推动科技进步，用科技改变教育,提供教育搜索引擎。")
                .field("createDate","2018-12-12").endObject());

        UpdateResponse response = client.update(request).get();
        LOGGER.info("更新文档数据:{}",response.getResult());
    }

    /**
     * upsert
     */
    @Test
    public void testUpSert() throws IOException, ExecutionException, InterruptedException {
        // 设置查询条件, 查找不到则添加
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME, "tal", "2")
                .source(XContentFactory.jsonBuilder().startObject().field("title", "搜索服务器")
                        .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，" +
                                "基于RESTful web接口。Elasticsearch是用Java开发的，" +
                                "并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。" +
                                "设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。")
                        .endObject());

        // 设置更新, 查找到更新下面的设置
        UpdateRequest upsert = new UpdateRequest("ericsson", "tal", "4")
                .doc(XContentFactory.jsonBuilder().startObject().field("user", "gao").endObject())
                .upsert(indexRequest);

        UpdateResponse response = client.update(upsert).get();
        LOGGER.info("upsert resp:{}",response.getResult());

    }

    /**
     * 测试删除文档
     */
    @Test
    public void testDeleteDoc(){
        DeleteResponse response = client.prepareDelete(INDEX_NAME, "tal", "2").get();
        LOGGER.info("删除操作结果:{}",response.getResult());
    }

    /**
     * 测试全匹配查询。
     */
    @Test
    public void testMatchAllQuery(){
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes("tal")
                .setQuery(QueryBuilders.matchAllQuery()).get();
        SearchHits hits = response.getHits();
        LOGGER.info("命中条数:{}",hits.totalHits);
        hits.forEach(hit->{
            LOGGER.info("命中结果：{}",hit.getSourceAsString());
        });
    }

    /**
     * 测试匹配单词查询
     */
    @Test
    public void testMatchWordQuery(){
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes("tal", "consultant")
                .setQuery(QueryBuilders.queryStringQuery("搜索")).get();

        SearchHits hits = response.getHits();
        LOGGER.info("命中条数:{}",hits.totalHits);
        hits.forEach(hit->{
            LOGGER.info("命中结果：{}",hit.getSourceAsString());
        });

    }

    /**
     * 测试通配符查询
     */
    @Test
    public void wildcardQuery() {

        // 1 通配符查询
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes("tal")
                .setQuery(QueryBuilders.wildcardQuery("content", "*搜*")).get();

        SearchHits hits = response.getHits();
        LOGGER.info("命中条数:{}",hits.totalHits);
        hits.forEach(hit->{
            LOGGER.info("命中结果：{}",hit.getSourceAsString());
        });

    }

    /**
     * 测试词条查询
     */
    @Test
    public void testItemQuery(){
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes("tal","consultant")
                .setQuery(QueryBuilders.termQuery("title", "搜索"))
                .get();
        SearchHits hits = response.getHits();
        LOGGER.info("命中条数:{}",hits.totalHits);
        hits.forEach(hit->{
            LOGGER.info("命中结果：{}",hit.getSourceAsString());
        });
    }

    /**
     * 测试模糊查询
     */
    @Test
    public void testFuzzyQuery(){
        SearchResponse response = client.prepareSearch(INDEX_NAME).setTypes("tal", "consultant")
                .setQuery(QueryBuilders.fuzzyQuery("title", "搜")).get();
        SearchHits hits = response.getHits();
        LOGGER.info("命中条数:{}",hits.totalHits);
        hits.forEach(hit->{
            LOGGER.info("命中结果：{}",hit.getSourceAsString());
        });
    }



    @After
    public void after(){
        LOGGER.info("开始关闭客户端连接");
        Optional<TransportClient> op = Optional.ofNullable(this.client);
        op.ifPresent(TransportClient::close);
        LOGGER.info("成功关闭客户端连接");
    }
}
