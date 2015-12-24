
import com.uttesh.ElastiSearchService;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHit;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/*
 *
 * @author Uttesh Kumar T.H.
 */
public class ElasticSearchCRUDTest extends BaseTest {

    ElastiSearchService elastiSearchService = null;

    @BeforeTest
    public void init() throws UnknownHostException {
        elastiSearchService = ElastiSearchService.getInstance();
    }

    @Test(enabled = true, priority = 1)
    public void createIndex() throws IOException {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("FirstName", "Uttesh");
        data.put("LastName", "Kumar T.H.");
        jsonBuilder.map(data);
        IndexResponse indexResponse = elastiSearchService.createIndex("users", "user", "1", jsonBuilder);
        Assert.assertSame(indexResponse.isCreated(), true);
    }

    @Test(enabled = true, priority = 2)
    public void findDocumentByIndex() {
        GetResponse response = elastiSearchService.findDocumentByIndex("users", "user", "1");
        Map<String, Object> source = response.getSource();
        System.out.println("------------------------------");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
        System.out.println("getFields: " + response.getFields());
        System.out.println(source);
        System.out.println("------------------------------");
        Assert.assertSame(response.isExists(), true);
    }

    @Test(enabled = true, priority = 3)
    public void findDocumentByValue() {
        SearchResponse response = elastiSearchService.findDocument("users", "user", "LastName", "Kumar T.H.");
        SearchHit[] results = response.getHits().getHits();
        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println("--------------HIT----------------");
            System.out.println("Index: " + hit.getIndex());
            System.out.println("Type: " + hit.getType());
            System.out.println("Id: " + hit.getId());
            System.out.println("Version: " + hit.getVersion());
            Map<String, Object> result = hit.getSource();
            System.out.println(result);
        }
        Assert.assertSame(response.getHits().totalHits() > 0, true);
    }

    @Test(enabled = true, priority = 4)
    public void UpdateDocument() throws IOException {
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("FirstName", "Uttesh Kumar");
        data.put("LastName", "TEST");
        jsonBuilder.map(data);
        UpdateResponse updateResponse = elastiSearchService.updateIndex("users", "user", "1", jsonBuilder);
        SearchResponse response = elastiSearchService.findDocument("users", "user", "LastName", "TEST");
        Assert.assertSame(response.getHits().totalHits() > 0, true);
    }

    @Test(enabled = true, priority = 5)
    public void RemoveDocument() throws IOException {
        DeleteResponse deleteResponse = elastiSearchService.removeDocument("users", "user", "1");
        Assert.assertSame(deleteResponse.isFound(), false);
    }
}
