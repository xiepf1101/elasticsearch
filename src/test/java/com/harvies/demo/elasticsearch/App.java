package com.harvies.demo.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class App {
	TransportClient client = null;

	@SuppressWarnings("resource")
	@Before
	public void init() {
		try {
			client = new PreBuiltTransportClient(Settings.EMPTY)
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.0.0.66"), 9300));
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
			throw new RuntimeException(e1);
		}
		System.out.println(client);
	}

	@Test
	public void index() {
		try {
			IndexResponse response = client.prepareIndex("twitter", "tweet", "1").setSource(jsonBuilder().startObject()
					.field("user", "kimchy").field("postDate", new Date()).field("message", "trying out Elasticsearch").endObject()).get();
			System.out.println(response);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void get() {
		GetResponse response = client.prepareGet("twitter", "tweet", "1").get();
		System.out.println(response);
	}

	@Test
	public void delete() {
		DeleteResponse response = client.prepareDelete("twitter", "tweet", "1").get();
		System.out.println(response);
	}

	@Test
	public void deleteByQuery() {
		BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
				.filter(QueryBuilders.matchQuery("message", "Elasticsearch")).source("twitter").get();
		System.out.println(response.toString());
		long deleted = response.getDeleted();
		System.out.println("deleted " + deleted + " records");
	}

	// @Test
	// public void deleteByQueryAsync() {
	// DeleteByQueryAction.INSTANCE.newRequestBuilder(client).filter(QueryBuilders.matchQuery("message",
	// "Elasticsearch"))
	// .source("twitter").execute(new ActionListener<BulkByScrollResponse>() {
	//
	// @Override
	// public void onResponse(BulkByScrollResponse response) {
	// // TODO Auto-generated method stub
	// long deleted = response.getDeleted();
	// System.out.println(deleted);
	// }
	//
	// @Override
	// public void onFailure(Exception e) {
	// // TODO Auto-generated method stub
	// e.printStackTrace();
	// }
	// });
	// }
	@Test
	public void update() {
		UpdateRequest updateRequest = new UpdateRequest();
		updateRequest.index("twitter");
		updateRequest.type("tweet");
		updateRequest.id("1");
		try {
			updateRequest.doc(jsonBuilder().startObject().field("message", "2017年莫斯科国际航空航天展览会").endObject());
			UpdateResponse updateResponse = client.update(updateRequest).get();
			System.out.println(updateResponse.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void preUpdate() {
		try {
			UpdateResponse updateResponse = client.prepareUpdate("twitter", "tweet", "1")
					.setDoc(jsonBuilder().startObject().field("gender", "male").endObject()).get();
			System.out.println(updateResponse.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	/**
	 * update insert(exists update updateRequest,not exist insert indexRequest)
	 */
	public void upsert() {
		IndexRequest indexRequest;
		try {
			indexRequest = new IndexRequest("index", "type2", "1")
					.source(jsonBuilder().startObject().field("name", "Joe Smith").field("gender", "male").endObject());
			UpdateRequest updateRequest = new UpdateRequest("index", "type2", "1")
					.doc(jsonBuilder().startObject().field("gender", "male2").endObject()).upsert(indexRequest);
			UpdateResponse updateResponse = client.update(updateRequest).get();
			System.out.println(updateResponse.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Test
	public void multiGet() {
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add("twitter", "tweet", "1")
				.add("twitter", "tweet", "2", "3", "4").add("another", "type", "foo").get();

		for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
			GetResponse response = itemResponse.getResponse();
			if (response.isExists()) {
				String json = response.getSourceAsString();
				System.out.println(json);
			}
		}
	}

	@Test
	public void bulk() {
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		// either use client#prepare, or use Requests# to directly build
		// index/delete requests
		try {
			bulkRequest.add(client.prepareIndex("twitter", "tweet", "1").setSource(jsonBuilder().startObject().field("user", "kimchy")
					.field("postDate", new Date()).field("message", "trying out Elasticsearch").endObject()));

			bulkRequest.add(client.prepareIndex("twitter", "tweet", "2").setSource(jsonBuilder().startObject().field("user", "kimchy")
					.field("postDate", new Date()).field("message", "another post").endObject()));

			BulkResponse bulkResponse = bulkRequest.get();
			for (BulkItemResponse bulkItemResponse : bulkResponse) {
				System.out.println(bulkItemResponse.getResponse());
			}
			if (bulkResponse.hasFailures()) {
				// process failures by iterating through each bulk response item
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Test
	public void search() {
		SearchResponse response = client.prepareSearch("index", "twitter").setTypes("tweet", "type2")
				.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("message", "莫")) // Query
				.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(24)) // Filter
				.setFrom(0).setSize(60).setExplain(true).get();
		System.out.println(response);
	}

	@After
	public void after() {
		client.close();
	}
}
