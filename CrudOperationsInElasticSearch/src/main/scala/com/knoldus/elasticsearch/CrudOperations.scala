package com.knoldus.elasticsearch

import java.io.PrintWriter

import org.elasticsearch.action.get.{MultiGetItemResponse, MultiGetResponse}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.client.Client
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.node.NodeBuilder._

import scala.io.Source

/**
  * Created by knoldus on 3/4/16.
  */
trait CrudOperations {

  val mappingBuilder = (jsonBuilder()
    .startObject()
    .startObject("twittes")
    .startObject("_timestamp")
    .field("enabled", true)
    .field("store", true)
    .field("path", "post_date")
    .endObject()
    .endObject()
    .endObject())

  /**
    * getClient method which returns java API client
    *
    * @return
    */

  def getClient(): Client = {
    val node = nodeBuilder().local(true).node()
    val client = node.client()
    client
  }

  def addMappingToIndex(indexName: String, client: Client) = {

    val settingsStr = ImmutableSettings.settingsBuilder().
      put("index.number_of_shards", 7).put("index.number_of_replicas", 1).build()
    client.admin().indices().prepareCreate(indexName)
      .setSettings(settingsStr)
      .addMapping(indexName, mappingBuilder).execute()
      .actionGet()

  }

  /**
    * method which takes each document from file and insert into index
    *
    * @param client
    * @return
    */
  def insertBulkDocument(client: Client) = {
    val jsonData = Source.fromFile("src/main/resources/bulk.json").getLines().toList
    val bulkRequest = client.prepareBulk()
    for (i <- 0 until jsonData.size) {
      bulkRequest.add(client.prepareIndex("twttes", "tweet", (i + 1).toString).setSource(jsonData(i)))
    }
    bulkRequest.execute().actionGet()
  }

  /**
    * This is  method which takes read from index
    *
    * @param client
    * @return
    */
  def reads(client: Client, indexName: String, indexTypeName: String) = {
    val multiGetItemResponses: MultiGetResponse = client.prepareMultiGet()
      .add(indexName, "type", indexTypeName)
      .get()
    val getResponseIterator = multiGetItemResponses.iterator()
    while (getResponseIterator.hasNext) {
      println(getResponseIterator.next.getResponse.getSourceAsString)
    }


  }

  /**
    * This is update index method which updates particular document by add one more field
    *
    * @param client
    * @param indexName
    * @param indexTypeName
    * @param id
    * @return
    */
  def updateIndex(client: Client, indexName: String, indexTypeName: String, id: String) = {


    val updateRequest = new UpdateRequest(indexName, indexTypeName, id)
      .doc(jsonBuilder()
        .startObject()
        .field("scala", "elasticsearch")
        .endObject())
    client.update(updateRequest).get()
  }

  /**
    * deleteDocumentById method which removes particular document from index
    *
    * @param client
    * @param indexName
    * @param indexTypeName
    * @param id
    * @return
    */
  def deleteDocumentById(client: Client, indexName: String, indexTypeName: String, id: String) = {

    val deleteResponse = client.prepareDelete(indexName, indexTypeName, id)
      .execute()
      .actionGet()
    deleteResponse
  }


  //val data = reads(getClient,"twtees","tweet")
  //Some(new PrintWriter("src/main/resources/output.json")).foreach{p =>
  // p.write(data.toString()); p.close
  //}
}
