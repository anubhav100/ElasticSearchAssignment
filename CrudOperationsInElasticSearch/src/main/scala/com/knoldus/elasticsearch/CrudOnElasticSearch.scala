package com.knoldus.elasticsearch

object CrudOnElasticSearch extends CrudOperations with App {

  val client = getClient()
  val mappingResponse = addMappingToIndex("twttes", client)
  println(" mapping is " + mappingResponse.isAcknowledged())
  val InsertResponse = insertBulkDocument(client)
  println(" number of documents inserted by bulk request  is " + InsertResponse.getItems.length)
  //  reads(client,"twttes","tweet")
  val updateResponse = updateIndex(client, "twttes", "tweet", "1")
  println(" update response document version is " + updateResponse.getVersion)
  val deleteDocument = deleteDocumentById(client, "twttes", "tweet", "1")
  println(" deleted document by id is " + deleteDocument.isFound)

}