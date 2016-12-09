# es-importer
A small tool for importing JSON bulk dumps into an Elasticsearch index.
This is using the ES Bulk API: [ES Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/2.3/docs-bulk.html)

## Source Format
The source file format consists of a simple JSON format describing a single ES document in 2 lines.
* Every line has to end with a newline character to ensure a successful import.
* The first line conaints all of the ES bulk import information (metadata) needed for importing the following document.
* The second line contains all the document data as a single JSON object.

### Example
```json
{ "create" : { "_type" : "Example", "_id" : "123" } }
{ "name" : "Example Dump", "site" : "Github", "awesomeness" : "9001" }
```

## Parameters
These can be provided in a instance.conf file or as VM options when starting the app.

| Name | Description |
|---|---|
| host | URL of your ES instance |
| port | The port for accessing your ES (default: 9200) |
| batchsize | How many datasets should be imported at once (default: 100). I advice against a high value here, as it can cause problems. |
| index | The name of the target index |
| source | The path to your source JSON file (preferrably an absolute path) |
