# go-delete-dynamo-table
CLI tool in Golang to clear an AWS DynamoDB table.

Loads just the keys of the table and then batch deletes the items.

For usage, type `dynamo-delete-table -help`

To install, clone this repository and call `go install` in the main directory.
# TODO

 - Tests, especially for larger amounts of data
 - "GitHubification" (coverage, builds, whatever else there is)
 - remove dependency on helixddb