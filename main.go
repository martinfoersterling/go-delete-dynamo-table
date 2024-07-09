package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/junderhill/helixddb"
	"github.com/junderhill/helixddb/helixddbiface"
	"log"
	"strings"
)

func main() {
	var tableName string
	var profile string
	var region string

	flag.StringVar(&tableName, "tableName", "", "The name of the table to delete.")
	flag.StringVar(&tableName, "t", "", "The name of the table to delete.")
	flag.StringVar(&profile, "profile", "", "The name of the AWS profile to use.")
	flag.StringVar(&profile, "p", "", "The name of the AWS profile to use.")
	flag.StringVar(&region, "region", "eu-west-1", "The name of the AWS region to use.")
	flag.StringVar(&region, "r", "eu-west-1", "The name of the AWS region to use.")
	flag.Parse()

	if tableName == "" {
		log.Println("Missing table name")
		flag.Usage()
		return
	}
	if profile == "" {
		log.Println("Missing AWS profile")
		flag.Usage()
		return
	}

	fmt.Printf("Deleting contents of table %s using AWS region %s and profile %s\n", tableName, region, profile)
	err := deleteTable(tableName, region, profile)
	if err != nil {
		fmt.Println(err)
		fmt.Println("Failure")
		return
	}
	fmt.Println("Success")
}

func deleteTable(tableName, region, profile string) error {

	//TODO remove helixdbb, currently just using it for creating the client
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithSharedConfigProfile(profile))
	if err != nil {
		return err
	}
	db := helixddb.New(cfg)

	description, err := db.Client().DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	scanOutput, err := db.Client().Scan(context.TODO(),
		&dynamodb.ScanInput{
			TableName:            &tableName,
			ProjectionExpression: aws.String(projectionExpression(description)),
		})
	if err != nil {
		return err
	}

	var writeRequests []types.WriteRequest
	for i, data := range scanOutput.Items {

		log.Println(i)

		writeRequests = append(writeRequests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: data,
			}})

		if len(writeRequests) == 25 {
			err = batchDelete(db.Client(), tableName, writeRequests)
			if err != nil {
				return err
			}
			writeRequests = make([]types.WriteRequest, 0)
		}
	}

	if len(writeRequests) > 0 {
		return batchDelete(db.Client(), tableName, writeRequests)
	}

	return nil
}

func batchDelete(client helixddbiface.DynamoDBAPI, tableName string, requests []types.WriteRequest) error {
	_, err := client.BatchWriteItem(context.TODO(),
		&dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: requests,
			},
		})
	return err
}

func projectionExpression(description *dynamodb.DescribeTableOutput) string {
	var keyNames []string
	for _, keySchema := range description.Table.KeySchema {
		keyNames = append(keyNames, *keySchema.AttributeName)
	}
	return strings.Join(keyNames, ", ")
}
