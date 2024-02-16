package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func handler(request events.S3Event) {
	// Initialize AWS session
	sess := session.Must(session.NewSession())

	// Create S3 service client
	s3Svc := s3.New(sess)

	for _, record := range request.Records {
		// Get CSV file from S3
		csvInput := &s3.GetObjectInput{
			Bucket: aws.String(record.S3.Bucket.Name),
			Key:    aws.String(record.S3.Object.Key),
		}
		csvResult, err := s3Svc.GetObject(csvInput)
		if err != nil {
			log.Printf("Failed to get object from S3: %s", err)
			continue
		}

		// Read CSV data
		csvData, err := ioutil.ReadAll(csvResult.Body)
		if err != nil {
			log.Printf("Failed to read CSV data: %s", err)
			continue
		}

		// Parse CSV
		reader := csv.NewReader(bytes.NewReader(csvData))
		csvRecords, err := reader.ReadAll()
		if err != nil {
			log.Printf("Failed to parse CSV: %s", err)
			continue
		}

		// Process CSV records and create JSON
		var jsonData []map[string]string
		for _, csvRecord := range csvRecords {
			// Assuming CSV structure, adjust this based on your CSV schema
			data := map[string]string{
				"field1": csvRecord[0],
				"field2": csvRecord[1],
				// Add more fields as needed
			}
			jsonData = append(jsonData, data)
		}

		// Convert JSON data to byte slice
		jsonBytes, err := json.Marshal(jsonData)
		if err != nil {
			log.Printf("Failed to marshal JSON: %s", err)
			continue
		}

		// Upload JSON to S3
		jsonInput := &s3.PutObjectInput{
			Bucket: aws.String("dataoutputs"),
			Key:    aws.String("output.json"),
			Body:   bytes.NewReader(jsonBytes),
		}
		_, err = s3Svc.PutObject(jsonInput)
		if err != nil {
			log.Printf("Failed to upload JSON to S3: %s", err)
			continue
		}

		log.Println("JSON data uploaded successfully.")
	}
}

func main() {
	lambda.Start(handler)
}
