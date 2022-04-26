package main

import (
	"context"
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"time"

	db "github.com/influxdata/influxdb-client-go/v2"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Read btc csv values
	file, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)
	data, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Write data into influx
	orgName := os.Getenv("INFLUX_ORG")
	bucketName := os.Getenv("INFLUX_BUCKET")
	url := os.Getenv("INFLUX_URL")
	token := os.Getenv("INFLUX_TOKEN")

	client := db.NewClient(url, token)
	defer client.Close()

	org, err := client.OrganizationsAPI().FindOrganizationByName(context.Background(), orgName)
	if err != nil {
		log.Fatal(err)
	}

	bucket, err := client.BucketsAPI().CreateBucketWithName(context.Background(), org, bucketName)
	if err != nil {
		log.Fatal(err)
	}

	writer := client.WriteAPI(orgName, bucket.Name)

	for i, row := range data {
		if i == 0 {
			continue
		}

		timestamp := time.Unix(ParseInt(row[0]), 0)

		log.Printf("Writing row %v", timestamp)

		writer.WritePoint(db.NewPoint(
			"prices",
			map[string]string{},
			map[string]interface{}{
				"Close":  ParseFloat(row[1]),
				"High":   ParseFloat(row[2]),
				"Low":    ParseFloat(row[3]),
				"Open":   ParseFloat(row[4]),
				"Volume": ParseFloat(row[5]),
			},
			timestamp))
	}

}

func ParseInt(value string) int64 {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return parsed
}

func ParseFloat(value string) float64 {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		log.Fatal(err)
	}
	return parsed
}
