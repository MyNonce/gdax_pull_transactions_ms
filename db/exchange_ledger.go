package db

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

	gdax "github.com/mynonce/gdax"
)

// ExchangeTransaction ...
type ExchangeTransaction struct {
	Time          time.Time `dynamodbav:"time,unixtime"`
	Product       string    `dynamodbav:"product,string"`
	MinedValue    float64
	ExchangeValue float64
	Payments      []Payment
	Orders        []gdax.Ledger
}

func (t ExchangeTransaction) String() string {
	return fmt.Sprintf("Exchange Transaction (%s):\n\tMined Value: %f\n\tExchange Value: %f\n",
		t.Time.Format(time.RFC3339), t.MinedValue, t.ExchangeValue)
}

// ExchangeLedger ...
type ExchangeLedger []ExchangeTransaction

// ExchangeLedgerRepo is the ExchangeLedger repository
type ExchangeLedgerRepo struct {
	repo *dynamodb.DynamoDB
}

// NewExchangeLedgerRepo instantiates ExchangeLedgerRepo
func NewExchangeLedgerRepo() *ExchangeLedgerRepo {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(*region)},
		Profile: *awsProfile,
	}))
	db := dynamodb.New(sess)
	return &ExchangeLedgerRepo{repo: db}
}

// Write the ExchangeLedgers documents
func (el ExchangeLedgerRepo) Write(ledger ExchangeLedger) error {
	for _, l := range ledger {
		av, err := dynamodbattribute.MarshalMap(l)
		if err != nil {
			log.Fatal("write:", err)
			return err
		}
		_, err = el.repo.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String("gdax_exchange"),
			Item:      av,
		})
		if err != nil {
			panic(fmt.Sprintf("failed to put Record to DynamoDB, %v", err))
		}
	}
	return nil
}
