package db

import (
	"flag"
	"log"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

var (
	awsProfile = flag.String("profile", "auto_user", "AWS Access Key Profile")
	region     = flag.String("region", "us-east-1", "your AWS region")
)

// Payment represent the payment tables item
type Payment struct {
	Pool    string  `json:"pool"`
	Time    int     `json:"time"`
	Amount  float64 `json:"amount"`
	Receipt bool    `json:"receipt"`
	USD     float64 `json:"usd"`
	XUSD    float64 `json:"x_usd"`
}

// PaymentRepo is the payments repository
type PaymentRepo struct {
	repo *dynamodb.DynamoDB
}

// NewPaymentRepo instantiates PaymentRepo
func NewPaymentRepo() *PaymentRepo {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:  aws.Config{Region: aws.String(*region)},
		Profile: *awsProfile,
	}))
	db := dynamodb.New(sess)
	return &PaymentRepo{repo: db}
}

// FetchPaymentsWithoutExchangeValue stuff
func (pr PaymentRepo) FetchPaymentsWithoutExchangeValue(pool string) []Payment {
	filt := expression.Name("x_usd").AttributeNotExists().And(expression.Name("pool").Equal(expression.Value(pool)))
	proj := expression.NamesList(
		expression.Name("pool"),
		expression.Name("time"),
		expression.Name("amount"),
		expression.Name("receipt"),
		expression.Name("usd"),
	)

	var result []Payment
	expr, err := expression.NewBuilder().WithFilter(filt).WithProjection(proj).Build()
	if err != nil {
		log.Fatal(err)
		return result
	}

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String("payments"),
	}

	// Make the DynamoDB Query API call
	scanResult, scanError := pr.repo.Scan(params)
	if scanError != nil {
		log.Println(scanError)
		return nil
	}

	for _, i := range scanResult.Items {
		item := Payment{}

		err = dynamodbattribute.UnmarshalMap(i, &item)

		if err != nil {
			log.Println(err)
			return nil
		}
		result = append(result, item)
	}
	return result
}

// UpdatePaymentAsProcessedForExchange ...
func (pr PaymentRepo) UpdatePaymentAsProcessedForExchange(item Payment) {

	// Create item in table Movies
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":x": {
				BOOL: aws.Bool(true),
			},
		},
		TableName: aws.String("payments"),
		Key: map[string]*dynamodb.AttributeValue{
			"pool": {
				S: aws.String(item.Pool),
			},
			"time": {
				N: aws.String(strconv.Itoa(item.Time)),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set x_usd = :x"),
	}
	_, err := pr.repo.UpdateItem(input)

	if err != nil {
		log.Println(err.Error())
		return
	}
}

// SumPayment returns the total recieved amount in USD
func (p Payment) SumPayment() float64 {
	return p.Amount * p.USD
}
