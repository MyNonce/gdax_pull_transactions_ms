package main

import (
	"gdax_pull_transactions_ms/db"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
)

var ()

// HandleRequest is what does the thing it does
func HandleRequest() {
	log.Println("Handler started")
	//	Sourcing payments from the database, combine GDAX transactions
	cl := NewCombinedLedger(os.Getenv("pool"), os.Getenv("product"))
	for _, l := range cl {
		log.Println(l)
	}

	// Sum up payments for orders to create an ExchangeTransaction
	et := NewExchangeLedger(cl, os.Getenv("product"))
	for _, l := range et {
		log.Println(l)
	}

	// Persist Exchange Transactions
	exchangeLedgeRepo := db.NewExchangeLedgerRepo()
	exchangeLedgeRepo.Write(et)

	// Update Payments so we don't process them next time
	paymentRepo := db.NewPaymentRepo()
	for _, ex := range et {
		for _, p := range ex.Payments {
			paymentRepo.UpdatePaymentAsProcessedForExchange(p)
		}
	}
}

func main() {
	lambda.Start(HandleRequest)
}
