package main

import (
	"errors"
	"fmt"
	"gdax_pull_transactions_ms/db"
	"log"
	"math"
	"sort"
	"time"

	"github.com/jinzhu/copier"
	gdax "github.com/mynonce/gdax"
)

const float64EqualityThreshold = 1e-8

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}

// CombinedItem ...
type CombinedItem struct {
	time    time.Time
	ledger  *gdax.Ledger
	payment *db.Payment
}

func (ci CombinedItem) String() string {
	pAmt := 0.0
	lAmt := 0.0
	lType := ""
	if ci.payment != nil {
		pAmt = ci.payment.Amount
	}
	if ci.ledger != nil {
		lAmt = ci.ledger.Amount
		lType = ci.ledger.Type
	}
	return fmt.Sprintf("Time: %s\n\tLedger: %f(%s)\n\tPayment: %f\n", ci.time.Format(time.RFC3339), lAmt, lType, pAmt)
}

// ByTime ...
type ByTime []CombinedItem

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].time.Before(a[j].time) }

// NewCombinedLedger will fetch payments from PaymentRepo, fetch GDAX ledger and create a
// combined cronological list of events
func NewCombinedLedger(pool, product string) []CombinedItem {
	items := []CombinedItem{}
	paymentRepo := db.NewPaymentRepo()
	payments := paymentRepo.FetchPaymentsWithoutExchangeValue(pool)
	for _, p := range payments {
		newP := &db.Payment{}
		copier.Copy(&newP, p)
		item := CombinedItem{time: time.Unix(int64(p.Time), 0), payment: newP}
		items = append(items, item)
	}

	if len(payments) == 0 {
		return items
	}

	// need to know how far back we need to go
	firstPayemntTime := time.Unix(int64(payments[0].Time), 0)
	ledger := gdax.GDAXClient.LedgerClient.PullTransactionsByProduct(product, firstPayemntTime)
	for _, l := range ledger {
		newL := &gdax.Ledger{}
		copier.Copy(&newL, l)
		item := CombinedItem{time: l.CreatedAt, ledger: newL}
		items = append(items, item)
	}

	sort.Sort(ByTime(items))
	return items
}

// NewExchangeLedger take a order combinedLedger and sums up payments and totals from orders to create
// a ExchangeLedger
func NewExchangeLedger(combinedLedger []CombinedItem, product string) db.ExchangeLedger {
	result := db.ExchangeLedger{}

	i := 0
	for {
		if i >= len(combinedLedger) {
			return result
		}

		// WAY TOO MANY ASSUMPTIONS HERE !!!!!!
		// Should begin with a payment item
		// 1. get a list of payments (usually 1 but can be more than 1)
		// 2. get a list of orders (usually 1 but can be more than 1)
		wPayments, next := getPayments(combinedLedger, i)
		// at this point there are no new payments, exit
		if len(wPayments) == 0 {
			return result
		}
		wOrders, next := getOrders(combinedLedger, *next)
		// at this point we may have payments but not yet exchanged them
		if len(wOrders) == 0 {
			return result
		}
		i = *next

		paymentSum := getPaymentUSDSum(wPayments)
		orderSum := getOrderUSDSum(wOrders)
		orderTime := getOrderTime(wOrders)

		exchangeTrans := db.ExchangeTransaction{
			Time:          orderTime,
			Product:       product,
			Payments:      wPayments,
			Orders:        wOrders,
			MinedValue:    paymentSum,
			ExchangeValue: orderSum,
		}
		result = append(result, exchangeTrans)
	}
}

func getPayments(combinedLedger []CombinedItem, start int) ([]db.Payment, *int) {
	result := []db.Payment{}
	rIdx := -1
	for i := start; i < len(combinedLedger); i++ {
		cl := combinedLedger[i]
		if cl.payment == nil {
			return result, &i
		}
		rIdx = i
		result = append(result, *cl.payment)
	}
	return result, &rIdx
}

func getOrders(combinedLedger []CombinedItem, start int) ([]gdax.Ledger, *int) {
	result := []gdax.Ledger{}
	rIdx := -1
	for i := start; i < len(combinedLedger); i++ {
		cl := combinedLedger[i]
		if cl.ledger == nil {
			return result, &i
		}
		if cl.ledger.Type == "transfer" {
			continue
		}
		rIdx = i
		result = append(result, *cl.ledger)
	}
	return result, &rIdx
}

func getOrderUSDSum(orders []gdax.Ledger) float64 {
	sum := 0.0
	orderid, err := validateOrderID(orders)
	if err != nil {
		log.Fatal(err)
		return sum
	}

	var fills []gdax.Fill
	cursor := gdax.GDAXClient.FillsClient.ListFills(orderid, "")
	for cursor.HasMore {
		if err = cursor.NextPage(&fills); err != nil {
			log.Fatal(err)
			return sum
		}
		for _, f := range fills {
			sum += f.SumOrder()
		}
	}
	return sum
}

func validateOrderID(orders []gdax.Ledger) (string, error) {
	var orderid string
	var err error
	for _, l := range orders {
		if orderid == "" {
			orderid = l.Details.OrderID
			continue
		}
		// validate the rest are the same orderid
		if orderid != l.Details.OrderID {
			err = errors.New("Order ID is busted")
		}
	}
	return orderid, err
}

func getPaymentUSDSum(payments []db.Payment) float64 {
	sum := 0.0
	for _, p := range payments {
		sum += p.SumPayment()
	}
	return sum
}

func getOrderTime(orders []gdax.Ledger) time.Time {
	t := time.Now()
	if len(orders) == 0 {
		return t
	}
	return orders[len(orders)-1].CreatedAt
}
