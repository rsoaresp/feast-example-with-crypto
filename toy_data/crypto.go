package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/shopspring/decimal"
)

type Coin struct {
	Symbol    string          `json:"symbol"`
	Price     decimal.Decimal `json:"price"`
	Timestamp time.Time       `json:"timestamp"`
}

func URLEncodeStringList(coins []string) (string, error) {
	jsonBytes, err := json.Marshal(coins)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	encoded := url.QueryEscape(string(jsonBytes))
	return encoded, nil
}

func GetAPIAddress(coins []string) string {
	symbols, _ := URLEncodeStringList(coins)
	return fmt.Sprintf("https://api.binance.com/api/v3/ticker/price?symbols=%s", symbols)
}

func CallAPI(coins []string) (*http.Response, error) {
	url := GetAPIAddress(coins)
	client := &http.Client{Timeout: 20 * time.Second}

	resp, err := client.Get(url)
	return resp, err
}

func Decode(response *http.Response) ([]Coin, error) {

	defer response.Body.Close()

	var coins []Coin
	err := json.NewDecoder(response.Body).Decode(&coins)

	now := time.Now()
	for i := range coins {
		coins[i].Timestamp = now
	}

	return coins, err
}

func GetPrices(coin []string) []Coin {

	api_response, api_err := CallAPI(coin)

	if api_err != nil {
		log.Fatal(api_err)
	}

	c, decode_err := Decode(api_response)

	if decode_err != nil {
		log.Fatal(decode_err)
	}

	return c
}

func SavePrice(db *sql.DB, c Coin) {
	insert := `INSERT INTO price (symbol, price, observed_at) VALUES (?, ?, ?)`
	_, err_insert := db.Exec(insert, c.Symbol, c.Price, c.Timestamp)
	if err_insert != nil {
		log.Fatal(err_insert)
	}
}

func CreatePriceTable(db *sql.DB) error {
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS price (symbol VARCHAR(10) NOT NULL, price NUMERIC(20, 8) NOT NULL, observed_at TIMESTAMP NOT NULL, PRIMARY KEY (symbol, observed_at))`)
	db.Exec("INSTALL delta; LOAD delta;")
	return err
}

func main() {
	db, err := sql.Open("duckdb", "mydb.db")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	create_error := CreatePriceTable(db)
	if create_error != nil {
		log.Fatal(create_error)
	}

	coins := GetPrices([]string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT"})

	for _, coin := range coins {
		SavePrice(db, coin)
	}

}
