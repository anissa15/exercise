package main

import (
	"time"
)

type Row struct {
	MarketCenter string    `db:"market_center"`
	Symbol       string    `db:"symbol"`
	Date         time.Time `db:"dt"`
	Time         time.Time `db:"tm"`
	ShortType    string    `db:"short_type"`
	Size         int       `db:"size"`
	Price        float64   `db:"price"`
}
