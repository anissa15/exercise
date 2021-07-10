package main

import (
	"archive/zip"
	"bufio"
	"log"
	"strconv"
	"strings"
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

func main() {
	// open zip file
	zipFileName := "files/FNSQsh202105_1.zip"
	zr, err := zip.OpenReader(zipFileName)
	if err != nil {
		log.Fatalln("failed to read", zipFileName, "with error:", err)
	}
	defer zr.Close()

	// read each file inside zip file
	for _, f := range zr.File {
		file, err := f.Open()
		if err != nil {
			log.Fatalln("failed to open", f.Name, "with error:", err)
		}
		defer file.Close()

		// read file by line
		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {

			// convert line into Row struct
			t := strings.Split(scanner.Text(), "|")
			date, err := time.Parse("20060102", t[2])
			if err != nil {
				continue
			}
			time, err := time.Parse("15:04:05", t[3])
			if err != nil {
				continue
			}
			size, err := strconv.Atoi(t[5])
			if err != nil {
				continue
			}
			price, err := strconv.ParseFloat(t[6], 64)
			if err != nil {
				continue
			}
			row := Row{
				MarketCenter: t[0],
				Symbol:       t[1],
				Date:         date,
				Time:         time,
				ShortType:    t[4],
				Size:         size,
				Price:        price,
			}
			log.Println("ROW:", row)
		}
	}
}
