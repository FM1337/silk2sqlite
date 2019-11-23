package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// db is our global database variable
var db *sql.DB

// rwcut is our rwcut output structure for inserting into the database
type rwcut struct {
	SourceIP        string
	DestinationIP   string
	SourcePort      int
	DestinationPort int
	Protocol        int
	Packets         int
	Bytes           int
	Flags           string
	StartTime       string
	Duration        float64
	EndTime         string
	Sensor          string
}

// initDB initializes the database connection
func initDB(databaseFile string) {
	var err error
	db, err = sql.Open("sqlite3", databaseFile)
	if err != nil {
		panic(err)
	}
	fmt.Println("Initalized connection with " + databaseFile + "!")
}

// createTable creates the netflow table if it doesn't already exist
func createTable() {
	statement := "create table IF NOT EXISTS netflow (id integer primary key AUTOINCREMENT, source_address text not null, destination_address text not null, source_port integer not null, destination_port integer not null, protocol integer not null, packets interger not null, bytes interger not null, flags text not null, start_time text not null, duration real not null, end_time text not null, sensor text not null);"
	_, err := db.Exec(statement)
	if err != nil {
		panic(err)
	}
	fmt.Println()
}

// parseRwcutOutput parses the table like format output from rwcut and puts each row into a list to be imported.
func parseRwcutOutput(file string) []rwcut {
	data := []rwcut{}
	in, err := os.Open(file)
	if err != nil {
		panic(err)
	}
	defer in.Close()
	scanner := bufio.NewScanner(in)
	skipFirst := true
	for scanner.Scan() {
		if skipFirst {
			skipFirst = false
			continue
		}
		record := strings.Split(scanner.Text(), "|")
		tmpRwcut := rwcut{
			SourceIP:      strings.TrimSpace(record[0]),
			DestinationIP: strings.TrimSpace(record[1]),
			Flags:         strings.TrimSpace(record[7]),
			StartTime:     strings.TrimSpace(record[8]),
			EndTime:       strings.TrimSpace(record[10]),
			Sensor:        strings.TrimSpace(record[11]),
		}

		sport, err := strconv.Atoi(strings.TrimSpace(record[2]))
		if err != nil {
			panic(err)
		}
		tmpRwcut.SourcePort = sport
		dport, err := strconv.Atoi(strings.TrimSpace(record[3]))
		if err != nil {
			panic(err)
		}
		tmpRwcut.DestinationPort = dport
		proto, err := strconv.Atoi(strings.TrimSpace(record[4]))
		if err != nil {
			panic(err)
		}
		tmpRwcut.Protocol = proto
		packets, err := strconv.Atoi(strings.TrimSpace(record[5]))
		if err != nil {
			panic(err)
		}
		tmpRwcut.Packets = packets
		bytes, err := strconv.Atoi(strings.TrimSpace(record[6]))
		if err != nil {
			panic(err)
		}
		tmpRwcut.Bytes = bytes
		duration, err := strconv.ParseFloat(strings.TrimSpace(record[9]), 32)
		if err != nil {
			panic(err)
		}
		tmpRwcut.Duration = duration

		data = append(data, tmpRwcut)
	}
	return data
}

// insertData inserts the rwcut data into the database
func insertData(data []rwcut) {
	transaction, err := db.Begin()
	if err != nil {
		panic(err)
	}
	statement, err := transaction.Prepare("insert into netflow (source_address, destination_address, source_port, destination_port, protocol, packets, bytes, flags, start_time, duration, end_time, sensor) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}
	defer statement.Close()
	for _, record := range data {
		_, err := statement.Exec(record.SourceIP, record.DestinationIP, record.SourcePort, record.DestinationPort, record.Protocol, record.Packets, record.Bytes, record.Flags, record.StartTime, record.Duration, record.EndTime, record.Sensor)
		if err != nil {
			panic(err)
		}
	}
	transaction.Commit()

	fmt.Println("Data has been inserted!")
}

func main() {
	launchArgs := os.Args
	if len(launchArgs) != 3 {
		fmt.Println("You're missing some launch arugments!")
		fmt.Println("Example usage: ./silk2sqlite flow.db flow_from_rwcut.txt")
		os.Exit(1)
	}
	initDB(launchArgs[1])
	createTable()
	data := parseRwcutOutput(launchArgs[2])
	fmt.Println("Inserting flow records from " + launchArgs[2] + " into database now!")
	insertData(data)
	db.Close()
	fmt.Println("Importing of silk rwcut netflow data into database has finished!")
}
