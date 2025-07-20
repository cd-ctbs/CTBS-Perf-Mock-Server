package main

import (
	"log"
	"os"
)

var (
	AppLogger *log.Logger // Important information
)

func init() {
	f, _ := os.OpenFile("log.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	AppLogger = log.New(f,
		"INFO: ",
		log.Ldate|log.Lmicroseconds|log.Lshortfile)
}
