package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
)

type Setting struct {
	Server      string `json:"server"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	RevQueue1   string `json:"rev_queue1"`
	RevQueue2   string `json:"rev_queue2"`
	SedQueue    string `json:"sed_queue"`
	ThreadCount string `json:"thread_count"`
	SleepTime   string `json:"sleep_time"`
	IsRunning   bool   `json:"-"`
}

const savedfile = "settings.json"

func (s *Setting) Load() {

	if _, err := os.Stat(savedfile); err == nil {
		file, err := os.ReadFile(savedfile)
		if err != nil {
			log.Fatal("Error reading setting file: ", err)
		}
		json.Unmarshal(file, &s)
	}
}

func (s *Setting) Save() {
	indent, err := json.MarshalIndent(s, "", " ")
	if err != nil {
		log.Fatal("Error saving settings: ", err)
	}
	os.WriteFile(savedfile, indent, 0644)
}

func CheckStringIsNumber(textToCheck string, lastChar rune) bool {
	if len(textToCheck) == 0 {
		return true
	}
	num, err := strconv.Atoi(textToCheck)
	if err != nil {
		return false
	}

	return num > 0 && num <= 100
}
