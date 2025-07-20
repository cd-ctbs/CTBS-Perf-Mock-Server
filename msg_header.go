package main

import (
	"errors"
	"strings"
)

const Header_LEN = 158

type MsgHeader struct {
	OrigSender   string
	OrigReceiver string
	OrigSendTime string
	MsgType      string
	MsgId        string
	OrgnlMsgId   string
}

func (m *MsgHeader) BuildHeader() string {
	header := "{H:01"
	header += FillBlankChar(m.OrigSender, 14)
	header += FillBlankChar(m.OrigReceiver, 14)
	header += FillBlankChar(m.OrigSendTime, 14)
	header += FillBlankChar(m.MsgType, 15)
	header += FillBlankChar(m.MsgId, 18)
	header += FillBlankChar(m.OrgnlMsgId, 18)
	header += FillBlankChar("U", 1)
	header += FillBlankChar("", 56)
	header += "}\r\n"
	return header
}

func (m *MsgHeader) ParseHeader(content string) error {

	content = content[strings.Index(content, "{H:01"):]
	if len(content) < Header_LEN {
		errors.New("header len less than 158")
	}
	index := 5

	m.OrigSender = content[index : index+14]
	index += 14

	m.OrigReceiver = content[index : index+14]
	index += 14

	m.OrigSendTime = content[index : index+14]
	index += 14

	m.MsgType = content[index : index+15]
	index += 15

	m.MsgId = content[index : index+18]
	index += 18

	m.OrgnlMsgId = content[index : index+18]
	return nil
}

func FillBlankChar(value string, totalLen int) string {
	value = strings.TrimSpace(value)
	for range totalLen - len(value) {
		value = value + " "
	}
	return value

}
