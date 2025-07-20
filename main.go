package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/rivo/tview"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var globalFrom *tview.Form

const FireButtonName = "Fire!"
const CeaseButtonName = "Cease"
const UseTestFun = true

type StatisticsData struct {
	RevMsg121Count uint64
	RevMsg900Count uint64
	SedMsg900Count uint64
	RevMsg990Count uint64
	SedMsg990Count uint64
}

func handleMsg(ctx context.Context, id int, setting *Setting, staticsData *StatisticsData) {
	client, err := NewCFMQClient(setting.Server, setting.Username, setting.Password)
	if err != nil {
		AppLogger.Printf("Worker %d create CFMQ clinet error: %s\n", id, err)
	}
	client.RevQueue = setting.RevQueue
	client.SedQueue = setting.SedQueue
	err = client.CreateQueue(client.SedQueue)
	if err != nil {
		AppLogger.Printf("Worker %d create sed queue error: %s\n", id, err)
	}
	defer client.Logout()
	go client.HeartBeat(ctx)
	client.PullMsg(ctx, func(msgStr string) error {
		revMsgHeader, err := parseMsgHeader(msgStr)
		if err != nil {
			return err
		}

		revMsgBody, err := parseMsgBody(msgStr)
		if err != nil {
			return err
		}
		revMsgBody.MsgType = revMsgHeader.MsgType

		if revMsgHeader.MsgType == "ctbs.990.001.01" {
			atomic.AddUint64(&staticsData.RevMsg990Count, 1)
		} else {
			// reply 990 firstly
			sendMsg990(revMsgHeader, client)
			atomic.AddUint64(&staticsData.SedMsg990Count, 1)

			if revMsgHeader.MsgType == "ctbs.900.001.01" {
				atomic.AddUint64(&staticsData.RevMsg900Count, 1)
			} else {
				// Treat all msg as ctbs.121
				atomic.AddUint64(&staticsData.RevMsg121Count, 1)

				sendMsg900(revMsgHeader, revMsgBody, client)
				atomic.AddUint64(&staticsData.SedMsg900Count, 1)
			}
		}
		return nil
	})
	AppLogger.Printf("Worker %d finished!\n", id)
}

func parseMsgHeader(msgStr string) (*MsgHeader, error) {
	headerStartIndex := strings.Index(msgStr, "{")
	if headerStartIndex < 0 {
		return nil, errors.New("can't find the msg start symbol {")
	}
	headerStr := msgStr[headerStartIndex : headerStartIndex+158]
	revMsgHeader := &MsgHeader{}
	revMsgHeader.ParseHeader(headerStr)
	return revMsgHeader, nil
}

func parseMsgBody(msgStr string) (*BaseMsg, error) {
	baseMsg := &BaseMsg{}
	contentStartIndex := strings.Index(msgStr, "<?xml")
	if contentStartIndex < 0 {
		return nil, errors.New("can't find the xml start symbol <?xml")
	}
	contentStr := msgStr[contentStartIndex:]

	err := xml.Unmarshal([]byte(contentStr), &baseMsg)
	return baseMsg, err

}

func sendMsg990(revMsgHeader *MsgHeader, client *CFMQClient) {
	now := time.Now()
	msgId := buildMsgId()
	msg990header := MsgHeader{
		OrigSender:   revMsgHeader.OrigReceiver,
		OrigReceiver: revMsgHeader.OrigSender,
		OrigSendTime: now.Format("20060102150405"),
		MsgType:      "ctbs.990.001.01",
		MsgId:        msgId,
		OrgnlMsgId:   revMsgHeader.MsgId,
	}
	msg990 := CTBS990Msg{
		MsgId:      msgId,
		OrgnlSndr:  strings.TrimSpace(revMsgHeader.OrigSender),
		OrgnlSndDt: revMsgHeader.OrigSendTime,
		OrgnlMsgId: revMsgHeader.MsgId,
		OrgnlMT:    revMsgHeader.MsgType,
		RtnCd:      "CT010000",
	}

	client.SendMsg(msg990header.BuildHeader() + msg990.Build990Msg())
}

func sendMsg900(revMsgHeader *MsgHeader, revMsgBody *BaseMsg, client *CFMQClient) {
	now := time.Now()
	msgId := buildMsgId()
	header := &MsgHeader{
		OrigSender:   revMsgHeader.OrigReceiver,
		OrigReceiver: revMsgHeader.OrigSender,
		OrigSendTime: now.Format("20060102150405"),
		MsgType:      "ctbs.900.001.01",
		MsgId:        msgId,
		OrgnlMsgId:   revMsgHeader.MsgId,
	}
	res := &CTBS900Msg{
		MsgId:         msgId,
		CreDtTm:       now.Format("2006-01-02T15:04:05"),
		InstgPty:      revMsgBody.InstdPty,
		InstdPty:      revMsgBody.InstgPty,
		OrgnlMsgId:    revMsgBody.MsgId,
		OrgnlInstgPty: revMsgBody.InstgPty,
		OrgnlMT:       revMsgBody.MsgType,
		PrcSts:        "PR00",
	}
	client.SendMsg(header.BuildHeader() + res.Build900Msg())
}

func buildMsgId() string {
	msgId := time.Now().Format("20060102150405")
	randId := strconv.Itoa(rand.Int() % 10000)

	for range 4 - len(randId) {
		msgId += "0"
	}
	msgId += randId
	return msgId
}

func displayStatistics(ctx context.Context, data *StatisticsData, list *tview.TextView, app *tview.Application) {
	for {
		select {
		case <-ctx.Done():
			AppLogger.Printf("Display worker stopping.\n")
			return
		default:
			app.QueueUpdateDraw(func() {
				list.Clear()
				fmt.Fprintf(list, "Rev ctbs.121 [%d]\n", data.RevMsg121Count)
				fmt.Fprintf(list, "Sed ctbs.900 [%d]\n", data.SedMsg900Count)
				fmt.Fprintf(list, "Rev ctbs.900 [%d]\n", data.RevMsg990Count)
				fmt.Fprintf(list, "Sed ctbs.990 [%d]\n", data.SedMsg990Count)
				fmt.Fprintf(list, "Rev ctbs.990 [%d]\n", data.RevMsg900Count)

				fmt.Fprintf(list, "\nCurrent Time is %s\n", time.Now().Format("2006-01-02T15:04:05"))
				time.Sleep(500 * time.Microsecond)
			})

		}
	}
}

func main() {
	var setting = &Setting{}
	setting.Load()
	setting.IsRunning = false

	ctx, cancel := context.WithCancel(context.Background())
	statisticdata := &StatisticsData{}
	var testClient *CFMQClient

	app := tview.NewApplication()

	statisticsList := tview.NewTextView().
		SetDynamicColors(true).
		SetRegions(true).
		SetWrap(true)
	statisticsList.SetBorder(true).SetTitle("Msg Statistics")

	form := tview.NewForm().
		AddInputField("ServerUrl", setting.Server, 50, nil, func(text string) { setting.Server = text }).
		AddInputField("Username", setting.Username, 50, nil, func(text string) { setting.Username = text }).
		AddPasswordField("Password", setting.Password, 50, '*', func(text string) { setting.Password = text }).
		AddInputField("RevQueue", setting.RevQueue, 50, nil, func(text string) { setting.RevQueue = text }).
		AddInputField("SedQueue", setting.SedQueue, 50, nil, func(text string) { setting.SedQueue = text }).
		AddInputField("ThreadCount", setting.ThreadCount, 50, CheckStringIsNumber, func(text string) { setting.ThreadCount = text }).
		AddButton(FireButtonName, func() {
			if globalFrom == nil {
				return
			}
			if setting.IsRunning {
				button := globalFrom.GetButton(globalFrom.GetButtonIndex(CeaseButtonName))
				button.SetLabel(FireButtonName)
				cancel()
				setting.IsRunning = false
			} else {
				button := globalFrom.GetButton(globalFrom.GetButtonIndex(FireButtonName))
				button.SetLabel(CeaseButtonName)
				atoi, _ := strconv.Atoi(setting.ThreadCount)
				for i := 0; i < atoi; i++ {
					go handleMsg(ctx, i, setting, statisticdata)
				}
				go displayStatistics(ctx, statisticdata, statisticsList, app)
				setting.IsRunning = true
			}
		}).
		AddButton("Quit", func() {
			setting.Save()
			if testClient != nil {
				testClient.Logout()
			}
			app.Stop()
		})
	form.SetBorder(true).SetTitle("Settings").SetTitleAlign(tview.AlignCenter)
	if UseTestFun {

		form.AddButton("Test", func() {
			if testClient == nil {
				testClient, _ = NewCFMQClient(setting.Server, "test", setting.Password)
				testClient.SedQueue = setting.RevQueue
				testClient.CreateQueue(testClient.SedQueue)
			}
			testClient.SendMsg(TestMsg)
		})
	}

	globalFrom = form

	flex := tview.NewFlex().
		AddItem(statisticsList, 0, 1, false).
		AddItem(form, 0, 1, true)

	if err := app.SetRoot(flex, true).EnableMouse(true).Run(); err != nil {
		panic(err)
	}
}
