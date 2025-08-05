package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const TOKEN = "CFMQ-Token"
const DESTINATION = "CFMQ-Destination"

type HandleReceivedMsgFunc func(msgStr string) error

type CFMQClient struct {
	ServerUrl string
	UserName  string
	Password  string
	Token     string
	RevQueue  string
	SedQueue  string
}

type CFMQResponse struct {
	Code int
	Msg  string
	Data map[string]interface{}
}

type Ack struct {
	ServerUrl   string
	Token       string
	Destination string
	SessionId   string
	SeqId       string
}

func NewCFMQClient(serverUrl string, userName string, password string) (*CFMQClient, error) {
	headers := make(map[string]string)
	headers["CFMQ-Username"] = userName
	headers["CFMQ-Password"] = password
	AppLogger.Print("[CFMQ] Login")
	res, err := doHttpRequest(serverUrl+"/login", headers)
	if err != nil {
		return nil, err
	}

	newClient := &CFMQClient{
		ServerUrl: serverUrl,
		UserName:  userName,
		Password:  password,
		Token:     res.Data[TOKEN].(string),
	}
	AppLogger.Printf("[CFMQ] got token: %s", newClient.Token)
	return newClient, nil
}

func (c *CFMQClient) CreateQueue(queueName string) error {
	headers := make(map[string]string)
	headers[TOKEN] = c.Token
	headers[DESTINATION] = queueName
	headers["CFMQ-Address-Type"] = "queue"
	AppLogger.Print("[CFMQ] Create Queue")
	_, err := doHttpRequest(c.ServerUrl+"/destination/create", headers)
	if err != nil {
		AppLogger.Printf("[CFMQ] Create Queue error: %s\n", err)
		return err
	}
	return nil
}

func (c *CFMQClient) SendMsg(msg string, bookOrgCode string) error {
	AppLogger.Printf("[CFMQ] sending message: %s", msg)
	headers := make(map[string]string)
	headers[TOKEN] = c.Token
	headers[DESTINATION] = c.SedQueue
	headers["Content-Type"] = "text/plain"
	headers["CFMQ-Msg-Property-bookorgcode"] = bookOrgCode
	res, err := doHttpRequestWithBody(c.ServerUrl+"/queue/send", headers, msg)
	if err != nil {
		AppLogger.Printf("[CFMQ] Error sending message: %s", err)
		return err
	}
	if res.Code != 0 {
		AppLogger.Printf("[CFMQ] Res error code is %d when sending msg.", res.Code)
		return errors.New(res.Msg)
	}
	return nil
}

func (c *CFMQClient) PullMsg(ctx context.Context, afterPullFunc HandleReceivedMsgFunc) {
	for {
		select {
		case <-ctx.Done():
			AppLogger.Printf("[CFMQ] pull msg got cancel done")
			return
		case <-time.After(200 * time.Microsecond):
			AppLogger.Printf("[CFMQ] pulling msg")
			msg, ack, err := c.doPullMsg()
			if err != nil {
				AppLogger.Printf("[CFMQ] pull msg error: %v", err)
				continue
			}

			if msg == "{\"code\": 114, \"msg\": \"pull msg timeout, no assigned msg now\"}" {
				AppLogger.Printf("[CFMQ] no assigned msg now")
				continue
			}
			if msg == "{\"code\": 100, \"msg\": \"invalid CFMQ-Token in header\"}" {
				AppLogger.Printf("[CFMQ] token expire, re-login again")

				c, err = NewCFMQClient(c.ServerUrl, c.UserName, c.Password)
				if err != nil {
					AppLogger.Printf("[CFMQ] re-login error: %v", err)
					continue
				}

				continue
			}
			AppLogger.Printf("[CFMQ] got msg: %s", msg)

			err = afterPullFunc(msg)
			if err != nil {
				AppLogger.Printf("[CFMQ] handle msg error: %v", err)
			}
			ack.Confirm() //Always ack the msg, due to the unknown msg can't be handled by other consumer.
		}
	}
}

func (c *CFMQClient) doPullMsg() (string, *Ack, error) {
	headers := make(map[string]string)
	headers[TOKEN] = c.Token
	headers[DESTINATION] = c.RevQueue
	headers["CFMQ-Address-Type"] = "queue"
	headers["CFMQ-Auto-Subscribe-Destination"] = "true"
	str, header, err := doHttpRequestRetStr(c.ServerUrl+"/msg/pull", headers)
	if err != nil {
		return "", nil, err
	}
	ack := &Ack{
		ServerUrl:   c.ServerUrl,
		Token:       header.Get("CFMQ-Subscription-Token"),
		Destination: header.Get("CFMQ-Destination"),
		SessionId:   header.Get("CFMQ-Session-ID"),
		SeqId:       header.Get("CFMQ-Sequence-ID"),
	}
	return str, ack, nil
}

func (c *CFMQClient) HeartBeat(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			AppLogger.Printf("[CFMQ] Heart Beat got cancel done")
			return
		case <-time.After(60 * time.Second):
			c.doHeartBeat()
		}
	}
}

func (c *CFMQClient) doHeartBeat() error {

	headers := make(map[string]string)
	headers[TOKEN] = c.Token

	AppLogger.Print("[CFMQ] Heart Beat")
	res, err := doHttpRequest(c.ServerUrl+"/heartbeat", headers)
	if err != nil {
		return err
	}
	if res.Msg != "success" {
		return errors.New(res.Msg)
	}
	return nil
}

func (c *CFMQClient) Logout() error {
	headers := make(map[string]string)
	headers[TOKEN] = c.Token

	AppLogger.Printf("[CFMQ] Logout, token is %s", c.Token)
	_, err := doHttpRequest(c.ServerUrl+"/logout", headers)
	if err != nil {
		return err
	}

	return nil
}

func (a *Ack) Confirm() error {
	headers := make(map[string]string)
	headers[TOKEN] = a.Token
	headers[DESTINATION] = a.Destination
	headers["CFMQ-Session-ID"] = a.SessionId
	headers["CFMQ-Sequence-ID"] = a.SeqId
	headers["CFMQ-End-Sequence-ID"] = a.SeqId
	AppLogger.Printf("[CFMQ] Confirm Ack")
	res, err := doHttpRequest(a.ServerUrl+"/msg/ack", headers)
	if err != nil {
		return err
	}
	if res.Msg != "success" {
		return errors.New(res.Msg)
	}
	return nil
}

func doHttpRequest(url string, headers map[string]string) (*CFMQResponse, error) {
	str, _, err := doHttpRequestRetStr(url, headers)
	if err != nil {
		return nil, err
	}
	target := &CFMQResponse{}
	err = json.Unmarshal([]byte(str), target)
	if err != nil {
		return nil, err
	}

	if target.Code != 0 {
		return nil, errors.New(target.Msg)
	}

	return target, nil
}

func doHttpRequestRetStr(url string, headers map[string]string) (string, http.Header, error) {
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return "", nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, err
	}

	defer res.Body.Close()
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", nil, err
	}
	return string(bodyBytes), res.Header, nil
}

func doHttpRequestWithBody(url string, headers map[string]string, body string) (*CFMQResponse, error) {
	reader := strings.NewReader(body)
	req, err := http.NewRequest(http.MethodPost, url, reader)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	response := &CFMQResponse{}
	err = json.NewDecoder(res.Body).Decode(response)
	return response, err
}
