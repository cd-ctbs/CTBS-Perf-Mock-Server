package main

import (
	"bytes"
	"text/template"
)

type BaseMsg struct {
	MsgId        string `xml:"GrpHdr>MsgId"`
	MsgType      string
	CreDtTm      string `xml:"GrpHdr>CreDtTm"`
	InstgPty     string `xml:"GrpHdr>InstgPty"`
	InstdPty     string ` xml:"GrpHdr>InstdPty"`
	ChckBatchNum string `xml:"ChckBatchNum"`
}

type CTBS900Msg struct {
	MsgId         string `xml:"GrpHdr>MsgId"`
	CreDtTm       string `xml:"GrpHdr>CreDtTm"`
	InstgPty      string `xml:"GrpHdr>InstgPty"`
	InstdPty      string `xml:"GrpHdr>InstdPty"`
	OrgnlMsgId    string `xml:"OrgnlGrpHdr>OrgnlMsgId"`
	OrgnlInstgPty string `xml:"OrgnlGrpHdr>OrgnlInstgPty"`
	OrgnlMT       string `xml:"CmonConfInf>OrgnlMT"`
	PrcSts        string `xml:"CmonConfInf>PrcSts"`
}

type CTBS990Msg struct {
	MsgId      string
	OrgnlSndr  string
	OrgnlSndDt string
	OrgnlMsgId string
	OrgnlMT    string
	RtnCd      string
}

func (msg *CTBS900Msg) Build900Msg() string {
	t, _ := template.New("c900").Parse(template900)

	var tpl bytes.Buffer
	t.Execute(&tpl, msg)

	return tpl.String()
}

var template900 = `<?xml version="1.0" encoding="UTF-8"?>
<MSG>
	<GrpHdr>
		<MsgId> {{- .MsgId -}}</MsgId>
		<CreDtTm> {{- .CreDtTm -}} </CreDtTm>
		<InstgPty> {{- .InstgPty -}} </InstgPty>
		<InstdPty> {{- .InstdPty -}} </InstdPty>
	</GrpHdr>
	<OrgnlGrpHdr>
		<OrgnlMsgId> {{- .OrgnlMsgId -}} </OrgnlMsgId>
		<OrgnlInstgPty> {{- .OrgnlInstgPty -}} </OrgnlInstgPty>
	</OrgnlGrpHdr>
	<CmonConfInf>
		<OrgnlMT> {{- .OrgnlMT -}} </OrgnlMT>
		<PrcSts> {{- .PrcSts -}} </PrcSts>
	</CmonConfInf>
</MSG>`

func (msg *CTBS990Msg) Build990Msg() string {
	t, _ := template.New("c990").Parse(template990)

	var tpl bytes.Buffer
	t.Execute(&tpl, msg)

	return tpl.String()
}

var template990 = `<?xml version="1.0" encoding="UTF-8"?>
<MSG>
	<GrpHdr>
		<MsgId> {{- .MsgId -}} </MsgId>
		<OrgnlSndr> {{- .OrgnlSndr -}} </OrgnlSndr>
		<OrgnlSndDt> {{- .OrgnlSndDt -}} </OrgnlSndDt>
		<OrgnlMsgId> {{- .OrgnlMsgId -}} </OrgnlMsgId>
		<OrgnlMT> {{- .OrgnlMT -}} </OrgnlMT>
		<RtnCd> {{- .RtnCd -}} </RtnCd>
	</GrpHdr>
</MSG>`

var TestMsg = `{H:01090009000004  103001        20250717222525ctbs.121.001.01202704102006723117                  D                                                        }
{S:MEQCIEw7AX27xvTVv8niDMCEWaP/DEItFnjmKGccDcKPHLRiAiAzfjpQ2X49JnhQyZDpWZp0FbtnbtUc6FkUIE/3jO4IlA==}
<?xml version="1.0" encoding="UTF-8"?>
<MSG>
    <GrpHdr>
        <MsgId>202704102006723117</MsgId>
        <CreDtTm>2025-07-17T22:25:25</CreDtTm>
        <InstgPty>090009000004</InstgPty>
        <InstdPty>314391075726</InstdPty>
    </GrpHdr>	
    <OrgnlGrpHdr>
        <OrgnlMsgId>202504281814515912</OrgnlMsgId>
        <OrgnlInstgPty>314391075726</OrgnlInstgPty>
    </OrgnlGrpHdr>
    <CmonConfInf>
        <OrgnlMT>ctbs.121.001.01</OrgnlMT>
        <PrcSts>PR19</PrcSts>
        <RjctInf>报文中的对账批次号[20250111001]与当前系统的对账批次号[20270410001]不一致</RjctInf>
    </CmonConfInf>
</MSG>`
