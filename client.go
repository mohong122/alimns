package alimns

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	//"github.com/mreiferson/go-httpclient"
	"net"
)

const (
	version = "2015-06-06"
)

const (
	DefaultTimeout int64 = 35
)

type Method string


const (
	GET    Method = "GET"
	PUT           = "PUT"
	POST          = "POST"
	DELETE        = "DELETE"
)

type MNSClient interface {
	Send(method Method, headers map[string]string, message interface{}, resource string) (resp *http.Response, err error)
	SetProxy(url string)
}

type AliMNSClient struct {
	Timeout      int64
	url          string
	credential   Credential
	accessKeyId  string
	clientLocker sync.Mutex
	client       *http.Client
	proxyURL     string
}

func NewAliMNSClient(url, accessKeyId, accessKeySecret string) MNSClient {
	if url == "" {
		panic("ali-mns: message queue url is empty")
	}

	credential := NewAliMNSCredential(accessKeySecret)

	aliMNSClient := new(AliMNSClient)
	aliMNSClient.credential = credential
	aliMNSClient.accessKeyId = accessKeyId
	aliMNSClient.url = url

	timeoutInt := DefaultTimeout

	if aliMNSClient.Timeout > 0 {
		timeoutInt = aliMNSClient.Timeout
	}

	timeout := time.Second * time.Duration(timeoutInt)

	var transports http.RoundTripper = &http.Transport{

		Proxy: aliMNSClient.proxy,
		DialContext: (&net.Dialer{
			Timeout:   time.Second * 3,
			DualStack: true,
		}).DialContext,

		ExpectContinueTimeout: timeout,
		ResponseHeaderTimeout: timeout + time.Second,
	}

	aliMNSClient.client = &http.Client{Transport: transports}

	return aliMNSClient
}

func (p *AliMNSClient) SetProxy(url string) {
	p.proxyURL = url
}

func (p *AliMNSClient) proxy(req *http.Request) (*url.URL, error) {
	if p.proxyURL != "" {
		return url.Parse(p.proxyURL)
	}
	return nil, nil
}

func (p *AliMNSClient) authorization(method Method, headers map[string]string, resource string) (authHeader string, err error) {
	if signature, e := p.credential.Signature(method, headers, resource); e != nil {
		return "", e
	} else {
		authHeader = fmt.Sprintf("MNS %s:%s", p.accessKeyId, signature)
	}

	return
}

func (p *AliMNSClient) Send(method Method, headers map[string]string, message interface{}, resource string) (*http.Response, error) {
	var xmlContent []byte
	var resp *http.Response
	var err error
	if message == nil {
		xmlContent = []byte{}
	} else {
		switch m := message.(type) {
		case []byte:
			{
				xmlContent = m
			}
		default:
			if bXml, e := xml.Marshal(message); e != nil {

				return resp, e
			} else {
				xmlContent = bXml
			}
		}
	}

	xmlMD5 := md5.Sum(xmlContent)
	strMd5 := fmt.Sprintf("%x", xmlMD5)

	if headers == nil {
		headers = make(map[string]string)
	}

	headers[MQ_VERSION] = version
	headers[CONTENT_TYPE] = "application/xml"
	headers[CONTENT_MD5] = base64.StdEncoding.EncodeToString([]byte(strMd5))
	headers[DATE] = time.Now().UTC().Format(http.TimeFormat)

	if authHeader, e := p.authorization(method, headers, fmt.Sprintf("/%s", resource)); e != nil {

		return resp, e
	} else {
		headers[AUTHORIZATION] = authHeader
	}

	url := p.url + "/" + resource

	postBodyReader := strings.NewReader(string(xmlContent))

	var req *http.Request

	if req, err = http.NewRequest(string(method), url, postBodyReader); err != nil {

		return resp, err
	}

	for header, value := range headers {
		req.Header.Add(header, value)
	}

	if resp, err = p.client.Do(req); err != nil {

		return resp, err
	}

	return resp, nil
}
