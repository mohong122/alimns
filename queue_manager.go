package alimns

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"errors"
)

type MNSLocation string

const (
	Beijing   MNSLocation = "cn-beijing"
	Hangzhou  MNSLocation = "cn-hangzhou"
	Qingdao   MNSLocation = "cn-qingdao"
	Singapore MNSLocation = "ap-southeast-1"
)

type AliQueueManager interface {
	CreateQueue(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	SetQueueAttributes(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	GetQueueAttributes(location MNSLocation, queueName string) (attr QueueAttribute, err error)
	DeleteQueue(location MNSLocation, queueName string) (err error)
	ListQueue(location MNSLocation, nextMarker Base64Bytes, retNumber int32, prefix string) (queues Queues, err error)
}

type MNSQueueManager struct {
	ownerId         string
	credential      Credential
	accessKeyId     string
	accessKeySecret string

	decoder MNSDecoder
}

func checkQueueName(queueName string) error {
	if len(queueName) > 256 {

		return errors.New("队列名最大 256")
	}
	return nil
}

func checkDelaySeconds(seconds int32) error {
	if seconds > 60480 || seconds < 0 {
		return errors.New("queue delay 取值范围为 (0~60480)")
	}
	return nil
}

func checkMaxMessageSize(maxSize int32) error {
	if maxSize < 1024 || maxSize > 65536 {
		return errors.New("max message 取值范围为 (1024~65536)")
	}
	return nil
}

func checkMessageRetentionPeriod(retentionPeriod int32) error {
	if retentionPeriod < 60 || retentionPeriod > 1296000 {
		return errors.New("retentionPeriod 取值范围为 60~1296000")
	}
	return nil
}

func checkVisibilityTimeout(visibilityTimeout int32) error {
	if visibilityTimeout < 1 || visibilityTimeout > 43200 {

		return errors.New("visibilityTimeout 取值范围为 1~43200")
	}
	return nil
}

func checkPollingWaitSeconds(pollingWaitSeconds int32) error {
	if pollingWaitSeconds < 0 || pollingWaitSeconds > 30 {
		return errors.New("visibilityTimeout 取值范围为 0~30")

	}
	return nil
}

func NewMNSQueueManager(ownerId, accessKeyId, accessKeySecret string) AliQueueManager {
	return &MNSQueueManager{
		ownerId:         ownerId,
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
		decoder:         new(AliMNSDecoder),
	}
}

func checkAttributes(delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) error {
	if err := checkDelaySeconds(delaySeconds); err != nil {
		return err
	}
	if err := checkMaxMessageSize(maxMessageSize); err != nil {
		return err
	}
	if err := checkMessageRetentionPeriod(messageRetentionPeriod); err != nil {
		return err
	}
	if err := checkVisibilityTimeout(visibilityTimeout); err != nil {
		return err
	}
	if err := checkPollingWaitSeconds(pollingWaitSeconds); err != nil {
		return err
	}
	return nil
}

func (p *MNSQueueManager) CreateQueue(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) error {
	queueName = strings.TrimSpace(queueName)

	if err := checkQueueName(queueName); err != nil {
		return err
	}

	if err := checkAttributes(delaySeconds,
		maxMessageSize,
		messageRetentionPeriod,
		visibilityTimeout,
		pollingWaitSeconds); err != nil {
		return err
	}

	message := CreateQueueRequest{
		DelaySeconds:           delaySeconds,
		MaxMessageSize:         maxMessageSize,
		MessageRetentionPeriod: messageRetentionPeriod,
		VisibilityTimeout:      visibilityTimeout,
		PollingWaitSeconds:     pollingWaitSeconds,
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	code, err := send(cli, p.decoder, PUT, nil, &message, "queues/"+queueName, nil)

	if code == http.StatusNoContent {

		return errors.New("队列已存在")
	}

	return err
}

func (p *MNSQueueManager) SetQueueAttributes(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) error {
	queueName = strings.TrimSpace(queueName)

	if err := checkQueueName(queueName); err != nil {
		return err
	}

	if err := checkAttributes(delaySeconds,
		maxMessageSize,
		messageRetentionPeriod,
		visibilityTimeout,
		pollingWaitSeconds); err != nil {
		return err
	}

	message := CreateQueueRequest{
		DelaySeconds:           delaySeconds,
		MaxMessageSize:         maxMessageSize,
		MessageRetentionPeriod: messageRetentionPeriod,
		VisibilityTimeout:      visibilityTimeout,
		PollingWaitSeconds:     pollingWaitSeconds,
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err := send(cli, p.decoder, PUT, nil, &message, fmt.Sprintf("queues/%s?metaoverride=true", queueName), nil)
	return err
}

func (p *MNSQueueManager) GetQueueAttributes(location MNSLocation, queueName string) (QueueAttribute, error) {
	queueName = strings.TrimSpace(queueName)

	attr := QueueAttribute{}
	if err := checkQueueName(queueName); err != nil {
		return attr, err
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err := send(cli, p.decoder, GET, nil, nil, "queues/"+queueName, &attr)

	return attr, err
}

func (p *MNSQueueManager) DeleteQueue(location MNSLocation, queueName string) error {
	queueName = strings.TrimSpace(queueName)

	if err := checkQueueName(queueName); err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err := send(cli, p.decoder, DELETE, nil, nil, "queues/"+queueName, nil)

	return err
}

func (p *MNSQueueManager) ListQueue(location MNSLocation, nextMarker Base64Bytes, retNumber int32, prefix string) (Queues, error) {

	queues := Queues{}
	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	header := map[string]string{}

	marker := ""
	if nextMarker != nil && len(nextMarker) > 0 {
		marker = strings.TrimSpace(string(nextMarker))
		if marker != "" {
			header["x-mns-marker"] = marker
		}
	}

	if retNumber > 0 {
		if retNumber >= 1 && retNumber <= 1000 {
			header["x-mns-ret-number"] = strconv.Itoa(int(retNumber))
		} else {

			return queues, errors.New("请求的参数错误")
		}
	}

	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		header["x-mns-prefix"] = prefix
	}

	_, err := send(cli, p.decoder, GET, header, nil, "queues", &queues)

	return queues, err
}
