package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

const referenceMsgIdentifierKey = "d3131a62e0224688b77a506fd333dac4"

var jsonReferenceMsgPrefix string

func init() {
	jsonReferenceMsgPrefix = fmt.Sprintf("{\"identifier\":\"%s\",", referenceMsgIdentifierKey)
}

// ReferenceMsg is what is sent to AWS SQS or AWS SNS in place of hefty message stored in AWS S3.
type ReferenceMsg struct {
	Identifier       string `json:"identifier"` // used to identify a reference message from other types of messages
	S3Region         string `json:"s3_region"`
	S3Bucket         string `json:"s3_bucket"`
	S3Key            string `json:"s3_key"`
	Md5DigestMsgBody string `json:"md5_digest_msg_body"`
	Md5DigestMsgAttr string `json:"md5_digest_msg_attr"`
}

type SNSMessage struct {
	Message string `json:"default"`
}

type SQSMessage struct {
	Message string `json:"Message"`
}

func NewReferenceMsg(s3Region, s3Bucket, s3Key, md5Body, md5Attr string) *ReferenceMsg {
	return &ReferenceMsg{
		Identifier:       referenceMsgIdentifierKey,
		S3Region:         s3Region,
		S3Bucket:         s3Bucket,
		S3Key:            s3Key,
		Md5DigestMsgBody: md5Body,
		Md5DigestMsgAttr: md5Attr,
	}
}

func ToReferenceMsg(msg string) (*ReferenceMsg, error) {
	var refMsg ReferenceMsg
	err := json.Unmarshal([]byte(msg), &refMsg)
	return &refMsg, err
}

func IsReferenceMsg(msg string) bool {
	return strings.HasPrefix(msg, jsonReferenceMsgPrefix)
}
