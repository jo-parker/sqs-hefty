package hefty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jo-parker/sqs-hefty/types"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/google/uuid"
	"github.com/jo-parker/sqs-hefty/internal/messages"
	"github.com/jo-parker/sqs-hefty/internal/utils"
)

type SnsClientWrapper struct {
	sns.Client
	bucket         string
	s3Client       *s3.Client
	uploader       *s3manager.Uploader
	downloader     *s3manager.Downloader
	alwaysSendToS3 bool
}

// NewSnsClientWrapper will create a new Hefty SNS client wrapper using an existing AWS SNS client and AWS S3 client.
// This Hefty SNS client wrapper will save large messages greater than MaxSqsSnsMessageLengthBytes to AWS S3 in the
// bucket that is specified via `bucketName`. The S3 client should have the ability of reading and writing to this bucket.
// This function will also check if the bucket exists and is accessible.
func NewSnsClientWrapper(snsClient *sns.Client, s3Client *s3.Client, bucketName string, opts ...Option) (*SnsClientWrapper, error) {
	// check if bucket exits
	if ok, err := utils.BucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("bucket %s does not exist or is not accessible", bucketName)
	}

	wrapper := &SnsClientWrapper{
		Client:     *snsClient,
		bucket:     bucketName,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(s3Client),
		downloader: s3manager.NewDownloader(s3Client),
	}

	// process available options
	var wrapperOptions options
	for _, opt := range opts {
		err := opt(&wrapperOptions)
		if err != nil {
			return nil, err
		}
	}
	wrapper.alwaysSendToS3 = wrapperOptions.alwaysSendToS3

	return wrapper, nil
}

// PublishHeftyMessage will calculate the messages size from `params` and determine if the MaxSqsSnsMessageLengthBytes is exceeded.
// If so, the message is saved in AWS S3 as a hefty message and a reference message is sent to AWS SNS instead.
// If not, the message is directly sent to AWS SNS.
//
// In the case of the reference message being sent, the message itself contains metadata about the hefty message saved in AWS S3
// including bucket name, S3 key, region, and md5 digests. Subscriptions to the AWS SNS topic used in this method should use
// 'Raw Message Delivery' as an option. This ensures that the hefty client can receive messages from these AWS SQS endpoints.
// Other endpoints like AWS Lambda can use the reference message directly and download the S3 message without using the
// hefty client.
//
// Note that this function's signature matches that of the AWS SNS SDK's Publish method.
func (wrapper *SnsClientWrapper) PublishHeftyMessage(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	// input validation; if invalid input let AWS SDK handle it
	if params == nil ||
		params.Message == nil ||
		len(*params.Message) == 0 {

		return wrapper.Publish(ctx, params, optFns...)
	}

	// normalize message attributes
	msgAttributes := messages.MapFromSnsMessageAttributeValues(params.MessageAttributes)

	// calculate message size
	msgSize, err := messages.MessageSize(params.Message, msgAttributes)
	if err != nil {
		return nil, fmt.Errorf("unable to get size of message. %v", err)
	}

	// validate message size
	if !wrapper.alwaysSendToS3 && msgSize <= MaxAwsMessageLengthBytes {
		return wrapper.Publish(ctx, params, optFns...)
	} else if msgSize > MaxHeftyMessageLengthBytes {
		return nil, fmt.Errorf("message size of %d bytes greater than allowed message size of %d bytes", msgSize, MaxHeftyMessageLengthBytes)
	}

	sqsRefMsg := types.SQSMessage{
		Message: *params.Message,
	}

	jsonSQSRefMsg, err := json.Marshal(sqsRefMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal message body to json. %v", err)
	}
	jsonSQSRefMsgString := string(jsonSQSRefMsg)
	params.Message = &jsonSQSRefMsgString

	// create and serialize hefty message
	heftyMsg := messages.NewHeftyMessage(params.Message, msgAttributes, msgSize)
	serialized, bodyOffset, msgAttrOffset, err := heftyMsg.Serialize()
	if err != nil {
		return nil, fmt.Errorf("unable to serialize message. %v", err)
	}

	// create md5 digests
	msgBodyHash := messages.Md5Digest(serialized[bodyOffset:msgAttrOffset])
	msgAttrHash := ""
	if len(heftyMsg.MessageAttributes) > 0 {
		msgAttrHash = messages.Md5Digest(serialized[msgAttrOffset:])
	}

	// create reference message
	refMsg, err := newSnsReferenceMessage(params.TopicArn, wrapper.bucket, wrapper.Options().Region, msgBodyHash, msgAttrHash)
	if err != nil {
		return nil, fmt.Errorf("unable to create reference message from topicArn. %v", err)
	}

	// upload hefty message to s3
	_, err = wrapper.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(wrapper.bucket),
		Key:    aws.String(refMsg.S3Key),
		Body:   bytes.NewReader(serialized),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to upload hefty message to s3. %v", err)
	}

	// replace incoming message body with reference message
	jsonRefMsg, err := json.Marshal(refMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal message to json. %v", err)
	}

	snsRefMsg := types.SNSMessage{
		Message: string(jsonRefMsg),
	}

	jsonSNSRefMsg, err := json.Marshal(snsRefMsg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal message to json. %v", err)
	}

	// Correctly reformat JSON that has been serialised twice
	refMsgStr := strings.ReplaceAll(string(jsonSNSRefMsg), "\n", "")
	refMsgStr = strings.ReplaceAll(refMsgStr, "\\\"", "\"")
	refMsgStr = strings.ReplaceAll(refMsgStr, "\\\\", "\\")

	params.Message = aws.String(refMsgStr)

	// clear out all message attributes
	orgMsgAttr := params.MessageAttributes
	params.MessageAttributes = nil

	// replace overwritten values with original values
	defer func() {
		params.Message = heftyMsg.Body
		params.MessageAttributes = orgMsgAttr
	}()

	log.Printf("%+v", &params)

	out, err := wrapper.Publish(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	return out, err
}

// Example topicArn: arn:aws:sns:us-west-2:765908583888:MyTopic
func newSnsReferenceMessage(topicArn *string, bucketName, region, msgBodyHash, msgAttrHash string) (*types.ReferenceMsg, error) {
	const expectedTokenCount = 6

	if topicArn != nil {
		tokens := strings.Split(*topicArn, ":")
		if len(tokens) != expectedTokenCount {
			return nil, fmt.Errorf("expected %d tokens when splitting topicArn by ':' but received %d", expectedTokenCount, len(tokens))
		} else {
			return types.NewReferenceMsg(
				region,
				bucketName,
				fmt.Sprintf("%s/%s", tokens[4], uuid.New().String()), // S3Key: topicArn/uuid,
				msgBodyHash,
				msgAttrHash), nil
		}
	}

	return nil, errors.New("topicArn is nil")
}
