package hefty

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const (
	MaxSqsMessageLengthBytes        = 262_144
	MaxHeftyMessageLengthBytes      = 26_214_400
	heftyClientVersionMessageKey    = "hefty-client-version"
	receiptHandlePrefix             = "hefty-message"
	expectedReceiptHandleTokenCount = 4
)

type SqsClientWrapper struct {
	sqs.Client
	bucket     string
	s3Client   *s3.Client
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
}

// NewSqsClientWrapper will create a new Hefty SQS client wrapper using an existing AWS SQS client and AWS S3 client.
// This Hefty SQS client wrapper will save large messages greater than MaxSqsMessageLengthBytes to AWS S3 in the
// bucket that is specified via `bucketName`. This function will also check if the bucket exists and is accessible.
func NewSqsClientWrapper(sqsClient *sqs.Client, s3Client *s3.Client, bucketName string) (*SqsClientWrapper, error) {
	// check if bucket exits
	if ok, err := bucketExists(s3Client, bucketName); !ok {
		if err != nil {
			return nil, err
		}

		return nil, fmt.Errorf("bucket %s does not exist or is not accessible", bucketName)
	}

	return &SqsClientWrapper{
		Client:     *sqsClient,
		bucket:     bucketName,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(s3Client),
		downloader: s3manager.NewDownloader(s3Client),
	}, nil
}

// SendHeftyMessage will calculate the messages size from `params` and determine if the message is large and should
// be saved in AWS S3 if the MaxSqsMessageLengthBytes is exceeded.
// Note that this function's signature matches that of the AWS SDK's SendMessage function.
func (client *SqsClientWrapper) SendHeftyMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	// input validation; if invalid input let AWS SDK handle it
	if params == nil ||
		params.MessageBody == nil ||
		len(*params.MessageBody) == 0 {

		return client.SendMessage(ctx, params, optFns...)
	}

	// calculate message size
	size, err := msgSize(params)
	if err != nil {
		return nil, fmt.Errorf("unable to check message size. %v", err)
	}

	// validate message size
	if size <= MaxSqsMessageLengthBytes {
		return client.SendMessage(ctx, params, optFns...)
	} else if size > MaxHeftyMessageLengthBytes {
		return nil, fmt.Errorf("message size of %d bytes greater than allowed message size of %d bytes", size, MaxHeftyMessageLengthBytes)
	}

	// create large message
	largeMsg := &largeSqsMsg{
		Body:              params.MessageBody,
		MessageAttributes: params.MessageAttributes,
	}

	// serialize large message
	serialized, bodyHash, attributesHash := largeMsg.Serialize(size)

	// create reference message
	refMsg, err := newSqsReferenceMessage(params.QueueUrl, client.bucket, client.Options().Region, bodyHash, attributesHash)
	if err != nil {
		return nil, fmt.Errorf("unable to create reference message from queueUrl. %v", err)
	}

	// upload large message to s3
	_, err = client.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(client.bucket),
		Key:    aws.String(refMsg.S3Key),
		Body:   bytes.NewReader(serialized),
	})
	if err != nil {
		return nil, fmt.Errorf("unable to upload large message to s3. %v", err)
	}

	// replace incoming message body with reference message
	jsonRefMsg, err := json.MarshalIndent(refMsg, "", "\t")
	if err != nil {
		return nil, fmt.Errorf("unable to marshal json message. %v", err)
	}
	params.MessageBody = aws.String(string(jsonRefMsg))

	//TODO: get correct library version
	// overwrite message attributes (if any) with hefty message attributes
	params.MessageAttributes = make(map[string]sqsTypes.MessageAttributeValue)
	params.MessageAttributes[heftyClientVersionMessageKey] = sqsTypes.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String("v0.1")}

	out, err := client.SendMessage(ctx, params, optFns...)
	if err != nil {
		return out, err
	}

	// overwrite md5 values
	out.MD5OfMessageBody = aws.String(bodyHash)
	out.MD5OfMessageAttributes = aws.String(attributesHash)

	return out, err
}

// SendHeftyMessageBatch is currently not supported and will use the underlying AWS SQS SDK's method `SendMessageBatch`
func (client *SqsClientWrapper) SendHeftyMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return client.SendMessageBatch(ctx, params, optFns...)
}

// ReceiveHeftyMessage will determine if a message received is a reference to a large message residing in AWS S3.
// This method will then download the large message and then place its body and message attributes in the returned
// ReceiveMessageOutput. Messages not in S3 will not modify the return type. It is important to use this function
// when `SendHeftyMessage` is used so that large messages can be downloaded from S3.
// Note that this function's signature matches that of the AWS SDK's ReceiveMessage function.
func (client *SqsClientWrapper) ReceiveHeftyMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// request hefty message attribute
	if params.MessageAttributeNames == nil {
		params.MessageAttributeNames = []string{heftyClientVersionMessageKey}
	} else {
		params.MessageAttributeNames = append(params.MessageAttributeNames, heftyClientVersionMessageKey)
	}

	out, err := client.ReceiveMessage(ctx, params, optFns...)
	if err != nil || len(out.Messages) == 0 {
		return out, err
	}

	for i := range out.Messages {
		if _, ok := out.Messages[i].MessageAttributes[heftyClientVersionMessageKey]; !ok {
			continue
		}

		// deserialize message body
		var refMsg referenceMsg
		err = json.Unmarshal([]byte(*out.Messages[i].Body), &refMsg)
		if err != nil {
			return nil, fmt.Errorf("unable to unmarshal reference message. %v", err)
		}

		// make call to s3 to get message
		buf := s3manager.NewWriteAtBuffer([]byte{})
		_, err := client.downloader.Download(ctx, buf, &s3.GetObjectInput{
			Bucket: &refMsg.S3Bucket,
			Key:    &refMsg.S3Key,
		})
		if err != nil {
			return nil, fmt.Errorf("unable to get message from s3. %v", err)
		}

		// decode message from s3
		largeMsg := &largeSqsMsg{}
		err = largeMsg.Deserialize(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("unable to decode bytes into large message type. %v", err)
		}

		// replace message body and attributes with s3 message
		out.Messages[i].Body = largeMsg.Body
		out.Messages[i].MessageAttributes = largeMsg.MessageAttributes

		// replace md5 hashes
		out.Messages[i].MD5OfBody = &refMsg.SqsMd5HashBody
		out.Messages[i].MD5OfMessageAttributes = &refMsg.SqsMd5HashMsgAttr

		// modify receipt handle to contain s3 bucket and key info
		newReceiptHandle := fmt.Sprintf("%s|%s|%s|%s", receiptHandlePrefix, *out.Messages[i].ReceiptHandle, refMsg.S3Bucket, refMsg.S3Key)
		newReceiptHandle = base64.StdEncoding.EncodeToString([]byte(newReceiptHandle))
		out.Messages[i].ReceiptHandle = &newReceiptHandle
	}

	return out, nil
}

// DeleteHeftyMessage will delete a message from AWS S3 if it is large and also from AWS SQS.
// It is important to use the `ReceiptHandle` from `ReceiveHeftyMessage` in this function as
// this is the only way to determine if a large message resides in AWS S3 or not.
// Note that this function's signature matches that of the AWS SDK's DeleteMessage function.
func (client *SqsClientWrapper) DeleteHeftyMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if params.ReceiptHandle == nil {
		return client.DeleteMessage(ctx, params, optFns...)
	}

	// decode receipt handle
	decoded, err := base64.StdEncoding.DecodeString(*params.ReceiptHandle)
	if err != nil {
		return nil, fmt.Errorf("could not decode receipt handle. %v", err)
	}
	decodedStr := string(decoded)

	// check if decoded receipt handle is for a hefty message
	if !strings.HasPrefix(decodedStr, receiptHandlePrefix) {
		return client.DeleteMessage(ctx, params, optFns...)
	}

	// get tokens from receipt handle
	tokens := strings.Split(decodedStr, "|")
	if len(tokens) != expectedReceiptHandleTokenCount {
		return nil, fmt.Errorf("expected number of tokens (%d) not available in receipt handle", expectedReceiptHandleTokenCount)
	}

	// delete hefty message from s3
	receiptHandle, s3Bucket, s3Key := tokens[1], tokens[2], tokens[3]
	_, err = client.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &s3Bucket,
		Key:    &s3Key,
	})
	if err != nil {
		return nil, fmt.Errorf("could not delete s3 object for large message. %v", err)
	}

	// replace receipt handle with real one to delete sqs message
	params.ReceiptHandle = &receiptHandle

	return client.DeleteMessage(ctx, params, optFns...)
}

// Example queueUrl: https://sqs.us-west-2.amazonaws.com/765908583888/MyTestQueue
func newSqsReferenceMessage(queueUrl *string, bucketName, region, bodyHash, attributesHash string) (*referenceMsg, error) {
	if queueUrl != nil {
		tokens := strings.Split(*queueUrl, "/")
		if len(tokens) != 5 {
			return nil, fmt.Errorf("expected 5 tokens when splitting queueUrl by '/' but only received %d", len(tokens))
		} else {
			return &referenceMsg{
				S3Region:          region,
				S3Bucket:          bucketName,
				S3Key:             fmt.Sprintf("%s/%s", tokens[4], uuid.New().String()), // S3Key: queueName/uuid
				SqsMd5HashBody:    bodyHash,
				SqsMd5HashMsgAttr: attributesHash,
			}, nil
		}
	}

	return nil, errors.New("queueUrl is nil")
}

// msgSize retrieves the size of the message being sent
// current sqs size constraints are 256KB for both the body and message attributes
func msgSize(params *sqs.SendMessageInput) (int, error) {
	var size int

	size += len(*params.MessageBody)

	if params.MessageAttributes != nil {
		for k, v := range params.MessageAttributes {
			dataType := aws.ToString(v.DataType)
			size += len(k)
			size += len(dataType)
			if strings.HasPrefix(dataType, "String") || strings.HasPrefix(dataType, "Number") {
				size += len(aws.ToString(v.StringValue))
			} else if strings.HasPrefix(dataType, "Binary") {
				size += len(v.BinaryValue)
			} else {
				return -1, fmt.Errorf("encountered unexpected data type for message: %s", dataType)
			}
		}
	}

	return size, nil
}
