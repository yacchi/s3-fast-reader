package s3_fast_reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"regexp"
	"strconv"
	"testing"
	"time"
)

var buf100MB = make([]byte, 1024*1024*100)
var buf1000MB = make([]byte, 1024*1024*1024)

type mockClient struct {
	data []byte
}

var rangeValueRegex = regexp.MustCompile(`bytes=(\d+)-(\d+)`)

func parseRange(rangeValue string) (start, fin int64) {
	rng := rangeValueRegex.FindStringSubmatch(rangeValue)
	start, _ = strconv.ParseInt(rng[1], 10, 64)
	fin, _ = strconv.ParseInt(rng[2], 10, 64)
	return start, fin
}

func (c *mockClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	start, fin := parseRange(aws.ToString(params.Range))
	fin++
	data := c.data

	if fin >= int64(len(data)) {
		fin = int64(len(data))
	}

	bodyBytes := data[start:fin]

	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(bodyBytes)),
		ContentRange:  aws.String(fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data))),
		ContentLength: int64(len(bodyBytes)),
	}, nil
}

func newDownloadRangeClient(data []byte) *mockClient {
	client := &mockClient{
		data: data,
	}
	return client
}

type mockClientWithError struct {
	error error
}

func (m *mockClientWithError) GetObject(ctx context.Context, input *s3.GetObjectInput, f ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, m.error
}

type shortWrite struct {
}

func (s shortWrite) Write(p []byte) (n int, err error) {
	return len(p) - 1, nil
}

func TestReader(t *testing.T) {
	ctx := context.TODO()

	client := newDownloadRangeClient(buf100MB)
	contentLength := len(buf100MB)

	downloader := NewDownloader(client, func(d *manager.Downloader) {
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(int(d.PartSize))
	})

	t.Run("read all use io.Copy", func(t *testing.T) {
		buf := bytes.NewBuffer(make([]byte, 0, contentLength))

		reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		})

		defer reader.Close()

		if _, err := io.Copy(buf, reader); err != nil {
			panic(err)
		}
	})

	t.Run("read all use io.ReadFull", func(t *testing.T) {
		buf := make([]byte, contentLength, contentLength+1)

		reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		})

		defer reader.Close()

		if _, err := io.ReadFull(reader, buf); err != nil {
			panic(err)
		}
	})

	t.Run("read all use Read", func(t *testing.T) {
		buf := make([]byte, contentLength)

		reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		})

		defer reader.Close()

		for {
			if _, err := reader.Read(buf); err != nil {
				if err == io.EOF {
					break
				} else {
					panic(err)
				}
			}
		}
	})

	t.Run("read chunk", func(t *testing.T) {
		buf := bytes.NewBuffer(make([]byte, 0, contentLength))

		reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		})

		defer reader.Close()

		chunk := make([]byte, 8)
		if n, err := reader.Read(chunk); err != nil {
			panic(err)
		} else {
			if n != 8 {
				panic("n != 8")
			}
		}

		if _, err := io.Copy(buf, reader); err != nil {
			panic(err)
		}
	})

	t.Run("read and ShortWriteError", func(t *testing.T) {
		reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
			Bucket: aws.String("bucket"),
			Key:    aws.String("key"),
		})

		defer reader.Close()

		if _, err := io.Copy(&shortWrite{}, reader); err != nil {
			if !errors.Is(err, io.ErrShortWrite) {
				t.Errorf("expect %v, got %v", io.ErrShortWrite, err)
			}
		}
	})
}

func TestBackPressure(t *testing.T) {
	ctx := context.TODO()

	client := newDownloadRangeClient(buf1000MB)

	downloader := NewDownloader(client, func(d *manager.Downloader) {
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(int(d.PartSize))
	})

	reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})

	defer reader.Close()

	buf := make([]byte, 1024*1024)

	for {
		if _, err := reader.Read(buf); err != nil {
			if err == io.EOF {
				break
			} else {
				panic(err)
			}
		}
		// slow reader
		time.Sleep(1 * time.Millisecond)
	}
}

func TestGetObjectError(t *testing.T) {
	ctx := context.TODO()

	client := &mockClientWithError{error: &types.NoSuchKey{}}

	downloader := NewDownloader(client, func(d *manager.Downloader) {
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(int(d.PartSize))
	})

	reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})

	defer reader.Close()

	_, err := io.Copy(io.Discard, reader)
	var noSuchKey *types.NoSuchKey
	if !errors.As(err, &noSuchKey) {
		t.Errorf("expect %v, got %v", &types.NoSuchKey{}, err)
	}
}
