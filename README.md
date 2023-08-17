# s3-fast-reader
[![Test](https://github.com/yacchi/s3-fast-reader/actions/workflows/test.yml/badge.svg)](https://github.com/yacchi/s3-fast-reader/actions/workflows/test.yml)

It's a package that provides an io.Reader interface for the s3 manager Downloader in aws-sdk-go-v2.

## How to use

```go
package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/yacchi/s3-fast-reader"
	"io"
)

func main() {
	// create aws session go sdk v2
	ctx := context.TODO()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}

	client := s3.NewFromConfig(cfg)

	downloader := s3_fast_reader.NewDownloader(client, func(d *manager.Downloader) {
		d.BufferProvider = manager.NewPooledBufferedWriterReadFromProvider(int(d.PartSize))
	})

	reader, _ := downloader.NewReader(ctx, &s3.GetObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("key"),
	})

	defer reader.Close()

	// implement io.Reader interface
	io.Copy(io.Discard, reader)
}
```