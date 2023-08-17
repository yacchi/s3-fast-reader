package s3_fast_reader

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"sync"
	"time"
)

type Downloader struct {
	*manager.Downloader
}

func NewDownloader(c manager.DownloadAPIClient, options ...func(downloader *manager.Downloader)) *Downloader {
	return &Downloader{manager.NewDownloader(c, options...)}
}

func (d *Downloader) NewReader(ctx context.Context, input *s3.GetObjectInput) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	w := &reader{
		pool: sync.Pool{
			New: func() any {
				return make([]byte, 0, d.PartSize)
			},
		},
		cond:           sync.NewCond(&sync.Mutex{}),
		pendingQueue:   make(chan *writeRequest),
		maxPendingSize: d.PartSize * int64(d.Concurrency),
	}

	go w.processQueue()

	var start time.Time
	_ = start

	go func() {
		defer func() {
			cancel()
			close(w.pendingQueue)
		}()
		start = time.Now()
		n, err := d.Download(ctx, w, input)
		if err == nil {
			err = io.EOF
		}
		w.err = err
		_, _ = w.WriteAt(nil, n)
	}()

	//go func() {
	//	ticker := time.NewTicker(1 * time.Second)
	//	defer ticker.Stop()
	//
	//	show := func() {
	//		since := time.Since(start).Seconds()
	//		fmt.Printf("Read: %.3f MiB/s, Write: %.3f MiB/s, TotalRead: %d MiB, TotalWrite: %d MiB\n",
	//			float64(w.totalRead)/since/1024/1024,
	//			float64(w.pos)/since/1024/1024,
	//			w.totalRead/1024/1024,
	//			w.pos/1024/1024,
	//		)
	//	}
	//
	//	for {
	//		select {
	//		case <-ticker.C:
	//			show()
	//		case <-ctx.Done():
	//			show()
	//			return
	//		}
	//	}
	//}()

	return w, nil
}
