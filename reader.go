package s3_fast_reader

import (
	"bytes"
	"container/heap"
	"io"
	"sync"
)

type writeRequest struct {
	pos int64
	buf *bytes.Buffer
}

type writeRequests []*writeRequest

func (w writeRequests) Len() int           { return len(w) }
func (w writeRequests) Less(i, j int) bool { return w[i].pos < w[j].pos }
func (w writeRequests) Swap(i, j int)      { w[i], w[j] = w[j], w[i] }

func (w *writeRequests) Push(x any) {
	*w = append(*w, x.(*writeRequest))
}

func (w *writeRequests) Pop() any {
	old := *w
	n := len(old)
	x := old[n-1]
	*w = old[0 : n-1]
	return x
}

type reader struct {
	pool           sync.Pool
	cond           *sync.Cond
	pending        writeRequests
	pendingSize    int64
	maxPendingSize int64
	pendingQueue   chan *writeRequest
	pos            int64
	err            error
	totalRead      int64
}

func (r *reader) Close() error {
	return nil
}

func (r *reader) newWriteRequest(pos int64, p []byte) *writeRequest {
	raw := r.pool.Get().([]byte)
	buf := bytes.NewBuffer(raw)
	buf.Write(p)
	return &writeRequest{
		pos: pos,
		buf: buf,
	}
}

func (r *reader) endWriteRequest(req *writeRequest) {
	req.buf.Reset()
	r.pool.Put(req.buf.Bytes())
	req.buf = nil
}

func (r *reader) check() (readable bool) {
	return len(r.pending) > 0 && r.pending[0].pos == r.pos
}

func (r *reader) Read(p []byte) (int, error) {
	r.cond.L.Lock()
	defer r.cond.L.Unlock()

	for !r.check() {
		r.cond.Wait()
	}

	req := heap.Pop(&r.pending).(*writeRequest)

	bufLen := int64(req.buf.Len())
	if bufLen == 0 && r.err != nil {
		return 0, r.err
	}

	wn, _ := req.buf.Read(p)
	written := int64(wn)

	r.pos += written
	r.pendingSize -= written

	if written == bufLen {
		r.cond.Broadcast()
		r.endWriteRequest(req)
	} else {
		req.pos = r.pos
		heap.Push(&r.pending, req)
	}

	return wn, nil
}

func (r *reader) WriteTo(w io.Writer) (n int64, err error) {
	write := func() (int64, error) {
		defer r.cond.L.Unlock()

		req := heap.Pop(&r.pending).(*writeRequest)

		bufLen := int64(req.buf.Len())
		if bufLen == 0 && r.err != nil {
			return n, r.err
		}

		written, err := io.Copy(w, req.buf)
		r.endWriteRequest(req)
		return written, err
	}

	for {
		r.cond.L.Lock()

		for !r.check() {
			r.cond.Wait()
		}

		written, err := write()

		r.pos += written
		r.pendingSize -= written
		n += written

		if err != nil {
			if r.err == io.EOF {
				return n, nil
			}
			return n, err
		} else {
			r.cond.Broadcast()
		}
	}
}

func (r *reader) processQueue() {
	for req := range r.pendingQueue {
		r.cond.L.Lock()
		if r.pendingSize > r.maxPendingSize {
			if req.buf != nil {
				r.cond.Wait()
			}
		}
		r.cond.L.Unlock()
	}
}

func (r *reader) WriteAt(p []byte, pos int64) (n int, err error) {
	req := r.newWriteRequest(pos, p)

	r.cond.L.Lock()
	r.pendingSize += int64(len(p))
	r.totalRead += int64(len(p))
	heap.Push(&r.pending, req)
	queued := r.pos < pos
	r.cond.L.Unlock()

	if queued {
		r.pendingQueue <- req
	} else {
		r.cond.Broadcast()
	}

	return len(p), nil
}
