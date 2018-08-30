package gearman

import (
	"context"
	"io"
	"sync"
	"testing"
)

func TestMockServerSingle(t *testing.T) {
	wg := &sync.WaitGroup{}

	mock := NewMockServer()

	wg.Add(2)

	ctx, cancel := context.WithCancel(context.Background())

	mockServerWorker(ctx, wg, mock, t)
	go mockServerClient(cancel, wg, mock, t)

	wg.Wait()
}

func mockServerClient(cancel context.CancelFunc, wg *sync.WaitGroup, cli Client, t *testing.T) {
	defer wg.Done()
	defer cancel()

	r := cli.Submit(NewTask("reverse", []byte("test")))

	if !validResult(t, []byte("tset"), nil)(r.Value()) {
		return
	}
}

func mockServerWorker(ctx context.Context, wg *sync.WaitGroup, wkr Worker, t *testing.T) {
	wkr.AddHandler("reverse", JobHandler(func(payload io.Reader, reply io.Writer) error {
		buff := make([]byte, 1<<16)
		s, _ := payload.Read(buff)
		buff = buff[0:s]

		for i := len(buff); i > 0; i-- {
			reply.Write([]byte{buff[i-1]})
		}

		return nil
	}))

	go func() {
		<-ctx.Done()
		wg.Done()
	}()
}
