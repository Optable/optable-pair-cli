package pair

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize = 1024

	maxOperationRunTime = 4 * time.Hour
)

type (
	pairIDReadWriter struct {
		r         *csv.Reader
		w         *csv.Writer
		writeLock *sync.Mutex
		read      int
		written   int
		batchSize int
		batch     chan [][]byte
		err       error
		cancel    context.CancelFunc
	}
)

type PAIROperation uint8

const (
	PAIROperationHashEncrypt PAIROperation = iota
	PAIROperationReEncrypt
)

func (p PAIROperation) String() string {
	switch p {
	case PAIROperationHashEncrypt:
		return "HashEncrypt"
	case PAIROperationReEncrypt:
		return "ReEncrypt"
	default:
		return "Unknown"
	}
}

func NewPAIRIDReadWriter(r io.Reader, w io.Writer) (*pairIDReadWriter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	p := &pairIDReadWriter{
		r:         csv.NewReader(r),
		w:         csv.NewWriter(w),
		writeLock: &sync.Mutex{},
		batchSize: batchSize,
		batch:     make(chan [][]byte, batchSize),
		cancel:    cancel,
	}

	if batchSize <= 0 {
		return nil, errors.New("batch size must be greater than 0")
	}

	// Start reading in the background
	go func() {
		defer close(p.batch)
		defer p.cancel()

		batch := 0
		ids := make([][]byte, 0, batchSize)
		for {
			record, err := p.r.Read()
			if errors.Is(err, io.EOF) {
				p.err = io.EOF
				// Write the last batch
				if len(ids) > 0 {
					p.batch <- ids
					p.read += len(ids)
				}

				return
			} else if err != nil {
				p.err = err
				return
			}

			// Input should have only one id column
			ids = append(ids, []byte(record[0]))
			batch++

			// sent a full batch of records to the channel.
			if batch == batchSize {
				select {
				case <-ctx.Done():
					p.err = ctx.Err()
					return
				case p.batch <- ids:
					// reset the batch
					ids = make([][]byte, 0, batchSize)
					batch = 0
					p.read += batchSize
				}
			}
		}
	}()

	return p, nil
}

func (p *pairIDReadWriter) HashEncrypt(ctx context.Context, numWorkers int, salt, privateKey string) error {
	return runPAIROperation(ctx, p, numWorkers, salt, privateKey, PAIROperationHashEncrypt)
}

func (p *pairIDReadWriter) ReEncrypt(ctx context.Context, numWorkers int, salt, privateKey string) error {
	return runPAIROperation(ctx, p, numWorkers, salt, privateKey, PAIROperationReEncrypt)
}

func runPAIROperation(ctx context.Context, p *pairIDReadWriter, numWorkers int, salt, privatKey string, op PAIROperation) error {
	// Cancel the context when the operation needs more than an 4 hours
	ctx, cancel := context.WithTimeout(ctx, maxOperationRunTime)
	defer cancel()

	var (
		logger    = zerolog.Ctx(ctx)
		startTime = time.Now()
		done      = make(chan struct{}, 1)
		errChan   = make(chan error, 1)
		once      sync.Once
	)

	// Limit the number of workers to 8
	if numWorkers > 8 {
		numWorkers = 8
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)

	for {
		select {
		case err := <-errChan:
			close(done)
			close(errChan)
			return err
		case <-done:
			if err := g.Wait(); err != nil {
				return fmt.Errorf("g.Wait: %w", err)
			}
			close(done)

			logger.Debug().Msgf("%s: read %d IDs, written %d PAIR IDs in %s", op, p.read, p.written, time.Since(startTime))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			g.Go(func() error {
				pk, err := keys.NewPAIRPrivateKey(salt, privatKey)
				if err != nil {
					err := fmt.Errorf("NewPAIRPrivateKey: %w", err)
					errChan <- err
					return err
				}

				var do func([]byte) ([]byte, error)
				switch op {
				case PAIROperationHashEncrypt:
					do = pk.Encrypt
				case PAIROperationReEncrypt:
					do = pk.ReEncrypt
				default:
					err := errors.New("invalid operation")
					errChan <- err
					return err
				}

				if err := p.operate(do); err != nil {
					if errors.Is(err, io.EOF) {
						once.Do(func() {
							done <- struct{}{}
						})
						return nil
					}

					err := fmt.Errorf("p.Operate: %w", err)
					errChan <- err
					return err
				}

				return nil
			})
		}
	}
}

// operate reads a batch of records from the input reader,
// runs the PAIR operation on the records and writes to the underlying writer.
func (p *pairIDReadWriter) operate(do func([]byte) ([]byte, error)) error {
	ids, ok := <-p.batch
	if !ok {
		return p.err
	}

	records := make([][]string, 0, len(ids))
	for _, id := range ids {
		pairID, err := do([]byte(id))
		if err != nil {
			return fmt.Errorf("Encrypt: %w", err)
		}

		records = append(records, []string{string(pairID)})
	}

	// write is not thread safe
	p.writeLock.Lock()
	defer p.writeLock.Unlock()
	if err := p.w.WriteAll(records); err != nil {
		return fmt.Errorf("w.WriteAll: %w", err)
	}
	p.written += len(records)

	if err := p.w.Error(); err != nil {
		return fmt.Errorf("p.w.Error: %w", err)
	}

	return nil
}
