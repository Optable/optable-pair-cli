package pair

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/optable/match/pkg/pair"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize = 1024

	maxOperationRunTime = 4 * time.Hour
)

type (
	pairIDReadWriter struct {
		reader    *pairIDReader
		w         *csv.Writer
		writeLock *sync.Mutex
		written   atomic.Uint64
	}

	pairIDReader struct {
		r         *csv.Reader
		read      atomic.Uint64
		batchSize int
		batch     chan [][]byte
		err       error
		cancel    context.CancelFunc
	}

	readWriterOption struct {
		secondaryWriter io.Writer
	}

	ReadWriterOption func(*readWriterOption)
)

func WithSecondaryWriter(w io.Writer) ReadWriterOption {
	return func(o *readWriterOption) {
		o.secondaryWriter = w
	}
}

type PAIROperation uint8

const (
	PAIROperationHashEncrypt PAIROperation = iota
	PAIROperationReEncrypt
	PAIROperationDecrypt
)

func (p PAIROperation) String() string {
	switch p {
	case PAIROperationHashEncrypt:
		return "HashEncrypt"
	case PAIROperationReEncrypt:
		return "ReEncrypt"
	case PAIROperationDecrypt:
		return "Decrypt"
	default:
		return "Unknown"
	}
}

type pairOps struct {
	do      func([]byte) ([]byte, error)
	shuffle bool
}

func NewPAIRIDReadWriter(r io.Reader, w io.Writer, opts ...ReadWriterOption) (*pairIDReadWriter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	rwOpt := &readWriterOption{}
	for _, opt := range opts {
		opt(rwOpt)
	}

	if rwOpt.secondaryWriter != nil {
		w = io.MultiWriter(w, rwOpt.secondaryWriter)
	}

	p := &pairIDReadWriter{
		w:         csv.NewWriter(w),
		writeLock: &sync.Mutex{},
		reader: &pairIDReader{
			r:         csv.NewReader(r),
			batchSize: batchSize,
			batch:     make(chan [][]byte, batchSize),
			cancel:    cancel,
		},
	}

	if batchSize <= 0 {
		return nil, errors.New("batch size must be greater than 0")
	}

	// Start reading in the background
	go readPAIRIDs(ctx, p.reader)

	return p, nil
}

func readPAIRIDs(ctx context.Context, p *pairIDReader) {
	defer close(p.batch)
	defer p.cancel()

	batch := 0
	ids := make([][]byte, 0, p.batchSize)
	for {
		record, err := p.r.Read()
		if errors.Is(err, io.EOF) {
			p.err = io.EOF
			// Write the last batch
			if len(ids) > 0 {
				p.batch <- ids
				p.read.Add(uint64(len(ids)))
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
				p.read.Add(uint64(batchSize))
			}
		}
	}
}

func (p *pairIDReadWriter) HashEncrypt(ctx context.Context, numWorkers int, salt, privateKey string) error {
	return runPAIROperation(ctx, p, numWorkers, salt, privateKey, PAIROperationHashEncrypt)
}

func (p *pairIDReadWriter) ReEncrypt(ctx context.Context, numWorkers int, salt, privateKey string) error {
	return runPAIROperation(ctx, p, numWorkers, salt, privateKey, PAIROperationReEncrypt)
}

func (p *pairIDReadWriter) Decrypt(ctx context.Context, numWorkers int, salt, privateKey string) error {
	return runPAIROperation(ctx, p, numWorkers, salt, privateKey, PAIROperationDecrypt)
}

func decryptAndBase64EncodeFunc(pk *pair.PrivateKey) func(ciphertext []byte) ([]byte, error) {
	return func(bytes []byte) ([]byte, error) {
		decrypted, err := pk.Decrypt(bytes)
		if err != nil {
			return nil, err
		}
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(decrypted)))
		base64.StdEncoding.Encode(dst, decrypted)
		return dst, nil
	}
}

func runPAIROperation(ctx context.Context, p *pairIDReadWriter, numWorkers int, salt, privateKey string, op PAIROperation) error {
	// Cancel the context when the operation needs more than an 4 hours
	ctx, cancel := context.WithTimeout(ctx, maxOperationRunTime)
	defer cancel()

	var (
		logger     = zerolog.Ctx(ctx)
		startTime  = time.Now()
		done       = make(chan struct{}, 1)
		errChan    = make(chan error, 1)
		once       sync.Once
		maxWorkers = runtime.GOMAXPROCS(0)
		operation  = &pairOps{}
	)

	// Limit the number of workers to 8
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
		logger.Warn().Msgf("Number of workers is limited to %d", numWorkers)
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

			logger.Debug().Msgf("%s: read %d IDs, written %d PAIR IDs in %s", op, p.reader.read.Load(), p.written.Load(), time.Since(startTime))
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			g.Go(func() error {
				pk, err := keys.NewPAIRPrivateKey(salt, privateKey)
				if err != nil {
					err := fmt.Errorf("NewPAIRPrivateKey: %w", err)
					errChan <- err
					return err
				}

				switch op {
				case PAIROperationHashEncrypt:
					operation.do = pk.Encrypt
				case PAIROperationReEncrypt:
					operation.do = pk.ReEncrypt
					operation.shuffle = true
				case PAIROperationDecrypt:
					operation.do = decryptAndBase64EncodeFunc(pk)
				default:
					err := errors.New("invalid operation")
					errChan <- err
					return err
				}

				if err := p.operate(operation); err != nil {
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
func (p *pairIDReadWriter) operate(op *pairOps) error {
	ids, ok := <-p.reader.batch
	if !ok {
		return p.reader.err
	}

	// Shuffle the ids in place before processing
	// Note that we already receive the batch of IDs
	// in a psuedo-random order from the reader.
	if op.shuffle {
		pair.Shuffle(ids)
	}

	records := make([][]string, 0, len(ids))
	for _, id := range ids {
		pairID, err := op.do([]byte(id))
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
	p.written.Add(uint64(len(records)))

	if err := p.w.Error(); err != nil {
		return fmt.Errorf("p.w.Error: %w", err)
	}

	return nil
}
