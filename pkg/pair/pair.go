package pair

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"

	"github.com/optable/match/pkg/pair"
)

type (
	pairIDReadWriter struct {
		pk        *pair.PrivateKey
		r         *csv.Reader
		w         *csv.Writer
		batchSize int
		batch     chan [][]byte
		err       error
		cancel    context.CancelFunc
	}
)

func NewPairIDReadWriter(r io.Reader, w io.Writer, batchSize int, pk *pair.PrivateKey) (*pairIDReadWriter, error) {
	ctx, cancel := context.WithCancel(context.Background())

	p := &pairIDReadWriter{
		pk:        pk,
		r:         csv.NewReader(r),
		w:         csv.NewWriter(w),
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
				}
			}
		}
	}()

	return p, nil
}

// Read reads a batch of records from the input reader,
// hash and encrypts the records and writes to the underlying writer.
func (p *pairIDReadWriter) Read() error {
	ids, ok := <-p.batch
	if !ok {
		return p.err
	}

	for _, id := range ids {
		pairID, err := p.pk.Encrypt([]byte(id))
		if err != nil {
			return fmt.Errorf("Encrypt: %w", err)
		}

		if err := p.w.Write(pairID); err != nil {
			return fmt.Errorf("Write: %w", err)
		}
	}

	p.w.Flush()
	if err := p.w.Error(); err != nil {
		return fmt.Errorf("p.w.Error: %w", err)
	}

	return nil
}
