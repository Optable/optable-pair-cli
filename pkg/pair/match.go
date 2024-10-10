package pair

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type (
	matcher struct {
		reader      *pairIDReader
		w           io.Writer
		written     int
		intersected []string
		hashMap     map[string]struct{}
	}
)

func NewMatcher(adv, pub []io.Reader, out string) (*matcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &matcher{
		reader: &pairIDReader{
			r:         csv.NewReader(io.MultiReader(pub...)),
			batchSize: batchSize,
			batch:     make(chan [][]byte, batchSize),
			cancel:    cancel,
		},
		intersected: make([]string, 0),
		hashMap:     make(map[string]struct{}),
	}

	// read publisher in background
	go readPAIRIDs(ctx, m.reader)

	// read all advertiser PAIR IDs into a map with a different context
	g, ctx := errgroup.WithContext(context.Background())
	maps := make([]map[string]struct{}, len(adv))
	for i, reader := range adv {
		maps[i] = make(map[string]struct{})
		r := csv.NewReader(reader)

		// read in background
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return fmt.Errorf("read publisher IDs: %w", ctx.Err())
				default:
					record, err := r.Read()
					if errors.Is(err, io.EOF) {
						return nil
					} else if err != nil {
						return err
					}

					// normalize and store
					maps[i][normalize([]byte(record[0]))] = struct{}{}
				}
			}
		})
	}

	// wait for all readers to finish
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// combine maps and clear
	for _, im := range maps {
		for k := range im {
			m.hashMap[k] = struct{}{}
		}

		clear(im)
	}

	return m, nil
}

func normalize(id []byte) string {
	return base64.StdEncoding.EncodeToString(id)
}

func (m *matcher) Match(ctx context.Context, numWorkers int) error {
	// Cancel the context when the operation needs more than an 4 hours
	ctx, cancel := context.WithTimeout(ctx, maxOperationRunTime)
	defer cancel()

	var (
		logger    = zerolog.Ctx(ctx)
		startTime = time.Now()
	)

	// Limit the number of workers to 8
	if numWorkers > 8 {
		numWorkers = 8
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(numWorkers)

	g.Go(func() error {
		for batchedIDs := range m.reader.batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				for _, id := range batchedIDs {
					if _, ok := m.hashMap[normalize(id)]; ok {
						m.intersected = append(m.intersected, string(id))
						m.written++
					}
				}
			}
		}

		logger.Debug().Msgf("Match: read %d IDs, written %d PAIR IDs in %s", m.reader.read, m.written, time.Since(startTime))
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if !errors.Is(m.reader.err, io.EOF) {
		return m.reader.err
	}

	return nil
}
