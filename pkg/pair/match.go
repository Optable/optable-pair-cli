package pair

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"optable-pair-cli/pkg/io"
	"optable-pair-cli/pkg/keys"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type (
	Matcher struct {
		reader      *pairIDReader
		writer      *writer
		intersected chan []byte
		advRead     atomic.Uint64
		hashMap     map[string]struct{}
	}

	writer struct {
		path    string
		writers []io.WriteCloser
		written atomic.Uint64
	}
)

func NewMatcher(adv, pub []io.Reader, out string) (*Matcher, error) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &Matcher{
		reader: &pairIDReader{
			r:         csv.NewReader(io.MultiReader(pub...)),
			batchSize: batchSize,
			batch:     make(chan [][]byte, batchSize),
			cancel:    cancel,
		},
		writer: &writer{
			path: out,
		},
		intersected: make(chan []byte, batchSize),
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
					m.advRead.Add(1)
					maps[i][record[0]] = struct{}{}
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

func (w *writer) NewWriter(index int) (*csv.Writer, error) {
	if w.path == "" {
		return csv.NewWriter(os.Stdout), nil
	}

	p := strings.TrimRight(w.path, string(filepath.Separator))
	f, err := os.Create(filepath.Join(p, fmt.Sprintf("result_%d.csv", index)))
	if err != nil {
		return nil, err
	}

	w.writers = append(w.writers, f)
	return csv.NewWriter(f), nil
}

func (w *writer) Close() error {
	for _, f := range w.writers {
		if err := f.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (m *Matcher) Match(ctx context.Context, numWorkers int, salt, privateKey string) error {
	// Cancel the context when the operation needs more than an 4 hours
	ctx, cancel := context.WithTimeout(ctx, maxOperationRunTime)
	defer cancel()

	var (
		logger        = zerolog.Ctx(ctx)
		startTime     = time.Now()
		maxNumWorkers = runtime.GOMAXPROCS(0)
	)

	if numWorkers > maxNumWorkers {
		numWorkers = maxNumWorkers

		logger.Warn().Msgf("Number of workers is limited to %d", numWorkers)
	}

	pk, err := keys.NewPAIRPrivateKey(salt, privateKey)
	if err != nil {
		return fmt.Errorf("NewPAIRPrivateKey: %w", err)
	}

	g, ctx := errgroup.WithContext(ctx)

	// producer
	g.Go(func() error {
		defer close(m.intersected)

		for batchedIDs := range m.reader.batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				for _, id := range batchedIDs {
					if _, ok := m.hashMap[string(id)]; ok {
						// remove from map, so it won't be matched again
						delete(m.hashMap, string(id))
						// send to consumer
						m.intersected <- id
					}
				}
			}
		}

		return nil
	})

	// consumer
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			// write to file
			w, err := m.writer.NewWriter(i)
			if err != nil {
				return err
			}

			for matched := range m.intersected {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					decrypted, err := pk.Decrypt(matched)
					if err != nil {
						return err
					}

					if err := w.Write([]string{string(decrypted)}); err != nil {
						return err
					}
					m.writer.written.Add(1)
				}
			}

			// flush and close
			w.Flush()
			return w.Error()
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if !errors.Is(m.reader.err, io.EOF) {
		return m.reader.err
	}

	logger.Debug().Msgf("Match: read %d advertiser and %d publisher IDs, written %d PAIR IDs in %s", m.advRead.Load(), m.reader.read.Load(), m.writer.written.Load(), time.Since(startTime))

	logger.Info().Msgf("Matched %.2f percent triple encrypted PAIR IDs, decrypted PAIR IDs are written to %s", normalizedMatchRate(int(m.writer.written.Load()), int(m.advRead.Load())), m.writer.path)

	return m.writer.Close()
}

func normalizedMatchRate(matched, total int) float64 {
	if total == 0 {
		return 0
	}

	rate := float64(matched) / float64(total) * 100
	if rate > 100 {
		return 100
	}

	return rate
}
