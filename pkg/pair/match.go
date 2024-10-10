package pair

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"optable-pair-cli/pkg/keys"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

type (
	matcher struct {
		reader      *pairIDReader
		writer      *writer
		intersected chan []byte
		hashMap     map[string]struct{}
	}

	writer struct {
		path    string
		writers []io.WriteCloser
		written atomic.Uint64
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

func normalize(id []byte) string {
	return base64.StdEncoding.EncodeToString(id)
}

func (w *writer) NewWriter(index int) (*csv.Writer, error) {
	if w.path == "" {
		return csv.NewWriter(os.Stdout), nil
	}

	s, err := os.Stat(w.path)
	if err != nil {
		return nil, err
	}

	if !s.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", w.path)
	}

	p := strings.TrimRight(w.path, "/")
	f, err := os.Create(fmt.Sprintf("%s/result_%d.csv", p, index))
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

func (m *matcher) Match(ctx context.Context, numWorkers int, salt, privateKey string) error {
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

		sent := 0
		for batchedIDs := range m.reader.batch {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				for _, id := range batchedIDs {
					if _, ok := m.hashMap[string(id)]; ok {
						m.intersected <- id
						sent++
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

	logger.Debug().Msgf("Match: read %d IDs, written %d PAIR IDs in %s", m.reader.read, m.writer.written.Load(), time.Since(startTime))

	logger.Info().Msgf("Matched %d triple encrypted PAIR IDs, decrypted PAIR IDs are written to %s", m.writer.written.Load(), m.writer.path)

	return m.writer.Close()
}
