// Command auditd is the monitor as a service: it subscribes to the change
// channel and the cause channel, closes windows on a timer, and writes every
// alert to a hash-chained report and to Postgres.
//
// It is subscribe-only. No service calls it, it calls no service, and it holds
// read-only credentials on application data. Stopping it stops the monitoring
// and nothing else — which is the property that makes it an independent control
// rather than another feature of the audited system.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"

	"github.com/baotnq/cause-justified-change-monitoring/internal/channels"
	"github.com/baotnq/cause-justified-change-monitoring/internal/idmap"
	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
	"github.com/baotnq/cause-justified-change-monitoring/internal/pgstore"
	"github.com/baotnq/cause-justified-change-monitoring/internal/report"
)

func main() {
	var (
		redisAddr  = flag.String("redis", env("REDIS_ADDR", "127.0.0.1:6379"), "Redis address for the change channel")
		redisDB    = flag.Int("redis-db", 0, "Redis database to watch")
		keyPrefix  = flag.String("key-prefix", "asset:account:", "protected key prefix; the actor id is the remainder")
		natsURL    = flag.String("nats", env("NATS_URL", nats.DefaultURL), "NATS URL for the cause channel")
		subject    = flag.String("subject", "cause.>", "cause channel subject")
		pgDSN      = flag.String("pg", os.Getenv("PG_DSN"), "Postgres DSN for alert history (optional)")
		reportPath = flag.String("report", "audit-report.jsonl", "hash-chained report file")
		windowSize = flag.Duration("window", time.Minute, "window length")
		grace      = flag.Duration("grace", 10*time.Second, "how long to keep a window open past its end")
		capacity   = flag.Uint("capacity", 1<<20, "id space the bit vectors cover")
		producers  = flag.String("producers", "", "comma-separated allow-list of cause producers; empty trusts every producer")
		amounts    = flag.Bool("check-amounts", false, "also require authorized amounts to account for observed movement")
	)
	flag.Parse()

	if err := run(*redisAddr, *redisDB, *keyPrefix, *natsURL, *subject, *pgDSN, *reportPath,
		*windowSize, *grace, uint32(*capacity), *producers, *amounts); err != nil {
		log.Fatalf("auditd: %v", err)
	}
}

func run(redisAddr string, redisDB int, keyPrefix, natsURL, subject, pgDSN, reportPath string,
	windowSize, grace time.Duration, capacity uint32, producers string, checkAmounts bool) error {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, DB: redisDB})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", redisAddr, err)
	}

	feed := channels.NewKeyspaceFeed(rdb, redisDB, keyPrefix)
	// Fatal, not a warning: a monitor whose change channel is silent reports
	// clean windows forever, which is indistinguishable from a healthy system.
	if err := feed.CheckConfig(ctx); err != nil {
		return err
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		return fmt.Errorf("nats %s: %w", natsURL, err)
	}
	defer nc.Close()

	f, err := os.OpenFile(reportPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open report: %w", err)
	}
	defer f.Close()
	rep := report.NewWriter(f)

	var pg *pgstore.Store
	if pgDSN != "" {
		pg, err = pgstore.Open(ctx, pgDSN)
		if err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		defer pg.Close()
	}

	cfg := monitor.Config{
		WindowSize:       windowSize,
		Grace:            grace,
		Capacity:         capacity,
		TrustedProducers: parseProducers(producers),
		DerivedCauseKinds: map[string]bool{
			"asset.account.updated": true,
		},
		RootCauseKinds: map[string]bool{
			"matching.order.matched": true,
			"transfer.completed":     true,
			"deposit.credited":       true,
			"payment.settled":        true,
		},
		CheckAmounts: checkAmounts,
	}
	m := monitor.New(cfg, idmap.New())

	changes := make(chan monitor.ChangeEvent, 4096)
	causes := make(chan monitor.CauseEvent, 4096)

	go func() {
		if err := feed.Run(ctx, changes); err != nil && ctx.Err() == nil {
			log.Printf("change channel stopped: %v", err)
		}
	}()
	bus := channels.NewCauseBus(nc, subject)
	bus.OnMalformed = func(subj string, data []byte, err error) {
		log.Printf("cause channel: unparseable message on %s: %v", subj, err)
	}
	go func() {
		if err := bus.Run(ctx, causes); err != nil && ctx.Err() == nil {
			log.Printf("cause channel stopped: %v", err)
		}
	}()

	log.Printf("watching %s* on redis %s (db %d), causes on %s %s; windows %s + %s grace, capacity %d",
		keyPrefix, redisAddr, redisDB, natsURL, subject, windowSize, grace, capacity)
	if pg != nil {
		log.Printf("alert history in postgres table %s", pg.Table())
	}
	log.Printf("report: %s", reportPath)

	tick := time.NewTicker(time.Second)
	defer tick.Stop()

	emit := func(alerts []monitor.Alert) {
		for _, a := range alerts {
			entry, err := rep.Append(a)
			if err != nil {
				log.Printf("report: %v", err)
				continue
			}
			if pg != nil {
				row := pgstore.AlertRow{
					Type:        a.Type,
					WindowStart: a.Window.Start,
					WindowEnd:   a.Window.End,
					ActorCount:  len(a.ActorIDs),
				}
				if err := pg.Append(ctx, entry, row); err != nil {
					log.Printf("postgres: %v", err)
				}
			}
			line, _ := json.Marshal(a)
			fmt.Println(string(line))
		}
	}

	for {
		select {
		case <-ctx.Done():
			// Close what is still open, so a stopped audit does not lose the
			// window it was in the middle of.
			emit(m.CloseAll())
			log.Printf("stopped; report head %s", rep.Head())
			return nil
		case ev := <-changes:
			m.ObserveChange(ev)
		case ev := <-causes:
			m.ObserveCause(ev)
		case now := <-tick.C:
			emit(m.CloseDue(now))
		}
	}
}

func parseProducers(s string) map[string]bool {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	out := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out[p] = true
		}
	}
	return out
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
