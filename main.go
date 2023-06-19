package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	_ "github.com/nakagami/firebirdsql"
	"github.com/segmentio/kafka-go"
	"io"
	"os"
	"time"
)

const ExitCodeMainError = 1
const dateFormat = "2006-01-02 15:04:05"

func main() {
	os.Exit(handleExitError(os.Stderr, runApp(os.Stdout)))
}

func runApp(out io.Writer) error {
	envFilename := ""
	if _, err := os.Stat(".env"); err == nil {
		envFilename = ".env"
	}

	config, err := loadConfig(envFilename)
	if err != nil {
		return errors.New("Failed to load config: " + err.Error())
	}

	db, err := sql.Open(config.dekanatDbDriverName, config.secondaryDekanatDbDSN)
	if err != nil {
		return errors.New("Wrong connection configuration for secondary Dekanat DB: " + err.Error())
	}

	importer := &LessonsImporter{
		out: out,
		db:  db,
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    events.RawLessonsTopic,
			Balancer: &kafka.LeastBytes{},
		},
		writeThreshold: 500,
	}

	metaEventbus := &MetaEventbus{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(config.kafkaHost),
			Topic:    events.MetaEventsTopic,
			Balancer: &kafka.LeastBytes{},
		},
	}

	eventLoop := &EventLoop{
		out:          out,
		importer:     importer,
		metaEventbus: metaEventbus,
		reader: kafka.NewReader(
			kafka.ReaderConfig{
				Brokers:     []string{config.kafkaHost},
				GroupID:     "secondary-db-lessons-importer",
				Topic:       events.MetaEventsTopic,
				MinBytes:    10,
				MaxBytes:    10e3,
				MaxWait:     time.Second,
				MaxAttempts: config.kafkaAttempts,
				Dialer: &kafka.Dialer{
					Timeout:   config.kafkaTimeout,
					DualStack: kafka.DefaultDialer.DualStack,
				},
			},
		),
	}

	defer func() {
		_ = eventLoop.reader.Close()
		_ = metaEventbus.writer.Close()
		_ = importer.writer.Close()
		_ = db.Close()
	}()

	return eventLoop.execute()
}

func handleExitError(errStream io.Writer, err error) int {
	if err != nil {
		fmt.Fprintln(errStream, err)
	}

	if err != nil {
		return ExitCodeMainError
	}

	return 0
}
