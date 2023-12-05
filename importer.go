package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"io"
	"time"
)

const LessonQuery = `SELECT ID, NUM_PREDM, DATEZAN, NUM_VARZAN, HALF,
    (case FSTATUS when 0 then 1 else 0 end) as isDeleted
FROM T_PRJURN WHERE REGDATE BETWEEN ? AND ? ORDER BY ID DESC`

const LessonTypesQuery = `SELECT ID, SHIRTNAME, LONGNAME FROM T_VARZAN`

const AdditionalDateRangeInDays = 2

type ImporterInterface interface {
	execute(startDatetime time.Time, endDatetime time.Time, year int) error
	importLessonTypes() ([]events.LessonType, error)
}

type LessonsImporter struct {
	out            io.Writer
	db             *sql.DB
	writer         events.WriterInterface
	writeThreshold int
}

func (importer *LessonsImporter) execute(startDatetime time.Time, endDatetime time.Time, year int) (err error) {
	if err = importer.db.Ping(); err != nil {
		return
	}

	startDatetime = time.Date(
		startDatetime.Year(), startDatetime.Month(), startDatetime.Day()-AdditionalDateRangeInDays,
		0, 0, 0, 0, startDatetime.Location(),
	)

	startedAt := time.Now()
	fmt.Fprintf(importer.out, "Start import lessons: \n")
	rows, err := importer.db.Query(
		LessonQuery,
		startDatetime.Format(dateFormat),
		endDatetime.Format(dateFormat),
	)
	if err != nil {
		return
	}

	defer rows.Close()

	var messages []kafka.Message
	var nextErr error
	writeMessages := func(threshold int) bool {
		if len(messages) != 0 && len(messages) >= threshold {
			nextErr = importer.writer.WriteMessages(context.Background(), messages...)
			messages = []kafka.Message{}
			fmt.Fprintf(importer.out, ".")
			if err == nil && nextErr != nil {
				err = nextErr
			}
		}
		return err == nil
	}

	var event events.LessonEvent
	i := 0
	for rows.Next() && writeMessages(importer.writeThreshold) {
		i++
		err = rows.Scan(&event.Id, &event.DisciplineId, &event.Date, &event.TypeId, &event.Semester, &event.IsDeleted)
		if err == nil {
			event.Year = year
			payload, _ := json.Marshal(event)
			messages = append(messages, kafka.Message{
				Key:   []byte(events.LessonEventName),
				Value: payload,
			})
		}
	}
	writeMessages(0)
	fmt.Fprintf(
		importer.out, " finished.\n Send %d lessons. Error: %v. Done in %d seconds \n",
		i, err, int(time.Now().Sub(startedAt).Seconds()),
	)

	return
}

func (importer *LessonsImporter) importLessonTypes() (list []events.LessonType, err error) {
	rows, err := importer.db.Query(LessonTypesQuery)
	if rows != nil {
		defer rows.Close()
	}

	var lessonType events.LessonType
	for err == nil && rows.Next() {
		err = rows.Scan(&lessonType.Id, &lessonType.ShortName, &lessonType.LongName)
		list = append(list, lessonType)
	}
	return
}
