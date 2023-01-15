package main

import (
	"context"
	"encoding/json"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
)

type MetaEventbusInterface interface {
	sendSecondaryDbLessonProcessedEventName(originEvent events.SecondaryDbLoadedEvent) error
	sendLessonTypesList(list []events.LessonType, year int) error
}

type MetaEventbus struct {
	writer events.WriterInterface
}

func (metaEventbus MetaEventbus) sendSecondaryDbLessonProcessedEventName(originEvent events.SecondaryDbLoadedEvent) error {
	event := events.SecondaryDbLessonProcessedEvent{
		CurrentSecondaryDatabaseDatetime:  originEvent.CurrentSecondaryDatabaseDatetime,
		PreviousSecondaryDatabaseDatetime: originEvent.PreviousSecondaryDatabaseDatetime,
		Year:                              originEvent.Year,
	}
	payload, _ := json.Marshal(event)

	return metaEventbus.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(events.SecondaryDbLessonProcessedEventName),
			Value: payload,
		},
	)
}

func (metaEventbus MetaEventbus) sendLessonTypesList(list []events.LessonType, year int) error {
	event := events.LessonTypesList{
		Year: year,
		List: list,
	}
	payload, _ := json.Marshal(event)

	return metaEventbus.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(events.LessonTypesListName),
			Value: payload,
		},
	)
}
