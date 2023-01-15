package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSendSecondaryDbLessonProcessedEventName(t *testing.T) {
	previousDatetime := time.Date(2023, 9, 1, 4, 0, 0, 0, time.Local)
	currentDatetime := time.Date(2023, 9, 2, 4, 0, 0, 0, time.Local)

	origEvent := events.SecondaryDbLoadedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatetime,
		Year:                              previousDatetime.Year(),
	}

	expectedError := errors.New("some error")

	payload, _ := json.Marshal(events.SecondaryDbLessonProcessedEvent{
		CurrentSecondaryDatabaseDatetime:  currentDatetime,
		PreviousSecondaryDatabaseDatetime: previousDatetime,
		Year:                              previousDatetime.Year(),
	})

	expectedMessage := kafka.Message{
		Key:   []byte(events.SecondaryDbLessonProcessedEventName),
		Value: payload,
	}

	t.Run("Success send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(nil)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendSecondaryDbLessonProcessedEventName(origEvent)

		assert.NoErrorf(t, err, "Not expect for error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})

	t.Run("Failed send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(expectedError)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendSecondaryDbLessonProcessedEventName(origEvent)

		assert.Errorf(t, err, "Expect for error")
		assert.Equal(t, expectedError, err, "Got unexpected error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})
}

func TestSendLessonTypesList(t *testing.T) {
	lessonTypesList := []events.LessonType{
		{
			Id:        30,
			ShortName: "Лек",
			LongName:  "Лекція",
		},
	}

	expectedError := errors.New("some error")
	expectedYear := 2045

	payload, _ := json.Marshal(events.LessonTypesList{
		Year: expectedYear,
		List: lessonTypesList,
	})

	expectedMessage := kafka.Message{
		Key:   []byte(events.LessonTypesListName),
		Value: payload,
	}

	t.Run("Success send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(nil)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendLessonTypesList(lessonTypesList, expectedYear)

		assert.NoErrorf(t, err, "Not expect for error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})

	t.Run("Failed send", func(t *testing.T) {
		writer := events.NewMockWriterInterface(t)
		writer.On("WriteMessages", context.Background(), expectedMessage).Return(expectedError)

		eventbus := MetaEventbus{writer: writer}
		err := eventbus.sendLessonTypesList(lessonTypesList, expectedYear)

		assert.Errorf(t, err, "Expect for error")
		assert.Equal(t, expectedError, err, "Got unexpected error")
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)
	})
}
