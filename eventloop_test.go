package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
	"time"
)

func TestEventLoopExecute(t *testing.T) {
	var out bytes.Buffer

	expectedError := errors.New("Expected error")
	breakLoopError := errors.New("breakLoop")
	matchContext := mock.MatchedBy(func(ctx context.Context) bool { return true })

	expectedStartDatetime := time.Date(2023, 4, 10, 4, 0, 0, 0, time.UTC)
	expectedEndDatetime := time.Date(2023, 4, 11, 4, 0, 0, 0, time.UTC)
	expectedYear := expectedEndDatetime.Year()

	event := events.SecondaryDbLoadedEvent{
		PreviousSecondaryDatabaseDatetime: expectedStartDatetime,
		CurrentSecondaryDatabaseDatetime:  expectedEndDatetime,
		Year:                              expectedYear,
	}

	payload, _ := json.Marshal(event)
	message := kafka.Message{
		Key:   []byte(events.SecondaryDbLoadedEventName),
		Value: payload,
	}

	t.Run("success process one valid message", func(t *testing.T) {
		lessonTypesList := []events.LessonType{
			{
				Id:        30,
				ShortName: "Лек",
				LongName:  "Лекція",
			},
		}

		metaEventbus := NewMockMetaEventbusInterface(t)
		metaEventbus.On("sendSecondaryDbLessonProcessedEventName", event).Return(nil)
		metaEventbus.On("sendLessonTypesList", lessonTypesList, expectedYear).Return(nil)

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)
		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime, expectedYear).Return(nil)
		importer.On("importLessonTypes").Return(lessonTypesList, nil)

		eventLoop := EventLoop{
			out:          &out,
			metaEventbus: metaEventbus,
			reader:       reader,
			importer:     importer,
		}

		err := eventLoop.execute()

		assert.Equal(t, breakLoopError, err)
		metaEventbus.AssertExpectations(t)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		metaEventbus.AssertNumberOfCalls(t, "sendSecondaryDbLessonProcessedEventName", 1)
	})

	t.Run("process one valid message with error on commit", func(t *testing.T) {
		lessonTypesList := make([]events.LessonType, 1)

		metaEventbus := NewMockMetaEventbusInterface(t)
		metaEventbus.On("sendSecondaryDbLessonProcessedEventName", event).Return(nil)
		metaEventbus.On("sendLessonTypesList", lessonTypesList, expectedYear).Return(nil)

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("CommitMessages", matchContext, message).Return(expectedError)

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime, expectedYear).Return(nil)
		importer.On("importLessonTypes").Return(lessonTypesList, nil)

		eventLoop := EventLoop{
			out:          &out,
			metaEventbus: metaEventbus,
			reader:       reader,
			importer:     importer,
		}

		err := eventLoop.execute()

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		metaEventbus.AssertExpectations(t)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		metaEventbus.AssertNotCalled(t, "sendSecondaryDbLessonProcessedEventName")
		reader.AssertNumberOfCalls(t, "FetchMessage", 1)
		reader.AssertNumberOfCalls(t, "CommitMessages", 1)
	})

	t.Run("process one valid message with error on importer execute", func(t *testing.T) {
		lessonTypesList := make([]events.LessonType, 1)

		metaEventbus := NewMockMetaEventbusInterface(t)
		metaEventbus.On("sendLessonTypesList", lessonTypesList, expectedYear).Return(nil)

		reader := mocks.NewReaderInterface(t)
		reader.On("FetchMessage", matchContext).Return(message, nil).Once()

		importer := NewMockImporterInterface(t)
		importer.On("execute", expectedStartDatetime, expectedEndDatetime, expectedYear).Return(expectedError)
		importer.On("importLessonTypes").Return(lessonTypesList, nil)

		eventLoop := EventLoop{
			out:          &out,
			metaEventbus: metaEventbus,
			reader:       reader,
			importer:     importer,
		}

		err := eventLoop.execute()

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		metaEventbus.AssertNotCalled(t, "sendSecondaryDbLessonProcessedEventName")
		reader.AssertNumberOfCalls(t, "FetchMessage", 1)
		reader.AssertNotCalled(t, "CommitMessages")
	})

	t.Run("process one ignore message", func(t *testing.T) {
		metaEventbus := NewMockMetaEventbusInterface(t)

		ignoreEvent := events.SecondaryDbScoreProcessedEvent{}
		payload, _ = json.Marshal(ignoreEvent)
		message = kafka.Message{
			Key:   []byte(events.SecondaryDbScoreProcessedEventName),
			Value: payload,
		}

		reader := mocks.NewReaderInterface(t)

		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)

		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)

		eventLoop := EventLoop{
			out:          &out,
			metaEventbus: metaEventbus,
			reader:       reader,
			importer:     importer,
		}

		err := eventLoop.execute()

		importer.AssertNotCalled(t, "execute")

		assert.Equal(t, breakLoopError, err)
		metaEventbus.AssertExpectations(t)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		metaEventbus.AssertNotCalled(t, "sendSecondaryDbLessonProcessedEventName")
	})

	t.Run("process one ignore message", func(t *testing.T) {
		event := events.SecondaryDbScoreProcessedEvent{}
		payload, _ := json.Marshal(event)
		message := kafka.Message{
			Key:   []byte(events.SecondaryDbScoreProcessedEventName),
			Value: payload,
		}

		metaEventbus := NewMockMetaEventbusInterface(t)

		reader := mocks.NewReaderInterface(t)

		reader.On("FetchMessage", matchContext).Return(message, nil).Once()
		reader.On("FetchMessage", matchContext).Return(kafka.Message{}, breakLoopError)

		reader.On("CommitMessages", matchContext, message).Return(nil)

		importer := NewMockImporterInterface(t)

		eventLoop := EventLoop{
			out:          &out,
			metaEventbus: metaEventbus,
			reader:       reader,
			importer:     importer,
		}

		err := eventLoop.execute()

		importer.AssertNotCalled(t, "execute")

		assert.Equal(t, breakLoopError, err)
		metaEventbus.AssertExpectations(t)
		reader.AssertExpectations(t)
		importer.AssertExpectations(t)

		metaEventbus.AssertNotCalled(t, "sendSecondaryDbLessonProcessedEventName")
	})

}
