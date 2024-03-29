package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/kneu-messenger-pigeon/events"
	"github.com/kneu-messenger-pigeon/events/mocks"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"log"
	"math/rand"
	"regexp"
	"testing"
	"time"
)

var expectedColumns = []string{"ID", "NUM_PREDM", "DATEZAN", "NUM_VARZAN", "HALF", "isDeleted"}

func TestImporterExecute(t *testing.T) {
	var startDatetime time.Time
	var endDatetime time.Time
	var year = 2030
	var out bytes.Buffer
	var event events.LessonEvent
	var matchContext = mock.MatchedBy(func(ctx context.Context) bool { return true })

	t.Run("valid lessons", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedSqlStartDatetime := time.Date(2023, 3, 5-AdditionalDateRangeInDays, 0, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedEvents := make([]events.LessonEvent, 0)
		rows := sqlmock.NewRows(expectedColumns)
		for i := uint(100); i < 115; i++ {
			event = events.LessonEvent{
				Id:           i,
				DisciplineId: 99,
				TypeId:       uint8(rand.Intn(10) + 1),
				Date:         time.Date(2022, 12, 20, 14, 36, 0, 0, time.Local),
				Year:         year,
				Semester:     uint8(rand.Intn(2) + 1),
				IsDeleted:    i%7 == 3,
			}

			rows = rows.AddRow(
				event.Id, event.DisciplineId, event.Date,
				event.TypeId, event.Semester, event.IsDeleted,
			)

			expectedEvents = append(expectedEvents, event)
		}

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonQuery)).WithArgs(
			expectedSqlStartDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := mocks.NewWriterInterface(t)

		chunkSize := 3

		messageArgumentMatcher := func(offset int) func(kafka.Message) bool {
			argumentExpectedEvent := make([]events.LessonEvent, 0)
			for i := offset; i < len(expectedEvents); i += chunkSize {
				argumentExpectedEvent = append(argumentExpectedEvent, expectedEvents[i])
			}

			return func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)

				return assert.Equal(t, events.LessonEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as DisciplineEvent: %v", message) &&
					assert.Containsf(
						t, argumentExpectedEvent, event,
						"Unexpected event: %v \n", event,
					)
			}
		}

		writer.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(messageArgumentMatcher(0)),
			mock.MatchedBy(messageArgumentMatcher(1)),
			mock.MatchedBy(messageArgumentMatcher(2)),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := LessonsImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: chunkSize,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.NoError(t, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 5)

		writer.AssertExpectations(t)
	})

	t.Run("sql error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedSqlStartDatetime := time.Date(2023, 3, 5-AdditionalDateRangeInDays, 0, 0, 0, 0, time.Local)

		expectedError := errors.New("expected test error")

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonQuery)).WithArgs(
			expectedSqlStartDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnError(expectedError)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := mocks.NewWriterInterface(t)
		// End Init Writer Mock and Expectation

		importer := LessonsImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 0)
	})

	t.Run("row error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 0, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedSqlStartDatetime := time.Date(2023, 3, 5-AdditionalDateRangeInDays, 0, 0, 0, 0, time.Local)

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := uint(20)
		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, 999, time.Time{}, 1, 1, false,
		).AddRow(
			21, nil, time.Time{}, 1, nil, false,
		)

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonQuery)).WithArgs(
			expectedSqlStartDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := mocks.NewWriterInterface(t)

		writer.On(
			"WriteMessages",
			matchContext,
			mock.MatchedBy(func(message kafka.Message) bool {
				err = json.Unmarshal(message.Value, &event)
				return assert.Equal(t, events.LessonEventName, string(message.Key)) &&
					assert.NoErrorf(t, err, "Failed to parse as DisciplineEvent: %v", message) &&
					assert.Equal(
						t, expectedId, event.Id,
						"Expected id: %v, actual: %d", expectedId, event.Id,
					)
			}),
		).Return(nil)
		// End Init Writer Mock and Expectation

		importer := LessonsImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 3,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.ErrorContains(t, err, "sql: Scan error on column index ")

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("writer error", func(t *testing.T) {
		startDatetime = time.Date(2023, 3, 5, 0, 0, 0, 0, time.Local)
		endDatetime = time.Date(2023, 3, 5, 4, 0, 0, 0, time.Local)
		expectedSqlStartDatetime := time.Date(2023, 3, 5-AdditionalDateRangeInDays, 0, 0, 0, 0, time.Local)
		expectedError := errors.New("expected test error")

		// Start  Init DB Mock
		db, dbMock, err := sqlmock.New()
		if err != nil {
			log.Fatalf("an error '%s' was not expected when opening a mock database connection", err)
		}

		expectedId := uint(20)
		rows := sqlmock.NewRows(expectedColumns).AddRow(
			expectedId, 999, time.Time{}, 1, 1, false,
		)

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonQuery)).WithArgs(
			expectedSqlStartDatetime.Format(dateFormat), endDatetime.Format(dateFormat),
		).WillReturnRows(rows)
		// End Init DB Mock

		// start Init Writer Mock and Expectation
		writer := mocks.NewWriterInterface(t)

		writer.On(
			"WriteMessages",
			matchContext,
			mock.Anything,
		).Return(expectedError)
		// End Init Writer Mock and Expectation

		importer := LessonsImporter{
			out:            &out,
			db:             db,
			writer:         writer,
			writeThreshold: 1,
		}

		err = importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedError, err)

		err = dbMock.ExpectationsWereMet()
		assert.NoErrorf(t, err, "there were unfulfilled expectations: %s", err)
		writer.AssertNumberOfCalls(t, "WriteMessages", 1)

		writer.AssertExpectations(t)
	})

	t.Run("db ping fails", func(t *testing.T) {
		expectedErr := errors.New("ping error")

		db, dbMock, _ := sqlmock.New(sqlmock.MonitorPingsOption(true))
		dbMock.ExpectPing().WillReturnError(expectedErr)

		importer := LessonsImporter{
			out:            &out,
			db:             db,
			writer:         nil,
			writeThreshold: 3,
		}

		err := importer.execute(startDatetime, endDatetime, year)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

}

func TestImportLessonsType(t *testing.T) {
	columns := []string{"ID", "NUM_PREDM", "DATEZAN"}
	t.Run("valid lesson types", func(t *testing.T) {
		var out bytes.Buffer

		// Start  Init DB Mock
		db, dbMock, _ := sqlmock.New()

		importer := LessonsImporter{
			out: &out,
			db:  db,
		}

		expectedLessonType := events.LessonType{
			Id:        65,
			ShortName: "Лек",
			LongName:  "Лекція",
		}

		rows := sqlmock.NewRows(columns).AddRow(
			expectedLessonType.Id, expectedLessonType.ShortName, expectedLessonType.LongName,
		)

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonTypesQuery)).WillReturnRows(rows)

		actualLessonTypes, actualErr := importer.importLessonTypes()

		assert.Equal(t, []events.LessonType{expectedLessonType}, actualLessonTypes)
		assert.NoError(t, actualErr)
	})

	t.Run("error lesson types", func(t *testing.T) {
		var out bytes.Buffer
		expectedError := errors.New("expected test error")
		// Start  Init DB Mock
		db, dbMock, _ := sqlmock.New()
		importer := LessonsImporter{
			out: &out,
			db:  db,
		}

		dbMock.ExpectQuery(regexp.QuoteMeta(LessonTypesQuery)).WillReturnError(expectedError)
		actualLessonTypes, actualErr := importer.importLessonTypes()

		assert.Nil(t, actualLessonTypes)
		assert.Error(t, actualErr)
		assert.Equal(t, expectedError, actualErr)
	})
}
