// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	events "github.com/kneu-messenger-pigeon/events"
	mock "github.com/stretchr/testify/mock"

	time "time"
)

// MockImporterInterface is an autogenerated mock type for the ImporterInterface type
type MockImporterInterface struct {
	mock.Mock
}

// execute provides a mock function with given fields: startDatetime, endDatetime, year
func (_m *MockImporterInterface) execute(startDatetime time.Time, endDatetime time.Time, year int) error {
	ret := _m.Called(startDatetime, endDatetime, year)

	var r0 error
	if rf, ok := ret.Get(0).(func(time.Time, time.Time, int) error); ok {
		r0 = rf(startDatetime, endDatetime, year)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// importLessonsType provides a mock function with given fields:
func (_m *MockImporterInterface) importLessonTypes() ([]events.LessonType, error) {
	ret := _m.Called()

	var r0 []events.LessonType
	if rf, ok := ret.Get(0).(func() []events.LessonType); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]events.LessonType)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

type mockConstructorTestingTNewMockImporterInterface interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockImporterInterface creates a new instance of MockImporterInterface. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockImporterInterface(t mockConstructorTestingTNewMockImporterInterface) *MockImporterInterface {
	mock := &MockImporterInterface{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
