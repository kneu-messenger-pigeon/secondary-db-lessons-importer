// Code generated by mockery v2.14.1. DO NOT EDIT.

package main

import (
	time "time"

	mock "github.com/stretchr/testify/mock"
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
