package storage

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestReturnsGroupWhenComplete(t *testing.T) {
	var wg sync.WaitGroup
	c := make(chan GroupingChannelElement)

	wg.Add(1)
	gc := NewGroupingChannel(3, c, &wg)
	sendElementsAndClose(c, []string{"a", "b", "c", "d"})
	wg.Wait()

	g := <- gc.groupedChannel
	assertEqualElements(t, []string{"a", "b", "c"}, g)
	g = <- gc.groupedChannel
	assert.Equal(t, 1, len(g))
	assertEqualElements(t, []string{"d"}, g)
}

func TestReturnsSingleElementWhenClosed(t *testing.T) {
	var wg sync.WaitGroup
	c := make(chan GroupingChannelElement)

	wg.Add(1)
	gc := NewGroupingChannel(3, c, &wg)
	sendElementsAndClose(c, []string{"a"})
	wg.Wait()

	g := <- gc.groupedChannel
	assertEqualElements(t, []string{"a"}, g)
}

func TestReturnsEmptyWhenClosedImmediately(t *testing.T) {
	var wg sync.WaitGroup
	c := make(chan GroupingChannelElement)

	wg.Add(1)
	gc := NewGroupingChannel(3, c, &wg)
	sendElementsAndClose(c, []string{})
	wg.Wait()

	_, hasValue := <- gc.groupedChannel
	assert.False(t, hasValue)
}

func sendElementsAndClose(c chan GroupingChannelElement, elements []string) {
	for _, element := range elements {
		c <- GroupingChannelElement(element)
	}
	close(c)
}

func assertEqualElements(t *testing.T, expected []string, actual []GroupingChannelElement) {
	assert.Equal(t, len(expected), len(actual))
	for i := 0; i < len(expected); i++ {
		assert.Equal(t, expected[i], actual[i])
	}
}