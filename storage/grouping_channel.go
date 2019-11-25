package storage

import (
	"sync"
)

type GroupingChannelElement interface{}

type GroupingChannel struct {
	inputChannel chan GroupingChannelElement
	groupedChannel chan []GroupingChannelElement
	size uint
}

func NewGroupingChannel(size uint, inputChannel chan GroupingChannelElement, wg *sync.WaitGroup) *GroupingChannel {
	var gc GroupingChannel
	gc.size = size
	gc.inputChannel = inputChannel
	gc.groupedChannel = make(chan []GroupingChannelElement, gc.size)

	go gc.Run(wg)

	return &gc
}

func (gc *GroupingChannel) WriteElementsAndClear(elements *[]GroupingChannelElement) {
	outputElements := make([]GroupingChannelElement, len(*elements))
	copy(outputElements, *elements)
	gc.groupedChannel <- outputElements
	*elements = (*elements)[:0]
}

func (gc *GroupingChannel) Run(wg* sync.WaitGroup) {
	defer wg.Done()
	defer close(gc.groupedChannel)

	pendingElements := make([]GroupingChannelElement, 0)

	for element := range gc.inputChannel {
		pendingElements = append(pendingElements, element)
		if len(pendingElements) == int(gc.size) {
			gc.WriteElementsAndClear(&pendingElements)
		}
	}
	if len(pendingElements) != 0 {
		gc.WriteElementsAndClear(&pendingElements)
	}
}
