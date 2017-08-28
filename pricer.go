package main

import (
	//"flag"
	"fmt"
	"os"
	"log"
	"bufio"
	"strconv"
	"container/heap"
	"strings"
	"math"
)

type AddOrder struct {
	timestamp uint64
	orderType string //always "A" for add order
	orderId string
	side string
	price float64
	size uint64
	index int //index of item in the heap
	usedSize uint64
	unusedSize uint64
}

type BidHeap []*AddOrder

type AskHeap []*AddOrder

//TODO:LR: how do we not repeat code for BidHeap and AskHeap? They just differ in implmentation of "Less" functionality
func (pq BidHeap) Len() int { return len(pq) }

func (pq BidHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *BidHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*AddOrder)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *BidHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq BidHeap) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].price > pq[j].price
}

func (pq AskHeap) Len() int { return len(pq) }

func (pq AskHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *AskHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*AddOrder)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *AskHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq AskHeap) Less(i, j int) bool {
	return pq[i].price < pq[j].price
}

var (
	totalBidQty, totalAskQty uint64 = 0, 0
	income, expense float64 = 0, 0
	//usedOrders map[string]AddOrder
	usedOrders = make(map[string]AddOrder)
	bidIdx, askIdx int = 0, 0
)

func printOutput(timestamp uint64, side string, proceeds float64, isProceedsNA bool) {

	if !isProceedsNA {
		fmt.Printf("%d, %s, %f\n", timestamp, side, proceeds)
	} else {
		fmt.Printf("%d, %s, %s\n", timestamp, side, "NA")
	}

}

func processOrder(line string, bidHeap *BidHeap, askHeap *AskHeap, targetSize uint64) {

	order := strings.Fields(line)
	if len(order) < 3 {
		log.Fatal("Order is not in the right format; must contain atleast timestamp, order type and order id")
	}
	timestamp, _ := strconv.ParseUint(order[0], 10, 64)
	orderType := order[1]
	orderId := order[2]
	if orderType == "A" {
		side := order[3]
		price, _ := strconv.ParseFloat(order[4], 64)
		size, _ := strconv.ParseUint(order[5], 10, 64)
		if side == "B" {
			ao := AddOrder{timestamp, orderType, orderId, side, price, size, askIdx, 0, size}
			totalAskQty += size
			heap.Push(bidHeap, &ao)
			askIdx++
			if totalAskQty >= targetSize {
				askQtyToReduce := targetSize
				proceeds := 0.
				for askQtyToReduce > 0 {
					if bidHeap.Len() > 0 {
						currAO := *(heap.Pop(bidHeap).(*AddOrder))
						usedSize := math.Min(float64(askQtyToReduce), float64(ao.unusedSize))
						askQtyToReduce -= uint64(usedSize)
						proceeds += currAO.price * usedSize
						currAO.usedSize = uint64(usedSize)
						currAO.unusedSize = currAO.size - uint64(usedSize)
						usedOrders[currAO.orderId] = currAO //we used this order to compute the proceeds
						//do we have to reinsert the remaining quantity back into the heap, if we don't fully exhaust it?
					} else {
						break
					}
				}
				if askQtyToReduce == 0 {
					printOutput(timestamp, "S", proceeds, false)
				}
			}

		} else if side == "S" { //lot of repeat code between B and S...make it DRY
			ao := AddOrder{timestamp, orderType, orderId, side, price, size, bidIdx, 0, size}
			totalBidQty += size
			heap.Push(askHeap, &ao)
			bidIdx++
			if totalBidQty >= targetSize {
				bidQtyToReduce := totalBidQty
				proceeds := 0.
				for bidQtyToReduce > 0 {
					if askHeap.Len() > 0 {
						currAO := *(heap.Pop(askHeap).(*AddOrder))
						proceeds += currAO.price * float64(currAO.size)
						bidQtyToReduce -= targetSize
						usedOrders[currAO.orderId] = currAO
					} else {
						break
					}
				}
				if bidQtyToReduce == 0 {
					printOutput(timestamp, "B", proceeds, false)
				}
			}
		}

	} else if orderType == "R" {
		if aoToReduce, ok := usedOrders[orderId]; ok {
			sizeToReduceBy, _ := strconv.ParseUint(order[3], 10, 64)
			if sizeToReduceBy > aoToReduce.unusedSize {
				side := aoToReduce.side
				if side == "B" {
					totalAskQty -= aoToReduce.usedSize
					printOutput(timestamp, side, 0, true)
				} else if side == "S" {
					totalBidQty -= aoToReduce.usedSize
					printOutput(timestamp, side, 0, true)
				}

			}
		}

	}

}

func readLineFromFile(filename string) (c chan string){

	// https://stackoverflow.com/questions/37458080/golang-read-file-generator
	c =  make(chan string, 1) //make the channel buffered otherwise it will read all of the file at once...TODO:LR: WHY?
	go func() {
		defer close(c)

		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
			//close(c) //TODO:LR: do we need to close channel here or does defer close(c) below still fine?
		}
		defer file.Close()


		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			c <- scanner.Text()
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()

	return c
}

func main() {

	//targetSizePtr := flag.Uint("targetsize", 0, "target size of shares bought or sold")
	//flag.Parse()
	//var targetSize = *targetSizePtr
	var targetSize = 200

	bidHeap := make(BidHeap, 0)
	heap.Init(&bidHeap)
	askHeap := make(AskHeap, 0)
	heap.Init(&askHeap)

	for line := range readLineFromFile("/Users/Labhesh/GoglandProjects/src/rgm_orderbook/log.txt") {
		processOrder(line, &bidHeap, &askHeap, uint64(targetSize))
	}
}
