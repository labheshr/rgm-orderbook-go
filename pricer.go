package main

import (
	//"flag"
	"fmt"
	"os"
	"log"
	"bufio"
	"strconv"
	"strings"
	"math"
	"github.com/emirpasic/gods/sets/treeset"
	"flag"
)

type AddOrder struct {
	timestamp uint64
	orderType string //always "A" for add order
	orderId string
	side string
	price float64
	size int
	usedSize int
}

func byPriceDescQtyTieBreaker(a, b interface{}) int {

	// Type assertion, program will panic if this is not respected
	c1 := a.(AddOrder)
	c2 := b.(AddOrder)

	switch {
	case c1.price == c2.price && c1.size > c2.size:
		return -1
	case c1.price == c2.price && c1.size < c2.size:
		return 1
	case c1.price > c2.price:
		return -1
	case c1.price < c2.price:
		return 1
	default:
		return 0
	}

}

func byPriceAscQtyTieBreaker(a, b interface{}) int {

	// Type assertion, program will panic if this is not respected
	c1 := a.(AddOrder)
	c2 := b.(AddOrder)

	switch {
	case c1.price == c2.price && c1.size > c2.size:
		return -1
	case c1.price == c2.price && c1.size < c2.size:
		return 1
	case c1.price > c2.price:
		return 1
	case c1.price < c2.price:
		return -1
	default:
		return 0
	}

}

var (
	targetSize int = 0
	totalBidQty, totalAskQty int = 0, 0
	currInc, currExp float64 = 0, 0
	isCurrIncNA, isCurrExpNA bool = false, false
	addOrdersById = make(map[string]*AddOrder)
	bidSet = treeset.NewWith(byPriceDescQtyTieBreaker)
	askSet = treeset.NewWith(byPriceAscQtyTieBreaker)
)

func printOutput(timestamp uint64, side string, proceeds float64, isProceedsNA bool) {

	if !isProceedsNA {
		fmt.Printf("%d %s %.2f\n", timestamp, side, proceeds)
	} else {
		fmt.Printf("%d %s %s\n", timestamp, side, "NA")
	}

}

func processOrder(line string) {

	order := strings.Fields(line)
	if len(order) < 3 {
		log.Fatal("Order is not in the right format; must contain atleast timestamp, order type and order id")
	}
	orderType := order[1]
	if orderType == "A" {
		processAddOrder(order)
	} else if orderType == "R" {
		processReduceOrder(order)
	} else {
		log.Fatal("Order type not supported , must be (A)dd, or (R)educe")
	}
	//fmt.Printf("Order: %s, totalAskQty: %d, totalBidQty: %d\n", line, totalAskQty, totalBidQty)
}

func processAddOrder(order[] string) {

	timestamp, _ := strconv.ParseUint(order[0], 10, 64)
	orderType := order[1]
	orderId := order[2]
	side := order[3]
	price, _ := strconv.ParseFloat(order[4], 64)
	size, _ := strconv.Atoi(order[5])
	//create add order and insert into the right heap
	if side == "B" {
		ao := AddOrder{timestamp, orderType, orderId, side, price, size,0}
		bidSet.Add(ao)
		addOrdersById[orderId] = &ao
		computeIncomeAndDoUpdates(size, timestamp)

	} else { //"S"
		ao := AddOrder{timestamp, orderType, orderId, side, price, size, 0}
		askSet.Add(ao)
		//fmt.Println(askSet)
		addOrdersById[orderId] = &ao
		computeExpenseAndDoUpdates(size, timestamp)
	}

}

func computeIncomeAndDoUpdates(size int, timestamp uint64) {
	if totalAskQty < targetSize {
		totalAskQty += size
	}

	if totalAskQty >= targetSize {
		askQtyToReduce := targetSize
		proceeds := 0.
		it := bidSet.Iterator()
		for askQtyToReduce > 0 && it.Next() {
			currAO := it.Value().(AddOrder)
			if currAO.size > 0 {
				usedSize := int(math.Min(float64(askQtyToReduce), float64(currAO.size)))
				askQtyToReduce -= usedSize
				proceeds += currAO.price * float64(usedSize)
				currAO.usedSize = usedSize
				//the map has the pointer to AddOrder...so this object is maintained in its current state in the map
				currMapAO := addOrdersById[currAO.orderId]
				currMapAO.usedSize = currAO.usedSize
			}
		}
		if askQtyToReduce == 0 {
			printOutput(timestamp, "S", proceeds, false)
			currInc = proceeds
			isCurrIncNA = false
		}
	}
}

func computeExpenseAndDoUpdates(size int, timestamp uint64) {
	if totalBidQty < targetSize {
		totalBidQty += size
	}
	if totalBidQty >= targetSize {
		bidQtyToReduce := targetSize
		proceeds := 0.
		it := askSet.Iterator()
		for bidQtyToReduce > 0 && it.Next() {
			currAO := it.Value().(AddOrder)
			if currAO.size > 0 {
				usedSize := int(math.Min(float64(bidQtyToReduce), float64(currAO.size)))
				bidQtyToReduce -= usedSize
				proceeds += currAO.price * float64(usedSize)
				currAO.usedSize = usedSize
				currMapAO := addOrdersById[currAO.orderId]
				currMapAO.usedSize = currAO.usedSize
			}
		}
		if bidQtyToReduce == 0 {
			printOutput(timestamp, "B", proceeds, false)
			currExp = proceeds
			isCurrExpNA = false
		}
	}
}

func processReduceOrder(order[] string) {

	timestamp, _ := strconv.ParseUint(order[0], 10, 64)
	orderId := order[2]

	if aoToReduce, ok := addOrdersById[orderId]; ok {
		sizeToReduceBy, _ := strconv.Atoi(order[3])
		newSize := int(math.Max(float64(aoToReduce.size)-float64(sizeToReduceBy),0))
		side := aoToReduce.side
		if side == "B" {
			if aoToReduce.usedSize > 0 {//we used this entry
				if aoToReduce.usedSize <= newSize {
					aoToReduce.size = newSize
				} else {
					//new size was lower than the used size, so we need to decrease total qty and set the proceeds to NA
					totalAskQty -= sizeToReduceBy
					totalAskQty = int(math.Max(float64(totalAskQty), 0))//never let this go below 0
					if newSize == 0 {
						bidSet.Remove(*aoToReduce) //will this work? esp that the size is changing?
					} else {
						bidSet.Remove(*aoToReduce)
						aoToReduce.size = newSize
						aoToReduce.usedSize = 0
						bidSet.Add(*aoToReduce)
					}
					remainingSizeInBidHeap := 0
					it := bidSet.Iterator()
					for it.Next() {
						remainingSizeInBidHeap += it.Value().(AddOrder).size
					}
					if !isCurrIncNA && remainingSizeInBidHeap < targetSize {
						printOutput(timestamp, "S", 0, true)
						isCurrIncNA = true
					}else {
						computeIncomeAndDoUpdates(remainingSizeInBidHeap, timestamp)
					}
				}
			} else {
				if totalAskQty < targetSize {
					totalAskQty -= sizeToReduceBy
				}
				bidSet.Remove(*aoToReduce)
				//aoToReduce.size = newSize
				//aoToReduce.usedSize = 0
			}
		} else {
			//side == "S"
			if aoToReduce.usedSize > 0 {//we used this entry
				if aoToReduce.usedSize <= newSize {
					aoToReduce.size = newSize
				} else {
					//new size was lower than the used size, so we need to decrease total qty and set the proceeds to NA
					totalBidQty -= sizeToReduceBy
					totalBidQty = int(math.Max(float64(totalBidQty), 0))//never let this go below 0
					if newSize == 0 {
						askSet.Remove(*aoToReduce)
					} else {
						//the sets no longer carry references...for clean soln, remove the current order and insert a new one
						askSet.Remove(*aoToReduce)
						aoToReduce.size = newSize
						aoToReduce.usedSize = 0
						askSet.Add(*aoToReduce)
					}
					remainingSizeInAskHeap := 0
					it := askSet.Iterator()
					for it.Next() {
						remainingSizeInAskHeap += it.Value().(AddOrder).size
					}
					if !isCurrExpNA && remainingSizeInAskHeap < targetSize  {
						printOutput(timestamp, "B", 0, true)
						isCurrExpNA = true
					} else {
						computeExpenseAndDoUpdates(remainingSizeInAskHeap, timestamp)
					}
				}
			} else {
				if totalBidQty < targetSize {
					totalBidQty -= sizeToReduceBy
				}
				askSet.Remove(*aoToReduce)
				//aoToReduce.size = newSize
				//aoToReduce.usedSize = 0

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

	targetSizePtr := flag.Int("targetsize", 0, "target size of shares bought or sold")
	flag.Parse()
	targetSize = *targetSizePtr
	//targetSize = 200
	for line := range readLineFromFile("/Users/Labhesh/GoglandProjects/src/rgm_orderbook/log.txt") {
		processOrder(line)
	}
}
