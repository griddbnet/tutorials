package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

var numSensors = 15

func generate_data() map[string][][]interface{} {

	containerNameList := make([]string, numSensors)
	for i, _ := range containerNameList {
		containerNameList[i] = "testing_" + strconv.Itoa(i)
	}
	fmt.Println("containerNameList: ", containerNameList)

	hours, _ := strconv.Atoi(os.Args[1])
	minutes, _ := strconv.Atoi(os.Args[2])

	duration := hours * 3600000  //converts to hours
	increment := minutes * 60000 // converts to minutes

	durationMilli := time.Duration(duration) * time.Millisecond
	incrementMilli := time.Duration(increment) * time.Millisecond

	d := durationMilli.Milliseconds()
	inc := incrementMilli.Milliseconds()

	arrLen := (d / inc) * int64(numSensors) //gives us the amount of times to emit sensor data total
	fmt.Println("arrLen: ", arrLen)

	times := make([][]time.Time, arrLen)
	data := make([][]float64, arrLen)
	temp := make([][]float64, arrLen)

	now := time.Now().UTC()
	fmt.Println("time now: ", now)

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	var inEntryList = make(map[string][][]interface{}, 0)
	fullData := make([][][]interface{}, 1)
	fullData[0] = make([][]interface{}, arrLen)

	for i := 0; i < int(arrLen); i++ {

		innerLen := numSensors
		fullData[0][i] = make([]interface{}, innerLen)
		times[i] = make([]time.Time, innerLen)
		data[i] = make([]float64, innerLen)
		temp[i] = make([]float64, innerLen)
		var rowList []interface{}

		for j := 0; j < innerLen; j++ {
			addedTime := i * minutes
			timeToAdd := time.Minute * time.Duration(addedTime)
			incTime := now.Add(timeToAdd)

			times[i][j] = incTime
			data[i][j] = (r1.Float64() * 100) + 5
			x := (r1.Float64() * 100) + 2
			temp[i][j] = math.Floor(x*100) / 100

			var row []interface{}
			row = append(row, times[i][j])
			row = append(row, data[i][j])
			row = append(row, temp[i][j])
			rowList = append(rowList, row)
		}
		fullData[0][i] = rowList
	}

	// form map with proper container names
	for _, val := range fullData {
		for i := 0; i < numSensors; i++ {
			rowList := [][]interface{}{}
			idx := containerNameList[i]
			for _, innerVal := range val {
				rowList = append(rowList, innerVal[i].([]interface{}))
			}
			inEntryList[idx] = rowList
		}
	}

	return inEntryList
}

func main() {
	factory := griddb_go.StoreFactoryGetInstance()
	defer griddb_go.DeleteStoreFactory(factory)

	// Get GridStore object
	gridstore, err := factory.GetStore(map[string]interface{}{
		"host":         "239.0.0.1",
		"port":         31999,
		"cluster_name": "defaultCluster",
		"username":     "admin",
		"password":     "admin"})
	if err != nil {
		fmt.Println(err)
		panic("err get store")
	}
	defer griddb_go.DeleteStore(gridstore)

	fullDataset := generate_data()

	fullDataset := generate_data()

	for containerName, _ := range fullDataset {
		fmt.Println("containerName: ", containerName)
		conInfo, err := griddb_go.CreateContainerInfo(map[string]interface{}{"name": containerName,
			"column_info_list": [][]interface{}{
				{"timestamp", griddb_go.TYPE_TIMESTAMP},
				{"data", griddb_go.TYPE_FLOAT},
				{"temperature", griddb_go.TYPE_FLOAT}},
			"type":    griddb_go.CONTAINER_TIME_SERIES,
			"row_key": true})
		if err != nil {
			fmt.Println("ERROR CreateContainerInfo")
		}
		defer griddb_go.DeleteContainerInfo(conInfo)

		_, err = gridstore.PutContainer(conInfo)
		if err != nil {
			fmt.Println("ERROR PutContainer, ", err)
		}
	}

	err = gridstore.MultiPut(fullDataset)
	if err != nil {
		fmt.Println("error from MultiPut: ", err)
	} else {
		fmt.Println("successfully Put: : ", fullDataset)
	}

}
