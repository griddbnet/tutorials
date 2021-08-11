package main

import(
    "github.com/griddb/go_client"
    "fmt"
    "math"
    "math/rand"
//    "os"
    "time"
)

numSensors := 5

func generate_data(duration, increment float64) [][]interface{} {

    hours := duration * 3600000 //converts to hours
    minutes := increment * 60000 // converts to minutes
    
    hoursMilli := time.Duration(hours) * time.Millisecond
    minutesMilli := time.Duration(minutes) * time.Millisecond

    d := hoursMilli.Milliseconds()
    inc := minutesMilli.Milliseconds()

    arrLen := (d / inc) * int64(numSensors) //gives us the amount of times to emit sensor data total

    times := make([][]time.Time, arrLen)
    id := make([][]int, arrLen)
    data := make([][]float64, arrLen)
    temp := make([][]float64, arrLen)
    
    now := time.Now()

	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

    fullData := make([][]interface{}, arrLen)

    for i := 0; i < int(arrLen); i++ {

        innerLen := numSensors
        fullData[i] = make([]interface{}, innerLen)
		times[i] = make([]time.Time, innerLen)
		id[i] = make([]int, innerLen)
		data[i] = make([]float64, innerLen)
		temp[i] = make([]float64, innerLen)

        var rowList []interface{}
        for j := 0; j < innerLen; j++ {
            addedTime := i * minutes
			timeToAdd := time.Minute * time.Duration(addedTime)
			incTime := now.Add(timeToAdd)

            times[i][j] = incTime
            id[i][j] = j
            data[i][j] = (r1.Float64() * 100) + 5
		    x := (r1.Float64() * 100) + 2
		    temp[i][j] = math.Floor(x*100) / 100

            var row []interface{}
            row = append(row, times[i][j])
			row = append(row, id[i][j])
			row = append(row, data[i][j])
			row = append(row, temp[i][j])
            rowList = append(rowList, row)
        }
        fullData[i] = rowList
    }
    
    return fullData

}

func main() {
    factory := griddb_go.StoreFactoryGetInstance()
    defer griddb_go.DeleteStoreFactory(factory)

    // Get GridStore object
    gridstore, err := factory.GetStore(map[string]interface{} {
        "host": "239.0.0.1",
        "port": 31999,
        "cluster_name": "defaultCluster",
        "username": "admin",
        "password": "admin"})
    if (err != nil) {
        fmt.Println(err)
        panic("err get store")
    }
    defer griddb_go.DeleteStore(gridstore)

    conInfo, err := griddb_go.CreateContainerInfo(map[string]interface{} {
        "name": "sensors8",
        "column_info_list":[][]interface{}{
            {"timestamp", griddb_go.TYPE_TIMESTAMP},
            {"id", griddb_go.TYPE_SHORT},
            {"data", griddb_go.TYPE_FLOAT},
            {"temperature", griddb_go.TYPE_FLOAT}},
        "type": griddb_go.CONTAINER_TIME_SERIES,
        "row_key": true})
    if (err != nil) {
        fmt.Println("Create containerInfo failed, err:", err)
        panic("err CreateContainerInfo")
    }
    defer griddb_go.DeleteContainerInfo(conInfo)

    ts, err := gridstore.PutContainer(conInfo, true)
    if (err != nil) {
        fmt.Println("put container failed, err:", err)
        panic("err PutContainer")
    }
  //  defer griddb_go.DeleteContainer(ts)
    fully := generate_data(24, 5)

    for _, outerVal := range fully {
        for _, innerVal := range outerVal {
            err := ts.Put(innerVal.([]interface{}))
            if err != nil {
                fmt.Println("put error: ", err)
                return
            } else {
                fmt.Println("successfully put: ", innerVal)
            }
        }
    } 

    //err2 := ts.MultiPut(fully)
     //   if err2 != nil {
      //      fmt.Println("failure to cooperate", err2)
       //     return
       // }

    // Create normal query for range of timestamp from 6 hours ago to now
    query,err := ts.Query("select *")
    if (err != nil) {
        fmt.Println("create query failed:", err)
        panic("err Query")
    }
    defer griddb_go.DeleteQuery(query)
    rs, err := query.Fetch()
    if (err != nil) {
        fmt.Println("create rs failed:", err)
        panic("err Fetch")
    }
    defer griddb_go.DeleteRowSet(rs)
    for rs.HasNext() {
        rrow, err := rs.NextRow()
        if (err != nil) {
            fmt.Println("NextRow from rs failed:", err)
        }
        fmt.Println("Time=", rrow[0]," ID=", rrow[1]," Data=", rrow[2], "Temp=", rrow[3])
    }


}