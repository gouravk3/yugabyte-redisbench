package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/panjiang/redisbench/config"
	"github.com/panjiang/redisbench/internal/datasize"
	"github.com/panjiang/redisbench/models"
	"github.com/panjiang/redisbench/statreader"
	"github.com/panjiang/redisbench/tester"
	"github.com/panjiang/redisbench/utils"
	"github.com/panjiang/redisbench/wares"

	"github.com/go-redis/redis/v8"
)

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

const (
	keyPrefix = "benchmark-set"
)

func executeSet(id int, times int, size int, redisClient redis.UniversalClient, arrChan chan []float64) {
	defer tester.Wg.Done()
	val := utils.RandSeq(size)
	arr := make([]float64, times)
	var (
		err error
	)
	for i := 0; i < times; i++ {
		key := fmt.Sprintf("%s.%d.%d", keyPrefix, id, i)
		t := time.Now()
		err = redisClient.Set(context.Background(), key, val, 0).Err()
		utils.FatalErr(err)

		arr[i] = time.Since(t).Seconds() * 1000
	}

	arrChan <- arr
	time.Sleep(time.Microsecond * 100)
}

func executeDel(id int, times int, redisClient redis.UniversalClient) {
	defer tester.Wg.Done()
	for i := 0; i < times; i++ {
		key := fmt.Sprintf("%s.%d.%d", keyPrefix, id, i)
		redisClient.Del(context.Background(), key)
	}
}

func executeGet(id int, times int, redisClient redis.UniversalClient, arrChan chan []float64) {
	defer tester.Wg.Done()
	arr := make([]float64, times)
	var err error
	for i := 0; i < times; i++ {
		key := fmt.Sprintf("%s.%d.%d", keyPrefix, id, i)
		t := time.Now()
		err = redisClient.Get(context.Background(), key).Err()
		utils.FatalErr(err)

		arr[i] = time.Since(t).Seconds() * 1000
	}

	arrChan <- arr
	time.Sleep(time.Microsecond * 100)
}

func createReadClients() (clients []redis.UniversalClient) {
	addrArr := strings.Split(config.ReadClientAddrs, ",")
	for _, addr := range addrArr {
		client := redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Password: config.RedisPassword,
			DB:       config.RedisDB,
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		_, err := client.Ping(ctx).Result()
		if err != nil {
			cancel()
			return nil
		}

		clients = append(clients, client)
		cancel()
	}

	return
}

func fileWriter(arrChan chan []float64, file *os.File) {
	for arr := range arrChan {
		if err := binary.Write(file, binary.LittleEndian, arr); err != nil {
			panic(err)
		}
	}
}

func fileReader(name string) {
	file, err := os.Open(name)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	var data []float64
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}

	numFloats := fileInfo.Size() / 8

	data = make([]float64, numFloats)
	if err := binary.Read(file, binary.LittleEndian, &data); err != nil {
		panic(err)
	}
	sort.Float64s(data)

	min := data[0]
	max := data[len(data)-1]

	p90 := statreader.Percentile(data, 90)
	p95 := statreader.Percentile(data, 95)
	p99 := statreader.Percentile(data, 99)

	if strings.Contains(name, "write") {
		log.Info().Msgf("Write percentile - p90: %.6f, p95: %.6f, p99: %.6f, min: %.6f, max: %.6f", p90, p95, p99, min, max)
	} else {
		log.Info().Msgf("Read percentile - p90: %.6f, p95: %.6f, p99: %.6f, min: %.6f, max: %.6f", p90, p95, p99, min, max)
	}
}

func main() {
	// Parse config arguments from command-line
	config.Parse()
	if config.MultiAddr != "" {
		tester.RPCRun()
	}

	if _, err := os.Stat("temp"); os.IsNotExist(err) {
		err := os.Mkdir("temp", os.ModePerm)
		if err != nil {
			panic(err)
		}
	}

	writeFilePath := fmt.Sprintf("temp/write_%v.txt", time.Now().Format(time.RFC3339))
	writeFile, err := os.OpenFile(writeFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	writerArrChan := make(chan []float64)
	go func() {
		fileWriter(writerArrChan, writeFile)
	}()

	readFilePath := fmt.Sprintf("temp/read_%v.txt", time.Now().Format(time.RFC3339))
	readFile, err := os.OpenFile(readFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	tester.Wg.Wait()

	// Print test initial information
	totalTimes := int64(config.ClientNum * config.TestTimes)
	totalSize := datasize.ByteSize(config.ClientNum * config.TestTimes * config.DataSize)
	log.Info().Str("addr", config.RedisAddr).Msg("Redis")

	log.Info().
		Int("clientNum", config.ClientNum).
		Int("testTimes", config.TestTimes).
		Stringer("dataSize", datasize.ByteSize(config.DataSize)).
		Msg("Config")

	log.Info().
		Int64("times", totalTimes).
		Stringer("size", totalSize).
		Msg("Total")

	// Create a new redis client
	redisClient, err := wares.NewUniversalRedisClient()
	utils.FatalErr(err)

	log.Info().Msg("Testing Write...")
	t1 := time.Now()
	for i := 0; i < config.ClientNum; i++ {
		tester.Wg.Add(1)
		go executeSet(i, config.TestTimes, config.DataSize, redisClient, writerArrChan)
	}
	tester.Wg.Wait()
	t2 := time.Now()

	log.Info().Msgf("Waiting %d seconds before testing read...", config.WaitTime)
	time.Sleep(time.Second * time.Duration(config.WaitTime))
	readArrChan := make(chan []float64)
	go func() {
		fileWriter(readArrChan, readFile)
	}()

	log.Info().Msg("Testing Read...")
	readClients := createReadClients()
	l := len(readClients)
	t3 := time.Now()
	for i := 0; i < config.ClientNum; i++ {
		index := i % l
		tester.Wg.Add(1)
		go executeGet(i, config.TestTimes, readClients[index], readArrChan)
	}
	tester.Wg.Wait()
	t4 := time.Now()

	// Calculate the duration
	dur := t2.Sub(t1)
	durRead := t4.Sub(t3)
	close(writerArrChan)
	close(readArrChan)

	order := 1
	if tester.Multi != nil {
		order = tester.Multi.Order
	}
	result := &models.NodeResult{Order: order, TotalTimes: totalTimes, TsBeg: t1, TsEnd: t2, TotalDur: dur}
	resultRead := &models.NodeResult{Order: order, TotalTimes: totalTimes, TsBeg: t3, TsEnd: t4, TotalDur: durRead}

	if tester.Multi == nil {
		log.Info().
			Int64("times", result.TotalTimes).
			Stringer("duration", result.TotalDur).
			Int64("tps", tester.CalTps(result.TotalTimes, result.TotalDur)).
			Msg("* Write Result")

		log.Info().
			Int64("times", resultRead.TotalTimes).
			Stringer("duration", resultRead.TotalDur).
			Int64("tps", tester.CalTps(resultRead.TotalTimes, resultRead.TotalDur)).
			Msg("* Read Result")

	} else {
		if !tester.Multi.IsMaster() {
			// Notice master to settle
			tester.Multi.NoticeMasterSettle(result)
			log.Info().Msg("* See summary info on node 1")
		} else {
			tester.Wg.Add(1) // Wait all others nodes settling call
			tester.Multi.NodeSettle(result)

			tester.Wg.Wait()
			time.Sleep(time.Second)
			// Summary all nodes result include self
			summary := tester.Multi.Summary()

			// Print testing result
			log.Info().
				Int64("times", summary.TotalTimes).
				Stringer("duration", summary.TotalDur).
				Int("tps", summary.TPS).
				Msg("* Summary")
		}
	}

	log.Debug().Msg("Deleting testing data...")
	for i := 0; i < config.ClientNum; i++ {
		tester.Wg.Add(1)
		go executeDel(i, config.TestTimes, redisClient)
	}

	fileReader(writeFilePath)
	fileReader(readFilePath)
	tester.Wg.Wait()
	log.Debug().Msg("Over")
}
