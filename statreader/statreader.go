package statreader

import (
	"bufio"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

func readNumbersFromFile(filename string) ([]float64, error) {
	var numbers []float64

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
			continue
		}
		num, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, err
		}
		numbers = append(numbers, num)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	sort.Float64s(numbers)

	return numbers, nil
}

func Percentile(numbers []float64, percentile float64) float64 {
	k := (percentile / 100) * float64(len(numbers)-1)
	f := int(k)
	c := k - float64(f)
	if f+1 < len(numbers) {
		return numbers[f] + c*(numbers[f+1]-numbers[f])
	}
	return numbers[f]
}

func minMax(numbers []float64) (float64, float64) {
	if len(numbers) == 0 {
		return -1, -1
	}
	return numbers[0], numbers[len(numbers)-1]
}

func PercentileCal(filename string) {
	numbers, err := readNumbersFromFile(filename)
	if err != nil {
		log.Error().Msgf("Error reading numbers from file: %s", err)
		log.Panic().Err(err)
	}

	min, max := minMax(numbers)

	p90 := Percentile(numbers, 90)
	p95 := Percentile(numbers, 95)
	p99 := Percentile(numbers, 99)

	if strings.Contains(filename, "write") {
		log.Info().Msgf("Write percentile- p90: %.6f, p95: %.6f, p99: %.6f, min: %.6f, max: %.6f", p90, p95, p99, min, max)
	} else {
		log.Info().Msgf("Read percentile- p90: %.6f, p95: %.6f, p99: %.6f, min: %.6f, max: %.6f", p90, p95, p99, min, max)
	}
}
