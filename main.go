package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"

	proto_struct "github.com/agnusarev/memcache_loader_go/proto"
)


type Options struct {
	idfa    string
	gaid    string
	adid    string
	dvid    string
	pattern string
}


const MaxErrRate = 0.01


func insert_appsinstalled(memc *memcache.Client, key string, ua *proto_struct.UserApps, dryRun bool) bool {
	packed, err := proto.Marshal(ua)
	if err != nil {
		panic(err)
	}
	if dryRun {
		log.Printf("%s -> %s", key, ua)
		return true
	}
	if err := memc.Set(&memcache.Item{Key: key, Value: packed, Flags: 0, Expiration: 0}); err != nil {
		log.Printf("Cannot write to memc key: %s, value: %s, with error: %v", key, packed, err)
		return false
	}
	return true
}


func process_file(fn string, clients map[string]*memcache.Client, dryRun bool) float64 {
    processed := 0
    errors := 0
    errrate := 0.0
	log.Printf("Processing %s", fn)
	fd, err := os.Open(fn)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	gzReader, err := gzip.NewReader(fd)
	if err != nil {
		log.Fatal(err)
	}
	defer gzReader.Close()
	scanner := bufio.NewScanner(gzReader)

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimRight(line, "\r")
		parts := strings.Split(line, "\t")
		if len(parts) != 5 {
			errors++
			log.Printf("Invalid line format: %s", line)
			continue
		}

		devType := parts[0]
		devID := parts[1]
		lat, err := strconv.ParseFloat(parts[2], 64)
		if err != nil {
			errors++
			log.Printf("Error parsing latitude: %s", line)
			continue
		}
		lon, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			errors++
			log.Printf("Error parsing longitude: %s", line)
			continue
		}

		rawApps := strings.Split(parts[4], ",")
		var apps []uint32
		for _, rawApp := range rawApps {
			app, err := strconv.Atoi(rawApp)
			if err == nil {
				apps = append(apps, uint32(app))
			}
		}
		ua := &proto_struct.UserApps{
			Lat:  proto.Float64(lat),
			Lon:  proto.Float64(lon),
			Apps: apps,
		}
		key := fmt.Sprintf("%s:%s", devType, devID)
		result := insert_appsinstalled(clients[devType], key, ua, dryRun)
		processed++
		if !result {
		    errors++
		}
		errrate := float64(errors) / float64(processed)
		if processed%1000 == 0 {
            fmt.Printf("Processed %d lines in %s, current error rate: %.4f\n", processed, fn, errrate)
        }
	}
	return errrate
}

func parse(options *Options, clients map[string]*memcache.Client, dryRun bool) bool {
	files, err := filepath.Glob(options.pattern)
	if err != nil {
		log.Fatal(err)
	}
	for _, fn := range files {
	    if _, name := filepath.Split(fn); name[0] == '.' {
	        continue
	    }
	    fmt.Println("File: " + fn)
		errrate := process_file(fn, clients, dryRun)
		if errrate <= MaxErrRate {
		    dot_rename(fn)
		} else {
		    fmt.Println("Error rate is too large.")
		}
	}
	return true
}


func dot_rename(fpath string) {
	dir, file := filepath.Split(fpath)
	file = "." + file
	os.Rename(fpath, filepath.Join(dir, file))
}


func main() {

	options := &Options{
		idfa:    "localhost:33013",
		gaid:    "localhost:33014",
		adid:    "localhost:33015",
		dvid:    "localhost:33016",
		pattern: "data/*.tsv.gz",
	}

	clients := make(map[string]*memcache.Client)

	clients["idfa"] = memcache.New(options.idfa)
	clients["gaid"] = memcache.New(options.gaid)
	clients["adid"] = memcache.New(options.adid)
	clients["dvid"] = memcache.New(options.dvid)


    parse(options, clients, false)
}
