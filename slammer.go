package main

import (
	"bufio"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	// Load the drivers
	// MySQL
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/golang/glog"
)

type config struct {
	connString    string
	db            string
	pauseInterval time.Duration
	workers       int
	debugMode     bool
	statsInterval time.Duration
	repeat        int
}

type result struct {
	start     time.Time
	end       time.Time
	dbTime    time.Duration
	workCount int
	errors    int
}

func main() {
	cfg, err := getConfig()
	if err != nil {
		log.Fatal(err)
	}

	// Open the connection to the DB - should each worker open it's own connection?
	// Food for thought
	db, err := sql.Open(cfg.db, cfg.connString)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(40)
	maxLife,_ := time.ParseDuration("60s")
	db.SetConnMaxLifetime(maxLife)
	db.SetMaxIdleConns(40)

	// Declare the channel we'll be using as a work queue
	inputChan := make(chan (string))
	// Declare the channel that will gather results
	outputChan := make(chan (result), cfg.workers)
	// Declare a waitgroup to help prevent log interleaving - I technically do not
	// need one, but without it, I find there are stray log messages creeping into
	// the final report. Setting sync() on STDOUT/ERR didn't seem to fix it.
	var wg sync.WaitGroup
	wg.Add(cfg.workers)

	// Start the pool of workers up, reading from the channel
	totalStart := time.Now()
	startWorkers(cfg.workers, inputChan, outputChan, db, &wg, cfg.pauseInterval, cfg.debugMode)
	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go statsWorker(outputChan,cfg.statsInterval, &statsWg)

	// Warm up error and line so I can use error in the for loop with running into
	// a shadowing issue
	err = nil
	line := ""
	totalWorkCount := 0
	// Read from STDIN in the main thread
	input := bufio.NewReader(os.Stdin)
	inbuf := []string{}
	for err != io.EOF {
		line, err = input.ReadString('\n')
		if err == nil {
			// Get rid of any unwanted stuff
			line = strings.TrimRight(line, "\r\n")
			// Push that onto the work queue
			inbuf = append(inbuf, line)
		} else if cfg.debugMode {
			log.Println(err)
		}
	}


	for i:=0; i<cfg.repeat; i++ {
		for _, l := range inbuf {
			inputChan <- l
			totalWorkCount++
		}
	}

	// Close the channel, since it's done receiving input
	close(inputChan)
	// As I mentioned above, because workers wont finish at the same time, I need
	// to use a waitgroup, otherwise the output below gets potentially mixed in with
	// debug or error messages from the workers. The waitgroup semaphore prevents this
	// even though it probably looks redundant
	wg.Wait()
	close(outputChan)
	statsWg.Wait()
	totalEnd := time.Now()
	wallTime := totalEnd.Sub(totalStart)
	// Collect all results, report them. This will block and wait until all results
	// are in
	fmt.Println("Slammer Status:")
	totalErrors := 0
	var totalDbTime time.Duration
	for i := 0; i < cfg.workers; i++ {
		r := <-outputChan
		workerDuration := r.end.Sub(r.start)
		totalErrors += r.errors
		totalDbTime += r.dbTime
		fmt.Printf("---- Worker #%d ----\n", i)
		fmt.Printf("  Started at %s , Ended at %s, Wall time %s, DB time %s\n", r.start.Format("2006-01-02 15:04:05"), r.end.Format("2006-01-02 15:04:05"), workerDuration.String(), r.dbTime)
		fmt.Printf("  Units of work: %d, Percentage work: %f, Average work over DB time: %f\n", r.workCount, float64(r.workCount)/float64(totalWorkCount), float64(r.workCount)/float64(r.dbTime))
		fmt.Printf("  Errors: %d , Percentage errors: %f, Average errors per second: %f\n", r.errors, float64(r.errors)/float64(r.workCount), float64(r.errors)/workerDuration.Seconds())
	}
	// TODO work on improving what we report here
	fmt.Printf("---- Overall ----\n")
	fmt.Printf("  Started at %s , Ended at %s, Wall time %s, DB time %s\n", totalStart.Format("2006-01-02 15:04:05"), totalEnd.Format("2006-01-02 15:04:05"), wallTime, totalDbTime)
	fmt.Printf("  Units of work: %d, Average work over DB time: %f\n", totalWorkCount, float64(totalWorkCount)/float64(totalDbTime))
	fmt.Printf("  Errors: %d, Percentage errors: %f\n", totalErrors, float64(totalErrors)/float64(totalWorkCount))
	// Lets just be nice and tidy
	//close(outputChan)
}

func startWorkers(count int, ic <-chan string, oc chan<- result, db *sql.DB, wg *sync.WaitGroup, pause time.Duration, debugMode bool) {
	// Start the pool of workers up, reading from the channel
	for i := 0; i < count; i++ {
		// register a signal chan for handling shutdown
		sc := make(chan os.Signal)
		signal.Notify(sc, os.Interrupt)
		// Pass in everything it needs
		go startWorker(i, ic, oc, sc, db, wg, pause, debugMode)
	}
}

func startWorker(workerNum int, ic <-chan string, oc chan<- result, sc <-chan os.Signal, db *sql.DB, done *sync.WaitGroup, pause time.Duration, debugMode bool) {
	// Prep the result object

	glog.Info("Worker starting")
	shouldExit := false
	for {
		// First thing is first - do a non blocking read from the signal channel, and
		// handle it if something came through the pipe
		select {
		case _ = <-sc:
			// UGH I ACTUALLY ALMOST USED A GOTO HERE BUT I JUST CANT DO IT
			// NO NO NO NO NO NO I WONT YOU CANT MAKE ME NO
			// I could put it into an anonymous function defer, though...
			shouldExit = true
		case line, ok := <- ic:
			r := result{start: time.Now()}
			if!ok {
				shouldExit = true
			}
			t := time.Now()
			_, err := db.Exec(line)
			r.dbTime += time.Since(t)
			// TODO should this be after the err != nil? It counts towards work attempted
			// but not work completed.
			r.workCount = 1
			if err != nil {
				r.errors = 1
				if debugMode {
					log.Printf("Worker #%d: %s - %s", workerNum, line, err.Error())
				}
			}
			r.end = time.Now()
			oc <- r
			time.Sleep(pause)

		}
		if shouldExit {
			break
		}
	}

	// Let everyone know we're done, and bail out

	done.Done()
}

func statsWorker(ic <-chan result, interval time.Duration, wg *sync.WaitGroup) {
	fmt.Printf("Status worker started, interval:%s\n", interval)
	errs := 0
	success := 0
	totalWork := 0
	lastErrs := errs
	//lastSuccess := success
	lastTotal := totalWork

	t := time.Now()
	shouldExit := false
	for {
		select {
		case r, ok := <-ic:
			if !ok {
				shouldExit = true
			}
			glog.V(5).Info("Read result!")
			if r.errors > 0 {
				errs++
			} else {
				success++
			}
			totalWork++

		}

		if time.Now().Sub(t) > interval {
			t = time.Now()

			periodErrs := errs - lastErrs
			periodWork := totalWork - lastTotal
			// 2006-01-02 15:04:05 Queries:1000 Period: Lost(1/200) (0.5%)
			fmt.Printf("%s Queries:%d Period: Lost(%d/%d) (%0.3f%%)\n",
				t.Format("2006-01-02 15:04:05.00"),
				totalWork,
				periodErrs,
				periodWork,
				float64(periodErrs)/float64(periodWork))

			lastTotal = totalWork
			lastErrs = errs
		}
		if shouldExit {
			break
		}
	}
	glog.Info("StatsWorker Done!")
	wg.Done()
}

func getConfig() (*config, error) {
	p := flag.String("p", "1s", "The time to pause between each call to the database")
	c := flag.String("c", "", "The connection string to use when connecting to the database")
	db := flag.String("db", "mysql", "The database driver to load. Defaults to mysql")
	w := flag.Int("w", 1, "The number of workers to use. A number greater than 1 will enable statements to be issued concurrently")
	d := flag.Bool("d", false, "Debug mode - turn this on to have errors printed to the terminal")
	r := flag.Int("r", 1, "How many times should the provided input be repeated, default 1")
	i := flag.String("i", "1s", "Time interval between printing statistics")
	// TODO support an "interactive" flag to drop you into a shell that outputs things like
	// sparklines of the current worker throughputs
	flag.Parse()

	if *c == "" {
		return nil, errors.New("You must provide a connection string using the -c option")
	}
	pi, err := time.ParseDuration(*p)
	if err != nil {
		return nil, errors.New("You must provide a proper duration value with -p")
	}


	ii, err := time.ParseDuration(*i)
	if err != nil {
		return nil, errors.New("You must provide a proper duration value with -i")
	}

	if *w <= 0 {
		return nil, errors.New("You must provide a worker count > 0 with -w")
	}

	return &config{db: *db, connString: *c, pauseInterval: pi, workers: *w, debugMode: *d, statsInterval: ii, repeat:*r}, nil
}
