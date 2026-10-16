package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"x2kafka/pkg/config"
	"x2kafka/pkg/egress"
	"x2kafka/pkg/kafka"
	"x2kafka/pkg/stats"
)

var (
	version  = "1.0.0"
	revision = "dev"
)

func printHelp() {
	fmt.Printf("kafka2x v%s-%s - Kafka Egress to HTTP/REST endpoints\n", version, revision)
	fmt.Println("Usage: kafka2x [options] [config.json|config.yml]")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  -c, -config <file>     Path to configuration file (.json or .yml) (default /etc/kafka2x/config.json)")
	fmt.Println("  -d, -debug             Enable verbose debug logging")
	fmt.Println("  -h, -help              Show this help message and exit")
	fmt.Println("  -stats                 Fetch and display live daemon stats from IPC socket or HTTP endpoint")
	fmt.Println("  -v, -version           Show version and exit")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  kafka2x -c /etc/kafka2x/config.json")
	fmt.Println("  kafka2x -c /etc/k2http/config.yml")
	fmt.Println("  kafka2x -stats")
}

func main() {
	var (
		configFile string
		debug      bool
		showStats  bool
		showVer    bool
		showHelp   bool
	)

	flag.StringVar(&configFile, "c", "/etc/kafka2x/config.json", "Path to configuration file")
	flag.StringVar(&configFile, "config", "/etc/kafka2x/config.json", "Path to configuration file")
	flag.BoolVar(&debug, "d", false, "Enable verbose debug logging")
	flag.BoolVar(&debug, "debug", false, "Enable verbose debug logging")
	flag.BoolVar(&showStats, "stats", false, "Fetch and display live daemon stats")
	flag.BoolVar(&showVer, "v", false, "Show version and exit")
	flag.BoolVar(&showVer, "version", false, "Show version and exit")
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.BoolVar(&showHelp, "help", false, "Show help")

	flag.Usage = printHelp
	flag.Parse()

	if showHelp {
		printHelp()
		os.Exit(0)
	}

	if showVer {
		fmt.Printf("kafka2x version %s (commit %s)\n", version, revision)
		os.Exit(0)
	}

	if len(flag.Args()) > 0 && configFile == "/etc/kafka2x/config.json" {
		configFile = flag.Arg(0)
	}

	log.Printf("Starting kafka2x v%s-%s with config: %s", version, revision, configFile)

	cfg, err := config.LoadEgressConfig(configFile)
	if err != nil {
		log.Fatalf("Fatal: Failed to load config: %v", err)
	}

	if showStats {
		fetchStats(cfg)
		os.Exit(0)
	}

	if debug {
		cfg.Debug = true
	}
	cfg.Pipeline.Debug = cfg.Debug

	// 1. Initialize Kafka Consumer
	consumer, err := kafka.NewConsumer(cfg.Kafka)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize Kafka consumer: %v", err)
	}

	// 2. Initialize HTTP Sender
	sender, err := egress.NewHTTPSender(cfg.HTTP)
	if err != nil {
		log.Fatalf("Fatal: Failed to initialize HTTP sender: %v", err)
	}

	// 3. Start Telemetry HTTP / Socket Server
	statsListener := startStatsServer(cfg)

	// 4. Start Pipeline
	pipeline := egress.NewPipeline(consumer, sender, cfg.Pipeline)
	pipeline.Start()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigs
	log.Printf("Received signal %v, shutting down...", sig)

	if statsListener != nil {
		_ = statsListener.Close()
		if cfg.StatsSocket != "" {
			_ = os.Remove(cfg.StatsSocket)
		}
	}

	pipeline.Stop()
	log.Printf("kafka2x exited cleanly.")
}

func startStatsServer(cfg *config.EgressConfig) net.Listener {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", stats.StatsHandler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	if cfg.StatsPort > 0 {
		bind := cfg.StatsBind
		if bind == "" {
			bind = "127.0.0.1"
		}
		addr := fmt.Sprintf("%s:%d", bind, cfg.StatsPort)
		go func() {
			srv := &http.Server{Addr: addr, Handler: mux}
			_ = srv.ListenAndServe()
		}()
	}

	if cfg.StatsSocket != "" {
		socketPath := cfg.StatsSocket
		_ = os.Remove(socketPath)
		if err := os.MkdirAll(filepath.Dir(socketPath), 0755); err != nil {
			log.Printf("[Stats] Failed to create socket directory: %v", err)
		}
		l, err := net.Listen("unix", socketPath)
		if err != nil {
			log.Printf("[Stats] Failed to listen on stats socket %s: %v", socketPath, err)
		} else {
			_ = os.Chmod(socketPath, 0666)
			log.Printf("[Stats] IPC stats server listening on unix socket %s", socketPath)
			go func() {
				for {
					conn, err := l.Accept()
					if err != nil {
						return
					}
					go handleIPCConn(conn)
				}
			}()
			return l
		}
	}
	return nil
}

func handleIPCConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil && len(line) == 0 {
		return
	}
	cmd := strings.TrimSpace(strings.ToLower(line))
	if cmd == "" || cmd == "stats" || cmd == "status" || strings.HasPrefix(cmd, "get ") {
		data, err := json.MarshalIndent(stats.GetStats(), "", "  ")
		if err == nil {
			_, _ = conn.Write(data)
			_, _ = conn.Write([]byte("\n"))
		}
	} else if cmd == "health" {
		_, _ = conn.Write([]byte(`{"status":"ok"}` + "\n"))
	} else {
		_, _ = conn.Write([]byte(`{"error": "unknown command"}` + "\n"))
	}
}

func fetchStats(cfg *config.EgressConfig) {
	if cfg.StatsSocket != "" {
		if conn, err := net.Dial("unix", cfg.StatsSocket); err == nil {
			defer conn.Close()
			_, _ = conn.Write([]byte("stats\n"))
			var buf bytes.Buffer
			if _, err := io.Copy(&buf, conn); err == nil && buf.Len() > 0 {
				var pretty bytes.Buffer
				if err := json.Indent(&pretty, buf.Bytes(), "", "  "); err == nil {
					fmt.Println(pretty.String())
					return
				}
				fmt.Println(buf.String())
				return
			}
		}
	}

	port := cfg.StatsPort
	if port <= 0 {
		port = 7989
	}
	bind := cfg.StatsBind
	if bind == "" || bind == "0.0.0.0" {
		bind = "127.0.0.1"
	}
	url := fmt.Sprintf("http://%s:%d/stats", bind, port)

	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error querying stats endpoint at %s: %v\n", url, err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading response: %v\n", err)
		os.Exit(1)
	}

	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "  "); err == nil {
		fmt.Println(pretty.String())
	} else {
		fmt.Println(string(body))
	}
}
