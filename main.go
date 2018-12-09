package main

import (
	"os"
	"io"
	"log"
	"time"
	"context"
	"strings"
	pb "pdu-server/protos"
	"google.golang.org/grpc"
	"os/exec"
	"bytes"
	"regexp"
	"strconv"
)

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func watchControl(client pb.PduServerClient, cid uint32, reload chan *pb.ClientControlResponse) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := &pb.ClientControlRequest{
		Cid: cid,
	}
	stream, err := client.WatchClientControl(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	for {
		res, err := stream.Recv()
		if err == io.EOF {
			log.Printf("cid=%d: and now your watch is ended\n", cid)
			return
		}
		if err != nil {
			log.Println(err)
			return
		}
		reload <- res
		log.Printf("ClientControlRequest new config: %v\n", res)
	}
}


func run(upstream pb.PduServerClient, cid uint32, config *pb.ClientControlResponse) (chan bool, context.CancelFunc) {
	r, _ := regexp.Compile(".*\\[\\d+\\] (\\w+)\\s*(\\d+)")

	done := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, config.Command, config.Args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	go func (cmd *exec.Cmd) {
		log.Println("+ Starting command")
		err := cmd.Run()
		if err != nil {
			// KILLED OR ERROR
			log.Printf("Run failed with %s\n", err)
		} else {
			// PARSE AND SEND
			//outStr, errStr := string(stdout.Bytes()), string(stderr.Bytes())
			//log.Printf("out:\n%s\nerr:\n%s\n", outStr, errStr)

			var data []*pb.MeasureData

			outArr := bytes.Split(stdout.Bytes(), []byte{'\n'})
			for i := 4; i < len(outArr); i++ {
				measure := r.FindAllSubmatch(outArr[i], -1)
				for j := range measure {
//					println(measure[j][1], "=", measure[j][2])
					v, _ := strconv.Atoi(string(measure[j][2]))
					if v > 0 {
						data = append(data, &pb.MeasureData{ Name: "sng_" + string(measure[j][1]), Value: uint64(v) })
					}
				}
			}

			ctx := context.Background()
			upstream.ClientCollect(ctx, &pb.CollectRequest{
				Cid: cid,
				Data: data,
				Time: time.Now().UnixNano(),
			})
		}

		log.Println("- Done")
		done <- err == nil
	}(cmd)

	return done, cancel
}

func runLoop(upstream pb.PduServerClient, cid uint32, reload chan *pb.ClientControlResponse)  {
Mainloop:
	for {
		var rest = false
		log.Println("* Waiting for config ...")
		var config = <-reload

		for {
			if ! config.Enabled {
				break
			}

			if rest {
				select {
				case <-time.NewTimer(time.Duration(config.RepeatDelay) * time.Millisecond).C:
//					log.Println("Zzz done")
					rest = false
					continue

				case config = <-reload:
//					log.Println("RELOAD while rest")
					rest = false
					continue

				}
			}

			done, cancel := run(upstream, cid, config)
			select {
			case <-done:
//				log.Println("Run was success? %b", success)
				if ! config.Repeat {
					continue Mainloop
				}
				rest = true
				continue

			case config = <-reload:
//				log.Println("RELOAD while running the command")
				cancel()
				continue

			}
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("One argument must be exists.")
	}

	var mode pb.ClientMode
	modeString := strings.ToLower(os.Args[1])
	switch modeString {
	case "measure":
		mode = pb.ClientMode_MEASURE
	case "stress":
		mode = pb.ClientMode_STRESS
	default:
		log.Fatalf("Invalid client mode: '%s' (must be one of 'measure' or 'stress')", modeString)
	}
	log.Printf("* Client MODE is %s\n", mode)

	envId := getEnv("ID", "ID1")
	envName := getEnv("NAME", "Testname1")
	envDial := getEnv("DIAL", "127.0.0.1:4001")

	log.Printf("* Client ID is %s\n", envId)
	log.Printf("* Client NAME is %s\n", envName)
	log.Printf("* Client UPSTREAM: %s\n", envDial)

	// Set up a connection to the pdu-collector.
	conn, err := grpc.Dial(envDial, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("* Cannot connect to the upstream (%s): %v", envDial, err)
	}
	defer conn.Close()
	c := pb.NewPduServerClient(conn)

	helloReq := &pb.HelloRequest{
		Id: envId,
		Name: envName,
		Mode: mode,
	}
	helloResp, helloError := c.ClientHello(context.Background(), helloReq)
	if helloError != nil {
		log.Fatal(helloError)
	}

	cid := helloResp.Cid
	log.Printf("* Client got CID: %d\n", cid)

	reload := make(chan *pb.ClientControlResponse)
	go runLoop(c, cid, reload)

	watchControl(c, cid, reload)
}
