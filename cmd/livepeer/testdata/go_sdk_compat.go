package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	liverunner "github.com/livepeer/golang-runner"
)

func main() {
	if len(os.Args) != 4 {
		panic("usage: go_sdk_compat <orchestrator> <runner> <bootstrap>")
	}
	for _, mode := range []string{liverunner.ModePersistent, liverunner.ModeSingleShot} {
		for _, proxy := range []bool{false, true} {
			check(mode, proxy)
		}
	}
	fmt.Println("Go SDK registration mode/proxy matrix passed")
}

func check(mode string, proxy bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reserved := make(chan string, 1)
	released := make(chan string, 1)
	r, err := liverunner.Register(ctx, liverunner.Options{
		OrchestratorURL: os.Args[1], RunnerURL: os.Args[2], BootstrapSecret: os.Args[3],
		App: "sdk-compat", Mode: mode, Proxy: proxy, Capacity: 1,
		OnReserve: func(ev liverunner.SessionEvent) { reserved <- ev.SessionID },
		OnRelease: func(ev liverunner.SessionEvent) { released <- ev.SessionID },
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = r.Close(context.Background()) }()
	if err := r.Start(ctx); err != nil {
		panic(err)
	}
	if mode == liverunner.ModeSingleShot {
		response, err := http.Get(os.Args[1] + "/discovery")
		if err != nil {
			panic(err)
		}
		var entries []struct{ Runners []struct{ App, URL string } }
		err = json.NewDecoder(response.Body).Decode(&entries)
		_ = response.Body.Close()
		if err != nil {
			panic(err)
		}
		appURL := ""
		for _, entry := range entries {
			for _, runner := range entry.Runners {
				if runner.App == "sdk-compat" {
					appURL = runner.URL
				}
			}
		}
		response, err = http.Get(appURL + "/hello")
		if err != nil {
			panic(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			panic(fmt.Sprintf("single-shot status %d", response.StatusCode))
		}
		var sid string
		select {
		case sid = <-reserved:
		case <-ctx.Done():
			panic("no single-shot reserve callback")
		}
		select {
		case got := <-released:
			if got != sid {
				panic("wrong single-shot release")
			}
		case <-ctx.Done():
			panic("no single-shot release callback")
		}
		return
	}
	response, err := http.Post(os.Args[1]+"/apps/"+r.RunnerID()+"/session", "application/json", nil)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("reservation status %d", response.StatusCode))
	}
	var session struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&session); err != nil {
		panic(err)
	}
	select {
	case got := <-reserved:
		if got != session.SessionID {
			panic("wrong reserved session")
		}
	case <-ctx.Done():
		panic("no SDK reserve callback")
	}
	stop, err := http.Post(os.Args[1]+"/apps/"+r.RunnerID()+"/session/"+session.SessionID+"/stop", "application/json", nil)
	if err != nil {
		panic(err)
	}
	_ = stop.Body.Close()
	if stop.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("stop status %d", stop.StatusCode))
	}
	select {
	case got := <-released:
		if got != session.SessionID {
			panic("wrong released session")
		}
	case <-ctx.Done():
		panic("no SDK release callback")
	}
	fmt.Println("Go SDK registration, reserve/release callbacks and unregister passed")
}
