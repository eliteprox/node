package orchestrator

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/livepeer/node/destination"
	discovery "github.com/livepeer/node/discovery/shared"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
)

type priceInfo struct {
	Price    json.Number `json:"price"`
	PriceUSD json.Number `json:"price_usd,omitempty" toml:"-"`
	Currency string      `json:"currency"`
	Unit     string      `json:"unit"`
}

type runnerGPU struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	VRAMMB int    `json:"vram_mb,omitempty"`
}

func (g *runnerGPU) UnmarshalJSON(data []byte) error {
	var index int
	if err := json.Unmarshal(data, &index); err == nil {
		if index < 0 {
			return errors.New("GPU index must be nonnegative")
		}
		*g = runnerGPU{ID: fmt.Sprintf("%d", index)}
		return nil
	}
	type plain runnerGPU
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	if value.VRAMMB < 0 {
		return errors.New("GPU VRAM must be nonnegative")
	}
	*g = runnerGPU(value)
	return nil
}

type appRequest struct {
	ID         string     `json:"id,omitempty"`
	RunnerURL  string     `json:"runner_url"`
	Version    string     `json:"version"`
	Metadata   string     `json:"metadata"`
	GPU        *runnerGPU `json:"gpu,omitempty"`
	Status     string     `json:"status"`
	Mode       string     `json:"mode"`
	Proxy      bool       `json:"proxy"`
	App        string     `json:"app"`
	Capacity   int        `json:"capacity"`
	PriceInfo  priceInfo  `json:"price_info"`
	SessionIDs []string   `json:"session_ids"`
}

type appResponse struct {
	ID         string   `json:"id"`
	Status     string   `json:"status"`
	SessionIDs []string `json:"session_ids"`
}

type heartbeatRequest struct {
	Seq        *uint64      `json:"seq,omitempty"`
	Apps       []appRequest `json:"apps,omitzero"`
	RunnerID   string       `json:"runner_id"`
	RunnerURL  string       `json:"runner_url"`
	Version    string       `json:"version"`
	Metadata   string       `json:"metadata"`
	GPU        *runnerGPU   `json:"gpu,omitempty"`
	Status     string       `json:"status"`
	Mode       string       `json:"mode"`
	Proxy      bool         `json:"proxy"`
	App        string       `json:"app"`
	Capacity   int          `json:"capacity"`
	PriceInfo  priceInfo    `json:"price_info"`
	SessionIDs []string     `json:"session_ids"`
}

type heartbeatResponse struct {
	Seq               *uint64         `json:"seq,omitempty"`
	RunnerID          string          `json:"runner_id"`
	Orchestrator      string          `json:"orchestrator,omitempty"`
	HeartbeatInterval string          `json:"heartbeat_interval"`
	HeartbeatTTL      string          `json:"heartbeat_ttl"`
	HeartbeatSecret   string          `json:"heartbeat_secret,omitempty"`
	SessionIDs        []string        `json:"session_ids,omitzero"`
	Apps              []appResponse   `json:"apps,omitzero"`
	O2R               *trickleChannel `json:"o2r,omitempty"`
}

// Presence matters: even null or zero-valued legacy fields conflict with apps.
func (req *heartbeatRequest) UnmarshalJSON(data []byte) error {
	type plain heartbeatRequest
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	normalized := make(map[string]json.RawMessage, len(fields))
	for key, raw := range fields {
		normalized[strings.ToLower(key)] = raw
	}
	if raw, exists := normalized["apps"]; exists {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return errors.New("apps must be an array")
		}
		for _, field := range []string{"app", "runner_url", "mode", "proxy", "capacity", "price_info", "status", "metadata", "gpu", "session_ids"} {
			if _, exists := normalized[field]; exists {
				return fmt.Errorf("apps cannot be mixed with %s", field)
			}
		}
	}
	*req = heartbeatRequest(value)
	return nil
}

func (req heartbeatRequest) scalarApp() appRequest {
	return appRequest{ID: req.RunnerID, RunnerURL: req.RunnerURL, Version: req.Version, Metadata: req.Metadata, GPU: req.GPU, Status: req.Status, Mode: req.Mode, Proxy: req.Proxy, App: req.App, Capacity: req.Capacity, PriceInfo: req.PriceInfo, SessionIDs: req.SessionIDs}
}

type trickleChannel struct {
	Name        string `json:"name"`
	ChannelName string `json:"channel_name"`
	URL         string `json:"url"`
	InternalURL string `json:"internal_url,omitempty"`
	MimeType    string `json:"mime_type"`
}

// runner is the authenticated owner of one or more app registrations.
type runner struct {
	ID, Version, Credential string
	Last                    time.Time
	Static, Confirmed       bool
	Seq                     *uint64
	Apps                    map[string]*app
}

type seqConflict uint64

func (seqConflict) Error() string { return "seq must increase after owner opt-in" }

// appLifetime distinguishes registration instances without retaining their state.
type appLifetime struct{ _ byte }

type app struct {
	lifetime *appLifetime
	appRequest
	owner                         *runner
	USDQuote                      priceInfo
	PriceError, Removing, Healthy bool
	HealthURL                     string
	HealthCode                    int
	Sessions                      map[string]*session
	healthFailures                int
	drainDeadline                 time.Time
}

type session struct {
	identity  observability.Identity
	target    *url.URL
	pending   bool
	ID        string
	Token     string
	Proxies   map[string]sessionProxy
	Created   time.Time
	PriceInfo priceInfo
	ctx       context.Context
	cancel    context.CancelCauseFunc
}

var errInvalidSessionToken = errors.New("invalid session token")
var errRunnerUnavailable = errors.New("runner unavailable")

var (
	errRunnerCapacity  = errors.New("runner at capacity")
	errSessionCapacity = errors.New("session capacity reached")
)

func capacityError(err error) bool {
	return errors.Is(err, errRunnerCapacity) || errors.Is(err, errSessionCapacity)
}

// runnerStopped bounds the time existing proxy requests have to finish.
type runnerStopped struct{ deadline time.Time }

func (runnerStopped) Error() string { return "session stopped by runner" }

type sessionProxy struct {
	target *url.URL
	runner bool
}

type Registry struct {
	logger        *slog.Logger
	metrics       *observability.Metrics
	server        *Server
	mu            sync.Mutex
	runners       map[string]*runner
	apps          map[string]*app
	sessions      map[string]*app
	proxies       map[string]*session
	secret        string
	service       string
	runnerService string
	interval      time.Duration
	ttl           time.Duration
	drainTimeout  time.Duration
	onEvent       func(runnerID, appID, event, sessionID, sessionToken string)
	weiPerUSD     *big.Rat
	rateUntil     time.Time
	proxyTemplate string
	closed        bool
}

const (
	maxRunners       = 256
	maxAppsPerRunner = 512
	maxApps          = 1024
	maxDrainTimeout  = 12 * time.Hour
)

func NewRegistry(secret, service string, interval, ttl time.Duration) *Registry {
	r := &Registry{metrics: observability.New("orchestrator"), runners: make(map[string]*runner), apps: make(map[string]*app), sessions: make(map[string]*app), proxies: make(map[string]*session), secret: secret, service: strings.TrimRight(service, "/"), interval: interval, ttl: ttl, drainTimeout: maxDrainTimeout}
	r.metrics.Snapshot(r.collectMetrics)
	r.metrics.Legacy(func(emit observability.Emit) {
		runners, sessions := r.Counts()
		emit("runners", float64(runners), nil)
		emit("sessions", float64(sessions), nil)
	})
	return r
}

func (r *Registry) runnerServiceURL() string {
	if r.runnerService != "" {
		return r.runnerService
	}
	return r.service
}

func (r *Registry) SetWeiPerUSD(rate *big.Rat) { r.setRate(rate, time.Time{}) }

func (r *Registry) setRate(rate *big.Rat, until time.Time) {
	if rate == nil || rate.Sign() <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.weiPerUSD = new(big.Rat).Set(rate)
	r.rateUntil = until
	r.metrics.Gauge("price_feed_valid_until_timestamp_seconds", nil).Set(float64(until.Unix()) * observability.Bool(!until.IsZero()))
	for _, item := range r.apps {
		quote := item.USDQuote
		item.PriceError = normalizePriceAt(&quote, r.weiPerUSD) != nil
		if !item.PriceError {
			item.PriceInfo = quote
		}
	}
}

func (r *Registry) priceAvailable(item *app) bool {
	return !item.PriceError && (item.PriceInfo.Price == "" || r.rateUntil.IsZero() || time.Now().Before(r.rateUntil))
}

func (r *Registry) normalizePrice(price *priceInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return normalizePriceAt(price, r.weiPerUSD)
}

func normalizePriceAt(price *priceInfo, rate *big.Rat) error {
	value := strings.TrimSpace(price.Price.String())
	numeric, ok := new(big.Rat).SetString(value)
	if rate != nil && (!ok || numeric.Sign() <= 0) {
		return errors.New("runner price must be positive in on-chain mode")
	}
	if value == "" || (ok && numeric.Sign() == 0) {
		*price = priceInfo{}
		return nil
	}
	if !ok || numeric.Sign() < 0 {
		return errors.New("runner price must be nonnegative")
	}
	if price.Currency != "" && strings.ToLower(strings.TrimSpace(price.Currency)) != "usd" {
		return errors.New("runner price currency must be USD")
	}
	if rate == nil {
		// Off-chain registration accepts the runner's usual quote, but does
		// not advertise or enforce payment requirements.
		*price = priceInfo{}
		return nil
	}
	wei, unit, err := pm.ConvertRunnerPrice(value, price.Unit, rate)
	if err != nil {
		return err
	}
	usd := numeric
	if unit == "seconds" {
		usd.Quo(usd, big.NewRat(3600, 1))
	}
	usdString := strings.TrimRight(strings.TrimRight(usd.FloatString(36), "0"), ".")
	*price = priceInfo{Price: json.Number(wei.String()), PriceUSD: json.Number(usdString), Currency: "wei", Unit: unit}
	return nil
}

func randomID(n int) string {
	raw := make([]byte, n)
	rand.Read(raw)
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw))
}

func equalSecret(a, b string) bool {
	return a != "" && b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func (r *Registry) acceptsHeartbeatCredential(auth string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if equalSecret(auth, r.secret) {
		return true
	}
	for _, item := range r.runners {
		if !item.Static && equalSecret(auth, item.Credential) {
			return true
		}
	}
	return false
}

func validateApp(req *appRequest) error {
	if req.Capacity == 0 {
		req.Capacity = 1
	}
	req.PriceInfo.Unit = strings.ToLower(strings.TrimSpace(req.PriceInfo.Unit))
	req.PriceInfo.Currency = strings.ToLower(strings.TrimSpace(req.PriceInfo.Currency))
	if req.ID != "" && !validRouteID(req.ID) {
		return errors.New("app id must contain only ASCII letters, digits, '-' or '_' and be at most 128 bytes")
	}
	if req.App == "" || req.Capacity < 1 || req.Capacity > 10000 {
		return errors.New("app and capacity between 1 and 10000 are required")
	}
	if _, err := destination.ParseURL(req.RunnerURL); err != nil {
		return errors.New("runner_url must be an absolute HTTP or HTTPS URL")
	}
	if req.Mode != "" && req.Mode != "persistent" && req.Mode != "single-shot" {
		return errors.New("mode must be persistent or single-shot")
	}
	if req.PriceInfo.Unit == "720p" {
		return errors.New("720p pricing is unsupported")
	}
	if req.PriceInfo.Unit != "" && req.PriceInfo.Unit != "hour" && req.PriceInfo.Unit != "fixed" {
		return errors.New("price_info.unit must be hour or fixed")
	}
	if req.GPU != nil && req.GPU.VRAMMB < 0 {
		return errors.New("GPU VRAM must be nonnegative")
	}
	if req.Mode == "" {
		req.Mode = "persistent"
	}
	if req.Status == "" {
		req.Status = "ready"
	}
	return nil
}

func validRouteID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return true
}

func (r *Registry) Heartbeat(req heartbeatRequest, auth string) (heartbeatResponse, int, error) {
	if req.RunnerID != "" && !validRouteID(req.RunnerID) {
		return heartbeatResponse{}, http.StatusBadRequest, errors.New("runner_id must be a route-safe identifier")
	}
	requests := slices.Clone(req.Apps)
	if req.Apps == nil {
		requests = []appRequest{req.scalarApp()}
	}
	if len(requests) > maxAppsPerRunner {
		return heartbeatResponse{}, http.StatusBadRequest, errors.New("too many apps in heartbeat")
	}
	seen := make(map[string]bool, len(requests))
	for i := range requests {
		entry := &requests[i]
		if err := validateApp(entry); err != nil {
			return heartbeatResponse{}, http.StatusBadRequest, err
		}
		if entry.ID != "" {
			if seen[entry.ID] {
				return heartbeatResponse{}, http.StatusBadRequest, errors.New("duplicate app id")
			}
			seen[entry.ID] = true
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return heartbeatResponse{}, http.StatusServiceUnavailable, errors.New("orchestrator shutting down")
	}
	resp := heartbeatResponse{Orchestrator: r.runnerServiceURL(), HeartbeatInterval: r.interval.String(), HeartbeatTTL: r.ttl.String()}
	current := r.runners[req.RunnerID]
	confirm := current != nil
	if current == nil {
		if !equalSecret(auth, r.secret) {
			return heartbeatResponse{}, http.StatusUnauthorized, errors.New("invalid runner credential")
		}
		if len(r.runners) >= maxRunners {
			return heartbeatResponse{}, http.StatusServiceUnavailable, errors.New("runner capacity reached")
		}
		id := req.RunnerID
		if id == "" {
			id = r.newID("app-", 5, seen)
		}
		current = &runner{ID: id, Credential: randomID(32), Apps: map[string]*app{}}
		resp.HeartbeatSecret = current.Credential
	} else {
		if current.Static {
			return heartbeatResponse{}, http.StatusForbidden, errors.New("static runner cannot heartbeat")
		}
		if !equalSecret(auth, current.Credential) {
			return heartbeatResponse{}, http.StatusUnauthorized, errors.New("invalid runner credential")
		}
		if current.Seq != nil && (req.Seq == nil || *req.Seq <= *current.Seq) {
			return heartbeatResponse{}, http.StatusConflict, seqConflict(*current.Seq)
		}
	}
	if req.Apps == nil {
		requests[0].ID = current.ID
		seen[current.ID] = true
	}
	now := time.Now()
	retained := len(requests)
	for id, item := range current.Apps {
		if req.Apps == nil && id != current.ID {
			return heartbeatResponse{}, http.StatusConflict, errors.New("scalar heartbeat cannot replace other app registrations")
		}
		keep := r.drainTimeout > 0
		if item.Removing {
			keep = now.Before(item.drainDeadline)
		}
		if !seen[id] && len(item.Sessions) != 0 && keep {
			retained++
		}
	}
	if retained > maxAppsPerRunner || len(r.apps)-len(current.Apps)+retained > maxApps {
		return heartbeatResponse{}, http.StatusServiceUnavailable, errors.New("app capacity reached")
	}
	quotes := make([]priceInfo, len(requests))
	for i := range requests {
		entry := &requests[i]
		if entry.ID == "" {
			entry.ID = r.newID("app-", 5, seen)
			seen[entry.ID] = true
		}
		if err := r.validateProxyApp(*entry); err != nil {
			return heartbeatResponse{}, http.StatusBadRequest, err
		}
		if r.proxies[entry.ID] != nil {
			return heartbeatResponse{}, http.StatusConflict, errors.New("app id conflicts with a generated proxy")
		}
		if existing := r.apps[entry.ID]; existing != nil && existing.owner != current {
			return heartbeatResponse{}, http.StatusConflict, errors.New("app id belongs to another runner")
		}
		quotes[i] = entry.PriceInfo
		if err := normalizePriceAt(&entry.PriceInfo, r.weiPerUSD); err != nil {
			return heartbeatResponse{}, http.StatusBadRequest, err
		}
	}
	// Nothing above this point mutates the registry, including existing prices.
	current.Version, current.Last = req.Version, now
	current.Confirmed = confirm
	if confirm && req.Seq != nil {
		accepted := *req.Seq
		current.Seq = &accepted
	}
	resp.Seq = current.Seq
	r.runners[current.ID] = current
	resp.RunnerID, resp.Apps = current.ID, make([]appResponse, 0, retained)
	for i, entry := range requests {
		item := current.Apps[entry.ID]
		if item == nil {
			item = &app{appRequest: entry, owner: current, lifetime: new(appLifetime), Healthy: true, Sessions: map[string]*session{}}
			current.Apps[entry.ID], r.apps[entry.ID] = item, item
			r.runnerEvent(entry.ID, item, "registered")
		}
		item.appRequest, item.USDQuote, item.PriceError, item.Removing = entry, quotes[i], false, false
		item.drainDeadline = time.Time{}
		resp.Apps = append(resp.Apps, item.response())
	}
	for _, id := range slices.Sorted(maps.Keys(current.Apps)) {
		if !seen[id] {
			item := current.Apps[id]
			r.drainApp(item, now)
			if current.Apps[id] != nil {
				resp.Apps = append(resp.Apps, item.response())
			}
		}
	}
	if req.Apps == nil {
		resp.SessionIDs, resp.Apps = resp.Apps[0].SessionIDs, nil
	}
	return resp, http.StatusOK, nil
}

// drainApp requires r.mu. Only the first withdrawal starts the drain clock.
func (r *Registry) drainApp(item *app, now time.Time) {
	if !item.Removing {
		item.Removing = true
		item.drainDeadline = now.Add(r.drainTimeout)
	}
	if len(item.Sessions) == 0 {
		r.removeApp(item)
	} else if !now.Before(item.drainDeadline) {
		for sid := range item.Sessions {
			r.releaseLocked(item.ID, sid, errRunnerUnavailable, "app_drain_expired")
		}
	}
}

func (item *app) response() appResponse {
	status := item.Status
	if item.Removing {
		status = "draining"
	}
	return appResponse{ID: item.ID, Status: status, SessionIDs: append([]string{}, slices.Sorted(maps.Keys(item.Sessions))...)}
}

// removeApp requires r.mu and an app with no remaining sessions.
func (r *Registry) removeApp(item *app) {
	r.runnerEvent(item.ID, item, "removed")
	delete(item.owner.Apps, item.ID)
	delete(r.apps, item.ID)
}

func (r *Registry) Unregister(id, auth string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.runners[id]
	if current == nil {
		return http.StatusNotFound, errors.New("runner not found")
	}
	if current.Static || !equalSecret(current.Credential, auth) {
		return http.StatusUnauthorized, errors.New("invalid runner credential")
	}
	r.removeRunner(current, nil, "runner_unregistered", "unregistered")
	return http.StatusOK, nil
}

// removeRunner requires r.mu. Session teardown precedes owner/channel removal.
func (r *Registry) removeRunner(current *runner, cause error, reason, event string) {
	for id, item := range current.Apps {
		item.Removing = false
		for sid := range item.Sessions {
			r.releaseLocked(id, sid, cause, reason)
		}
		r.runnerEvent(id, item, event)
		delete(r.apps, id)
	}
	if r.onEvent != nil {
		r.onEvent(current.ID, "", event, "", "")
	}
	delete(r.runners, current.ID)
}

type discoveryRunner struct {
	ObservedAt        string     `json:"observed_at"`
	URL               string     `json:"url"`
	App               string     `json:"app"`
	Version           string     `json:"version,omitempty"`
	Metadata          string     `json:"metadata,omitempty"`
	GPU               *runnerGPU `json:"gpu,omitempty"`
	Mode              string     `json:"mode"`
	Capacity          int        `json:"capacity"`
	CapacityUsed      int        `json:"capacity_used"`
	CapacityAvailable int        `json:"capacity_available"`
	PriceInfo         priceInfo  `json:"price_info,omitzero"`
}

func (r discoveryRunner) DiscoveryIdentity() (string, string, string) {
	return r.URL, r.App, r.ObservedAt
}

type discoveryEntry = discovery.Entry[discoveryRunner]

func (r *Registry) Discovery() []discoveryEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := []discoveryRunner{}
	observed := time.Now().UTC().Format(time.RFC3339Nano)
	for _, id := range slices.Sorted(maps.Keys(r.apps)) {
		item := r.apps[id]
		if !r.usable(item) || !r.priceAvailable(item) {
			continue
		}
		available := max(0, item.Capacity-len(item.Sessions))
		result = append(result, discoveryRunner{ObservedAt: observed, URL: r.discoveryURL(id, item), App: item.App, Version: item.Version, Metadata: item.Metadata, GPU: item.GPU, Mode: item.Mode, Capacity: item.Capacity, CapacityUsed: len(item.Sessions), CapacityAvailable: available, PriceInfo: item.PriceInfo})
	}
	return []discoveryEntry{{Address: r.service, Runners: result}}
}

// Admission depends on readiness; admitted sessions depend only on liveness.
func (r *Registry) usable(item *app) bool {
	return (item.owner.Static || item.owner.Confirmed) && r.serving(item) && !item.Removing && item.Status == "ready"
}

func (r *Registry) serving(item *app) bool {
	return !r.closed && (item.owner.Static || time.Since(item.owner.Last) <= r.ttl) && item.Healthy
}

func (r *Registry) appURL(id, mode, sessionID string) string {
	if mode == "single-shot" {
		return r.service + "/apps/" + url.PathEscape(id) + "/app"
	}
	if sessionID == "" {
		return r.service + "/apps/" + url.PathEscape(id) + "/session"
	}
	return r.service + "/apps/" + url.PathEscape(id) + "/session/" + url.PathEscape(sessionID) + "/app"
}

func (r *Registry) Reserve(id string) (string, string, string, int, error) {
	return r.reserveWithPrice(id, "", nil, "persistent")
}

func (r *Registry) ReserveWithID(id, requestedID string) (string, string, string, int, error) {
	return r.reserveWithPrice(id, requestedID, nil, "")
}

// checkCapacity checks current availability without reserving a session.
func (r *Registry) checkCapacity(id string) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || !r.usable(item) {
		return http.StatusNotFound, errRunnerUnavailable
	}
	if len(r.sessions) >= 10000 {
		return http.StatusServiceUnavailable, errSessionCapacity
	}
	if len(item.Sessions) >= item.Capacity {
		return http.StatusServiceUnavailable, errRunnerCapacity
	}
	return http.StatusOK, nil
}

func (r *Registry) reserveWithPrice(id, requestedID string, agreed *appQuote, mode string) (sid, app, control string, status int, result error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item != nil {
		defer func() {
			outcome := "success"
			if result != nil {
				outcome = "rejected"
				if capacityError(result) {
					outcome = "capacity"
				} else if status == 503 {
					outcome = "unavailable"
				}
			}
			r.metrics.Add("session_reservations_total", 1, appIdentity(id, item).Labels().With("outcome", outcome))
		}()
	}
	if item == nil || !r.usable(item) {
		return "", "", "", http.StatusNotFound, errors.New("runner unavailable")
	}
	if agreed != nil && agreed.registration != item.lifetime {
		return "", "", "", http.StatusConflict, errors.New("app registration changed; request a new payment challenge")
	}
	if agreed == nil && !r.priceAvailable(item) {
		return "", "", "", http.StatusServiceUnavailable, errors.New("runner price unavailable")
	}
	if (mode != "" && item.Mode != mode) || (item.Mode != "persistent" && !(item.Mode == "single-shot" && requestedID != "")) {
		return "", "", "", http.StatusBadRequest, errors.New("only persistent runners have sessions")
	}
	if len(r.sessions) >= 10000 {
		return "", "", "", http.StatusServiceUnavailable, errSessionCapacity
	}
	if len(item.Sessions) >= item.Capacity {
		return "", "", "", http.StatusServiceUnavailable, errRunnerCapacity
	}
	sessionID := requestedID
	if sessionID == "" {
		sessionID = "session_" + randomID(10)
	}
	if !validRouteID(sessionID) || r.sessions[sessionID] != nil {
		return "", "", "", http.StatusConflict, errors.New("session already exists or invalid")
	}
	token := randomID(32)
	price := item.PriceInfo
	if agreed != nil {
		price = agreed.priceInfo
	}
	target, _ := destination.ParseURL(item.RunnerURL)
	ctx, cancel := context.WithCancelCause(context.Background())
	item.Sessions[sessionID] = &session{target: target, identity: appIdentity(id, item), pending: agreed != nil, ID: sessionID, Token: token, Created: time.Now(), Proxies: map[string]sessionProxy{}, PriceInfo: price, ctx: ctx, cancel: cancel}
	r.sessions[sessionID] = item
	appURL := r.appURL(id, item.Mode, sessionID)
	if item.Proxy && item.Mode == "persistent" {
		proxyID := r.newID("", 10, nil)
		item.Sessions[sessionID].Proxies[proxyID] = sessionProxy{target: target, runner: true}
		r.proxies[proxyID] = item.Sessions[sessionID]
		appURL = r.proxyURL(proxyID)
	}
	if agreed == nil && r.onEvent != nil {
		r.onEvent(item.owner.ID, id, "reserved", sessionID, token)
	}
	return sessionID, appURL, r.service + "/apps/" + url.PathEscape(id) + "/session/" + url.PathEscape(sessionID), http.StatusOK, nil
}

// appQuote carries an agreed price and the registration that offered it.
// The registration is process-local and never introduces another wire identity.
type appQuote struct {
	priceInfo
	registration *appLifetime
}

func (r *Registry) appRegistration(id string) *appLifetime {
	r.mu.Lock()
	defer r.mu.Unlock()
	if item := r.apps[id]; item != nil {
		return item.lifetime
	}
	return nil
}

// PriceForRunner returns the price for an app registration ID (a runner ID for legacy apps).
func (r *Registry) PriceForRunner(id string) (priceInfo, int, error) {
	quote, status, err := r.quoteForApp(id)
	return quote.priceInfo, status, err
}

func (r *Registry) quoteForApp(id string) (appQuote, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || !r.usable(item) {
		return appQuote{}, http.StatusNotFound, errors.New("runner unavailable")
	}
	if !r.priceAvailable(item) {
		return appQuote{}, http.StatusServiceUnavailable, errors.New("runner price unavailable")
	}
	return appQuote{priceInfo: item.PriceInfo, registration: item.lifetime}, http.StatusOK, nil
}

func (r *Registry) ModeForRunner(id string) (string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || !r.usable(item) {
		return "", http.StatusNotFound, errors.New("runner unavailable")
	}
	return item.Mode, http.StatusOK, nil
}

func (r *Registry) PriceForSession(id, sid string) (priceInfo, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || item.Sessions[sid] == nil || item.Sessions[sid].pending {
		return priceInfo{}, http.StatusNotFound, errors.New("session not found")
	}
	return item.Sessions[sid].PriceInfo, http.StatusOK, nil
}

func (r *Registry) PaidSessions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []string
	for _, item := range r.apps {
		for id, sess := range item.Sessions {
			if !sess.pending && sess.PriceInfo.Price != "" {
				ids = append(ids, id)
			}
		}
	}
	return ids
}

func (r *Registry) ReleaseBySession(sid string, reasons ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if item := r.sessions[sid]; item != nil {
		r.releaseLocked(item.ID, sid, nil, reasons...)
	}
}

func (r *Registry) release(id, sid, token string, callback bool) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || item.Sessions[sid] == nil {
		return http.StatusNotFound, errors.New("session not found")
	}
	if callback && !equalSecret(item.Sessions[sid].Token, token) {
		return http.StatusUnauthorized, errInvalidSessionToken
	}
	var cause error
	reason := "client_stop"
	if callback {
		cause, reason = runnerStopped{deadline: time.Now().Add(time.Second)}, "runner_stop"
	}
	r.releaseLocked(id, sid, cause, reason)
	return http.StatusOK, nil
}

// releaseLocked is the single teardown path for capacity and active requests.
// The caller holds r.mu; cancellation never waits for the request to finish.
func (r *Registry) releaseLocked(id, sid string, cause error, reasons ...string) {
	item := r.apps[id]
	if item == nil || item.Sessions[sid] == nil {
		return
	}
	sess := item.Sessions[sid]
	reason := "released"
	if len(reasons) != 0 {
		reason = reasons[0]
	}
	if sess.pending && (reason == "released" || reason == "completed") {
		reason = "reservation_failed"
	}
	r.metrics.Add("session_ends_total", 1, sess.identity.Labels().With("reason", reason))
	if !sess.pending {
		r.metrics.Observe("session_duration_seconds", time.Since(sess.Created).Seconds(), sess.identity.Labels())
	}
	if r.logger != nil {
		r.logger.Info("session ended", "event", "session_ended", "operation", "release", "outcome", reason, "app", sess.identity.App, "runner_id", sess.identity.Runner, "app_id", id, "session_id", sid)
	}
	sess.cancel(cause)
	delete(item.Sessions, sid)
	delete(r.sessions, sid)
	for proxyID := range sess.Proxies {
		delete(r.proxies, proxyID)
	}
	if r.onEvent != nil {
		r.onEvent(item.owner.ID, id, "released", sid, "")
	}
	if item.Removing && len(item.Sessions) == 0 {
		r.removeApp(item)
	}
}

func (r *Registry) sessionContext(id, sid string) (context.Context, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || item.Sessions[sid] == nil {
		return nil, false
	}
	return item.Sessions[sid].ctx, true
}

func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, current := range r.runners {
		r.removeRunner(current, nil, "shutdown", "unregistered")
	}
}

func (r *Registry) sessionTarget(id, sid string) (*url.URL, string, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || !r.serving(item) || item.Sessions[sid] == nil || item.Sessions[sid].pending {
		return nil, "", http.StatusNotFound, errors.New("session not found")
	}
	return item.Sessions[sid].target, item.Sessions[sid].Token, http.StatusOK, nil
}

func (r *Registry) validSessionToken(id, sid, token string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	return item != nil && item.Sessions[sid] != nil && !item.Sessions[sid].pending && equalSecret(item.Sessions[sid].Token, token)
}

func (r *Registry) addProxy(id, sid, token string, target *url.URL) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || item.Sessions[sid] == nil || !equalSecret(item.Sessions[sid].Token, token) {
		return "", errInvalidSessionToken
	}
	if len(item.Sessions[sid].Proxies) >= 25 {
		return "", errors.New("too many session proxies")
	}
	proxyID := r.newID("", 10, nil)
	item.Sessions[sid].Proxies[proxyID] = sessionProxy{target: target}
	r.proxies[proxyID] = item.Sessions[sid]
	return proxyID, nil
}

// newID requires r.mu and avoids both registered and staged identifiers.
func (r *Registry) newID(prefix string, size int, seen map[string]bool) string {
	for {
		id := prefix + randomID(size)
		if r.runners[id] == nil && r.apps[id] == nil && r.proxies[id] == nil && !seen[id] {
			return id
		}
	}
}

func (r *Registry) proxyTarget(proxyID string) (sessionProxy, string, string, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sess := r.proxies[proxyID]
	if sess != nil && !sess.pending {
		if item := r.sessions[sess.ID]; item != nil && r.serving(item) {
			return sess.Proxies[proxyID], item.ID, sess.ID, sess.Token, true
		}
	}
	return sessionProxy{}, "", "", "", false
}

func (r *Registry) Expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for _, current := range r.runners {
		if !current.Static && now.Sub(current.Last) > r.ttl {
			r.removeRunner(current, errRunnerUnavailable, "runner_expired", "expired")
			continue
		}
		for _, item := range current.Apps {
			if item.Removing {
				r.drainApp(item, now)
			}
		}
	}
}

type StaticRunner struct {
	ID         string
	Proxy      bool
	RunnerURL  string
	Version    string
	Metadata   string
	GPU        *runnerGPU
	App        string
	Mode       string
	Status     string
	Capacity   int
	HealthURL  string
	HealthCode int
	PriceInfo  priceInfo
}

func (r *Registry) AddStatic(config StaticRunner) error {
	if !validRouteID(config.ID) {
		return errors.New("static runner id must be a route-safe identifier")
	}
	req := appRequest{ID: config.ID, Proxy: config.Proxy, RunnerURL: config.RunnerURL, Version: config.Version, Metadata: config.Metadata, GPU: config.GPU, App: config.App, Mode: config.Mode, Status: config.Status, Capacity: config.Capacity, PriceInfo: config.PriceInfo}
	if err := validateApp(&req); err != nil {
		return fmt.Errorf("static runner %s: %w", config.ID, err)
	}
	if err := r.validateProxyApp(req); err != nil {
		return err
	}
	quote := req.PriceInfo
	if config.HealthURL != "" {
		if _, err := destination.ParseURL(config.HealthURL); err != nil {
			return fmt.Errorf("static runner %s has invalid health URL", config.ID)
		}
	}
	if config.HealthCode == 0 {
		config.HealthCode = http.StatusOK
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := normalizePriceAt(&req.PriceInfo, r.weiPerUSD); err != nil {
		return fmt.Errorf("static runner %s: %w", config.ID, err)
	}
	if r.runners[config.ID] != nil || r.apps[config.ID] != nil || r.proxies[config.ID] != nil {
		return fmt.Errorf("duplicate static runner id %s", config.ID)
	}
	if len(r.runners) >= maxRunners || len(r.apps) >= maxApps {
		return errors.New("static runner capacity reached")
	}
	owner := &runner{ID: config.ID, Version: config.Version, Static: true, Apps: map[string]*app{}}
	item := &app{owner: owner, lifetime: new(appLifetime), USDQuote: quote, appRequest: req, Healthy: config.HealthURL == "", HealthURL: config.HealthURL, HealthCode: config.HealthCode, Sessions: map[string]*session{}}
	owner.Apps[config.ID] = item
	r.runners[config.ID], r.apps[config.ID] = owner, item
	r.runnerEvent(config.ID, item, "registered")
	return nil
}

func (r *Registry) CheckStaticHealth(ctx context.Context, client *http.Client) {
	r.mu.Lock()
	checks := make(map[string]string)
	for id, item := range r.apps {
		if item.owner.Static && item.HealthURL != "" {
			checks[id] = item.HealthURL
		}
	}
	r.mu.Unlock()
	var workers sync.WaitGroup
	jobs := make(chan string)
	for range min(8, len(checks)) {
		workers.Go(func() {
			for id := range jobs {
				r.checkStaticHealth(ctx, client, id, checks[id])
			}
		})
	}
	for id := range checks {
		select {
		case jobs <- id:
		case <-ctx.Done():
		}
	}
	close(jobs)
	workers.Wait()
}

func (r *Registry) checkStaticHealth(ctx context.Context, client *http.Client, id, raw string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return
	}
	response, err := client.Do(request)
	healthy := false
	if err == nil {
		_ = response.Body.Close()
		r.mu.Lock()
		item := r.apps[id]
		healthy = item != nil && response.StatusCode == item.HealthCode
		r.mu.Unlock()
	}
	r.mu.Lock()
	if item := r.apps[id]; item != nil && item.owner.Static {
		r.metrics.Add("runner_health_checks_total", 1, appIdentity(id, item).Labels().With("outcome", map[bool]string{true: "success", false: "error"}[healthy]))
		if healthy {
			item.healthFailures = 0
		} else {
			item.healthFailures++
			healthy = item.Healthy && item.healthFailures < 3
		}
		if item.Healthy != healthy {
			event := "unhealthy"
			if healthy {
				event = "healthy"
			}
			r.runnerEvent(id, item, event)
		}
		item.Healthy = healthy
		if !healthy {
			for sid := range item.Sessions {
				r.releaseLocked(id, sid, errRunnerUnavailable, "runner_unhealthy")
			}
		}
	}
	r.mu.Unlock()
}

func (r *Registry) Counts() (runners, sessions int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	runners, sessions = len(r.runners), len(r.sessions)
	return
}

func (r *Registry) String() string {
	runners, sessions := r.Counts()
	return fmt.Sprintf("runners=%d sessions=%d", runners, sessions)
}

func (r *Registry) activateSession(id, sid string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.apps[id]
	if item == nil || !r.serving(item) || item.Sessions[sid] == nil {
		return false
	}
	sess := item.Sessions[sid]
	if sess.pending {
		sess.pending = false
		if r.onEvent != nil {
			r.onEvent(item.owner.ID, id, "reserved", sid, sess.Token)
		}
	}
	return true
}
