// QueueTicket admission and ticket conversion adapt Nico Vergauwen's and
// Yondon Fu's go-livepeer/server/redeemer.go at afaaa36e743d39e6f460e7993d516d52b3a81341.
// HTTP transport, deployment binding and validation are implemented for Node.
// MIT notice: LICENSE. No max-float RPCs are included.
package redemption

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/livepeer/node/destination"
	"github.com/livepeer/node/pm"
)

const MaxTicketBody = 16 << 10

type TicketJSON struct {
	Payer         common.Address `json:"payer_address"`
	Recipient     common.Address `json:"recipient"`
	FaceValue     string         `json:"face_value"`
	WinProb       string         `json:"win_prob"`
	Nonce         uint32         `json:"ticket_nonce"`
	RandHash      common.Hash    `json:"recipient_rand_hash"`
	CreationRound string         `json:"creation_round"`
	RoundHash     common.Hash    `json:"creation_round_block_hash"`
	Expiration    string         `json:"params_expiration_block"`
	Signature     hexutil.Bytes  `json:"sig"`
	Randomness    string         `json:"recipient_rand"`
}

type TicketRequest struct {
	ChainID    string         `json:"chain_id"`
	Controller common.Address `json:"controller"`
	Ticket     TicketJSON     `json:"ticket"`
}

type TicketResponse struct {
	Hash common.Hash `json:"ticket_hash"`
}

func ticketJSON(t *pm.SignedTicket) TicketJSON {
	return TicketJSON{t.PayerAddress, t.Recipient, t.FaceValue.String(), t.WinProb.String(), t.TicketNonce, t.RecipientRandHash, strconv.FormatInt(t.CreationRound, 10), t.CreationRoundBlockHash, t.ParamsExpirationBlock.String(), t.Sig, t.RecipientRand.String()}
}

func (j TicketJSON) SignedTicket() (*pm.SignedTicket, error) {
	var values []*big.Int
	for _, raw := range []string{j.FaceValue, j.WinProb, j.Expiration, j.Randomness} {
		if len(raw) == 0 || len(raw) > 78 || strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
			return nil, errors.New("invalid ticket integer")
		}
		n, ok := new(big.Int).SetString(raw, 10)
		if !ok || n.BitLen() > 256 {
			return nil, errors.New("ticket integer out of range")
		}
		values = append(values, n)
	}
	round, err := strconv.ParseInt(j.CreationRound, 10, 64)
	if err != nil || round < 0 {
		return nil, errors.New("invalid creation round")
	}
	return &pm.SignedTicket{Ticket: &pm.Ticket{PayerAddress: j.Payer, Recipient: j.Recipient, FaceValue: values[0], WinProb: values[1], TicketNonce: j.Nonce, RecipientRandHash: j.RandHash, CreationRound: round, CreationRoundBlockHash: j.RoundHash, ParamsExpirationBlock: values[2]}, Sig: []byte(j.Signature), RecipientRand: values[3]}, nil
}

func decodeJSON(r io.Reader, value any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func Handler(s *RedeemerDB) http.Handler {
	mux := http.NewServeMux()
	slots := make(chan struct{}, 32)
	mux.HandleFunc("POST /tickets", func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			http.Error(w, "ticket admission busy", 503)
			return
		}
		var req TicketRequest
		if err := decodeJSON(http.MaxBytesReader(w, r.Body, MaxTicketBody), &req); err != nil {
			http.Error(w, "invalid ticket request", 400)
			return
		}
		if s.binding == nil || req.ChainID != s.binding.ChainID || req.Controller != s.binding.Controller {
			http.Error(w, "ticket deployment mismatch", 400)
			return
		}
		t, err := req.Ticket.SignedTicket()
		if err == nil {
			err = ValidateTicket(t)
		}
		if err != nil {
			http.Error(w, "invalid winning ticket", 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err = s.Admit(ctx, t); err != nil {
			status := 503
			if errors.Is(err, ErrConflict) {
				status = 409
			}
			http.Error(w, http.StatusText(status), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(TicketResponse{t.Hash()})
	})
	return mux
}

// RedeemerURL is normalized before both binding and dispatch. Redirects must
// never forward ticket randomness to a destination selected by the server.
func RedeemerURL(u *url.URL) (*url.URL, error) {
	if err := destination.ValidateURL(u); err != nil {
		return nil, err
	}
	if u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, errors.New("redeemer URL must not contain credentials, query or fragment")
	}
	copy := *u
	copy.Host = strings.ToLower(copy.Host)
	copy.Path = strings.TrimRight(copy.Path, "/")
	copy.RawPath = ""
	return &copy, nil
}

type Rejection struct{ Status int }

func (e *Rejection) Error() string {
	return fmt.Sprintf("redeemer rejected ticket (HTTP %d)", e.Status)
}

type HTTPDestination struct {
	endpoint string
	client   *http.Client
	binding  Binding
}

func NewHTTPDestination(u *url.URL, policy destination.Policy, b Binding) (*HTTPDestination, error) {
	normalized, err := RedeemerURL(u)
	if err != nil {
		return nil, err
	}
	endpoint, err := url.JoinPath(normalized.String(), "tickets")
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: policy.Transport(10 * time.Second), Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &HTTPDestination{endpoint, client, b}, nil
}

func (d *HTTPDestination) Close() { d.client.CloseIdleConnections() }

func (d *HTTPDestination) Submit(ctx context.Context, t *pm.SignedTicket) error {
	payload, err := json.Marshal(TicketRequest{d.binding.ChainID, d.binding.Controller, ticketJSON(t)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(payload))
	if err != nil {
		return errors.New("invalid redeemer request")
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := d.client.Do(req)
	if err != nil {
		return errors.New("redeemer request unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 && res.StatusCode < 500 && res.StatusCode != 408 && res.StatusCode != 429 {
		return &Rejection{res.StatusCode}
	}
	if res.StatusCode != 202 {
		return errors.New("redeemer did not acknowledge ticket")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, MaxTicketBody+1))
	if err != nil || len(body) > MaxTicketBody {
		return errors.New("invalid redeemer acknowledgment")
	}
	var reply TicketResponse
	if decodeJSON(bytes.NewReader(body), &reply) != nil || reply.Hash != t.Hash() {
		return errors.New("redeemer acknowledgment hash mismatch")
	}
	return nil
}
