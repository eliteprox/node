package orchestrator

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"strconv"
	"sync"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/pm"
	"github.com/livepeer/node/pm/wire"
)

var maxWinProb = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

// PaymentEngine owns process-local payment challenges and session accounting.
// Winning tickets and redemption state are durable in the orchestrator redeemer.
type PaymentEngine struct {
	metrics       *observability.Metrics
	identity      func(string, string) observability.Identity
	registration  func(string) *appLifetime
	mu            sync.Mutex
	store         *RedeemerDB
	chain         pm.PaymentChain
	recipient     ethcommon.Address
	faceValue     *big.Int
	winProb       *big.Int
	secret        [32]byte
	authSecret    [32]byte
	sessions      map[string]*paymentSession
	ticketNonces  map[string]*recipientNonces
	nonceCount    int
	lastSeenBlock *big.Int
}

// Session scope, the latest response, and accounting are process-local, just as
// the app sessions are. Issued PM parameters and secrets are never persisted.
// The cached response serves refresh/retry routes; it does not authenticate PM.
type paymentSession struct {
	registration *appLifetime
	appID        string
	payer        ethcommon.Address
	price        wire.PriceInfo
	unit         string
	info         []byte
	balance      *big.Rat
	lastCharge   time.Time
	updated      time.Time
}

func NewPaymentEngine(store *RedeemerDB, chain pm.PaymentChain, recipient ethcommon.Address, faceValue, winProb *big.Int) (*PaymentEngine, error) {
	if store == nil || chain == nil || recipient == (ethcommon.Address{}) || faceValue == nil || faceValue.Sign() <= 0 || winProb == nil || winProb.Sign() <= 0 || winProb.Cmp(maxWinProb) >= 0 {
		return nil, errors.New("invalid payment engine configuration")
	}
	e := &PaymentEngine{store: store, chain: chain, recipient: recipient, faceValue: new(big.Int).Set(faceValue), winProb: new(big.Int).Set(winProb), sessions: make(map[string]*paymentSession), ticketNonces: make(map[string]*recipientNonces), lastSeenBlock: new(big.Int)}
	// Port of Yondon Fu's go-livepeer/pm.NewRecipient: a fresh 256-bit HMAC
	// key per recipient lifetime. AuthToken has its own independent key.
	rand.Read(e.secret[:])
	rand.Read(e.authSecret[:])
	return e, nil
}

type Challenge struct {
	PaymentParams string `json:"payment_params"`
	Orchestrator  string `json:"orchestrator"`
	ManifestID    string `json:"manifest_id"`
	PaymentURL    string `json:"payment_url"`
}

// MakeChallenge returns a Python/Go runner compatible 402 body. Ticket
// parameters are authenticated by the recipient HMAC.
func (e *PaymentEngine) MakeChallenge(ctx context.Context, appID, manifest string, payer ethcommon.Address, price int64, unit, service string) (challenge Challenge, result error) {
	var registration *appLifetime
	if e.registration != nil {
		registration = e.registration(appID)
	}
	return e.makeAppChallenge(ctx, appID, manifest, payer, price, unit, service, registration)
}

func (e *PaymentEngine) makeAppChallenge(ctx context.Context, appID, manifest string, payer ethcommon.Address, price int64, unit, service string, registration *appLifetime) (challenge Challenge, result error) {
	labels := e.paymentLabels(appID, manifest)
	defer func() { e.recordPayment("challenge", result, labels) }()
	defer e.observeDuration("challenge", time.Now(), labels)
	return e.makeChallenge(ctx, appID, manifest, payer, price, unit, service, registration)
}

func (e *PaymentEngine) makeChallenge(ctx context.Context, appID, manifest string, payer ethcommon.Address, price int64, unit, service string, registration *appLifetime) (Challenge, error) {
	if appID == "" || manifest == "" || payer == (ethcommon.Address{}) || price <= 0 || (unit != "seconds" && unit != "fixed") {
		return Challenge{}, errors.New("invalid payment challenge scope")
	}
	snapshot, err := e.chain.Snapshot(ctx)
	if err != nil {
		return Challenge{}, err
	}
	active, err := e.chain.IsActiveAt(ctx, e.recipient, snapshot)
	if err != nil {
		return Challenge{}, err
	}
	if !active {
		return Challenge{}, errors.New("orchestrator is not active on chain")
	}
	if snapshot.Block == nil || snapshot.Round == nil || snapshot.Block.Sign() < 0 || snapshot.Round.Sign() <= 0 || snapshot.RoundHash == (ethcommon.Hash{}) {
		return Challenge{}, errors.New("chain snapshot unavailable")
	}
	if !snapshot.ValidTicketRound(snapshot.Round.Int64()) {
		return Challenge{}, errors.New("initialized round is outside ticket validity")
	}
	seed := make([]byte, 32)
	rand.Read(seed)
	priceInfo := wire.PriceInfo{PricePerUnit: price, UnitsPerPrice: 1}
	expiration := &pm.TicketExpirationParams{CreationRound: snapshot.Round.Int64(), CreationRoundBlockHash: snapshot.RoundHash}
	expiresAtBlock := new(big.Int).Add(snapshot.Block, big.NewInt(40))
	recipientRand := e.recipientRand(new(big.Int).SetBytes(seed), payer, e.faceValue, e.winProb, expiresAtBlock, big.NewRat(price, 1), expiration)
	randHash := crypto.Keccak256Hash(ethcommon.LeftPadBytes(recipientRand.Bytes(), 32))
	info := wire.OrchestratorInfo{Transcoder: service, Address: e.recipient.Bytes(), Price: priceInfo,
		TicketParams: wire.TicketParams{Recipient: e.recipient.Bytes(), FaceValue: e.faceValue.Bytes(), WinProb: e.winProb.Bytes(), RecipientRandHash: randHash.Bytes(), Seed: seed, ExpirationBlock: expiresAtBlock.Bytes(), Expiration: wire.ExpirationParams{CreationRound: expiration.CreationRound, CreationRoundBlockHash: expiration.CreationRoundBlockHash.Bytes()}},
		Auth:         e.authToken(manifest, time.Now().Add(time.Hour).Unix())}
	encoded := wire.EncodeOrchestratorInfo(info)
	challenge := paymentChallenge(appID, manifest, service, encoded)
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := snapshot.Validate(); err != nil {
		return Challenge{}, err
	}
	e.observeBlock(snapshot.Block)
	current := e.sessions[manifest]
	if current == nil {
		if len(e.sessions) >= 100000 {
			return Challenge{}, errPaymentSessionCapacity
		}
		current = &paymentSession{appID: appID, registration: registration, payer: payer, price: priceInfo, unit: unit, balance: new(big.Rat)}
		e.sessions[manifest] = current
	} else if current.appID != appID || current.registration != registration || current.payer != payer || current.price != priceInfo || current.unit != unit {
		return Challenge{}, errors.New("payment session scope conflict")
	}
	current.info, current.updated = encoded, time.Now()
	return challenge, nil
}

func paymentChallenge(appID, manifest, service string, info []byte) Challenge {
	return Challenge{PaymentParams: base64.StdEncoding.EncodeToString(info), Orchestrator: service, ManifestID: manifest, PaymentURL: service + "/apps/" + appID + "/session/" + manifest + "/payment"}
}

func (e *PaymentEngine) ChallengeInfo(manifest string) (wire.OrchestratorInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.sessions[manifest]
	if current == nil {
		return wire.OrchestratorInfo{}, ErrMissingChallenge
	}
	return wire.DecodeOrchestratorInfo(current.info)
}

func (e *PaymentEngine) ChallengeForManifest(appID, manifest, service string) (Challenge, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.sessions[manifest]
	if current == nil || current.appID != appID {
		return Challenge{}, ErrMissingChallenge
	}
	return paymentChallenge(appID, manifest, service, current.info), nil
}

func (e *PaymentEngine) RefreshChallenge(ctx context.Context, manifest string, payer ethcommon.Address, service string) (challenge Challenge, result error) {
	labels := e.paymentLabels("", manifest)
	defer func() { e.recordPayment("refresh", result, labels) }()
	defer e.observeDuration("refresh", time.Now(), labels)
	e.mu.Lock()
	current := e.sessions[manifest]
	if current == nil {
		e.mu.Unlock()
		return Challenge{}, ErrMissingChallenge
	}
	appID, expectedPayer, price, unit, registration := current.appID, current.payer, current.price, current.unit, current.registration
	e.mu.Unlock()
	if payer != expectedPayer {
		return Challenge{}, ErrInvalidPayment
	}
	return e.makeChallenge(ctx, appID, manifest, payer, price.PricePerUnit, unit, service, registration)
}

func ManifestFromSegment(header string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(header)
	if err != nil {
		return "", ErrInvalidPayment
	}
	segment, err := wire.DecodeSegData(data)
	if err != nil || segment.Auth.SessionID == "" || string(segment.ManifestID) != segment.Auth.SessionID {
		return "", ErrInvalidPayment
	}
	return segment.Auth.SessionID, nil
}

var ErrInvalidPayment = errors.New("invalid payment")
var ErrInsufficientBalance = errors.New("insufficient payment balance")
var ErrMissingChallenge = errors.New("payment challenge not found")
var ErrFixedAlreadyCharged = errors.New("fixed-price session already charged")
var errPaymentSessionCapacity = errors.New("payment session capacity reached")
var errPaymentTicketCapacity = errors.New("payment ticket capacity reached")

func decodeHeaders(paymentHeader, segmentHeader string) (wire.Payment, wire.SegData, error) {
	paymentBytes, err := base64.StdEncoding.DecodeString(paymentHeader)
	if err != nil {
		return wire.Payment{}, wire.SegData{}, ErrInvalidPayment
	}
	segmentBytes, err := base64.StdEncoding.DecodeString(segmentHeader)
	if err != nil {
		return wire.Payment{}, wire.SegData{}, ErrInvalidPayment
	}
	payment, err := wire.DecodePayment(paymentBytes)
	if err != nil {
		return wire.Payment{}, wire.SegData{}, ErrInvalidPayment
	}
	segment, err := wire.DecodeSegData(segmentBytes)
	if err != nil {
		return wire.Payment{}, wire.SegData{}, ErrInvalidPayment
	}
	return payment, segment, nil
}

func (e *PaymentEngine) Receive(ctx context.Context, appID, manifest, paymentHeader, segmentHeader string) (payerResult ethcommon.Address, credit *big.Rat, result error) {
	labels := e.paymentLabels(appID, manifest)
	defer func() { e.recordPayment("receive", result, labels) }()
	defer e.observeDuration("receive", time.Now(), labels)
	payment, segment, err := decodeHeaders(paymentHeader, segmentHeader)
	if err != nil {
		return ethcommon.Address{}, nil, err
	}
	if len(payment.PayerAddress) != 20 || len(segment.Hash) != 32 || string(segment.ManifestID) != manifest || segment.Auth.SessionID != manifest || len(segment.Signature) != 65 {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}
	payer := ethcommon.BytesToAddress(payment.PayerAddress)
	e.mu.Lock()
	session := e.sessions[manifest]
	if session == nil {
		e.mu.Unlock()
		return ethcommon.Address{}, nil, ErrMissingChallenge
	}
	appMatches, payerMatches, price := session.appID == appID, session.payer == payer, session.price
	e.mu.Unlock()
	params, recipientRand, err := e.authenticatePayment(payment, segment.Auth)
	if err != nil || !appMatches || !payerMatches || payment.ExpectedPrice != price || len(payment.PayerParams) == 0 || len(payment.PayerParams) > 100 {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}
	flatten := append([]byte(manifest), make([]byte, 32)...)
	flatten = append(flatten, segment.Hash...)
	if !(pm.DefaultSigVerifier{}).Verify(payer, crypto.Keccak256(flatten), segment.Signature) {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}
	funds, err := e.chain.PayerFunds(ctx, payer, e.recipient)
	if err != nil {
		return ethcommon.Address{}, nil, paymentObservation{err, "error"}
	}
	snapshot := funds.Snapshot
	active, err := e.chain.IsActiveAt(ctx, e.recipient, snapshot)
	if err != nil {
		return ethcommon.Address{}, nil, paymentObservation{err, "error"}
	}
	if !active {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}

	if snapshot.Block == nil || snapshot.Round == nil || !snapshot.ValidTicketRound(params.ExpirationParams.CreationRound) {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}
	if err := pm.ValidatePayerFunds(funds); err != nil {
		return ethcommon.Address{}, nil, paymentObservation{err, "rejected"}
	}
	if funds.Reserve.Cmp(params.FaceValue) < 0 || funds.Deposit.Cmp(params.FaceValue) < 0 {
		if e.store != nil && e.store.logger != nil {
			e.store.logger.WarnContext(ctx, "orchestrator payer deposit or claimable reserve below ticket face value", "event", "payment", "operation", "receive", "outcome", "rejected", "payer", payer, "deposit", funds.Deposit, "claimable_reserve", funds.Reserve, "face_value", params.FaceValue)
		}
		return ethcommon.Address{}, nil, pm.ErrPayerUnavailable
	}
	batchNonces := make(map[uint32]bool, len(payment.PayerParams))
	batchCredit := new(big.Rat)
	var winners []*pm.SignedTicket
	validator := pm.NewValidator(pm.DefaultSigVerifier{})
	for _, sp := range payment.PayerParams {
		if sp.TicketNonce == 0 || sp.TicketNonce >= 600 || len(sp.Sig) != 65 || batchNonces[sp.TicketNonce] {
			return ethcommon.Address{}, nil, ErrInvalidPayment
		}
		ticket := pm.NewTicket(&params, params.ExpirationParams, payer, sp.TicketNonce)
		if err := validator.ValidateTicket(e.recipient, ticket, sp.Sig, recipientRand); err != nil {
			return ethcommon.Address{}, nil, ErrInvalidPayment
		}
		batchNonces[sp.TicketNonce] = true
		batchCredit.Add(batchCredit, ticket.EV())
		if validator.IsWinningTicket(ticket, sp.Sig, recipientRand) {
			winners = append(winners, &pm.SignedTicket{Ticket: ticket, Sig: sp.Sig, RecipientRand: recipientRand})
		}
	}

	// Record accepted credit after unlocking; credit is returned only on success.
	defer func() {
		if credit != nil {
			e.metrics.Add("payment_credit_eth_total", observability.ETH(batchCredit), labels)
			e.metrics.Add("payment_tickets_total", float64(len(batchNonces)), labels.With("kind", "accepted"))
			e.metrics.Add("payment_tickets_total", float64(len(winners)), labels.With("kind", "winning"))
		}
	}()

	e.mu.Lock()
	defer e.mu.Unlock()
	// Store winning tickets before publishing in-memory replay guards and credit.
	// The lock serializes receipt and accounting; failed batches change neither.
	if e.sessions[manifest] != session {
		return ethcommon.Address{}, nil, ErrMissingChallenge
	}
	if err := snapshot.Validate(); err != nil {
		return ethcommon.Address{}, nil, paymentObservation{err, "error"}
	}
	e.observeBlock(snapshot.Block)
	if params.ExpirationBlock.Cmp(e.lastSeenBlock) <= 0 {
		return ethcommon.Address{}, nil, ErrInvalidPayment
	}
	if e.nonceCount+len(payment.PayerParams) > 1000000 {
		return ethcommon.Address{}, nil, errPaymentTicketCapacity
	}
	randKey := recipientRand.String()
	previous := e.ticketNonces[randKey]
	for nonce := range batchNonces {
		if previous != nil && previous.nonceSeen[nonce] {
			return ethcommon.Address{}, nil, ErrInvalidPayment
		}
	}
	balance := new(big.Rat).Add(session.balance, batchCredit)
	if err := e.store.storeWinningTickets(ctx, payer, params.RecipientRandHash, new(big.Int).Add(funds.Deposit, funds.Reserve), winners, snapshot); err != nil {
		return ethcommon.Address{}, nil, err
	}
	if previous == nil {
		previous = &recipientNonces{nonceSeen: make(map[uint32]bool), expirationBlock: new(big.Int).Set(params.ExpirationBlock)}
		e.ticketNonces[randKey] = previous
	}
	for nonce := range batchNonces {
		previous.nonceSeen[nonce] = true
	}
	e.nonceCount += len(batchNonces)
	session.balance, session.updated = balance, time.Now()
	return payer, new(big.Rat).Set(balance), nil
}

func (e *PaymentEngine) Charge(ctx context.Context, manifest string, now time.Time) (result error) {
	labels := e.paymentLabels("", manifest)
	observe := true
	defer func() {
		if observe {
			e.recordPayment("charge", result, labels)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.sessions[manifest]
	if current == nil {
		return ErrMissingChallenge
	}
	units := int64(0)
	if current.unit == "fixed" {
		if !current.lastCharge.IsZero() {
			current.updated = time.Now()
			observe = false
			return ErrFixedAlreadyCharged
		}
		units = 1
	} else if current.unit == "seconds" && !current.lastCharge.IsZero() {
		units = max(int64(now.Sub(current.lastCharge)/time.Second), 0)
		if units > 3600 {
			return ErrInsufficientBalance
		}
		if units == 0 {
			observe = false
			return nil
		}
		now = current.lastCharge.Add(time.Duration(units) * time.Second)
	}
	fee := new(big.Rat).Mul(big.NewRat(current.price.PricePerUnit, current.price.UnitsPerPrice), big.NewRat(units, 1))
	if units == 0 {
		observe = false
	}
	if current.balance.Cmp(fee) < 0 {
		return ErrInsufficientBalance
	}
	e.metrics.Add("payment_charge_eth_total", observability.ETH(fee), labels)
	if units > 0 {
		if current.unit == "seconds" {
			e.metrics.Add("payment_billed_seconds_total", float64(units), labels)
		} else {
			e.metrics.Add("payment_fixed_charges_total", float64(units), labels)
		}
	}
	current.balance.Sub(current.balance, fee)
	current.lastCharge, current.updated = now, time.Now()
	return nil
}

func (e *PaymentEngine) Balance(manifest string) (*big.Rat, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.sessions[manifest]
	if current == nil {
		return nil, ErrMissingChallenge
	}
	return new(big.Rat).Set(current.balance), nil
}

// ChallengePrice returns the agreed quote for a pending reservation, scoped to
// its app registration. App and feed updates must not change a previously issued quote.
func (e *PaymentEngine) ChallengePrice(appID, manifest string) (int64, string, error) {
	quote, err := e.challengeQuote(appID, manifest)
	price, _ := quote.Price.Int64()
	return price, quote.Unit, err
}

func (e *PaymentEngine) challengeQuote(appID, manifest string) (appQuote, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	current := e.sessions[manifest]
	if current == nil || current.appID != appID {
		return appQuote{}, ErrMissingChallenge
	}
	return appQuote{priceInfo: priceInfo{Price: json.Number(strconv.FormatInt(current.price.PricePerUnit, 10)), Currency: "wei", Unit: current.unit}, registration: current.registration}, nil
}

// authenticatePayment reconstructs the PM commitment from the supplied fields,
// using the recipient HMAC.
func (e *PaymentEngine) authenticatePayment(payment wire.Payment, auth wire.AuthToken) (pm.TicketParams, *big.Int, error) {
	p := payment.TicketParams
	// Accept either complete expiration copy; if both are supplied they must match.
	expiration := payment.Expiration
	if expiration.CreationRound == 0 && len(expiration.CreationRoundBlockHash) == 0 {
		expiration = p.Expiration
	}
	if len(payment.PayerAddress) != 20 || len(p.Recipient) != 20 || ethcommon.BytesToAddress(p.Recipient) != e.recipient ||
		len(p.FaceValue) == 0 || len(p.FaceValue) > 32 || len(p.WinProb) == 0 || len(p.WinProb) > 32 ||
		len(p.Seed) == 0 || len(p.Seed) > 32 || len(p.ExpirationBlock) == 0 || len(p.ExpirationBlock) > 32 || len(p.RecipientRandHash) != 32 ||
		expiration.CreationRound <= 0 || len(expiration.CreationRoundBlockHash) != 32 ||
		payment.ExpectedPrice.PricePerUnit <= 0 || payment.ExpectedPrice.UnitsPerPrice <= 0 || auth.SessionID == "" || time.Now().Unix() > auth.Expiration ||
		!hmac.Equal(auth.Token, e.authToken(auth.SessionID, auth.Expiration).Token) {
		return pm.TicketParams{}, nil, ErrInvalidPayment
	}
	if (p.Expiration.CreationRound != 0 || len(p.Expiration.CreationRoundBlockHash) != 0) &&
		(expiration.CreationRound != p.Expiration.CreationRound || !bytes.Equal(expiration.CreationRoundBlockHash, p.Expiration.CreationRoundBlockHash)) {
		return pm.TicketParams{}, nil, ErrInvalidPayment
	}
	params := pm.TicketParams{Recipient: e.recipient, FaceValue: new(big.Int).SetBytes(p.FaceValue), WinProb: new(big.Int).SetBytes(p.WinProb), RecipientRandHash: ethcommon.BytesToHash(p.RecipientRandHash), Seed: new(big.Int).SetBytes(p.Seed), ExpirationBlock: new(big.Int).SetBytes(p.ExpirationBlock), ExpirationParams: &pm.TicketExpirationParams{CreationRound: expiration.CreationRound, CreationRoundBlockHash: ethcommon.BytesToHash(expiration.CreationRoundBlockHash)}}
	if params.FaceValue.Sign() <= 0 || params.WinProb.Sign() <= 0 || params.WinProb.Cmp(maxWinProb) >= 0 {
		return pm.TicketParams{}, nil, ErrInvalidPayment
	}
	recipientRand := e.recipientRand(params.Seed, ethcommon.BytesToAddress(payment.PayerAddress), params.FaceValue, params.WinProb, params.ExpirationBlock, big.NewRat(payment.ExpectedPrice.PricePerUnit, payment.ExpectedPrice.UnitsPerPrice), params.ExpirationParams)
	if crypto.Keccak256Hash(ethcommon.LeftPadBytes(recipientRand.Bytes(), 32)) != params.RecipientRandHash {
		return pm.TicketParams{}, nil, ErrInvalidPayment
	}
	return params, recipientRand, nil
}
