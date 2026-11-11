package redemption

import "context"

// A rolled-back empty write validates the payment table and SQLite's ability to
// enter a write transaction, without changing payment or redemption state.
func (s *RedeemerDB) CheckStorage(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE winning_tickets SET enqueued_at=enqueued_at WHERE 0`); err != nil {
		return err
	}
	return tx.Rollback()
}
