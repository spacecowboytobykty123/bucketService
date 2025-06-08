package postgres

import (
	"bucketService/internal/data"
	"context"
	"database/sql"
	bckt "github.com/spacecowboytobykty123/bucketProto/gen/go/bucket"
	"log"
	"time"
)

type Storage struct {
	db *sql.DB
}

const (
	emptyValue = 0
)

type StorageDetails struct {
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  string
}

func OpenDB(details StorageDetails) (*Storage, error) {
	var db *sql.DB
	var err error
	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", details.DSN)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
		log.Printf("retrying DB connection... (%d/10)", i+1)
	}

	if err != nil {

		log.Fatal("failed to connect to database after retries:", err)
	}

	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(details.MaxOpenConns)
	db.SetMaxIdleConns(details.MaxIdleConns)

	duration, err := time.ParseDuration(details.MaxIdleTime)

	db.SetConnMaxIdleTime(duration)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Storage{
		db: db,
	}, err
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) AddToBucket(ctx context.Context, toysID []data.ToyShort, userId int64) (bckt.OperationStatus, string) {
	println("db part")

	// Step 1: Get bucketId by userId
	bucketQuery := `SELECT id FROM bucket WHERE user_id = $1`
	var bucketId int64

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := s.db.QueryRowContext(ctx, bucketQuery, userId).Scan(&bucketId)
	if err != nil {
		println("failed to find bucket for user:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "bucket not found"
	}

	// Step 2: Insert or update items in transaction
	insertQuery := `
		INSERT INTO bucket_item (bucket_id, toy_id, quantity)
		VALUES ($1, $2, $3)
		ON CONFLICT (bucket_id, toy_id)
		DO UPDATE SET quantity = bucket_item.quantity + EXCLUDED.quantity
		RETURNING id
	`

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		println("cannot start transaction:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "cannot start SQL transaction"
	}
	defer tx.Rollback()

	for _, toy := range toysID {
		var id int64
		err := tx.QueryRowContext(ctx, insertQuery, bucketId, toy.ID, toy.Qty).Scan(&id)
		if err != nil {
			println("failed to insert toy:", err.Error())
			return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "failed to add to bucket"
		}
	}

	if err := tx.Commit(); err != nil {
		println("commit failed:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "could not commit transaction"
	}

	println("db ends")
	return bckt.OperationStatus_STATUS_OK, "added to bucket"
}

func (s *Storage) DelFromBucket(ctx context.Context, toyIDs []int64, userId int64) (bckt.OperationStatus, string) {
	// Step 1: Find bucket ID for the user
	bucketQuery := `SELECT id FROM bucket WHERE user_id = $1`
	var bucketId int64

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := s.db.QueryRowContext(ctx, bucketQuery, userId).Scan(&bucketId)
	if err != nil {
		println("failed to find bucket:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "bucket not found"
	}

	// Step 2: Prepare transaction for multiple deletions
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		println("cannot start transaction:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "cannot start SQL transaction"
	}
	defer tx.Rollback()

	deleteQuery := `DELETE FROM bucket_item WHERE bucket_id = $1 AND toy_id = $2`

	for _, toyID := range toyIDs {
		result, err := tx.ExecContext(ctx, deleteQuery, bucketId, toyID)
		if err != nil {
			println("failed to delete toy:", err.Error())
			return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "failed to delete item"
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			println("failed to get affected rows:", err.Error())
			return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "internal error"
		}
		if rowsAffected == 0 {
			println("toy not found in bucket:", toyID)
			return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "toy not found in bucket"
		}
	}

	if err := tx.Commit(); err != nil {
		println("transaction commit failed:", err.Error())
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "could not commit transaction"
	}

	return bckt.OperationStatus_STATUS_OK, "toys successfully deleted from bucket"
}

func (s *Storage) CreateBucket(ctx context.Context, userId int64) (bckt.OperationStatus, string) {
	query := `INSERT INTO bucket (user_id)
VALUES ($1)
RETURNING id`

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var bucketId int64
	err := s.db.QueryRowContext(ctx, query, userId).Scan(&bucketId)
	if err != nil {
		return bckt.OperationStatus_STATUS_INTERNAL_ERROR, "cannot query to db"
	}
	return bckt.OperationStatus_STATUS_OK, "bucket creation successful"

}

func (s *Storage) GetBucket(ctx context.Context, userId int64) ([]*data.Toy, int32) {
	query1 := `SELECT id from bucket
WHERE user_id = $1
`
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var bucketId int64

	err := s.db.QueryRowContext(ctx, query1, userId).Scan(&bucketId)
	if err != nil {
		println(err.Error())
		return []*data.Toy{}, 0
	}

	query := `SELECT toy_id, quantity FROM bucket_item
WHERE bucket_id = $1
`

	rows, err := s.db.QueryContext(ctx, query, bucketId)
	if err != nil {
		println(err.Error())
		return []*data.Toy{}, 0
	}
	var qty int32
	toys := []*data.Toy{}
	for rows.Next() {
		var toy data.Toy

		err := rows.Scan(
			&toy.ID,
			&toy.Quantity,
		)
		qty += toy.Quantity

		if err != nil {
			println(err.Error())
			return []*data.Toy{}, 0
		}
		toys = append(toys, &toy)
	}

	if err = rows.Err(); err != nil {
		println(err.Error())
		return []*data.Toy{}, 0
	}

	return toys, qty
}
