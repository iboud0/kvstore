package util

type DB interface {
	Set(key []byte, value []byte) error

	Get(key []byte) ([]byte, error)

	Del(key []byte) ([]byte, error)
}
