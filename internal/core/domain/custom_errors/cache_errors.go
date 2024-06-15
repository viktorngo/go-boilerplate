package custom_errors

func NewNotfoundInCacheErr() error {
	return &NotfoundInCacheErr{}
}

type NotfoundInCacheErr struct {
}

func (e *NotfoundInCacheErr) Error() string {
	return "not found in cache"
}
