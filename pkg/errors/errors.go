package errors

import "fmt"

// ErrorWithMessage
type ErrorWithMessage struct {
	Name string
}

func (e ErrorWithMessage) Error() string {
	return fmt.Sprintf("somthing is err with message: %s", e.Name)
}

// Error indicating insufficient identifiers have been supplied on a resource
// management request (create, apply, update, get, delete).
type ErrorInsufficientIdentifiers struct {
	Name string
}

func (e ErrorInsufficientIdentifiers) Error() string {
	return fmt.Sprintf("insufficient identifiers, missing '%s'", e.Name)
}

// Error indicating a problem connecting to the backend.
type ErrorDatastoreError struct {
	Err error
}

func (e ErrorDatastoreError) Error() string {
	return e.Err.Error()
}

type ErrorResourceDoesNotExist struct {
	Key string
}

func (e ErrorResourceDoesNotExist) Error() string {
	return fmt.Sprintf("resource doesn't exist with key:%s", e.Key)
}

type ErrorResourceUpdateConflict struct {
	Key string
}

func (e ErrorResourceUpdateConflict) Error() string {
	return fmt.Sprintf("resource conflict with key:%s", e.Key)
}

type ErrorNoSubnet struct {
	Message string
}

func (e ErrorNoSubnet) Error() string {
	return fmt.Sprintf("no subnet available: %s", e.Message)
}
