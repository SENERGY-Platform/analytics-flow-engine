package lib

import "testing"

func TestNewLogger(t *testing.T) {
	GetLogger().Info("test")
}
