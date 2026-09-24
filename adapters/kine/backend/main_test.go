package backend_test

import (
	"os"
	"testing"

	"github.com/sirupsen/logrus"
)

// The pinned bridge logs every refused request through logrus; the
// negative cases below make that noise, so it is silenced for the run.
func TestMain(m *testing.M) {
	logrus.SetLevel(logrus.PanicLevel)
	os.Exit(m.Run())
}
