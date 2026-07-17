package main

import (
	"os"

	"github.com/go-logr/logr"
	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	"github.com/jmelis/postgres-controller-backend/pkg/pgruntime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("greeting-controller")

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://greeting:greeting@localhost:5432/greetings?sslmode=disable"
	}

	mgr, err := pgruntime.NewManager(pgruntime.Options{
		Scheme: greeting.Scheme,
		DSN:    dsn,
		Logger: logr.Logger(log),
	})
	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	if err := (&greeting.GreetingReconciler{Client: mgr.GetClient()}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to create controller")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
