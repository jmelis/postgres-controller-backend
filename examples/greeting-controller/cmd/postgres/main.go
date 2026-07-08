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

	// For multi-replica deployments, configure BucketIDs and BucketAssigner
	// to partition work across replicas. BucketAssigner is called on Create to
	// decide which bucket a new object is written to. It must be deterministic.
	//
	// Shard by object (namespace + name):
	//
	//   BucketAssigner: func(ns, name string) int {
	//       return int(crc32.ChecksumIEEE([]byte(ns+"/"+name))) % totalBuckets
	//   },
	//
	// Shard by namespace only (all objects in a namespace land in the same bucket):
	//
	//   BucketAssigner: func(ns, _ string) int {
	//       return int(crc32.ChecksumIEEE([]byte(ns))) % totalBuckets
	//   },
	//
	// By default, a single bucket (0) is used — suitable for single-replica controllers.
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
