package main

import (
	"os"

	"github.com/jmelis/postgres-controller-backend/examples/greeting-controller/greeting"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log := ctrl.Log.WithName("greeting-controller")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: greeting.Scheme,
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
