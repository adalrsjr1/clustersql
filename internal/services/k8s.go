package services

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

var (
	Clientset   kubernetes.Interface
	ClientsetVS *metricsv.Clientset
	log         = logrus.New().WithField("pkg", "services")
)

func StartKubernetes() error {
	var kubeConfig *rest.Config
	inClusterConfig, err := rest.InClusterConfig()
	kubeConfig = inClusterConfig

	if err != nil {
		log.WithError(err).Warnf("failed to connect whitin the cluster, fallback to use homedir config")
		userHomeDir, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("error getting .kube at %s: %w", userHomeDir, err)
		}

		kubeConfigPath := filepath.Join(userHomeDir, ".kube", "config")
		log.Infof("using kubeconfig '%s'", kubeConfigPath)

		homeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
		if err != nil {
			return fmt.Errorf("error getting Kubernetes config at %s: %w", kubeConfigPath, err)
		}
		kubeConfig = homeConfig
	}

	Clientset, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("error getting kubernetes clientset: %w", err)
	}

	ClientsetVS, err = metricsv.NewForConfig(kubeConfig)
	if err != nil {
		return fmt.Errorf("error getting kubernetes metricvs: %w", err)
	}

	return nil
}
